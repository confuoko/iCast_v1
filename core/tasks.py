

import os

import json


import boto3
import xlsxwriter
from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings
import requests
from django.utils import timezone
from yandex_cloud_ml_sdk import YCloudML


from core.models import OutboxEvent, EventTypeChoices, MediaTask

from backend.celery import app as celery_app
from core.services import build_prompt


@celery_app.task(queue="handler")
def handler_task():
    """
    Проверяет OutboxEvent и запускает задачи по нужной очереди
    """
    print("=== Запуск handler_task ===")

    # Материализуем queryset, чтобы удаление не ломало итерацию
    events = list(OutboxEvent.objects.all())
    print(f"🔍 Найдено OutboxEvent: {len(events)} шт.")

    for event in events:
        try:
            media_task_id = event.media_task_id
            print(f"Объект MediaTask ID #{event.media_task_id}")
            print(f"Текущий EVENT_TYPE #{event.event_type}")
        except Exception as e:
            print(f"⚠️ Ошибка при выводе события {event.id}: {e}")

        if event.event_type == EventTypeChoices.GPT_RESULT_READY:
            print(f"📝 Запускаем сохранение Excel для MediaTask #{media_task_id}")
            save_excel_to_yandex_task.delay(media_task_id)
            event.delete()
        if event.event_type == EventTypeChoices.AUDIO_WAV_UPLOADED:
            print(f"🎧 Запускаем upload_audio_to_yandex_task для MediaTask #{media_task_id}")
            upload_audio_to_yandex_task.delay(media_task_id)
            event.delete()

        elif event.event_type == EventTypeChoices.AUDIO_SEND_TO_YANDEX:
            print(f"📝 Запускаем transcribe_task для MediaTask #{media_task_id}")
            transcribe_task.delay(media_task_id)
            event.delete()

        elif event.event_type == EventTypeChoices.TEMPLATE_SELECTED:
            print(f"🧩 Обнаружен TEMPLATE_SELECTED для MediaTask #{media_task_id} — проверяю готовность транскрибации...")

            audio_ready_event = (
                OutboxEvent.objects
                .filter(
                    media_task_id=media_task_id,
                    event_type=EventTypeChoices.AUDIO_TRANSCRIBATION_READY,
                )
                .order_by("id")
                .first()
            )

            if audio_ready_event:
                print(f"✅ Транскрипт готов. Запускаю gpt_task для MediaTask #{media_task_id}")
                gpt_task.delay(media_task_id)

                # удаляем оба события: текущий TEMPLATE_SELECTED и найденный AUDIO_TRANSCRIBATION_READY
                audio_ready_event.delete()
                event.delete()
            else:
                print(f"⏳ Транскрипт ещё не готов для MediaTask #{media_task_id}. Ждём событие AUDIO_TRANSCRIBATION_READY.")
        else:
            print(f"⚠️ Необработанный тип события: {event.event_type!r}")

    return f"Processed {len(events)} events"


@celery_app.task(queue="processing")
def gpt_task(media_task_id):
    """
    Задача по выделению ключевой информации через GPT.
    """
    print("=== 🚀 Запуск задачи gpt_task ===")
    try:
        media_obj = MediaTask.objects.get(id=media_task_id)

        diarization_segments = media_obj.diarization_segments
        if isinstance(diarization_segments, str):
            diarization_segments = json.loads(diarization_segments)

        interview_text = "\n".join(
            [f"[{seg['speaker']}] {seg['text']}" for seg in diarization_segments]
        )

        system_prompt_start = (
            "Вы являетесь кастдев-интервьюером и задаете ряд вопросов о вашем продукте "
            "потенциальному пользователю. Вам нужно найти ответы в данном интервью на список вопросов ниже. "
            "Если в тексте нет ответа на вопрос — верните \"Нет ответа\". "
            "Ответ нужно давать буквально прямыми цитатами, как их сказал пользователь, не перефразировать. "
            "Ответ верните строго в формате JSON (только JSON, без комментариев), где ключ — номер вопроса, а значение — текст ответа.\n"
            "Пример:\n"
            "{\n"
            "  \"1\": \"ответ на вопрос 1\",\n"
            "  \"2\": \"Нет ответа\"\n"
            "}\n\n"
        )
        questions_text = build_prompt()
        user_prompt = f"Интервью:\n{interview_text}"

        messages = [
            {"role": "system", "text": system_prompt_start + questions_text},
            {"role": "user", "text": user_prompt},
        ]

        sdk = YCloudML(
            folder_id=settings.YANDEX_FOLDER_ID,
            auth=settings.YANDEX_OAUTH_TOKEN,
        )

        tokenized = sdk.models.completions("yandexgpt").tokenize(messages)
        print(f"🔢 Количество токенов: {len(tokenized)}")

        result = sdk.models.completions("yandexgpt").configure(temperature=0.3).run(messages)

        # result — это список Alternative, нужно взять .text
        gpt_raw_text = result[0].text if result else "{}"
        print(f"=== 📝 Ответ GPT ===\n{gpt_raw_text}")

        # Очистка от обрамляющих ```` ``` ```` и лишних пробелов
        gpt_cleaned_text = gpt_raw_text.strip()
        if gpt_cleaned_text.startswith("```") and gpt_cleaned_text.endswith("```"):
            gpt_cleaned_text = gpt_cleaned_text.strip("`").strip()

        # Попробуем снова распарсить
        try:
            gpt_json = json.loads(gpt_cleaned_text)
            print("✅ Успешно распарсили JSON")
        except json.JSONDecodeError as e:
            print(f"⚠️ Ошибка парсинга JSON: {e}")
            gpt_json = None

        # Сохраняем как есть (сырой и/или распарсенный)
        media_obj.gpt_raw_response = gpt_raw_text  # Оригинал, с бэктиками
        if gpt_json:
            # красиво сериализуем и сохраняем
            media_obj.gpt_result = json.dumps(gpt_json, ensure_ascii=False, indent=2)
        else:
            media_obj.gpt_result = None

        media_obj.save(update_fields=["gpt_raw_response", "gpt_result"])

        OutboxEvent.objects.create(
            media_task=media_obj,
            event_type=EventTypeChoices.GPT_RESULT_READY,
            payload={"media_task_id": media_task_id},
        )

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")
    except Exception as e:
        print(f"❌ Ошибка при работе с gpt_task: {e}")



@celery_app.task(queue="processing")
def transcribe_task(media_task_id):
    """
    Задача по транскрибации аудио через Nexara.
    """
    print("=== 🚀 Запуск задачи transcribe_task ===")
    NEXARA_API_KEY = settings.NEXARA_API_KEY

    if NEXARA_API_KEY:
        print("✅ NEXARA_API_KEY найден!")
    else:
        print("❌ NEXARA_API_KEY не найден, задача не будет выполнена")
        return

    try:
        # 1. Получаем объект MediaTask
        media_obj = MediaTask.objects.get(id=media_task_id)

        # 2. Берём ссылку на аудио-файл в хранилище Яндекс
        audio_yandex_url = media_obj.audio_storage_url
        if not audio_yandex_url:
            print(f"❌ У MediaTask #{media_task_id} нет ссылки на аудио (audio_storage_url)")
            return

        print(f"🔗 Отправляем аудио в Nexara: {audio_yandex_url}")

        # 3. Формируем запрос в Nexara
        url = "https://api.nexara.ru/api/v1/audio/transcriptions"
        headers = {
            "Authorization": f"Bearer {NEXARA_API_KEY}",
        }
        data = {
            "task": "diarize",                # всегда диаризация
            "response_format": "verbose_json" # просим развернутый JSON
        }
        # Важно: Nexara поддерживает передачу ссылки на файл через поле "url"
        files = None
        data["url"] = audio_yandex_url

        # 4. Делаем POST-запрос
        response = requests.post(url, headers=headers, data=data, files=files)
        print(f"🌐 Nexara ответила со статусом {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Результат получен, сохраняем в MediaTask")

            # Сохраняем основные поля
            media_obj.diarization_text = result.get("text", "")
            media_obj.diarization_segments = result.get("segments", [])
            media_obj.audio_duration_seconds = result.get("duration")
            media_obj.nexara_completed_at = timezone.now()
            media_obj.save()

            # Создаем OutboxEvent, чтобы другие части системы знали о готовом результате
            OutboxEvent.objects.create(
                media_task=media_obj,
                event_type=EventTypeChoices.AUDIO_TRANSCRIBATION_READY,
                payload={"info": "Диаризация успешно выполнена"}
            )

        else:
            print(f"❌ Ошибка от Nexara: {response.text}")
            media_obj.nexara_error = response.text
            media_obj.save()

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")


@celery_app.task(queue="processing")
def upload_audio_to_yandex_task(media_task_id):
    """
    Задача по загрузке аудио-файла из Сервиса в хранилище Яндекс.
    """
    print("=== Запуск upload_audio_to_yandex_task ===")
    # === Константы для S3 ===
    AWS_ACCESS_KEY_ID = settings.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = settings.AWS_SECRET_ACCESS_KEY
    BUCKET_NAME = settings.BUCKET_NAME
    REGION = settings.REGION
    ENDPOINT_URL = settings.ENDPOINT_URL

    try:
        media_obj = MediaTask.objects.get(id=media_task_id)
        file_name = media_obj.audio_title_saved

        if not file_name:
            print(f"❌ Нет файла для загрузки у MediaTask #{media_obj.id}")
            return

        local_file_path = os.path.join(settings.MEDIA_ROOT, "media_uploads", file_name)
        object_name = f"media_uploads/{file_name}"

        print(f"📤 Загружаем файл {local_file_path} в {object_name}...")

        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
            region_name=REGION
        )

        # Загрузка в бакет
        with open(local_file_path, "rb") as f:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=f)

        # Обновляем URL в MediaTask
        public_url = f"{ENDPOINT_URL}/{BUCKET_NAME}/{object_name}"
        media_obj.audio_storage_url = public_url
        media_obj.save()

        print(f"✅ Успешно загружено: {public_url}")

        # === Создаём новое событие OutboxEvent ===
        OutboxEvent.objects.create(
            media_task=media_obj,
            event_type=EventTypeChoices.AUDIO_SEND_TO_YANDEX,
            payload={
                "filename": file_name,
                "storage_url": public_url,
                "uploaded_by": "system_task",
            }
        )
        print(f"📨 Создано OutboxEvent: AUDIO_SEND_TO_YANDEX для MediaTask #{media_task_id}")

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except (BotoCoreError, ClientError) as e:
        print(f"❌ Ошибка при загрузке в S3: {e}")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")


# Очередь 2: processing — тяжелые задачи (например, обработка файла)
@celery_app.task(queue="processing")
def upload_task(media_task_id):
    """
    Задача по загрузке медиа-файла из Сервиса в хранилище Яндекс.
    """

    # === Константы для S3 ===
    AWS_ACCESS_KEY_ID = settings.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = settings.AWS_SECRET_ACCESS_KEY
    BUCKET_NAME = settings.BUCKET_NAME
    REGION = settings.REGION
    ENDPOINT_URL = settings.ENDPOINT_URL

    # Вывод всех настроек S3 для отладки
    print("=== 🌐 Настройки S3 из settings.py ===")
    print(f"AWS_ACCESS_KEY_ID: {settings.AWS_ACCESS_KEY_ID}")
    print(f"AWS_SECRET_ACCESS_KEY: {settings.AWS_SECRET_ACCESS_KEY}")
    print(f"BUCKET_NAME: {settings.BUCKET_NAME}")
    print(f"REGION: {settings.REGION}")
    print(f"ENDPOINT_URL: {settings.ENDPOINT_URL}")
    print("======================================")

    try:
        media_obj = MediaTask.objects.get(id=media_task_id)
        file_name = media_obj.video_title_saved

        if not file_name:
            print(f"❌ Нет файла для загрузки у MediaTask #{media_obj.id}")
            return

        local_file_path = os.path.join(settings.MEDIA_ROOT, "media_uploads", file_name)
        object_name = f"media_uploads/{file_name}"

        print(f"📤 Загружаем файл {local_file_path} в {object_name}...")

        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
            region_name=REGION
        )

        # Загрузка в бакет
        with open(local_file_path, "rb") as f:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=f)

        # Обновляем URL
        public_url = f"{ENDPOINT_URL}/{BUCKET_NAME}/{object_name}"
        media_obj.storage_url = public_url
        media_obj.save()

        print(f"✅ Успешно загружено: {public_url}")

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except (BotoCoreError, ClientError) as e:
        print(f"❌ Ошибка при загрузке в S3: {e}")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")



@celery_app.task(queue="processing")
def save_excel_to_yandex_task(media_task_id):
    # === S3 Конфигурация ===
    AWS_ACCESS_KEY_ID = settings.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = settings.AWS_SECRET_ACCESS_KEY
    BUCKET_NAME = settings.BUCKET_NAME
    REGION = settings.REGION
    ENDPOINT_URL = settings.ENDPOINT_URL

    try:
        media_obj = MediaTask.objects.get(id=media_task_id)
        file_base = media_obj.audio_title_saved

        if not file_base:
            print(f"❌ Нет названия файла у MediaTask #{media_obj.id}")
            return

        gpt_result = media_obj.gpt_result
        if not gpt_result:
            print(f"❌ Нет gpt_result от GPT для MediaTask #{media_obj.id}")
            return

        try:
            parsed_json = json.loads(gpt_result)
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга gpt_result: {e}")
            return

        # === Подготовка путей ===
        os.makedirs(os.path.join(settings.MEDIA_ROOT, "excel_uploads"), exist_ok=True)
        local_excel_file_path = os.path.join(settings.MEDIA_ROOT, "excel_uploads", f"{file_base}.xlsx")
        object_name = f"excel_uploads/{file_base}.xlsx"

        # === Создание Excel ===
        workbook = xlsxwriter.Workbook(local_excel_file_path)
        worksheet = workbook.add_worksheet("Ответы")

        # Форматы
        header_format = workbook.add_format({
            "bold": True, "bg_color": "#D9E1F2",
            "align": "center", "valign": "vcenter",
            "border": 1
        })
        cell_format = workbook.add_format({
            "text_wrap": True,
            "valign": "top",
            "border": 1
        })

        # Заголовки
        worksheet.write(0, 0, "№", header_format)
        worksheet.write(0, 1, "Ответ", header_format)

        # Заполнение строк
        row = 1
        for key in sorted(parsed_json.keys(), key=lambda x: int(x)):
            answer = parsed_json[key]
            worksheet.write(row, 0, key, cell_format)
            worksheet.write(row, 1, answer, cell_format)
            row += 1

        # Настройка ширины колонок
        worksheet.set_column(0, 0, 5)
        worksheet.set_column(1, 1, 100)

        workbook.close()
        print(f"💾 Excel-файл успешно создан: {local_excel_file_path}")

        # === Загрузка в Яндекс Object Storage ===
        print(f"📤 Загружаем файл {object_name} в хранилище...")

        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
            region_name=REGION
        )

        with open(local_excel_file_path, "rb") as f:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=f)

        public_url = f"{ENDPOINT_URL}/{BUCKET_NAME}/{object_name}"
        media_obj.excel_path = public_url
        media_obj.save(update_fields=["excel_path"])

        print(f"✅ Excel-файл загружен в Яндекс S3: {public_url}")

        # === OutboxEvent ===
        OutboxEvent.objects.create(
            media_task=media_obj,
            event_type=EventTypeChoices.EXCEL_FILE_SAVED_TO_YANDEX,
            payload={
                "filename": f"{file_base}.xlsx",
                "storage_url": public_url,
                "uploaded_by": "system_task",
            }
        )

        print(f"📨 OutboxEvent EXCEL_FILE_SAVED_TO_YANDEX создан для MediaTask #{media_task_id}")

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except (BotoCoreError, ClientError) as e:
        print(f"❌ Ошибка при загрузке в S3: {e}")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")




