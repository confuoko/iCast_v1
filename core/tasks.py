

import os
import json
import io
import boto3
from boto3.session import Session
import xlsxwriter
from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings
import requests
from django.utils import timezone
from yandex_cloud_ml_sdk import YCloudML


from core.models import OutboxEvent, EventTypeChoices, MediaTask, MediaTaskStatusChoices

from backend.celery import app as celery_app


@celery_app.task(queue="handler")
def handler_task():
    """
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


        if event.event_type == EventTypeChoices.TEMPLATE_SELECTED:
            print(f"Обнаружен событие TEMPLATE_SELECTED для MediaTask #{media_task_id}")

            audio_uploaded_event = (
                OutboxEvent.objects
                .filter(
                    media_task_id=media_task_id,
                    event_type=EventTypeChoices.AUDIO_UPLOADED_TO_YANDEX,
                )
                .order_by("id")
                .first()
            )
            if audio_uploaded_event:
                transcribe_task.delay(media_task_id)
                # Удаляем оба события
                audio_uploaded_event.delete()
                event.delete()
                print(f"Удалены события TEMPLATE_SELECTED и AUDIO_UPLOADED_TO_YANDEX для MediaTask #{media_task_id}")
            else:
                print(f"Нет AUDIO_UPLOADED_TO_YANDEX для MediaTask #{media_task_id}, ждем...")

        if event.event_type == EventTypeChoices.AUDIO_TRANSCRIBATION_READY:
            print(f"Обнаружен событие AUDIO_TRANSCRIBATION_READY для MediaTask #{media_task_id}")
            gpt_task.delay(media_task_id)
            event.delete()
            print(f"Удалено событие AUDIO_TRANSCRIBATION_READY для MediaTask #{media_task_id}")
        if event.event_type == EventTypeChoices.GPT_RESULT_READY:
            print(f"Обнаружено событие GPT_RESULT_READY для MediaTask #{media_task_id}")
            save_excel_task.delay(media_task_id)
            event.delete()
            print(f"Удалено событие GPT_RESULT_READY для MediaTask #{media_task_id}")



        else:
            print(f"⚠️ Необработанный тип события: {event.event_type!r}")

    return f"Processed {len(events)} events"


def save_transcription_to_s3(media_obj, segments):
    """
    Сохраняет транскрибацию посегментно (в порядке Nexara) в .txt файл в S3.
    Возвращает публичный URL.
    """
    # --- Формируем текст посегментно ---
    lines = []
    for seg in segments:
        speaker = seg.get("speaker", "unknown")
        text = seg.get("text", "").strip()
        if text:
            lines.append(f"{speaker}: {text}")

    # --- Объединяем строки в один текст ---
    txt_content = "\n".join(lines)

    # DEBUG
    print("[DEBUG] Текст для сохранения:")
    print(txt_content[:1000])

    # --- Преобразуем в байты ---
    byte_stream = io.BytesIO(txt_content.encode("utf-8"))

    # --- Имя и путь в S3 ---
    txt_filename = f"{media_obj.audio_title_saved.rsplit('.', 1)[0]}.txt"
    s3_txt_path = f"media_transcripts/{txt_filename}"

    # --- S3 клиент ---
    session = Session()
    s3_client = session.client(
        service_name="s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        endpoint_url=settings.ENDPOINT_URL,
        region_name=settings.REGION
    )

    # --- Загрузка в S3 ---
    s3_client.put_object(
        Bucket=settings.BUCKET_NAME,
        Key=s3_txt_path,
        Body=byte_stream,
        ContentType="text/plain; charset=utf-8"
    )

    return f"{settings.ENDPOINT_URL}/{settings.BUCKET_NAME}/{s3_txt_path}"


@celery_app.task(queue="processing")
def transcribe_task(media_task_id):
    """
    Задача по транскрибации аудио через Nexara.
    """
    print("=== Запуск задачи transcribe_task ===")

    NEXARA_API_KEY = settings.NEXARA_API_KEY
    if not NEXARA_API_KEY:
        print("❌ NEXARA_API_KEY не найден, задача не будет выполнена")
        return

    try:
        # --- Получаем MediaTask ---
        media_obj = MediaTask.objects.get(id=media_task_id)
        media_obj.status = MediaTaskStatusChoices.PROCESS_TRANSCRIBATION
        media_obj.save()

        audio_yandex_url = media_obj.audio_storage_url
        if not audio_yandex_url:
            print(f"❌ У MediaTask #{media_task_id} нет ссылки на аудио")
            return

        print(f"🔗 Отправляем аудио в Nexara: {audio_yandex_url}")

        # --- Запрос в Nexara ---
        url = "https://api.nexara.ru/api/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {NEXARA_API_KEY}"}
        data = {
            "task": "diarize",
            "response_format": "verbose_json",
            "url": audio_yandex_url
        }

        response = requests.post(url, headers=headers, data=data)
        print(f"Nexara ответила со статусом {response.status_code}")

        if response.status_code != 200:
            print(f"❌ Ошибка от Nexara: {response.text}")
            media_obj.nexara_error = response.text
            media_obj.save()
            return

        # --- Обработка ответа ---
        result = response.json()
        segments = result.get("segments", [])
        duration = result.get("duration")

        if segments:
            last_end = segments[-1].get("end")
            print(f"[DEBUG] Последнее значение end: {last_end} ({type(last_end)})")
        else:
            print("[DEBUG] Список сегментов пуст")

        if duration:
            print(f"[DEBUG] Продолжительность (duration): {duration}")
        else:
            print("[DEBUG] Ключ duration отсутствует")

        # --- Сохраняем .txt в S3 ---
        transcribation_url = save_transcription_to_s3(media_obj, segments)

        # --- Обновляем MediaTask ---
        media_obj.diarization_segments = segments
        media_obj.audio_duration_seconds_nexara = duration
        media_obj.nexara_completed_at = timezone.now()
        media_obj.transcribation_path = transcribation_url
        media_obj.status = MediaTaskStatusChoices.TRANSCRIBATION_SUCCESS
        media_obj.save()

        # --- Создаём OutboxEvent ---
        OutboxEvent.objects.create(
            media_task=media_obj,
            event_type=EventTypeChoices.AUDIO_TRANSCRIBATION_READY,
            payload={"info": "Диаризация успешно выполнена"}
        )

        print("✅ Транскрибация и сохранение завершены")

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except Exception as e:
        print(f"❌ Общая ошибка в transcribe_task: {e}")



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
def gpt_task(media_task_id):
    """
    Задача по выделению ключевой информации через GPT (YandexGPT).
    """
    print("=== Запуск задачи gpt_task ===")

    try:
        # --- Получаем MediaTask ---
        media_obj = MediaTask.objects.get(id=media_task_id)
        media_obj.status = MediaTaskStatusChoices.PROCESS_DATA_EXTRACTION
        media_obj.save()

        # --- Проверяем наличие пути к транскрипции ---
        transcribation_path = media_obj.transcribation_path
        if not transcribation_path:
            print(f"❌ У MediaTask #{media_task_id} отсутствует transcribation_path")
            return

        print(f"📥 Загружаем файл из Object Storage: {transcribation_path}")

        # === Настройки доступа ===
        ENDPOINT_URL = "https://storage.yandexcloud.net"
        BUCKET_NAME = "bucketnew"

        # --- Создаём S3 клиент ---
        session = Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
            region_name=settings.REGION,
        )

        # --- Извлекаем object_key ---
        prefix = f"{ENDPOINT_URL}/{BUCKET_NAME}/"
        if not transcribation_path.startswith(prefix):
            print(f"⚠️ Неожиданный формат пути: {transcribation_path}")
            return
        object_key = transcribation_path[len(prefix):]

        print(f"[DEBUG] bucket_name={BUCKET_NAME}")
        print(f"[DEBUG] object_key={object_key}")

        # --- Загружаем текст транскрипции ---
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        interview_text = response["Body"].read().decode("utf-8")
        print(f"✅ Файл считан ({len(interview_text)} символов)")

        # --- Загружаем и обрабатываем список вопросов ---
        template = getattr(media_obj, "cast_template", None)
        if not template or not template.questions:
            print(f"❌ У MediaTask #{media_task_id} отсутствует шаблон с вопросами")
            return

        # Универсальный парсинг
        if isinstance(template.questions, dict):
            questions_dict = template.questions
        elif isinstance(template.questions, str):
            try:
                questions_dict = json.loads(template.questions)
            except json.JSONDecodeError:
                print("⚠️ Ошибка парсинга JSON с вопросами, используем пустой список")
                questions_dict = {}
        else:
            print(f"⚠️ Неожиданный тип поля questions: {type(template.questions)}")
            questions_dict = {}

        questions_text = "\n".join([f"{qid}. {qtext}" for qid, qtext in questions_dict.items()])
        print(f"✅ Загружено {len(questions_dict)} вопросов для GPT")

        # --- Формируем system prompt ---
        system_prompt_start = template.promt

        user_prompt = f"Интервью:\n{interview_text}"

        messages = [
            {"role": "system", "text": system_prompt_start + questions_text},
            {"role": "user", "text": user_prompt},
        ]

        # --- GPT (Yandex Cloud) ---
        sdk = YCloudML(
            folder_id=settings.YANDEX_FOLDER_ID,
            auth=settings.YANDEX_OAUTH_TOKEN,
        )

        try:
            tokenized = sdk.models.completions("yandexgpt").tokenize(messages)
            print(f"🔢 Количество токенов: {len(tokenized)}")
        except Exception as e:
            print(f"⚠️ Не удалось подсчитать токены: {e}")

        print("🤖 Отправляем запрос в YandexGPT...")
        result = sdk.models.completions("yandexgpt").configure(temperature=0.3).run(messages)

        gpt_raw_text = result[0].text if result else "{}"
        print(f"=== 📝 Ответ GPT ===\n{gpt_raw_text}")

        # --- Очистка и парсинг JSON ---
        gpt_cleaned_text = gpt_raw_text.strip()
        if gpt_cleaned_text.startswith("```") and gpt_cleaned_text.endswith("```"):
            gpt_cleaned_text = gpt_cleaned_text.strip("`").strip()

        try:
            gpt_json = json.loads(gpt_cleaned_text)
            print("✅ JSON успешно распознан")
        except json.JSONDecodeError as e:
            print(f"⚠️ Ошибка парсинга JSON: {e}")
            gpt_json = None

        # --- Сохраняем результат ---
        media_obj.gpt_raw_response = gpt_raw_text
        media_obj.gpt_result = json.dumps(gpt_json, ensure_ascii=False, indent=2) if gpt_json else None
        media_obj.status = MediaTaskStatusChoices.DATA_EXTRACTION_SUCCESS
        media_obj.save(update_fields=["gpt_raw_response", "gpt_result", "status"])

        # --- OutboxEvent ---
        OutboxEvent.objects.create(
            media_task=media_obj,
            event_type=EventTypeChoices.GPT_RESULT_READY,
            payload={"media_task_id": media_task_id},
        )

        print("✅ GPT-задача завершена успешно")

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except Exception as e:
        print(f"❌ Ошибка при работе с gpt_task: {e}")



@celery_app.task(queue="processing")
def save_excel_task(media_task_id):

    # === S3 Конфигурация ===
    AWS_ACCESS_KEY_ID = settings.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = settings.AWS_SECRET_ACCESS_KEY
    BUCKET_NAME = settings.BUCKET_NAME
    REGION = settings.REGION
    ENDPOINT_URL = settings.ENDPOINT_URL

    try:
        media_obj = MediaTask.objects.get(id=media_task_id)
        media_obj.status = MediaTaskStatusChoices.SAVE_EXCEL_START
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

        # === Загружаем вопросы из шаблона ===
        template = getattr(media_obj, "cast_template", None)
        if not template or not template.questions:
            print(f"⚠️ У MediaTask #{media_obj.id} нет шаблона или вопросов")
            questions_dict = {}
        else:
            # поле questions — это JSONField, может быть dict или str
            if isinstance(template.questions, dict):
                questions_dict = template.questions
            elif isinstance(template.questions, str):
                try:
                    questions_dict = json.loads(template.questions)
                except json.JSONDecodeError:
                    print("⚠️ Ошибка парсинга JSON вопросов")
                    questions_dict = {}
            else:
                print("⚠️ Неожиданный тип данных в questions")
                questions_dict = {}

        # === Подготовка путей ===
        os.makedirs(os.path.join(settings.MEDIA_ROOT, "excel_uploads"), exist_ok=True)
        local_excel_file_path = os.path.join(
            settings.MEDIA_ROOT, "excel_uploads", f"{file_base}.xlsx"
        )
        object_name = f"excel_uploads/{file_base}.xlsx"

        # === Создание Excel ===
        workbook = xlsxwriter.Workbook(local_excel_file_path)
        worksheet = workbook.add_worksheet("Ответы")

        # Форматы
        header_format = workbook.add_format({
            "bold": True,
            "bg_color": "#D9E1F2",
            "align": "center",
            "valign": "vcenter",
            "border": 1,
        })
        cell_format = workbook.add_format({
            "text_wrap": True,
            "valign": "top",
            "border": 1,
        })

        # Заголовки
        worksheet.write(0, 0, "№", header_format)
        worksheet.write(0, 1, "Вопрос", header_format)
        worksheet.write(0, 2, "Ответ", header_format)

        # Заполнение строк
        row = 1
        for key in sorted(parsed_json.keys(), key=lambda x: int(x)):
            answer = parsed_json[key]
            question_text = questions_dict.get(key, "— Вопрос не найден —")
            worksheet.write(row, 0, key, cell_format)
            worksheet.write(row, 1, question_text, cell_format)
            worksheet.write(row, 2, answer, cell_format)
            row += 1

        # Настройка ширины колонок
        worksheet.set_column(0, 0, 5)     # №
        worksheet.set_column(1, 1, 120)   # Вопрос
        worksheet.set_column(2, 2, 100)   # Ответ

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
            region_name=REGION,
        )

        with open(local_excel_file_path, "rb") as f:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=f)

        public_url = f"{ENDPOINT_URL}/{BUCKET_NAME}/{object_name}"
        media_obj.excel_path = public_url
        media_obj.status = MediaTaskStatusChoices.SAVE_EXCEL_FINISH
        media_obj.save(update_fields=["excel_path", "status"])

        print(f"✅ Excel-файл загружен в Яндекс S3: {public_url}")

        # === OutboxEvent ===
        OutboxEvent.objects.create(
            media_task=media_obj,
            event_type=EventTypeChoices.EXCEL_FILE_SAVED_TO_YANDEX,
            payload={
                "filename": f"{file_base}.xlsx",
                "storage_url": public_url,
                "uploaded_by": "system_task",
            },
        )

        print(f"📨 OutboxEvent EXCEL_FILE_SAVED_TO_YANDEX создан для MediaTask #{media_task_id}")

    except MediaTask.DoesNotExist:
        print(f"❌ MediaTask #{media_task_id} не найден")

    except (BotoCoreError, ClientError) as e:
        print(f"❌ Ошибка при загрузке в S3: {e}")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")