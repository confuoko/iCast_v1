

import os

import json


import boto3
from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings
import requests
from django.utils import timezone
from yandex_cloud_ml_sdk import YCloudML


from core.models import OutboxEvent, EventTypeChoices, MediaTask

from backend.celery import app as celery_app


@celery_app.task(queue="handler")
def handler_task():
    """
    Проверяет OutboxEvent и запускает задачи по нужной очереди
    """
    print("=== Запуск handler_task ===")
    events = OutboxEvent.objects.all()
    print(f"🔍 Найдено OutboxEvent: {events.count()} шт.")

    # core/tasks.py (фрагмент)

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
            media_task_id = event.media_task_id

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

        questions = media_obj.cast_template.questions if media_obj.cast_template else []
        if isinstance(questions, str):
            questions = json.loads(questions)

        questions_text = "\n".join([f"{q['id']}. {q['text']}" for q in questions])

        system_prompt = (
            "Вы являетесь кастдев-интервьюером и задаете ряд вопросов о вашем продукте "
            "потенциальному пользователю. Вам нужно найти ответы в данном интервью на список вопросов ниже. "
            "Если в тексте нет ответа на вопрос — верните \"Нет ответа\". "
            "Ответ нужно давать буквально прямыми цитатами, как их сказал пользователь, не перефразировать. "
            "Ответ верните в формате JSON вида: {\"номер вопроса\": \"ответ\"}."
        )

        user_prompt = f"Интервью:\n{interview_text}\n\nСписок вопросов:\n{questions_text}"

        messages = [
            {"role": "system", "text": system_prompt},
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

        # Пытаемся распарсить как JSON
        try:
            gpt_json = json.loads(gpt_raw_text)
        except json.JSONDecodeError:
            print("⚠️ Ответ GPT не является валидным JSON, сохраняю как raw string")
            gpt_json = None

        # Сохраняем в MediaTask
        media_obj.gpt_raw_response = gpt_raw_text
        media_obj.gpt_result = gpt_json
        media_obj.save(update_fields=["gpt_result", "gpt_raw_response"])

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







