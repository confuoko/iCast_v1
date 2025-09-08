# core/tasks.py
import os
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings
import requests
from django.utils import timezone

from backend.settings import NEXARA_API_KEY
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

    for event in events:
        print(f"🔸 Событие: id={event.id}, type={event.event_type}, media_task_id={event.media_task.id}")
        media_task_id = event.media_task.id

        if event.event_type == EventTypeChoices.AUDIO_WAV_UPLOADED:
            print(f"Запускаем upload_audio_to_yandex_task для MediaTask #{media_task_id}")

            # Запускаем асинхронную задачу во второй очереди
            upload_audio_to_yandex_task.delay(media_task_id)

            # Удаляем событие, чтобы оно не обрабатывалось повторно
            event.delete()
        elif event.event_type == EventTypeChoices.AUDIO_SEND_TO_YANDEX:
            print(f"Запускаем transcribe_task для MediaTask #{media_task_id}")
            transcribe_task.delay(media_task_id)
            event.delete()

    return f"Processed {events.count()} events"


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







