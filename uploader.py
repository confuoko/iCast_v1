import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ==== 🔐 КОНСТАНТЫ (заполни своими значениями) ====
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
BUCKET_NAME = "bucketnew"
REGION = "ru-central1"
ENDPOINT_URL = "https://storage.yandexcloud.net"

# Путь к файлу, который нужно загрузить
LOCAL_FILE_PATH = "my_local_file.pdf"
# Имя объекта в хранилище (в бакете), которое будет присвоено загруженному файлу
OBJECT_NAME = "folder_in_bucket/my_uploaded_file.pdf"

def upload_file_to_s3():
    try:
        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
            region_name=REGION
        )

        with open(LOCAL_FILE_PATH, "rb") as f:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME, Body=f)

        print(f"✅ Успешно загружено: {OBJECT_NAME}")

    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")


if __name__ == "__main__":
    upload_file_to_s3()
