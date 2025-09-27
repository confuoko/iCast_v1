FROM python:3.10-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копирование и установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY . .

# Создание директорий для медиа файлов
RUN mkdir -p media/excel_uploads media/media_uploads media/uploads media/video_uploads

# Установка прав доступа
RUN chmod +x manage.py

# Переменные окружения
ENV PYTHONPATH=/app
ENV DJANGO_SETTINGS_MODULE=backend.settings

EXPOSE 8000

# Команда по умолчанию
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
