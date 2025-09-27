# Инструкции по запуску с Docker

## Подготовка

1. Скопируйте файл `env.example` в `.env` и заполните необходимые переменные окружения:
```bash
cp env.example .env
```

2. Отредактируйте `.env` файл, указав корректные значения для:
   - DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD (внешняя PostgreSQL)
   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME
   - NEXARA_API_KEY
   - YANDEX_OAUTH_TOKEN, YANDEX_FOLDER_ID

## Запуск всех сервисов

Для запуска всех сервисов (веб-приложение, RabbitMQ, Celery воркеры и планировщик):

```bash
docker-compose up -d
```

## Запуск только Celery сервисов

Если вы хотите запустить только Celery воркеры и планировщик (при условии, что внешняя база данных уже доступна):

```bash
docker-compose up -d celery-worker-handler celery-worker-processing celery-beat
```

## Отдельные команды

### Запуск только воркера handler:
```bash
docker-compose up -d celery-worker-handler
```

### Запуск только воркера processing:
```bash
docker-compose up -d celery-worker-processing
```

### Запуск только планировщика:
```bash
docker-compose up -d celery-beat
```

## Просмотр логов

### Логи всех сервисов:
```bash
docker-compose logs -f
```

### Логи конкретного сервиса:
```bash
docker-compose logs -f celery-worker-handler
docker-compose logs -f celery-worker-processing
docker-compose logs -f celery-beat
```

## Остановка сервисов

```bash
docker-compose down
```

## Полезные команды

### Пересборка образов:
```bash
docker-compose build
```

### Проверка статуса сервисов:
```bash
docker-compose ps
```

### Подключение к контейнеру для отладки:
```bash
docker-compose exec celery-worker-handler bash
```

## Мониторинг

- **Django Admin**: http://localhost:8000/admin/

## Структура сервисов

- `web` - Django веб-приложение
- `celery-worker-handler` - Celery воркер для очереди handler
- `celery-worker-processing` - Celery воркер для очереди processing  
- `celery-beat` - Планировщик задач Celery
- `rabbitmq` - Брокер сообщений RabbitMQ

**Внешние сервисы** (настраиваются через .env):
- PostgreSQL база данных (облачный сервис)
