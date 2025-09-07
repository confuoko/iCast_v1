# iCast v1

Django проект с системой авторизации и регистрации пользователей.

## Установка и запуск

### 1. Клонирование и установка зависимостей

```bash
# Установка зависимостей
pip install -r requirements.txt
```

### 2. Настройка переменных окружения

Создайте файл `.env` на основе `env_example.txt`:

```bash
cp env_example.txt .env
```

Отредактируйте `.env` файл при необходимости.

### 3. Запуск сервисов через Docker

```bash
# Запуск PostgreSQL, RabbitMQ и Redis
docker-compose up -d

# Проверка статуса сервисов
docker-compose ps
```

**Примечание**: Данные контейнеров будут храниться в папках рядом с проектом:
- `../icast_postgres_data/` - данные PostgreSQL
- `../icast_rabbitmq_data/` - данные RabbitMQ  
- `../icast_redis_data/` - данные Redis

### 4. Настройка базы данных

```bash
# Создание миграций
python manage.py makemigrations

# Применение миграций
python manage.py migrate

# Создание суперпользователя (опционально)
python manage.py createsuperuser
```

### 5. Запуск Django сервера

```bash
python manage.py runserver
```

## Доступные страницы

- **Главная**: http://127.0.0.1:8000/
- **Регистрация**: http://127.0.0.1:8000/register/
- **Вход**: http://127.0.0.1:8000/login/
- **Выход**: http://127.0.0.1:8000/auth/logout/
- **Админка**: http://127.0.0.1:8000/admin/

## Сервисы

- **PostgreSQL**: localhost:5432
- **RabbitMQ Management**: http://localhost:15672 (rabbitmq/pass)
- **Redis**: localhost:6379

## Остановка сервисов

```bash
docker-compose down
```