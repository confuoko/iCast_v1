#!/bin/bash

# Скрипт для запуска Django и Celery сервисов
# Убедитесь, что виртуальное окружение активировано перед запуском

echo "🚀 Запуск сервисов iCast..."

# Проверяем, что мы в правильной директории
if [ ! -f "manage.py" ]; then
    echo "❌ Ошибка: manage.py не найден. Убедитесь, что вы находитесь в корневой директории проекта."
    exit 1
fi

# Функция для остановки процессов при получении сигнала завершения
cleanup() {
    echo ""
    echo "🛑 Остановка сервисов..."
    
    # Останавливаем все фоновые процессы
    if [ ! -z "$DJANGO_PID" ]; then
        kill $DJANGO_PID 2>/dev/null
        echo "✅ Django сервер остановлен"
    fi
    
    if [ ! -z "$CELERY_HANDLER_PID" ]; then
        kill $CELERY_HANDLER_PID 2>/dev/null
        echo "✅ Celery handler worker остановлен"
    fi
    
    if [ ! -z "$CELERY_PROCESSING_PID" ]; then
        kill $CELERY_PROCESSING_PID 2>/dev/null
        echo "✅ Celery processing worker остановлен"
    fi
    
    if [ ! -z "$CELERY_BEAT_PID" ]; then
        kill $CELERY_BEAT_PID 2>/dev/null
        echo "✅ Celery beat остановлен"
    fi
    
    echo "👋 Все сервисы остановлены"
    exit 0
}

# Устанавливаем обработчик сигналов
trap cleanup SIGINT SIGTERM

# Применяем миграции Django
echo "📦 Применение миграций Django..."
python manage.py migrate
if [ $? -ne 0 ]; then
    echo "❌ Ошибка при применении миграций"
    exit 1
fi

# Собираем статические файлы (если нужно)
echo "📁 Сбор статических файлов..."
python manage.py collectstatic --noinput 2>/dev/null || echo "⚠️  Статические файлы не собраны (возможно, не настроено)"

echo ""
echo "🌐 Запуск Django сервера на http://127.0.0.1:8000..."
python manage.py runserver 127.0.0.1:8000 &
DJANGO_PID=$!

# Ждем немного, чтобы Django запустился
sleep 3

echo "⚡ Запуск Celery worker для очереди handler..."
celery -A backend worker -Q handler -n worker_handler@%h --loglevel=INFO --pool=solo &
CELERY_HANDLER_PID=$!

# Ждем немного, чтобы handler worker запустился
sleep 2

echo "⚡ Запуск Celery worker для очереди processing..."
celery -A backend worker -Q processing -n worker_processing@%h --loglevel=INFO --pool=solo &
CELERY_PROCESSING_PID=$!

# Ждем немного, чтобы processing worker запустился
sleep 2

echo "⏰ Запуск Celery beat scheduler..."
celery -A backend beat --loglevel=INFO &
CELERY_BEAT_PID=$!

echo ""
echo "✅ Все сервисы запущены успешно!"
echo ""
echo "📋 Статус сервисов:"
echo "   🌐 Django: http://127.0.0.1:8000 (PID: $DJANGO_PID)"
echo "   ⚡ Celery Handler Worker (PID: $CELERY_HANDLER_PID)"
echo "   ⚡ Celery Processing Worker (PID: $CELERY_PROCESSING_PID)"
echo "   ⏰ Celery Beat (PID: $CELERY_BEAT_PID)"
echo ""
echo "💡 Для остановки всех сервисов нажмите Ctrl+C"
echo ""

# Ожидаем завершения процессов
wait
