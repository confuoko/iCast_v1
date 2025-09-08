import os
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")

app = Celery("backend")

app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

app.conf.beat_schedule = {
    "sync_all_outbox_events": {
        "task": "core.tasks.handler_task",
        "schedule": timedelta(seconds=10),
    },
}


@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")
