from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone

User = get_user_model()

class Integration(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        verbose_name="Пользователь"
    )
    endpoint = models.CharField(
        max_length=255,
        verbose_name="Endpoint",
        blank=True,
        null=True
    )

    def __str__(self):
        return f"Интеграция для пользователя {self.user.username}"


class Project(models.Model):
    """
    Проект, к которому может относиться MediaTask.
    """

    project_title = models.CharField(
        max_length=255,
        verbose_name="Название проекта"
    )
    description = models.TextField(
        blank=True,
        null=True,
        verbose_name="Описание проекта"
    )
    integration = models.ForeignKey(
        Integration,
        on_delete=models.CASCADE,
        related_name="projects",
        verbose_name="Интеграция"
    )

    def __str__(self):
        return self.project_title





class TemplateTypeChoices(models.TextChoices):
    JTBD = "jtbd", "JTBD"
    SCORE = "score", "Score"
    CUSTOM = "custom", "Custom"


class CastTemplate(models.Model):
    """
    Шаблон для обработки кастдева или транскрипта.
    """
    integration = models.ForeignKey(
        "Integration",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        verbose_name="Интеграция",
    )
    title = models.CharField(
        max_length=255,
        verbose_name="Template Title",
        blank=True,
        null=True
    )
    help_text = models.TextField(
        verbose_name="Полный текст после диаризации",
        blank=True,
        null=True,
        help_text="Описание шаблона."
    )

    questions = models.JSONField(
        null=True,
        blank=True,
        verbose_name="Вопросы (JSON)"
    )
    template_type = models.CharField(
        max_length=20,
        choices=TemplateTypeChoices.choices,
        default=TemplateTypeChoices.CUSTOM,
        verbose_name="Тип шаблона"
    )

    def __str__(self):
        return f"Шаблон (ID {self.id})"


class MediaTaskStatusChoices(models.TextChoices):
    LOADED = "loaded", "Загружен"
    TRANSCRIBATION = "transcribation", "Транскрибация выполняется"
    DATA_EXTRACTION = "data_extraction", "Извлечение данных выполняется"
    SUCCESS = "success", "Успешно завершено"
    FAILED = "failed", "Ошибка"


class MediaTask(models.Model):
    integration = models.ForeignKey(
            'Integration',
            on_delete=models.SET_NULL,
            null=True,
            blank=True,
            verbose_name="Интеграция"
        )
    project = models.ForeignKey(
        Project,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="media_tasks",
        verbose_name="Проект"
    )
    status = models.CharField(
        max_length=32,
        choices=MediaTaskStatusChoices.choices,
        default=MediaTaskStatusChoices.LOADED,
        verbose_name="Статус задачи",
    )
    cast_template = models.ForeignKey(
        "CastTemplate",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="media_tasks",
        verbose_name="Шаблон обработки"
    )
    video_uploaded_title = models.CharField(
        max_length=255,
        verbose_name="Оригинальное название видео",
        blank=True,
        null=True
    )
    video_title_saved = models.CharField(
        max_length=255,
        verbose_name="Системное имя видео",
        blank=True,
        null=True
    )
    video_local_uploaded_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Время загрузки видео в Сервис iCast"
    )
    duration_seconds = models.FloatField(
        verbose_name="Длительность видео (в секундах)",
        blank=True,
        null=True
    )
    video_extension = models.CharField(
        max_length=20,
        verbose_name="Расширение видеофайла",
        blank=True,
        null=True
    )
    video_storage_url = models.URLField(
        verbose_name="Ссылка на видеофайл в хранилище Яндекс",
        blank=True,
        null=True
    )
    audio_uploaded_title = models.CharField(
        max_length=255,
        verbose_name="Оригинальное название аудио",
        blank=True,
        null=True
    )
    audio_title_saved = models.CharField(
        max_length=255,
        verbose_name="Системное имя аудио",
        blank=True,
        null=True
    )
    audio_local_uploaded_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Время загрузки аудио в Сервис iCast"
    )
    audio_duration_seconds = models.FloatField(
        verbose_name="Длительность аудио (в секундах)",
        blank=True,
        null=True
    )
    audio_extension_uploaded = models.CharField(
        max_length=20,
        verbose_name="Расширение загруженного аудиофайла",
        blank=True,
        null=True
    )
    audio_storage_url = models.URLField(
        verbose_name="Ссылка на аудио в хранилище Яндекс",
        blank=True,
        null=True
    )
    # === Статус обработки ===
    nexara_status = models.CharField(
        max_length=20,
        choices=[
            ("pending", "В очереди"),
            ("processing", "Обработка"),
            ("done", "Готово"),
            ("error", "Ошибка"),
        ],
        default="pending",
        verbose_name="Статус обработки Nexara",
    )

    # === Основной текст транскрипции ===
    diarization_text = models.TextField(
        verbose_name="Полный текст после диаризации",
        blank=True,
        null=True,
        help_text="Склеенный текст всех сегментов."
    )

    # === Сегменты (JSON) ===
    diarization_segments = models.JSONField(
        verbose_name="Сегменты диаризации",
        blank=True,
        null=True,
        help_text="Массив сегментов из Nexara (speaker, start, end, text)."
    )
    audio_duration_seconds_nexara = models.FloatField(
        verbose_name="Фактическая длительность аудио (по Nexara)",
        blank=True,
        null=True
    )
    # === Ошибки ===
    nexara_error = models.TextField(
        verbose_name="Текст ошибки Nexara",
        blank=True,
        null=True,
        help_text="Сохраняем ответ об ошибке, если запрос неудачный"
    )
    nexara_requested_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Когда был отправлен запрос в Nexara"
    )
    nexara_completed_at = models.DateTimeField(
        blank=True,
        null=True,
        verbose_name="Когда Nexara вернула результат"
    )
    gpt_result = models.JSONField(
        verbose_name="Результат GPT анализа",
        null=True,
        blank=True,
        help_text="Сохранённый ответ от GPT в формате JSON"
    )
    gpt_raw_response = models.TextField(
        verbose_name="Сырой ответ GPT",
        null=True,
        blank=True
    )
    token_count = models.PositiveIntegerField(
        null=True,
        blank=True,
        verbose_name="Количество токенов"
    )
    total_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="Итоговая цена"
    )
    excel_path = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="Путь к Excel в хранилище"
    )
    pdf_path = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="Путь к PDF в хранилище"
    )
    transcribation_path = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="Путь к транскрипции в хранилище"
    )
    finished_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name="Дата и время завершения"
    )


class EventTypeChoices(models.TextChoices):
    VIDEO_UPLOADED_LOCAL = "video_uploaded", "Видео загружено"
    VIDEO_UPLOADED_YANDEX = "video_uploaded_yandex", "Видео загружено в хранилище Яндекс"

    AUDIO_WAV_UPLOADED = "audio_wav_uploaded", "Аудио WAV загружено"
    AUDIO_SEND_TO_YANDEX = "audio_send_to_yandex", "Аудио отправлено в хранилище Яндекс"

    AUDIO_TRANSCRIBE_STARTED = "audio_transcribe_started", "Транскрибация аудио начата"
    AUDIO_TRANSCRIBATION_READY = "audio_transcribation_ready", "Транскрибация успешно завершена"

    TEMPLATE_SELECTED = "template_selected", "Шаблон выбран"
    GPT_RESULT_READY = "gpt_result_ready", "Результат GPT готов"

    DOCUMENT_CREATED = "document_created", "Документ создан"
    CASE_SYNCED = "case_synced", "Синхронизация дела"
    EMAIL_SENT = "email_sent", "Email отправлен"
    WEBHOOK_TRIGGERED = "webhook_triggered", "Вебхук вызван"
    # можешь добавлять сюда свои типы событий


class OutboxEvent(models.Model):
    """
    Событие Outbox, связанное с MediaTask.
    """

    media_task = models.ForeignKey(
        "MediaTask",
        on_delete=models.CASCADE,
        related_name="outbox_events",
        verbose_name="Источник события (MediaTask)"
    )

    event_type = models.CharField(
        max_length=64,
        choices=EventTypeChoices.choices,
        verbose_name="Тип события"
    )

    payload = models.JSONField(
        verbose_name="Полезная нагрузка (например, путь к файлу, результат анализа и т.д.)"
    )

    created_at = models.DateTimeField(
        default=timezone.now,
        verbose_name="Когда событие создано"
    )

    processed = models.BooleanField(
        default=False,
        verbose_name="Обработано?"
    )

    processed_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name="Когда событие было обработано"
    )

    error_message = models.TextField(
        null=True,
        blank=True,
        verbose_name="Сообщение об ошибке (если было)"
    )


    class Meta:
        verbose_name = "Outbox-событие"
        verbose_name_plural = "Outbox-события"
        ordering = ["created_at"]




class Template(models.Model):
    integration = models.ForeignKey(
        'Integration',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        verbose_name="Интеграция"
    )
    is_custom = models.BooleanField(
        default=False,
        verbose_name="Пользовательский шаблон"
    )
    text = models.TextField(
        verbose_name="Текст шаблона",
        blank=True,
        null=True
    )

    def __str__(self):
        return f"Template #{self.pk} (custom: {self.is_custom})"

