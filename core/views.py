import os

from botocore.exceptions import BotoCoreError, ClientError
from django.contrib import messages
from django.utils import timezone
from mutagen import File as MutagenFile
from boto3.session import Session
from django import forms
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import login
from django.contrib.auth.views import LoginView
from django.contrib.auth.forms import UserCreationForm
from django.views import View
from django.views.generic import CreateView, TemplateView, ListView, UpdateView
from django.urls import reverse_lazy
from django.views.generic.edit import FormMixin

from core.models import MediaTask, OutboxEvent, EventTypeChoices, CastTemplate, Project, MediaTaskStatusChoices, \
    IntegrationSettings, UploadChoices


class HomeView(LoginRequiredMixin, ListView):
    model = Project
    template_name = 'home.html'
    context_object_name = 'projects'

    def get_queryset(self):
        # Получаем проекты только текущей интеграции пользователя
        return Project.objects.filter(integration=self.request.user.integration)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context



class ProjectCreateView(LoginRequiredMixin, CreateView):
    model = Project
    template_name = 'project_create.html'
    fields = ['project_title', 'description']  # пока без выбора integration
    success_url = reverse_lazy('home')

    def form_valid(self, form):
        # автоматически проставляем интеграцию пользователя
        form.instance.integration = self.request.user.integration
        return super().form_valid(form)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context


class VideoUploadForm(forms.Form):
    file = forms.FileField(
        label="Выберите файл для загрузки",
        widget=forms.ClearableFileInput(attrs={
            "class": "form-control",
            "accept": "video/*,audio/wav"
        }),
        help_text="Поддерживаются форматы: MP4, MOV, AVI, MKV, WAV"
    )



class ProjectTaskListView(LoginRequiredMixin, FormMixin, ListView):
    model = MediaTask
    template_name = "project_tasks.html"
    context_object_name = "tasks"
    form_class = VideoUploadForm

    def get_queryset(self):
        self.project = get_object_or_404(
            Project,
            pk=self.kwargs["pk"],
            integration=self.request.user.integration
        )
        return MediaTask.objects.filter(project=self.project).order_by("-video_local_uploaded_at")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["project"] = self.project
        context["form"] = self.get_form()
        return context

    def post(self, request, *args, **kwargs):
        self.project = get_object_or_404(
            Project,
            pk=self.kwargs["pk"],
            integration=request.user.integration
        )
        form = self.get_form()

        if not form.is_valid():
            return self.form_invalid(form)

        file = form.cleaned_data["file"]
        original_name = file.name
        ext = original_name.split(".")[-1].lower()

        if ext != "wav":
            messages.error(request, f"❌ Пока неподдерживаемый тип файла: {ext}")
            return self.form_invalid(form)

        try:
            integration_settings = request.user.integration.settings
        except IntegrationSettings.DoesNotExist:
            messages.error(request, "❌ Не найдены настройки интеграции.")
            return self.form_invalid(form)

        upload_mode = integration_settings.upload_mode
        saved_name = f"{timezone.now().strftime('%Y%m%d%H%M%S')}_{original_name}"
        s3_key = f"media_uploads/{saved_name}"

        try:
            session = Session()
            s3_client = session.client(
                service_name="s3",
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                endpoint_url=settings.ENDPOINT_URL,
                region_name=settings.REGION
            )

            if upload_mode == UploadChoices.FULL:
                print("📦 Загрузка файла целиком (FULL)...")
                s3_client.upload_fileobj(file, settings.BUCKET_NAME, s3_key)
                public_url = f"{settings.ENDPOINT_URL}/{settings.BUCKET_NAME}/{s3_key}"

            elif upload_mode == UploadChoices.PARTS:
                print("🧩 Составная загрузка (PARTS) начата...")

                init_response = s3_client.create_multipart_upload(
                    Bucket=settings.BUCKET_NAME,
                    Key=s3_key,
                    ACL="public-read"
                )
                upload_id = init_response["UploadId"]
                print(f"✅ Upload ID: {upload_id}")

                part_size = 40 * 1024 * 1024  # 40MB
                total_size = file.size
                total_parts = (total_size + part_size - 1) // part_size
                total_mb = total_size / (1024 * 1024)

                print(f"📁 Общий размер файла: {total_mb:.2f} MB")
                print(f"🔢 Будет разбит на {total_parts} частей (~{part_size // (1024 * 1024)}MB каждая)")

                offset = 0
                part_number = 1
                parts = []

                while offset < total_size:
                    file.seek(offset)
                    chunk = file.read(part_size)

                    if len(chunk) < 5 * 1024 * 1024 and offset + len(chunk) != total_size:
                        raise ValueError(f"❌ Размер части слишком мал: {len(chunk)} байт")

                    print(f"📤 Загружаем часть {part_number}/{total_parts} ({len(chunk)} байт)...")

                    response = s3_client.upload_part(
                        Bucket=settings.BUCKET_NAME,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )

                    parts.append({
                        "ETag": response["ETag"],
                        "PartNumber": part_number
                    })

                    print(f"✅ Часть {part_number} загружена. ETag: {response['ETag']}")

                    offset += len(chunk)
                    part_number += 1

                print("📦 Завершаем составную загрузку...")

                s3_client.complete_multipart_upload(
                    Bucket=settings.BUCKET_NAME,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts}
                )

                public_url = f"{settings.ENDPOINT_URL}/{settings.BUCKET_NAME}/{s3_key}"
                print(f"🎉 Загрузка завершена: {public_url}")

            else:
                messages.error(request, f"❌ Неизвестный режим загрузки: {upload_mode}")
                return self.form_invalid(form)

            media_task = MediaTask.objects.create(
                project=self.project,
                integration=request.user.integration,
                audio_uploaded_title=original_name,
                audio_title_saved=saved_name,
                audio_extension_uploaded=ext,
                audio_storage_url=public_url,
                status=MediaTaskStatusChoices.LOADED,
            )

            OutboxEvent.objects.create(
                media_task=media_task,
                event_type=EventTypeChoices.AUDIO_UPLOADED_TO_YANDEX,
                payload={
                    "filename": saved_name,
                    "extension": ext,
                    "uploaded_by": request.user.username,
                    "project_id": self.project.pk,
                    "storage_url": public_url,
                    "upload_mode": upload_mode,
                },
            )

            return redirect("upload_success", pk=media_task.pk)

        except (BotoCoreError, ClientError) as e:
            print(f"❌ Ошибка S3: {e}")
            messages.error(request, f"Ошибка загрузки в хранилище: {e}")
            return self.form_invalid(form)

        except Exception as e:
            print(f"❌ Общая ошибка: {e}")
            messages.error(request, f"Ошибка при обработке: {e}")
            return self.form_invalid(form)






class MainView(LoginRequiredMixin, View):
    template_name = 'main.html'

    def get(self, request):
        form = VideoUploadForm()
        return render(request, self.template_name, {
            'form': form,
            'project_id': request.GET.get('project_id'),  # пробрасываем в шаблон
            'integration': request.user.integration
        })

    def post(self, request):
        project_id = request.POST.get("project_id")
        project = None
        if project_id:
            project = get_object_or_404(Project, pk=project_id)

        form = VideoUploadForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['file']
            original_name = file.name
            ext = os.path.splitext(original_name)[1].lower().lstrip('.')  # расширение

            saved_name = f"{timezone.now().strftime('%Y%m%d%H%M%S')}_{original_name}"
            save_path = os.path.join('media_uploads', saved_name)
            full_path = os.path.join(settings.MEDIA_ROOT, save_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            with open(full_path, 'wb+') as destination:
                for chunk in file.chunks():
                    destination.write(chunk)

            # === Создаем MediaTask (с привязкой к проекту, если передан) ===
            create_kwargs = {
                "project": project,
            }

            if ext in ["mp4", "mov", "avi", "mkv"]:
                create_kwargs.update({
                    "video_uploaded_title": original_name,
                    "video_title_saved": saved_name,
                    "video_extension": ext,
                    "video_storage_url": os.path.join(settings.MEDIA_URL, save_path),
                })
                media_task = MediaTask.objects.create(**create_kwargs)

                OutboxEvent.objects.create(
                    media_task=media_task,
                    event_type=EventTypeChoices.VIDEO_UPLOADED_LOCAL,
                    payload={
                        "filename": saved_name,
                        "extension": ext,
                        "uploaded_by": request.user.username,
                        "project_id": project_id,
                    }
                )

            elif ext == "wav":
                create_kwargs.update({
                    "audio_uploaded_title": original_name,
                    "audio_title_saved": saved_name,
                    "audio_extension_uploaded": ext,
                    "audio_storage_url": os.path.join(settings.MEDIA_URL, save_path),
                })
                media_task = MediaTask.objects.create(**create_kwargs)

                OutboxEvent.objects.create(
                    media_task=media_task,
                    event_type=EventTypeChoices.AUDIO_WAV_UPLOADED,
                    payload={
                        "filename": saved_name,
                        "extension": ext,
                        "uploaded_by": request.user.username,
                        "project_id": project_id,
                    }
                )
            else:
                messages.error(request, f"Неподдерживаемый тип файла: {ext}")
                return render(request, self.template_name, {
                    'form': form,
                    'integration': request.user.integration
                })

            return redirect('upload_success', pk=media_task.pk)

        return render(request, self.template_name, {
            'form': form,
            'integration': request.user.integration
        })


class RegisterView(CreateView):
    form_class = UserCreationForm
    template_name = 'registration/register.html'
    success_url = reverse_lazy('home')
    
    def form_valid(self, form):
        response = super().form_valid(form)
        login(self.request, self.object)
        return response


class CustomLoginView(LoginView):
    template_name = 'registration/login.html'
    redirect_authenticated_user = True


class VideoUploadForm2(forms.Form):
    file = forms.FileField(label="Выберите видео или аудиофайл")


class UploadSuccessView(LoginRequiredMixin, UpdateView):
    model = MediaTask
    fields = ["cast_template"]
    template_name = "upload_success.html"
    context_object_name = "media_task"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        integration = self.object.integration
        context["cast_templates"] = CastTemplate.objects.filter(integration=integration)
        context['integration'] = self.request.user.integration
        return context

    def form_valid(self, form):
        """
        Этот метод вызывается, когда форма прошла валидацию.
        Здесь мы можем добавить кастомные действия — например, создание OutboxEvent.
        """
        response = super().form_valid(form)

        media_task = self.object  # уже сохранённый объект
        if media_task.cast_template:
            # Создаём OutboxEvent, чтобы запустить обработку по выбранному шаблону
            OutboxEvent.objects.create(
                media_task=media_task,
                event_type=EventTypeChoices.TEMPLATE_SELECTED,
                payload={
                    "cast_template": media_task.cast_template.id,
                    "selected_by": self.request.user.username,
                },
            )
            messages.success(self.request, "Шаблон выбран, задача на обработку создана!")

        return response

    def get_success_url(self):
        return reverse_lazy("home")


class MyTemplatesView(LoginRequiredMixin, ListView):
    """
    Список всех шаблонов CastTemplate
    """
    model = CastTemplate
    template_name = "templates_list.html"
    context_object_name = "templates"

    def get_queryset(self):
        # Если нужно, можно фильтровать по пользователю или интеграции
        return CastTemplate.objects.filter(integration = self.request.user.integration)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context


class MyTasksView(LoginRequiredMixin, ListView):
    """
    Список всех шаблонов CastTemplate
    """
    model = MediaTask
    template_name = "tasks_list.html"
    context_object_name = "tasks"

    def get_queryset(self):
        user = self.request.user
        integration = getattr(user, "integration", None)
        return MediaTask.objects.filter()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context


class CastTemplateCreateView(LoginRequiredMixin, CreateView):
    """
    Создание нового шаблона
    """
    model = CastTemplate
    template_name = "template_form.html"
    fields = ["questions", "template_type", "title"]
    success_url = reverse_lazy("my_templates")
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context


class CastTemplateUpdateView(LoginRequiredMixin, UpdateView):
    """
    Редактирование существующего шаблона
    """
    model = CastTemplate
    template_name = "template_form.html"
    fields = ["questions", "template_type", "title"]
    success_url = reverse_lazy("my_templates")
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context

class IntegrationSettingsView(LoginRequiredMixin, UpdateView):
    model = IntegrationSettings
    fields = ["upload_mode"]  # укажи нужные поля настроек
    template_name = "integration_settings.html"
    context_object_name = "settings"

    def get_object(self, queryset=None):
        # Получаем интеграцию текущего пользователя
        integration = self.request.user.integration
        # Получаем или создаем настройки для этой интеграции
        settings_obj, created = IntegrationSettings.objects.get_or_create(
            integration=integration,
            defaults={'upload_mode': 'full'}
        )
        return settings_obj
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['integration'] = self.request.user.integration
        return context
    
    def get_success_url(self):
        return reverse_lazy('home')