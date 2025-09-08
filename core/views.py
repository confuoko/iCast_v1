import os

from django.contrib import messages
from django.utils import timezone

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

from core.models import MediaTask, OutboxEvent, EventTypeChoices, CastTemplate


class HomeView(LoginRequiredMixin, TemplateView):
    template_name = 'home.html'

class MainView(LoginRequiredMixin, View):
    template_name = 'main.html'

    def get(self, request):
        form = VideoUploadForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request):
        form = VideoUploadForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['file']
            original_name = file.name
            ext = os.path.splitext(original_name)[1].lower().lstrip('.')  # расширение

            saved_name = f"{timezone.now().strftime('%Y%m%d%H%M%S')}_{original_name}"
            save_path = os.path.join('media_uploads', saved_name)  # универсальная папка для аудио/видео
            full_path = os.path.join(settings.MEDIA_ROOT, save_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            with open(full_path, 'wb+') as destination:
                for chunk in file.chunks():
                    destination.write(chunk)

            # === Логика выбора: видео или аудио ===
            if ext in ["mp4", "mov", "avi", "mkv"]:
                # Создаем MediaTask для видео
                media_task = MediaTask.objects.create(
                    video_uploaded_title=original_name,
                    video_title_saved=saved_name,
                    video_extension=ext,
                    video_storage_url=os.path.join(settings.MEDIA_URL, save_path),
                )

                # Создаем OutboxEvent для видео
                OutboxEvent.objects.create(
                    media_task=media_task,
                    event_type=EventTypeChoices.VIDEO_UPLOADED_LOCAL,
                    payload={
                        "filename": saved_name,
                        "extension": ext,
                        "uploaded_by": request.user.username,
                    }
                )

            elif ext == "wav":
                # Создаем MediaTask для аудио
                media_task = MediaTask.objects.create(
                    audio_uploaded_title=original_name,
                    audio_title_saved=saved_name,
                    audio_extension_uploaded=ext,
                    audio_storage_url=os.path.join(settings.MEDIA_URL, save_path),
                )

                # Создаем OutboxEvent для аудио
                OutboxEvent.objects.create(
                    media_task=media_task,
                    event_type=EventTypeChoices.AUDIO_WAV_UPLOADED,
                    payload={
                        "filename": saved_name,
                        "extension": ext,
                        "uploaded_by": request.user.username,
                    }
                )
            else:
                # Если расширение неизвестно — можно вернуть ошибку
                messages.error(request, f"Неподдерживаемый тип файла: {ext}")
                return render(request, self.template_name, {'form': form})

            # ✅ редирект на страницу успеха с pk
            return redirect('upload_success', pk=media_task.pk)

        return render(request, self.template_name, {'form': form})


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


class VideoUploadForm(forms.Form):
    file = forms.FileField(label="Выберите видео или аудиофайл")


class UploadSuccessView(LoginRequiredMixin, View):
    template_name = "upload_success.html"

    def get(self, request, pk):
        media_task = get_object_or_404(MediaTask, pk=pk)
        return render(request, self.template_name, {"media_task": media_task})


class MyTemplatesView(LoginRequiredMixin, ListView):
    """
    Список всех шаблонов CastTemplate
    """
    model = CastTemplate
    template_name = "templates_list.html"
    context_object_name = "templates"

    def get_queryset(self):
        # Если нужно, можно фильтровать по пользователю или интеграции
        return CastTemplate.objects.all()


class CastTemplateCreateView(LoginRequiredMixin, CreateView):
    """
    Создание нового шаблона
    """
    model = CastTemplate
    template_name = "template_form.html"
    fields = ["questions", "template_type", "title"]
    success_url = reverse_lazy("my_templates")


class CastTemplateUpdateView(LoginRequiredMixin, UpdateView):
    """
    Редактирование существующего шаблона
    """
    model = CastTemplate
    template_name = "template_form.html"
    fields = ["questions", "template_type", "title"]
    success_url = reverse_lazy("my_templates")