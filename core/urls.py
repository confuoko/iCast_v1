from django.conf import settings
from django.conf.urls.static import static
from django.urls import path
from . import views

urlpatterns = [
    path('', views.HomeView.as_view(), name='home'),
    path('projects/create/', views.ProjectCreateView.as_view(), name='project-create'),
    path('projects/<int:pk>/', views.ProjectTaskListView.as_view(), name='project-tasks'),
    path('main/', views.MainView.as_view(), name='main'),
    path('uploads/', views.MainUploadsView.as_view(), name='main-uploads'),
    path("upload-success/<int:pk>/", views.UploadSuccessView.as_view(), name="upload_success"),
    path("integration-settings/", views.IntegrationSettingsView.as_view(), name="integration_settings"),
    path("my-templates/", views.MyTemplatesView.as_view(), name="my_templates"),
    path("my-tasks/", views.MyTasksView.as_view(), name="my_tasks"),
    path("templates/create/", views.CastTemplateCreateView.as_view(), name="template_create"),
    path("templates/<int:pk>/edit/", views.CastTemplateUpdateView.as_view(), name="template_edit"),
    path('register/', views.RegisterView.as_view(), name='register'),
    path('login/', views.CustomLoginView.as_view(), name='login'),
]+ static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
