from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model
from .models import Integration, CastTemplate

User = get_user_model()

@receiver(post_save, sender=User)
def create_user_integration(sender, instance, created, **kwargs):
    if created:
        integration = Integration.objects.create(user=instance)

        for template in CastTemplate.objects.filter(default=True, integration__isnull=True):
            CastTemplate.objects.create(
                integration=integration,
                title=template.title,
                promt_text=template.promt_text,
                help_text=template.help_text,
                questions=template.questions,
                template_type=template.template_type,
                default=True,
            )
