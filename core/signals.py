from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model
from .models import Integration

User = get_user_model()

@receiver(post_save, sender=User)
def create_user_integration(sender, instance, created, **kwargs):
    if created:
        Integration.objects.create(user=instance)
