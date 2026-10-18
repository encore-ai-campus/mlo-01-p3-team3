from django.apps import AppConfig


class OperationsConfig(AppConfig):
    default_auto_field='django.db.models.BigAutoField'
    name='operations'

    def ready(self):
        from . import tracking  # Register immutable event metadata on insertion.
