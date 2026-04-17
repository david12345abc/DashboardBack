from django.apps import AppConfig


class GetkpiConfig(AppConfig):
    name = 'getkpi'

    def ready(self):
        from . import cache_manager
        cache_manager.start_warming()
