from django.apps import AppConfig


class GetkpiConfig(AppConfig):
    name = 'getkpi'

    def ready(self):
        from django.db.models.signals import post_delete, post_save

        from . import cache_manager
        from . import commercial_cache_scheduler
        from . import prod_deputy_cache_scheduler
        from . import restore_scheduler
        from .kpi_definitions_cache import bump_kpi_definitions_cache_version
        from .models import KpiDefinition

        cache_manager.start_warming()
        cache_manager.start_midnight_cache_scheduler()
        commercial_cache_scheduler.start_commercial_cache_scheduler()
        prod_deputy_cache_scheduler.start_prod_deputy_cache_scheduler()
        restore_scheduler.start_restore_scheduler()

        def _invalidate_kpi_definitions_cache(**_kwargs):
            bump_kpi_definitions_cache_version()

        post_save.connect(
            _invalidate_kpi_definitions_cache,
            sender=KpiDefinition,
            dispatch_uid='invalidate_kpi_definitions_cache_on_save',
        )
        post_delete.connect(
            _invalidate_kpi_definitions_cache,
            sender=KpiDefinition,
            dispatch_uid='invalidate_kpi_definitions_cache_on_delete',
        )
