"""Прогрев файловых кэшей дашборда.

Использование:
    py manage.py warm_caches
    py manage.py warm_caches --all-months
    py manage.py warm_caches --all-months --force
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

from getkpi import cache_manager


class Command(BaseCommand):
    help = 'Прогреть кэши KPI дашборда (текущий месяц или все месяцы с 2026-01)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--all-months',
            action='store_true',
            help='Прогреть кэши за каждый месяц с 2026-01 по текущий',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Пересчитать даже свежие кэши',
        )

    def handle(self, *args, **options):
        force = bool(options['force'])
        if options['all_months']:
            self.stdout.write('Прогрев кэшей за все месяцы...')
            cache_manager.warm_all_caches_all_months(force=force)
        else:
            self.stdout.write('Прогрев кэшей за текущий месяц...')
            cache_manager.warm_all_caches(force=force)
        self.stdout.write(self.style.SUCCESS('Готово'))
