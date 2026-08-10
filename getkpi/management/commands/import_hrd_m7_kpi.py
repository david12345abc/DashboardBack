"""Upsert KPI HRD-M7 «Производительность труда (НПО)» для СУП.

Использование:
    py manage.py import_hrd_m7_kpi
    py manage.py import_hrd_m7_kpi --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

DEPARTMENT = "Служба управления персоналом"

KPI = {
    "kpi_id": "HRD-M7",
    "name": "Производительность труда (НПО)",
    "block": "плитка",
    "frequency": "Ежемесячно",
    "perspective": "Персонал",
    "goal": "Оценивать производительность труда как выручку на одного сотрудника (НПО + Алмаз)",
    "formula": "Выручка (факт FND-T1) / (ССЧ НПО + ССЧ Алмаз)",
    "unit": "руб./чел.",
    "source": "FND-T1 (calc_dengi_fact) / erp_pm кадровая история НПО + Алмаз",
    "monthly_target": None,
    "quarterly_target": None,
    "yearly_target": None,
    "green_threshold": None,
    "yellow_threshold": None,
    "red_threshold": None,
    "weight_pct": 0.0,
    "position": 8,
}


class Command(BaseCommand):
    help = "Upsert HRD-M7 для «Служба управления персоналом»"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Только показать, что будет сделано, без записи в БД",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        kpi_id = KPI["kpi_id"]
        defaults = {k: v for k, v in KPI.items() if k != "kpi_id"}
        exists = KpiDefinition.objects.filter(
            department=DEPARTMENT, kpi_id=kpi_id,
        ).exists()
        tag = "UPDATE" if exists else "CREATE"
        self.stdout.write(f"[{tag}] {kpi_id} — {KPI['name']}")
        if dry_run:
            self.stdout.write(self.style.WARNING("[DRY-RUN] запись не изменена"))
            return
        _, is_new = KpiDefinition.objects.update_or_create(
            department=DEPARTMENT,
            kpi_id=kpi_id,
            defaults=defaults,
        )
        msg = "создана" if is_new else "обновлена"
        self.stdout.write(self.style.SUCCESS(f"HRD-M7 {msg}"))
