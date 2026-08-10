"""Upsert KPI HRD-M9 «Укомплектованность штата» для СУП.

Использование:
    py manage.py import_hrd_m9_kpi
    py manage.py import_hrd_m9_kpi --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

DEPARTMENT = "Служба управления персоналом"

KPI = {
    "kpi_id": "HRD-M9",
    "name": "Укомплектованность штата",
    "block": "плитка",
    "frequency": "Ежемесячно",
    "perspective": "Персонал",
    "goal": "Контролировать долю закрытых позиций штата НПО",
    "formula": (
        "Сотрудники НПО (не уволенные на конец месяца) / "
        "(сотрудники НПО + актуальные вакансии) × 100%"
    ),
    "unit": "%",
    "source": "erp_pm кадровая история НПО / HC_сводный Вакансии",
    "monthly_target": "≥90%",
    "quarterly_target": None,
    "yearly_target": "≥90%",
    "green_threshold": "≥90%",
    "yellow_threshold": "80–89,9%",
    "red_threshold": "<80%",
    "weight_pct": 0.0,
    "position": 9,
}


class Command(BaseCommand):
    help = "Upsert HRD-M9 для «Служба управления персоналом»"

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
        self.stdout.write(self.style.SUCCESS(f"HRD-M9 {msg}"))
