"""
Плитки плана производства для «Начальник сервисной службы».

    py manage.py import_servhead_production_tiles
    py manage.py import_servhead_production_tiles --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.kpi_definitions_cache import bump_kpi_definitions_cache_version
from getkpi.models import KpiDefinition

DEPARTMENT = "Начальник сервисной службы"
SOURCE = "MS Project, планы СМР ПНР"
REMOVED_KPI_IDS = ("SH-M7",)

ON_TIME = {
    "monthly_target": "≥90%",
    "quarterly_target": None,
    "yearly_target": "≥90%",
    "green_threshold": "≥90%",
    "yellow_threshold": "80–89,9%",
    "red_threshold": "<80%",
}
LATE = {
    "monthly_target": "≤5%",
    "quarterly_target": None,
    "yearly_target": "≤5%",
    "green_threshold": "≤5%",
    "yellow_threshold": "5,1–7%",
    "red_threshold": ">7%",
}

TILES: list[dict] = [
    {
        "kpi_id": "SH-M6",
        "name": "План производства сервисной службы",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Процессы",
        "goal": "Выполнять план производства сервисной службы",
        "formula": (
            "Сводные задачи месяца со 100% завершения / "
            "Все сводные задачи под месяцем × 100%"
        ),
        "unit": "шт.",
        "source": SOURCE,
        "description": (
            "План: сводные задачи, которые лежат прямо под строкой месяца "
            "в квартальном плане MS Project.\n"
            "Факт: из них задачи с процентом завершения 100%."
        ),
        "weight_pct": None,
        "position": 6,
        **ON_TIME,
    },
    {
        "kpi_id": "SH-M8",
        "name": "Отставания в этапах",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Процессы",
        "goal": "Не допускать отставания этапов плана производства",
        "formula": (
            "Задачи под сводными месяца, срок которых уже прошёл "
            "и процент завершения не 100% / Все такие задачи × 100%"
        ),
        "unit": "шт.",
        "source": SOURCE,
        "description": (
            "План: обычные задачи под сводными задачами месяца "
            "в квартальном плане MS Project.\n"
            "Факт: из них задачи, у которых дата окончания по плану уже прошла, "
            "а процент завершения не 100%."
        ),
        "weight_pct": None,
        "position": 7,
        **LATE,
    },
]


class Command(BaseCommand):
    help = "Плитки плана производства для «Начальник сервисной службы»"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Только показать, что будет сделано, без записи в БД",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        stale = list(
            KpiDefinition.objects.filter(
                department=DEPARTMENT, kpi_id__in=REMOVED_KPI_IDS,
            )
        )
        for item in stale:
            self.stdout.write(f"[DELETE] {item.kpi_id} — {item.name}")
        if not dry_run and stale:
            KpiDefinition.objects.filter(id__in=[item.id for item in stale]).delete()
        for item in TILES:
            kpi_id = item["kpi_id"]
            exists = KpiDefinition.objects.filter(
                department=DEPARTMENT, kpi_id=kpi_id,
            ).exists()
            tag = "UPDATE" if exists else "CREATE"
            self.stdout.write(f"[{tag}] {kpi_id} — {item['name']}")
            if dry_run:
                continue
            defaults = {key: value for key, value in item.items() if key != "kpi_id"}
            _, created = KpiDefinition.objects.update_or_create(
                department=DEPARTMENT,
                kpi_id=kpi_id,
                defaults=defaults,
            )
            verb = "создана" if created else "обновлена"
            self.stdout.write(self.style.SUCCESS(f"  {kpi_id} {verb}"))

        if dry_run:
            self.stdout.write(self.style.WARNING("[DRY-RUN] записи в БД не менялись"))
            return

        bump_kpi_definitions_cache_version()
        self.stdout.write(self.style.SUCCESS("Кэш справочника KPI сброшен"))
