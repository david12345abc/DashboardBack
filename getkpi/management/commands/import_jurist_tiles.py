"""
Плитки договоров для «Юрисконсультант»: количество и сумма по каждой компании.

    py manage.py import_jurist_tiles
    py manage.py import_jurist_tiles --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.kpi_definitions_cache import bump_kpi_definitions_cache_version
from getkpi.models import KpiDefinition
from jurist.companies import COMPANIES, DEPARTMENT

SOURCE = "1С ERP (SQL erp_pm) / договоры контрагентов"
RULE = (
    "Договоры контрагентов организации без пометки удаления, "
    "у которых дата «от» рядом с номером попадает в выбранный месяц. "
    "Учитываются статусы «Действует», «Закрыт» и «НеСогласован». "
    "Не учитываются цели «Передача на хранение с правом продажи» "
    "и «Производство у переработчика». "
    "Сумма берётся из поля «Сумма» без пересчёта валюты.\n"
    "На обороте плитки — разбивка по цели договора "
    "(поле «Хозяйственная операция», на форме «Цель договора»)."
)


def _tiles() -> list[dict]:
    rows: list[dict] = []
    position = 1
    for company in COMPANIES:
        org_note = f"Организация в 1С: «{company['odata_name']}»."
        rows.append({
            "kpi_id": company["count_kpi_id"],
            "name": f"Общее кол-во договоров — {company['title']}",
            "block": "плитка",
            "frequency": "Ежемесячно",
            "perspective": "Процессы",
            "goal": "Видеть объём договорной базы компании",
            "formula": (
                f"Число договоров контрагентов организации «{company['odata_name']}» "
                "без пометки удаления, у которых дата «от» в выбранном месяце"
            ),
            "unit": "шт.",
            "source": SOURCE,
            "description": f"{org_note}\n{RULE}",
            "weight_pct": None,
            "position": position,
        })
        position += 1
        rows.append({
            "kpi_id": company["sum_kpi_id"],
            "name": f"Сумма договоров — {company['title']}",
            "block": "плитка",
            "frequency": "Ежемесячно",
            "perspective": "Финансы",
            "goal": "Видеть сумму договорной базы компании",
            "formula": (
                f"Сумма поля «Сумма» по договорам контрагентов организации "
                f"«{company['odata_name']}» без пометки удаления, "
                "у которых дата «от» в выбранном месяце"
            ),
            "unit": "руб.",
            "source": SOURCE,
            "description": f"{org_note}\n{RULE}",
            "weight_pct": None,
            "position": position,
        })
        position += 1
    return rows


class Command(BaseCommand):
    help = "Плитки количества и суммы договоров для «Юрисконсультант»"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Только показать, что будет сделано, без записи в БД",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        for item in _tiles():
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
