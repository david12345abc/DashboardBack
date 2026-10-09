"""
Две плитки на формы 03-17 / 03-18 / 03-19: «всего» и «в работе».

Использование:
    py manage.py import_qualdir_form_work_tiles
    py manage.py import_qualdir_form_work_tiles --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.kpi_definitions_cache import bump_kpi_definitions_cache_version
from getkpi.models import KpiDefinition

DEPARTMENT = "Заместитель тех. директора по качеству"

# Текущий порядок плиток, пары «всего» / «в работе» стоят рядом.
TILE_ORDER = (
    "QD-M1",
    "QD-M1W",
    "QD-M10",
    "QD-M3",
    "QD-M4",
    "QD-M5",
    "QD-M5W",
    "QD-M6",
    "QD-M7",
    "QD-M8",
    "QD-M8W",
    "QD-M9",
    "QD-Q1",
    "QD-Q2",
)

WORK_TILES = (
    ("QD-M8", "QD-M8W"),
    ("QD-M5", "QD-M5W"),
    ("QD-M1", "QD-M1W"),
)

TOTAL_FORMULA = (
    "Выполнено (дата устранения факт, статус «Завершено») / "
    "Всего форм (дата документа + 2 рабочих дня) × 100%"
)
WORK_FORMULA = (
    "В работе без просрочки / Всего в работе × 100%. "
    "Просрочка: дата документа + 2 рабочих дня + 30 календарных дней. "
    "Статус «Исполнение КМ» не просрочивается."
)
TOTAL_DESCRIPTION = (
    "План:\n"
    "Формы, у которых дата документа плюс 2 рабочих дня попадает в выбранный месяц. "
    "Не входят статусы «Отменена» (аннулировано), «Не согласовано» и «Подготовлен».\n"
    "Факт:\n"
    "Формы в статусе «Выполнено» (в интерфейсе «Завершено»), "
    "у которых дата устранения факт попадает в месяц."
)
WORK_DESCRIPTION = (
    "План:\n"
    "Формы в статусах «На согласовании», «На согласовании потребителя», "
    "«На рассмотрении поставщика», «На проверке оформления», «Требуется корректировка», "
    "«Спор / требуется решение», «Определение причин и разработка КМ», "
    "«Согласование КМ», «Исполнение КМ».\n"
    "Факт:\n"
    "Из них без просрочки. Срок — дата документа + 2 рабочих дня + 30 календарных дней. "
    "Статус «Исполнение КМ» всегда без просрочки.\n"
    "Просрочено = план − факт.\n"
    "В месяц входят формы, у которых дата документа + 2 рабочих дня попадает в этот месяц "
    "и статус всё ещё из списка «в работе». Для текущего месяца срок смотрится на сегодня."
)


def _base_name(name: str) -> str:
    text = (name or "").strip()
    for suffix in (", всего", ", в работе"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return text


class Command(BaseCommand):
    help = "Плитки qualdir: формы 03-17/03-18/03-19 — «всего» и «в работе»"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        sources = {
            kpi_id: KpiDefinition.objects.filter(department=DEPARTMENT, kpi_id=kpi_id).first()
            for kpi_id, _work_id in WORK_TILES
        }
        missing = [kpi_id for kpi_id, row in sources.items() if row is None]
        if missing:
            self.stderr.write(self.style.ERROR(f"Не найдены плитки: {', '.join(missing)}"))
            return

        if dry_run:
            for source_id, work_id in WORK_TILES:
                base = _base_name(sources[source_id].name)
                self.stdout.write(f"  [UPDATE] {source_id} — {base}, всего")
                self.stdout.write(f"  [UPSERT] {work_id} — {base}, в работе")
            self.stdout.write(self.style.WARNING("[DRY-RUN] записи в БД не менялись"))
            return

        for source_id, work_id in WORK_TILES:
            source = sources[source_id]
            base = _base_name(source.name)
            source.name = f"{base}, всего"
            source.formula = TOTAL_FORMULA
            source.description = TOTAL_DESCRIPTION
            source.unit = source.unit or "шт."
            source.save(update_fields=["name", "formula", "description", "unit"])

            defaults = {
                "name": f"{base}, в работе",
                "block": source.block,
                "frequency": source.frequency,
                "perspective": source.perspective,
                "goal": source.goal,
                "formula": WORK_FORMULA,
                "unit": "шт.",
                "source": source.source,
                "description": WORK_DESCRIPTION,
                "monthly_target": source.monthly_target,
                "quarterly_target": source.quarterly_target,
                "yearly_target": source.yearly_target,
                "green_threshold": source.green_threshold,
                "yellow_threshold": source.yellow_threshold,
                "red_threshold": source.red_threshold,
                "weight_pct": source.weight_pct,
                "chart_type": source.chart_type,
                "chart_type_label": source.chart_type_label,
                "position": 0,
            }
            _, created = KpiDefinition.objects.update_or_create(
                department=DEPARTMENT,
                kpi_id=work_id,
                defaults=defaults,
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"  {'Создано' if created else 'Обновлено'}: {work_id} — {defaults['name']}"
                )
            )
            self.stdout.write(self.style.SUCCESS(f"  Обновлено: {source_id} — {source.name}"))

        position_by_id = {kpi_id: index + 1 for index, kpi_id in enumerate(TILE_ORDER)}
        for row in KpiDefinition.objects.filter(department=DEPARTMENT):
            position = position_by_id.get(row.kpi_id)
            if position is None or row.position == position:
                continue
            row.position = position
            row.save(update_fields=["position"])
        bump_kpi_definitions_cache_version()
        self.stdout.write(self.style.SUCCESS("Порядок плиток обновлён, кэш справочника сброшен"))
