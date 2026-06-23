"""
Добавляет плитки QD-M9 (НПО) и QD-M10 (АЛМАЗ) на основе QD-M6.

Использование:
    py manage.py import_qualdir_qd_m9_m10
    py manage.py import_qualdir_qd_m9_m10 --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

DEPARTMENT = "Заместитель тех. директора по качеству"
SOURCE_KPI_ID = "QD-M6"

NEW_KPIS = (
    {
        "kpi_id": "QD-M9",
        "name": "Предъявление продукции по входному контролю (НПО)",
    },
    {
        "kpi_id": "QD-M10",
        "name": "Предъявление продукции по входному контролю (АЛМАЗ)",
    },
)

COPY_FIELDS = (
    "block",
    "frequency",
    "perspective",
    "goal",
    "formula",
    "unit",
    "source",
    "description",
    "monthly_target",
    "quarterly_target",
    "yearly_target",
    "green_threshold",
    "yellow_threshold",
    "red_threshold",
    "weight_pct",
    "chart_type",
    "chart_type_label",
    "position",
)


class Command(BaseCommand):
    help = "Создать QD-M9 и QD-M10 как копии QD-M6 (qualdir)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Только показать, что будет сделано",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        source = KpiDefinition.objects.filter(
            department=DEPARTMENT,
            kpi_id=SOURCE_KPI_ID,
        ).first()
        if source is None:
            self.stderr.write(
                self.style.ERROR(f"Не найден {SOURCE_KPI_ID} в «{DEPARTMENT}»"),
            )
            return

        created = updated = 0
        for item in NEW_KPIS:
            defaults = {field: getattr(source, field) for field in COPY_FIELDS}
            defaults["name"] = item["name"]

            if dry_run:
                exists = KpiDefinition.objects.filter(
                    department=DEPARTMENT,
                    kpi_id=item["kpi_id"],
                ).exists()
                tag = "UPDATE" if exists else "CREATE"
                self.stdout.write(
                    f"  [{tag}] {DEPARTMENT} / {item['kpi_id']} — {item['name']}",
                )
                if exists:
                    updated += 1
                else:
                    created += 1
                continue

            _, is_new = KpiDefinition.objects.update_or_create(
                department=DEPARTMENT,
                kpi_id=item["kpi_id"],
                defaults=defaults,
            )
            if is_new:
                created += 1
            else:
                updated += 1
            self.stdout.write(
                self.style.SUCCESS(
                    f"  {'Создано' if is_new else 'Обновлено'}: {item['kpi_id']} — {item['name']}",
                ),
            )

        total = created + updated
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"\n[DRY-RUN] Итого: {total} ({created} новых, {updated} обновлённых)",
                ),
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nИтого: {total} ({created} новых, {updated} обновлённых)",
                ),
            )
