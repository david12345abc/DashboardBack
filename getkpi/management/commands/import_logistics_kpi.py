"""
Синхронизация KPI начальника службы логистики.

Использование:
    py manage.py import_logistics_kpi
    py manage.py import_logistics_kpi --dry-run
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from getkpi.models import KpiDefinition


DEPARTMENT = "Начальник службы логистики"

LOGISTICS_KPI_DEFINITIONS: list[dict] = [
    {
        "kpi_id": "LOG-M1",
        "name": "Обеспечение поставок ТМЦ в срок",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить своевременные поставки ТМЦ для производства и внутренних заказчиков",
        "formula": "Поставки ТМЦ в срок / Все поставки ТМЦ периода × 100%",
        "unit": "%",
        "source": "1С ERP / закупки и склад / паспорта KPI",
        "monthly_target": "≥95%",
        "quarterly_target": None,
        "yearly_target": "≥95%",
        "green_threshold": "≥95%",
        "yellow_threshold": "90–94,9%",
        "red_threshold": "<90%",
        "weight_pct": 20.0,
        "position": 0,
    },
    {
        "kpi_id": "LOG-M2",
        "name": "Отклонение закупочной цены от проектной / нормативной",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать закупочные цены относительно проектной или нормативной базы",
        "formula": "(Фактическая цена закупки − Проектная/нормативная цена) / Проектная/нормативная цена × 100%",
        "unit": "руб.",
        "source": "1С ERP / закупки / паспорта KPI",
        "monthly_target": "≤5%",
        "quarterly_target": None,
        "yearly_target": "≤5%",
        "green_threshold": "≤5%",
        "yellow_threshold": "5,1–10%",
        "red_threshold": ">10%",
        "weight_pct": 15.0,
        "position": 1,
    },
    {
        "kpi_id": "LOG-M3.B",
        "name": "Бюджет блока в пределах лимита",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Удерживать бюджет службы логистики в утвержденном лимите",
        "formula": "Факт затрат / Утвержденный лимит бюджета × 100%",
        "unit": "руб.",
        "source": "1С ERP / БДР / паспорта KPI",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 7.5,
        "position": 2,
    },
    {
        "kpi_id": "LOG-M3.F",
        "name": "ФОТ блока в пределах лимита",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Удерживать ФОТ службы логистики в утвержденном лимите",
        "formula": "Факт ФОТ / Утвержденный лимит ФОТ × 100%",
        "unit": "руб.",
        "source": "1С ERP / ФОТ / паспорта KPI",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 7.5,
        "position": 3,
    },
    {
        "kpi_id": "LOG-Q1",
        "name": "Доля квалифицированных поставщиков",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Поставщики",
        "goal": "Повышать долю квалифицированных поставщиков в базе службы логистики",
        "formula": "Поставщики с суммой баллов оценки > 45 / Все поставщики из оценки периода × 100%",
        "unit": "%",
        "source": "1С ERP / Регистр сведений ТД_ОценкаПоставщиков",
        "monthly_target": "≥80%",
        "quarterly_target": None,
        "yearly_target": "≥80%",
        "green_threshold": "≥80%",
        "yellow_threshold": "70–79,9%",
        "red_threshold": "<70%",
        "weight_pct": 15.0,
        "position": 4,
    },
    {
        "kpi_id": "LOG-Q2",
        "name": "Текучесть персонала логистики",
        "block": "плитка",
        "frequency": "ежеквартально",
        "perspective": "Персонал",
        "goal": "Снижать текучесть персонала службы логистики",
        "formula": "Уволившиеся сотрудники / Среднесписочная численность × 100%",
        "unit": "%",
        "source": "1С / HR / паспорта KPI",
        "monthly_target": None,
        "quarterly_target": "≤5%",
        "yearly_target": "≤5%",
        "green_threshold": "≤5%",
        "yellow_threshold": "5,1–7%",
        "red_threshold": ">7%",
        "weight_pct": 15.0,
        "position": 5,
    },
    {
        "kpi_id": "LOG-M4",
        "name": "RAG по KPI / паспортам",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Управление",
        "goal": "Обеспечить актуальность паспортов KPI и отсутствие красных статусов",
        "formula": "Количество KPI в зеленой зоне / Все KPI службы логистики × 100%",
        "unit": "%",
        "source": "паспорта KPI",
        "monthly_target": "≥95%",
        "quarterly_target": None,
        "yearly_target": "≥95%",
        "green_threshold": "≥95%",
        "yellow_threshold": "80–94,9%",
        "red_threshold": "<80%",
        "weight_pct": 20.0,
        "position": 6,
    },
]


class Command(BaseCommand):
    help = "Создать/обновить KPI начальника службы логистики."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать изменения без записи в базу.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        desired_ids = {item["kpi_id"] for item in LOGISTICS_KPI_DEFINITIONS}
        existing_qs = KpiDefinition.objects.filter(department__iexact=DEPARTMENT)
        existing = list(existing_qs.order_by("position", "kpi_id"))
        stale = [item for item in existing if item.kpi_id not in desired_ids]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN: изменения не будут сохранены."))
            self.stdout.write(f"Найдено KPI в базе для '{DEPARTMENT}': {len(existing)}")
            self.stdout.write(f"Будет удалено устаревших KPI: {len(stale)}")
            for item in stale:
                self.stdout.write(f"  DELETE {item.kpi_id} — {item.name}")
            for item in LOGISTICS_KPI_DEFINITIONS:
                self.stdout.write(f"  UPSERT {item['kpi_id']} — {item['name']}")
            return

        with transaction.atomic():
            if stale:
                KpiDefinition.objects.filter(id__in=[item.id for item in stale]).delete()

            for item in LOGISTICS_KPI_DEFINITIONS:
                payload = dict(item)
                payload["department"] = DEPARTMENT
                kpi_id = payload.pop("kpi_id")
                KpiDefinition.objects.update_or_create(
                    department=DEPARTMENT,
                    kpi_id=kpi_id,
                    defaults=payload,
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"KPI начальника службы логистики синхронизированы: {len(LOGISTICS_KPI_DEFINITIONS)} шт."
            )
        )
        if stale:
            for item in stale:
                self.stdout.write(f"Удалён устаревший KPI: {item.kpi_id} — {item.name}")
