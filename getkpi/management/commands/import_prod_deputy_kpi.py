"""
Синхронизация KPI заместителя операционного директора-директора по производству.

Использование:
    py manage.py import_prod_deputy_kpi
    py manage.py import_prod_deputy_kpi --dry-run
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from getkpi.models import KpiDefinition

DEPARTMENT = "Заместитель операционного директора-директор по производству"

PD_KPI_DEFINITIONS: list[dict] = [
    {
        "kpi_id": "PD-M1",
        "name": "Выполнение производственного плана",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить ритмичное выполнение производственной программы",
        "formula": "Фактический выпуск / План выпуска × 100%",
        "unit": "%",
        "source": "ERP / 1С / производственный учет",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 25.0,
        "position": 0,
    },
    {
        "kpi_id": "PD-M2",
        "name": "OTIF / выполнение плана отгрузок",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить своевременную и полную отгрузку производственной программы",
        "formula": "OTIF по количеству; план / факт отгрузок показываются за месяц в шт.",
        "unit": "шт.",
        "source": "1С ERP / контрольные даты исполнения договора / регистр распоряжений на отгрузку",
        "monthly_target": "≥90%",
        "quarterly_target": None,
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 20.0,
        "position": 1,
    },
    {
        "kpi_id": "PD-M3.1",
        "name": "Бюджет в пределах лимита",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать бюджет производственного блока",
        "formula": "Факт затрат / План затрат × 100%",
        "unit": "руб.",
        "source": "БДР / 1С / управленческая отчетность",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
        "position": 2,
    },
    {
        "kpi_id": "PD-M3.2",
        "name": "ФОТ блока в пределах лимита",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать ФОТ производственного блока",
        "formula": "Факт ФОТ / План ФОТ × 100%",
        "unit": "руб.",
        "source": "1С / ЗУП / HRIS",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
        "position": 3,
    },
    {
        "kpi_id": "PD-Q1",
        "name": "Выполнение проектов улучшений / сокращения потерь",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Развитие",
        "goal": "Ускорять производственные улучшения и сокращение потерь",
        "formula": "Проекты без просроченных вех / Активные проекты месяца × 100%",
        "unit": "%",
        "source": "TurboProject / проекты Производственный цех №1 и Производственный цех №2",
        "monthly_target": "≥90%",
        "quarterly_target": "≥90%",
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 20.0,
        "position": 4,
    },
    {
        "kpi_id": "PD-Q2",
        "name": "Текучесть персонала производства",
        "block": "плитка",
        "frequency": "ежеквартально",
        "perspective": "Персонал",
        "goal": "Снизить текучесть в производственном контуре",
        "formula": "Факт / План × 100%",
        "unit": "чел.",
        "source": "HR-учет / 1С ЗУП",
        "monthly_target": None,
        "quarterly_target": "≤100%",
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 15.0,
        "position": 5,
    },
]


class Command(BaseCommand):
    help = "Создать/обновить KPI заместителя операционного директора-директора по производству."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать изменения без записи в базу.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        desired_ids = {item["kpi_id"] for item in PD_KPI_DEFINITIONS}
        existing_qs = KpiDefinition.objects.filter(department__iexact=DEPARTMENT)
        existing = list(existing_qs.order_by("position", "kpi_id"))
        stale = [item for item in existing if item.kpi_id not in desired_ids]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN: изменения не будут сохранены."))
            self.stdout.write(f"Найдено KPI в базе для '{DEPARTMENT}': {len(existing)}")
            self.stdout.write(f"Будет удалено устаревших KPI: {len(stale)}")
            for item in stale:
                self.stdout.write(f"  DELETE {item.kpi_id} — {item.name}")
            for item in PD_KPI_DEFINITIONS:
                self.stdout.write(f"  UPSERT {item['kpi_id']} — {item['name']}")
            return

        with transaction.atomic():
            if stale:
                KpiDefinition.objects.filter(id__in=[item.id for item in stale]).delete()

            for item in PD_KPI_DEFINITIONS:
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
                f"KPI заместителя операционного директора-директора по производству синхронизированы: {len(PD_KPI_DEFINITIONS)} шт."
            )
        )
        if stale:
            for item in stale:
                self.stdout.write(f"Удалён устаревший KPI: {item.kpi_id} — {item.name}")
