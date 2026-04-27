"""
Синхронизация KPI операционного директора.

Использование:
    py manage.py import_operational_director_kpi
    py manage.py import_operational_director_kpi --dry-run
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from getkpi.models import KpiDefinition

DEPARTMENT = "Операционный директор"

OD_KPI_DEFINITIONS: list[dict] = [
    {
        "kpi_id": "OD-M1",
        "name": "Выручка (без НДС) - выполнение плана",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Обеспечить выполнение БДР по выручке как базу для прибыли и ликвидности",
        "formula": "Фактическая выручка (без НДС) / План выручки периода × 100%",
        "unit": "%",
        "source": "БДР / управленческая отчётность / 1С",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "Не менее 2 414 157 тыс. руб.",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 30.0,
        "position": 0,
    },
    {
        "kpi_id": "OD-M2",
        "name": "Чистая прибыль - выполнение плана",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Обеспечить итоговый финансовый результат компании",
        "formula": "Фактическая чистая прибыль / План чистой прибыли периода × 100%",
        "unit": "%",
        "source": "БДР / управленческая отчётность / 1С",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "Не менее 333 683 848 руб.",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 30.0,
        "position": 1,
    },
    {
        "kpi_id": "OD-M3.1",
        "name": "Бюджет управленческого контура в пределах лимита",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Привязать роль к контролю затрат управленческого контура",
        "formula": "Факт затрат управленческого контура / План затрат управленческого контура × 100%",
        "unit": "руб.",
        "source": "БДР / 1С / управленческая отчётность",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "<100%",
        "yellow_threshold": "100–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
        "position": 2,
    },
    {
        "kpi_id": "OD-M3.2",
        "name": "ФОТ операционного директора в пределах лимита",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать ФОТ подразделений в периметре операционного директора",
        "formula": "Дебетовый оборот счёта 26 по статьям ФОТ / План ФОТ × 100%",
        "unit": "руб.",
        "source": "1С / РегистрБухгалтерии.Хозрасчетный / счёт 26",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "<100%",
        "yellow_threshold": "100–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
        "position": 3,
    },
    {
        "kpi_id": "OD-Q1",
        "name": "Выполнение ключевых инициатив и проектов 2026",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Развитие",
        "goal": "Удерживать не только финансовый результат, но и темп развития системы управления",
        "formula": "KPI = (План - Факт) / План × 100%, где план — количество проектов с data_1c, факт — количество проектов с просроченными вехами",
        "unit": "%",
        "source": "Реестр проектов / КПД / PMO",
        "monthly_target": "≥90%",
        "quarterly_target": None,
        "yearly_target": "≥90% годового плана инициатив",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 15.0,
        "position": 4,
    },
    {
        "kpi_id": "OD-Q2",
        "name": "Текучесть персонала компании",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Персонал",
        "goal": "Снизить потери производительности и затрат на замещение персонала",
        "formula": "Факт / План × 100%, где план и факт возвращаются по месяцам в чел.",
        "unit": "чел.",
        "source": "HR-учёт / 1С ЗУП",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
        "position": 5,
    },
]


class Command(BaseCommand):
    help = "Создать/обновить KPI операционного директора и удалить устаревшие записи."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать изменения без записи в базу.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        desired_ids = {item["kpi_id"] for item in OD_KPI_DEFINITIONS}
        existing_qs = KpiDefinition.objects.filter(department__iexact=DEPARTMENT)
        existing = list(existing_qs.order_by("position", "kpi_id"))
        stale = [item for item in existing if item.kpi_id not in desired_ids]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN: изменения не будут сохранены."))
            self.stdout.write(f"Найдено KPI в базе для '{DEPARTMENT}': {len(existing)}")
            self.stdout.write(f"Будет удалено устаревших KPI: {len(stale)}")
            for item in stale:
                self.stdout.write(f"  DELETE {item.kpi_id} — {item.name}")
            for item in OD_KPI_DEFINITIONS:
                self.stdout.write(f"  UPSERT {item['kpi_id']} — {item['name']}")
            return

        with transaction.atomic():
            if stale:
                KpiDefinition.objects.filter(id__in=[item.id for item in stale]).delete()

            for item in OD_KPI_DEFINITIONS:
                payload = dict(item)
                payload["department"] = DEPARTMENT
                kpi_id = payload.pop("kpi_id")
                KpiDefinition.objects.update_or_create(
                    department=DEPARTMENT,
                    kpi_id=kpi_id,
                    defaults=payload,
                )

        self.stdout.write(self.style.SUCCESS(f"KPI операционного директора синхронизированы: {len(OD_KPI_DEFINITIONS)} шт."))
        if stale:
            for item in stale:
                self.stdout.write(f"Удалён устаревший KPI: {item.kpi_id} — {item.name}")
