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
        "kpi_id": "PD-M1.1.M",
        "name": "Выполнение производственного плана: месяц (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить выполнение производственной программы Турбулентность-Дон за последний месяц",
        "formula": "Фактический выпуск / План выпуска × 100%",
        "unit": "руб.",
        "source": "MSSQL erp_pm: Document_ТД_ПроизводственныйПлан (_Document185292) / ПланРубМесяц и ФактРубМесяц",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 4.17,
        "position": 1,
    },
    {
        "kpi_id": "PD-M1.1.W",
        "name": "Выполнение производственного плана: неделя (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "еженедельно",
        "perspective": "Процессы",
        "goal": "Обеспечить выполнение производственной программы Турбулентность-Дон за последнюю неделю",
        "formula": "Фактический выпуск / Недельный план выпуска × 100%",
        "unit": "руб.",
        "source": "MSSQL erp_pm: Document_ТД_ПроизводственныйПлан (_Document185292) / ПланРуб и ФактРуб",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 4.17,
        "position": 0,
    },
    {
        "kpi_id": "PD-M1.1.T",
        "name": "Выполнение производственного плана: итого (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить выполнение производственной программы Турбулентность-Дон итого за выбранный месяц",
        "formula": "Факт итого за выбранный месяц / План итого за выбранный месяц × 100%",
        "unit": "руб.",
        "source": "MSSQL erp_pm: Document_ТД_ПроизводственныйПлан (_Document185292) / ПланРубИтого и ФактРубИтого",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 4.16,
        "position": 2,
    },
    {
        "kpi_id": "PD-M1.2.M",
        "name": "Выполнение производственного плана: месяц (Алмаз)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить выполнение производственной программы Алмаза за последний месяц",
        "formula": "Фактический выпуск / План выпуска × 100%",
        "unit": "шт.",
        "source": "MSSQL erp_pm: Document_ТД_ПроизводственныйПлан (_Document185292) / ПланШтМесяц и ФактШтМесяц",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 4.17,
        "position": 4,
    },
    {
        "kpi_id": "PD-M1.2.W",
        "name": "Выполнение производственного плана: неделя (Алмаз)",
        "block": "плитка",
        "frequency": "еженедельно",
        "perspective": "Процессы",
        "goal": "Обеспечить выполнение производственной программы Алмаза за последнюю неделю",
        "formula": "Фактический выпуск / Недельный план выпуска × 100%",
        "unit": "шт.",
        "source": "MSSQL erp_pm: Document_ТД_ПроизводственныйПлан (_Document185292) / ПланШт и ФактШт",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 4.17,
        "position": 3,
    },
    {
        "kpi_id": "PD-M1.2.T",
        "name": "Выполнение производственного плана: итого (Алмаз)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить выполнение производственной программы Алмаза итого за выбранный месяц",
        "formula": "Факт итого за выбранный месяц / План итого за выбранный месяц × 100%",
        "unit": "шт.",
        "source": "MSSQL erp_pm: Document_ТД_ПроизводственныйПлан (_Document185292) / ПланШтИтого и ФактШтИтого",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 4.16,
        "position": 5,
    },
    {
        "kpi_id": "PD-M2.1",
        "name": "OTIF / выполнение плана отгрузок (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить своевременную и полную отгрузку производственной программы Турбулентность-Дон",
        "formula": "OTIF по количеству; план / факт отгрузок показываются за месяц в шт.",
        "unit": "шт.",
        "source": "MSSQL erp_pm: _InfoRg127734 / _AccumRg169757 / _Document704 (НПО+ООО)",
        "monthly_target": "≥90%",
        "quarterly_target": None,
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 10.0,
        "position": 6,
    },
    {
        "kpi_id": "PD-M2.2",
        "name": "OTIF / выполнение плана отгрузок (Алмаз)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Процессы",
        "goal": "Обеспечить своевременную и полную отгрузку производственной программы Алмаза",
        "formula": "OTIF по количеству; план / факт отгрузок показываются за месяц в шт.",
        "unit": "шт.",
        "source": "MSSQL erp_pm: _InfoRg127734 / _AccumRg169757 / _Document704 (Алмаз)",
        "monthly_target": "≥90%",
        "quarterly_target": None,
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 10.0,
        "position": 6,
    },
    {
        "kpi_id": "PD-M3.B1",
        "name": "Бюджет (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать бюджет Турбулентность-Дон",
        "formula": "Факт затрат / План затрат × 100%",
        "unit": "руб.",
        "source": "БДР / 1С / управленческая отчетность (Турбулентность-Дон)",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 5.0,
        "position": 7,
    },
    {
        "kpi_id": "PD-M3.B2",
        "name": "Бюджет (Алмаз)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать бюджет Алмаза",
        "formula": "Факт затрат / План затрат × 100%",
        "unit": "руб.",
        "source": "БДР / 1С / управленческая отчетность (Алмаз)",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 5.0,
        "position": 8,
    },
    {
        "kpi_id": "PD-M3.F1",
        "name": "ФОТ (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать ФОТ Турбулентность-Дон",
        "formula": "Факт ФОТ / План ФОТ × 100%",
        "unit": "руб.",
        "source": "1С / ЗУП / HRIS (Турбулентность-Дон)",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 5.0,
        "position": 9,
    },
    {
        "kpi_id": "PD-M3.F2",
        "name": "ФОТ (Алмаз)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Финансы",
        "goal": "Контролировать ФОТ Алмаза",
        "formula": "Факт ФОТ / План ФОТ × 100%",
        "unit": "руб.",
        "source": "1С / ЗУП / HRIS (Алмаз)",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 5.0,
        "position": 10,
    },
    {
        "kpi_id": "PD-Q1",
        "name": "Доля проектов/заказов «в срок»",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Развитие",
        "goal": "Контролировать долю проектов и заказов без просроченных вех",
        "formula": "Проекты без просроченных вех / Активные проекты месяца × 100%",
        "unit": "%",
        "source": "TurboProject / проекты Турбулентность-Дон и Алмаза",
        "monthly_target": "≥90%",
        "quarterly_target": "≥90%",
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 20.0,
        "position": 11,
    },
    {
        "kpi_id": "PD-Q3",
        "name": "Выполнение проектов улучшений / сокращение потерь",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Развитие",
        "goal": "Контролировать выполнение проектов развития и улучшений",
        "formula": "Проекты в работе / (проекты в работе + планируются + без статуса) × 100%",
        "unit": "%",
        "source": "TurboProject / проекты типа РазвитияИУлучшений, где Целищев или Ермаков — куратор или РП",
        "monthly_target": "≥90%",
        "quarterly_target": "≥90%",
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 10.0,
        "position": 12,
    },
    {
        "kpi_id": "PD-Q2.1",
        "name": "Текучесть персонала (Турбулентность-Дон)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Персонал",
        "goal": "Снизить текучесть персонала в Турбулентность-Дон",
        "formula": "Факт / План × 100%",
        "unit": "чел.",
        "source": "1С / Document_ТД_ТекучестьПерсонала (подразделения Турбулентность-Дон)",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 7.5,
        "position": 13,
    },
    {
        "kpi_id": "PD-Q2.2",
        "name": "Текучесть персонала (Алмаз)",
        "block": "плитка",
        "frequency": "ежемесячно",
        "perspective": "Персонал",
        "goal": "Снизить текучесть персонала в Алмазе",
        "formula": "Факт / План × 100%",
        "unit": "чел.",
        "source": "1С / Document_ТД_ТекучестьПерсонала (подразделения Алмаза)",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100%",
        "yellow_threshold": "100,1–110%",
        "red_threshold": ">110%",
        "weight_pct": 7.5,
        "position": 14,
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
