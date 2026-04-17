"""
Импорт KPI блока «Председатель совета директоров / коммерция» (плитки MRK-01 … MRK-10).

Использование:
    py manage.py import_chairman_commerce_kpi
    py manage.py import_chairman_commerce_kpi --dry-run
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

DEPARTMENT = "Председатель совета директоров"

# Плитки по макету дашборда коммерции (позиции 200+ — после FND в общем списке при сортировке по position).
COMMERCE_TILES: list[dict] = [
    {
        "kpi_id": "MRK-01",
        "name": "Общий план отгрузок 2026",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "База плана 2026 • целевая выручка по отгрузке",
        "formula": "План отгрузок 2026 (агрегат)",
        "unit": "млн руб.",
        "source": "План продаж / коммерция",
        "green_threshold": "—",
        "yellow_threshold": "—",
        "red_threshold": "—",
        "weight_pct": 10.0,
        "position": 200,
    },
    {
        "kpi_id": "MRK-02",
        "name": "Общий план договоров 2026",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Контрактная база • верхняя часть воронки продаж",
        "formula": "План договоров 2026 (агрегат)",
        "unit": "млн руб.",
        "source": "CRM / коммерция",
        "green_threshold": "—",
        "yellow_threshold": "—",
        "red_threshold": "—",
        "weight_pct": 10.0,
        "position": 201,
    },
    {
        "kpi_id": "MRK-03",
        "name": "Общий план денег 2026",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Поступление денежных средств • денежный контур",
        "formula": "План ДС 2026 (агрегат)",
        "unit": "млн руб.",
        "source": "БДДС / коммерция",
        "green_threshold": "—",
        "yellow_threshold": "—",
        "red_threshold": "—",
        "weight_pct": 10.0,
        "position": 202,
    },
    {
        "kpi_id": "MRK-04",
        "name": "Рост отгрузок 2026 к 2025",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "План агрессивный • требует ресурсов и ритмики исполнения",
        "formula": "(Отгрузки 2026 / Отгрузки 2025 − 1) × 100%",
        "unit": "%",
        "source": "Управленческая отчётность",
        "green_threshold": "≥ целевого роста",
        "yellow_threshold": "—",
        "red_threshold": "< целевого роста",
        "weight_pct": 10.0,
        "position": 203,
    },
    {
        "kpi_id": "MRK-05",
        "name": "Кассовый разрыв",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Деньги − отгрузки • отрицательный разрыв = потребность в финансировании",
        "formula": "План ДС − План отгрузок (млн руб.)",
        "unit": "млн руб.",
        "source": "Планирование / коммерция",
        "green_threshold": "≥ 0",
        "yellow_threshold": "—",
        "red_threshold": "< 0",
        "weight_pct": 10.0,
        "position": 204,
    },
    {
        "kpi_id": "MRK-06",
        "name": "Доля Газпром + БМИ в отгрузке",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Норма ≤ 70% • концентрация портфеля выше целевого уровня",
        "formula": "Доля отгрузок (Газпром + БМИ) / Все отгрузки × 100%",
        "unit": "%",
        "source": "1С / коммерция",
        "green_threshold": "≤70%",
        "yellow_threshold": "70,1–75%",
        "red_threshold": ">75%",
        "weight_pct": 10.0,
        "position": 205,
    },
    {
        "kpi_id": "MRK-07",
        "name": "Конверсия договоров в отгрузки",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Норма ≥ 95% • отставание до порога",
        "formula": "Отгрузки / Договоры × 100%",
        "unit": "%",
        "source": "1С / CRM",
        "green_threshold": "≥95%",
        "yellow_threshold": "90–94,9%",
        "red_threshold": "<90%",
        "weight_pct": 10.0,
        "position": 206,
    },
    {
        "kpi_id": "MRK-08",
        "name": "Конверсия отгрузок в деньги",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Норма ≥ 95% • денежная дисциплина в среднем по портфелю",
        "formula": "ДС / Отгрузки × 100%",
        "unit": "%",
        "source": "1С / БДДС",
        "green_threshold": "≥95%",
        "yellow_threshold": "90–94,9%",
        "red_threshold": "<90%",
        "weight_pct": 10.0,
        "position": 207,
    },
    {
        "kpi_id": "MRK-09",
        "name": "% выигранных тендеров БМИ",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "Цель ≥ 25% • проигрыши по цене и техслужбам",
        "formula": "Выигранные тендеры БМИ / Участия БМИ × 100%",
        "unit": "%",
        "source": "CRM / тендерный отдел",
        "green_threshold": "≥25%",
        "yellow_threshold": "15–24,9%",
        "red_threshold": "<15%",
        "weight_pct": 10.0,
        "position": 208,
    },
    {
        "kpi_id": "MRK-10",
        "name": "Красные инициативы / риски",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Коммерция",
        "goal": "БМИ, ВЭД, клиентская концентрация, декабрьский пик отгрузки",
        "formula": "Количество инициатив со статусом «красный»",
        "unit": "шт.",
        "source": "Реестр рисков / PMO",
        "green_threshold": "0–1",
        "yellow_threshold": "2–3",
        "red_threshold": ">3",
        "weight_pct": 10.0,
        "position": 209,
    },
]

FIELDS = (
    "name", "block", "frequency", "perspective", "goal",
    "formula", "unit", "source",
    "monthly_target", "quarterly_target", "yearly_target",
    "green_threshold", "yellow_threshold", "red_threshold",
    "weight_pct", "chart_type", "chart_type_label", "position",
)


class Command(BaseCommand):
    help = "Импорт плиток MRK (блок «Председатель / коммерция»)"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Без записи в БД")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        created = updated = 0
        self.stdout.write(
            f"\n{'[DRY-RUN] ' if dry_run else ''}=== {DEPARTMENT} / коммерция ({len(COMMERCE_TILES)} плиток) ==="
        )

        for kpi in COMMERCE_TILES:
            defaults = {}
            for f in FIELDS:
                val = kpi.get(f)
                if val is not None:
                    defaults[f] = val

            if dry_run:
                exists = KpiDefinition.objects.filter(
                    department=DEPARTMENT, kpi_id=kpi["kpi_id"],
                ).exists()
                self.stdout.write(f"  [{'UPDATE' if exists else 'CREATE'}] {kpi['kpi_id']} — {kpi['name']}")
                updated += exists
                created += not exists
            else:
                _, is_new = KpiDefinition.objects.update_or_create(
                    department=DEPARTMENT,
                    kpi_id=kpi["kpi_id"],
                    defaults=defaults,
                )
                if is_new:
                    created += 1
                else:
                    updated += 1

        msg = f"Итого: {created + updated} ({created} новых, {updated} обновлено)"
        self.stdout.write(self.style.SUCCESS(f"\n{msg}") if not dry_run else self.style.WARNING(f"\n[DRY-RUN] {msg}"))
