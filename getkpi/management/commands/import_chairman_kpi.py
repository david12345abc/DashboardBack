"""
Импорт KPI для «Председатель совета директоров» (дашборд учредителя).

Данные берутся из листа Методика_дашборд_учредителя файла
KPI_подразделений_и_методики_руководителей_v10_полный_комплект_дашбордов.xlsx.

Использование:
    py manage.py import_chairman_kpi           # создать/обновить записи
    py manage.py import_chairman_kpi --dry-run  # только показать, что будет сделано
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

DEPARTMENT = "Председатель совета директоров"

FULL_DASHBOARD: list[dict] = [
    # ── Плитки KPI (8 шт.) ──────────────────────────────────────────────────
    {
        "kpi_id": "FND-T1",
        "name": "Выручка (без НДС) — план/факт",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Видеть исполнение финансового плана верхнего уровня",
        "formula": "Фактическая выручка без НДС / План выручки периода × 100%",
        "unit": "%",
        "source": "БДР / управленческая отчётность / 1С",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 15.0,
    },
    {
        "kpi_id": "FND-T2",
        "name": "EBITDA — план/факт",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Видеть качество заработка, а не только объём выручки",
        "formula": "Фактическая EBITDA / План EBITDA периода × 100%",
        "unit": "%",
        "source": "БДР / управленческая отчётность",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "90–99,9%",
        "red_threshold": "<90%",
        "weight_pct": 15.0,
    },
    {
        "kpi_id": "FND-T3",
        "name": "ДДС 8 недель / отсутствие кассового разрыва",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Раннее выявление угрозы ликвидности на горизонте 1–2 месяцев",
        "formula": "Минимальный прогнозный остаток ДС на горизонте 8 недель / Минимально допустимый остаток ДС × 100%",
        "unit": "%",
        "source": "Казначейство / платёжный календарь / БДДС",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100% и нет отрицательных недель",
        "yellow_threshold": "90–99,9%",
        "red_threshold": "<90% или есть отрицательная неделя",
        "weight_pct": 10.0,
    },
    {
        "kpi_id": "FND-T4",
        "name": "OTIF отгрузки",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Процессы",
        "goal": "Понимать, насколько компания исполняет обещания клиентам по сроку и комплектности",
        "formula": "Количество отгрузок, выполненных в срок и в полном объёме / Общее количество отгрузок периода × 100%",
        "unit": "%",
        "source": "1С ERP / логистика / склад / коммерция",
        "monthly_target": "≥95%",
        "quarterly_target": None,
        "yearly_target": "≥95%",
        "green_threshold": "≥95%",
        "yellow_threshold": "90–94,9%",
        "red_threshold": "<90%",
        "weight_pct": 10.0,
    },
    {
        "kpi_id": "FND-T5",
        "name": "Качество: К1–К2 рекламации и просроченные CAPA",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Процессы",
        "goal": "Раннее выявление риска потери клиента и системных проблем качества",
        "formula": "0,5 × KPI(К1–К2 рекламации в лимите) + 0,5 × KPI(Просроченные CAPA в лимите)",
        "unit": "%",
        "source": "QMS / журнал рекламаций / CAPA / ОТК",
        "monthly_target": "≥95%",
        "quarterly_target": None,
        "yearly_target": "≥95%",
        "green_threshold": "≥95%",
        "yellow_threshold": "85–94,9%",
        "red_threshold": "<85%",
        "weight_pct": 10.0,
    },
    {
        "kpi_id": "FND-T6",
        "name": "Портфель проектов RAG",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Развитие",
        "goal": "Видеть состояние стратегических и обязательных проектов без погружения в детали",
        "formula": "Главный статус = худший статус среди ключевых проектов; количество Green / Yellow / Red и stage-gates overdue",
        "unit": "шт. / статус",
        "source": "PMO / MS Project / реестр КПД",
        "monthly_target": "0 red-проектов",
        "quarterly_target": None,
        "yearly_target": "0 red-проектов",
        "green_threshold": "0 red-проектов и stage-gates overdue = 0",
        "yellow_threshold": "1–2 red-проекта или stage-gates overdue ≤2",
        "red_threshold": ">2 red-проектов или stage-gates overdue >2",
        "weight_pct": 10.0,
    },
    {
        "kpi_id": "FND-T7",
        "name": "Дебиторская задолженность 60+",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Видеть риск невозврата денег и концентрацию проблемного портфеля",
        "formula": "Сумма ДЗ старше 60 дней / Утверждённый лимит ДЗ 60+ × 100%",
        "unit": "% к лимиту",
        "source": "1С / реестр ДЗ / финансовая отчётность",
        "monthly_target": "≤100%",
        "quarterly_target": None,
        "yearly_target": "≤100%",
        "green_threshold": "≤100% лимита",
        "yellow_threshold": "100,1–120% лимита",
        "red_threshold": ">120% лимита",
        "weight_pct": 10.0,
    },
    {
        "kpi_id": "FND-T9",
        "name": "Выпуск — план/факт",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Процессы",
        "goal": "Контролировать исполнение производственного плана как основу выручки и маржи",
        "formula": "Фактический выпуск / План выпуска периода × 100%",
        "unit": "%",
        "source": "1С ERP / производственная отчётность",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 10.0,
    },
    # ── Графики (3 шт.) ─────────────────────────────────────────────────────
    {
        "kpi_id": "FND-C1",
        "name": "Тренд 12 месяцев: выручка / EBITDA / валовая маржа",
        "block": "график",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "За 30 секунд показать динамику бизнеса и качество заработка",
        "formula": "12 последних месяцев: выручка (столбцы), EBITDA и валовая маржа (линии)",
        "unit": "руб. / %",
        "source": "БДР / управленческая отчётность",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
        "chart_type": "combo_bar_line_monthly",
        "chart_type_label": "Тренд 12 месяцев",
    },
    {
        "kpi_id": "FND-C2",
        "name": "Waterfall EBITDA: план → факт",
        "block": "график",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Показать факторы отклонения EBITDA, а не только итог",
        "formula": "Разложение отклонения EBITDA на 4–6 драйверов: выручка, маржа, скидки, постоянные расходы, разовые эффекты",
        "unit": "руб.",
        "source": "БДР / управленческий анализ",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
        "chart_type": "waterfall",
        "chart_type_label": "Waterfall EBITDA",
    },
    {
        "kpi_id": "FND-C3",
        "name": "Heatmap по директорам / контурам",
        "block": "график",
        "frequency": "Ежемесячно",
        "perspective": "Управление",
        "goal": "Сразу показать, где у руководителей красная и жёлтая зона",
        "formula": "Итоговый статус роли = худший статус из ключевых KPI роли; по строкам роли, по столбцам ключевые KPI",
        "unit": "RAG",
        "source": "Дашборды директоров / KPI Pack / CEO Pack",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
        "chart_type": "heatmap_rag",
        "chart_type_label": "Heatmap RAG",
    },
    # ── Таблицы (2 шт.) ─────────────────────────────────────────────────────
    {
        "kpi_id": "FND-B1",
        "name": "ТОП-10 отклонений",
        "block": "таблица",
        "frequency": "Ежемесячно",
        "perspective": "Управление",
        "goal": "Перевести сигналы дашборда в список управленческих проблем",
        "formula": "Сортировка по совокупному влиянию: показатель, план, факт, отклонение, причина, действие, владелец, срок, статус",
        "unit": "—",
        "source": "CEO Pack / данные всех контуров",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
    },
    {
        "kpi_id": "FND-B2",
        "name": "ТОП-10 решений / эскалаций",
        "block": "таблица",
        "frequency": "Ежемесячно",
        "perspective": "Управление",
        "goal": "Показать, что именно нужно утвердить или эскалировать учредителю",
        "formula": "Вопрос, требуемое решение, рекомендуемый вариант, эффект / риск, владелец, срок, статус",
        "unit": "—",
        "source": "CEO Pack / PMO / владельцы контуров",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
    },
]

FIELDS = (
    "name", "block", "frequency", "perspective", "goal",
    "formula", "unit", "source",
    "monthly_target", "quarterly_target", "yearly_target",
    "green_threshold", "yellow_threshold", "red_threshold",
    "weight_pct", "chart_type", "chart_type_label",
)


class Command(BaseCommand):
    help = "Импорт KPI для «Председатель совета директоров» (дашборд учредителя)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Только показать, что будет сделано, без записи в БД",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        created = updated = 0

        self.stdout.write(
            f"\n{'[DRY-RUN] ' if dry_run else ''}"
            f"=== {DEPARTMENT} (полный дашборд: {len(FULL_DASHBOARD)} KPI) ==="
        )

        for pos, kpi in enumerate(FULL_DASHBOARD):
            defaults = {"position": pos}
            for f in FIELDS:
                val = kpi.get(f)
                if val is not None:
                    defaults[f] = val

            if dry_run:
                exists = KpiDefinition.objects.filter(
                    department=DEPARTMENT, kpi_id=kpi["kpi_id"],
                ).exists()
                tag = "UPDATE" if exists else "CREATE"
                self.stdout.write(f"  [{tag}] {kpi['kpi_id']} — {kpi['name']}")
                if exists:
                    updated += 1
                else:
                    created += 1
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

        total = created + updated
        msg = f"Итого: {total} записей ({created} новых, {updated} обновлённых)"
        if dry_run:
            self.stdout.write(self.style.WARNING(f"\n[DRY-RUN] {msg}"))
        else:
            self.stdout.write(self.style.SUCCESS(f"\n{msg}"))
