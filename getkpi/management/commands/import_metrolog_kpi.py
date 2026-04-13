"""
Импорт KPI для «Главный метролог» и всех наследников из structure.json.

Данные берутся из листов KPI_подразделения и Методики_руководители файла
KPI_подразделений_и_методики_руководителей_v10_полный_комплект_дашбордов.xlsx.

Использование:
    py manage.py import_metrolog_kpi           # создать/обновить записи
    py manage.py import_metrolog_kpi --dry-run  # только показать, что будет сделано
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

# ─── Полный дашборд «Главный метролог» (tiles + charts + tables) ─────────────
# Источник: KPI_подразделения + Методики_руководители (Метрологическая служба)
FULL_DASHBOARD: list[dict] = [
    {
        "kpi_id": "МЕТ-M1",
        "name": "План производства в части МС",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Выполнить план по услугам поверки/ремонта",
        "formula": "Факт выполнения плана производства в части МС / План производства в части МС × 100%",
        "unit": "%",
        "source": "1С",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 25.0,
    },
    {
        "kpi_id": "МЕТ-M2",
        "name": "SLA по поверке / ремонту",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Процессы",
        "goal": "Соблюдать сроки услуг",
        "formula": "Работы, выполненные в срок / Все работы × 100%",
        "unit": "%",
        "source": "1С",
        "monthly_target": "≥95%",
        "quarterly_target": None,
        "yearly_target": "≥95%",
        "green_threshold": "≥95%",
        "yellow_threshold": "90–94,9%",
        "red_threshold": "<90%",
        "weight_pct": 20.0,
    },
    {
        "kpi_id": "МЕТ-M3-1",
        "name": "ФОТ подразделения в пределах лимита",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Привязать руководителя направления к контролю ФОТ",
        "formula": "Факт ФОТ / План ФОТ (по подразделению)",
        "unit": "%",
        "source": "1С",
        "monthly_target": "<=100%",
        "quarterly_target": None,
        "yearly_target": "<=100%",
        "green_threshold": "<100%",
        "yellow_threshold": "100–110%",
        "red_threshold": ">110%",
        "weight_pct": 30.0,
    },
    {
        "kpi_id": "МЕТ-M3-2",
        "name": "Бюджет подразделения в пределах лимита",
        "block": "плитка",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Привязать руководителя направления к контролю затрат",
        "formula": "Факт затрат / План затрат (по статьям расходов подразделения)",
        "unit": "%",
        "source": "1С",
        "monthly_target": "<=100%",
        "quarterly_target": None,
        "yearly_target": "<=100%",
        "green_threshold": "<100%",
        "yellow_threshold": "100–110%",
        "red_threshold": ">110%",
        "weight_pct": 30.0,
    },
    {
        "kpi_id": "МЕТ-Q4-1",
        "name": "Доля проектов МС без отклонения >10 р.д.",
        "block": "плитка",
        "frequency": "Ежеквартально",
        "perspective": "Развитие",
        "goal": "Обеспечить выполнение сроков разработки",
        "formula": "Проекты без отклонения >10 р.д. / Все активные проекты × 100%",
        "unit": "%",
        "source": "Turbo Project",
        "monthly_target": "≥90%",
        "quarterly_target": None,
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 25.0,
    },
    {
        "kpi_id": "МЕТ-Q4-2",
        "name": "Доля проектов МС без отклонения бюджета <10%",
        "block": "плитка",
        "frequency": "Ежеквартально",
        "perspective": "Развитие",
        "goal": "Обеспечить выполнение сроков разработки",
        "formula": "Проекты без отклонения бюджета <10% / Все активные проекты × 100%",
        "unit": "%",
        "source": "Turbo Project",
        "monthly_target": "≥90%",
        "quarterly_target": None,
        "yearly_target": "≥90%",
        "green_threshold": "≥90%",
        "yellow_threshold": "80–89,9%",
        "red_threshold": "<80%",
        "weight_pct": 25.0,
    },
    {
        "kpi_id": "МЕТ-Q5",
        "name": "Текучесть персонала МС",
        "block": "плитка",
        "frequency": "Ежеквартально",
        "perspective": "Персонал",
        "goal": "Сохранить квалифицированных поверителей",
        "formula": "Уволенные / Среднесписочная численность × 100%",
        "unit": "%",
        "source": "HR",
        "monthly_target": None,
        "quarterly_target": "≤5%",
        "yearly_target": "≤20%",
        "green_threshold": "≤5%",
        "yellow_threshold": "5,1–7%",
        "red_threshold": ">7%",
        "weight_pct": 15.0,
    },
    {
        "kpi_id": "МЕТ-C1",
        "name": "Тренд: План производства в части МС / SLA по поверке / ремонту",
        "block": "график",
        "frequency": "Ежемесячно",
        "perspective": "Финансы",
        "goal": "Визуализация динамики ключевых показателей за 12 месяцев",
        "formula": "Факт / План × 100%",
        "unit": "%",
        "source": "1С",
        "monthly_target": "≥100%",
        "quarterly_target": None,
        "yearly_target": "≥100%",
        "green_threshold": "≥100%",
        "yellow_threshold": "95–99,9%",
        "red_threshold": "<95%",
        "weight_pct": 0.0,
        "chart_type": "multi_line_plan_fact_monthly",
        "chart_type_label": "График тренда",
    },
    {
        "kpi_id": "МЕТ-B1",
        "name": "ТОП-5 отклонений и корректирующие действия",
        "block": "таблица",
        "frequency": "Ежемесячно",
        "perspective": "Управление",
        "goal": "Фокусировать внимание на критичных отклонениях",
        "formula": "—",
        "unit": "—",
        "source": "CEO Pack / управленческая отчётность",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
    },
    {
        "kpi_id": "МЕТ-B2",
        "name": "Риски / решения / сроки / ответственный",
        "block": "таблица",
        "frequency": "Ежемесячно",
        "perspective": "Управление",
        "goal": "Обеспечить прозрачность управленческих решений",
        "formula": "—",
        "unit": "—",
        "source": "CEO Pack / реестр рисков",
        "monthly_target": None,
        "quarterly_target": None,
        "yearly_target": None,
        "green_threshold": None,
        "yellow_threshold": None,
        "red_threshold": None,
        "weight_pct": 0.0,
    },
]

# Плитки, наследуемые подчинёнными (без графиков и таблиц)
TILE_IDS = ("МЕТ-M1", "МЕТ-M2", "МЕТ-M3-1", "МЕТ-M3-2", "МЕТ-Q4-1", "МЕТ-Q5")

TILES_ONLY = [k for k in FULL_DASHBOARD if k["kpi_id"] in TILE_IDS]

# ─── Подразделения из structure.json ─────────────────────────────────────────
HEAD = "Главный метролог"

SUBORDINATES = [
    "Заместитель главного метролога",
    "Ведущий инженер по метрологии",
    "Инженер-метролог",
    "Инженер-диспетчер",
    "Ремонтный участок",
    "Бригадир участка",
    "Инженер по ремонту",
    "Диспетчер",
    "Отдел метрологии и сертификации",
    "Начальник ОМиС",
    "Руководитель проектов по сертификации",
    "Инженер по метрологии",
]

FIELDS = (
    "name", "block", "frequency", "perspective", "goal",
    "formula", "unit", "source",
    "monthly_target", "quarterly_target", "yearly_target",
    "green_threshold", "yellow_threshold", "red_threshold",
    "weight_pct", "chart_type", "chart_type_label",
)


def _upsert(department: str, kpi_list: list[dict], *, out, dry_run: bool) -> tuple[int, int]:
    created = updated = 0
    for pos, kpi in enumerate(kpi_list):
        defaults = {"position": pos}
        for f in FIELDS:
            val = kpi.get(f)
            if val is not None:
                defaults[f] = val

        if dry_run:
            exists = KpiDefinition.objects.filter(
                department=department, kpi_id=kpi["kpi_id"],
            ).exists()
            tag = "UPDATE" if exists else "CREATE"
            out.write(f"  [{tag}] {department} / {kpi['kpi_id']} — {kpi['name']}\n")
            if exists:
                updated += 1
            else:
                created += 1
        else:
            _, is_new = KpiDefinition.objects.update_or_create(
                department=department,
                kpi_id=kpi["kpi_id"],
                defaults=defaults,
            )
            if is_new:
                created += 1
            else:
                updated += 1
    return created, updated


class Command(BaseCommand):
    help = "Импорт KPI для «Главный метролог» и всех наследников"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Только показать, что будет сделано, без записи в БД",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        total_c = total_u = 0

        self.stdout.write(f"\n{'[DRY-RUN] ' if dry_run else ''}=== {HEAD} (полный дашборд: {len(FULL_DASHBOARD)} KPI) ===")
        c, u = _upsert(HEAD, FULL_DASHBOARD, out=self.stdout, dry_run=dry_run)
        total_c += c
        total_u += u

        for dept in SUBORDINATES:
            self.stdout.write(f"\n{'[DRY-RUN] ' if dry_run else ''}=== {dept} (плитки: {len(TILES_ONLY)} KPI) ===")
            c, u = _upsert(dept, TILES_ONLY, out=self.stdout, dry_run=dry_run)
            total_c += c
            total_u += u

        total = total_c + total_u
        msg = f"Итого: {total} записей ({total_c} новых, {total_u} обновлённых)"
        if dry_run:
            self.stdout.write(self.style.WARNING(f"\n[DRY-RUN] {msg}"))
        else:
            self.stdout.write(self.style.SUCCESS(f"\n{msg}"))
