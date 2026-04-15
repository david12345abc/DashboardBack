"""
Замена KPI коммерческого директора и его дочерних отделов.

Удаляет все предыдущие KPI для коммерческого директора и его детей,
создаёт 11 общих ежемесячных KPI (включая KD-M11 «Текучесть персонала»)
+ 3 контейнера графиков.

    py manage.py import_komdir_new_kpi
"""
from django.core.management.base import BaseCommand

from getkpi.models import KpiDefinition

KOMDIR_DEPT = 'коммерческий директор'

CHILD_DEPTS_DB_KEYS = [
    'ОВЭД',
    'ОПЭОиУ',
    'Отдел продаж БМИ',
    'Отдел по работе с ключевыми клиентами',
    'Отдел по работе с ПАО «Газпром»',
    'Отдел дилерских продаж',
]

CHILD_DEPTS_STRUCTURE_KEYS = [
    'Отдел ВЭД',
    'Отдел ПМУ и ЭО',
    'Отдел продаж БМИ',
    'Отдел по работе с ключевыми клиентами',
    'Отдел по работе с ПАО «Газпром»',
    'Отдел дилерских продаж промышленного оборудования',
]

ALL_OLD_DEPT_KEYS = (
    [KOMDIR_DEPT]
    + CHILD_DEPTS_DB_KEYS
    + CHILD_DEPTS_STRUCTURE_KEYS
    + [
        'Заместитель коммерческого директора по развитию продаж',
        'Коммерческая служба',
        'Тендерный отдел',
    ]
)

TILE_KPIS = [
    {
        'kpi_id': 'KD-M1',
        'name': 'Деньги',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Контроль денежных поступлений',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{План}} \times 100\%',
        'unit': 'руб.',
        'source': '1С / БДР',
        'green_threshold': '≥100%',
        'yellow_threshold': '90–99,9%',
        'red_threshold': '<90%',
        'weight_pct': 15.0,
    },
    {
        'kpi_id': 'KD-M2',
        'name': 'Отгрузки',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Контроль объёмов отгрузки',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{План}} \times 100\%',
        'unit': 'руб.',
        'source': '1С / БДР',
        'green_threshold': '≥100%',
        'yellow_threshold': '90–99,9%',
        'red_threshold': '<90%',
        'weight_pct': 15.0,
    },
    {
        'kpi_id': 'KD-M3',
        'name': 'Договоры',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Контроль заключённых договоров',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{План}} \times 100\%',
        'unit': 'руб.',
        'source': '1С / CRM',
        'green_threshold': '≥100%',
        'yellow_threshold': '90–99,9%',
        'red_threshold': '<90%',
        'weight_pct': 10.0,
    },
    {
        'kpi_id': 'KD-M4',
        'name': 'Дебиторская задолженность',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Контроль дебиторской задолженности',
        'formula': r'\text{Факт на дату}',
        'unit': 'руб.',
        'source': '1С / Реестр ДЗ',
        'green_threshold': '<100%',
        'yellow_threshold': '100–110%',
        'red_threshold': '>110%',
        'weight_pct': 10.0,
    },
    {
        'kpi_id': 'KD-M5',
        'name': 'Просроченная Дебиторская Задолженность',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Снижение просроченной дебиторской задолженности',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{Лимит}} \times 100\%',
        'unit': 'руб.',
        'source': '1С / Реестр ДЗ',
        'green_threshold': '<100%',
        'yellow_threshold': '100–110%',
        'red_threshold': '>110%',
        'weight_pct': 10.0,
    },
    {
        'kpi_id': 'KD-M6',
        'name': 'Валовая прибыль',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Обеспечение плановой валовой прибыли',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{План}} \times 100\%',
        'unit': 'руб.',
        'source': '1С / БДР / Управленческая отчётность',
        'green_threshold': '≥100%',
        'yellow_threshold': '90–99,9%',
        'red_threshold': '<90%',
        'weight_pct': 10.0,
    },
    {
        'kpi_id': 'KD-M7',
        'name': 'Расходы',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Контроль расходов в пределах лимита',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{Лимит}} \times 100\%',
        'unit': 'руб.',
        'source': '1С / БДР',
        'green_threshold': '≤100%',
        'yellow_threshold': '100,1–110%',
        'red_threshold': '>110%',
        'weight_pct': 10.0,
    },
    {
        'kpi_id': 'KD-M8',
        'name': 'ФОТ',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Персонал',
        'goal': 'Контроль фонда оплаты труда',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{Лимит}} \times 100\%',
        'unit': 'руб.',
        'source': '1С ЗУП / HRIS',
        'green_threshold': '≤100%',
        'yellow_threshold': '100,1–110%',
        'red_threshold': '>110%',
        'weight_pct': 5.0,
    },
    {
        'kpi_id': 'KD-M9',
        'name': 'Скидка / МЦР',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': 'Контроль уровня предоставляемых скидок',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{Норма}} \times 100\%',
        'unit': '%',
        'source': '1С / CRM',
        'green_threshold': '≤100%',
        'yellow_threshold': '100,1–110%',
        'red_threshold': '>110%',
        'weight_pct': 10.0,
    },
    {
        'kpi_id': 'KD-M10',
        'name': 'ТКП в SLA',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Процессы',
        'goal': 'Соблюдение SLA по технико-коммерческим предложениям',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{Норма}} \times 100\%',
        'unit': '%',
        'source': 'CRM / тендерный отдел',
        'green_threshold': '≥100%',
        'yellow_threshold': '90–99,9%',
        'red_threshold': '<90%',
        'weight_pct': 5.0,
    },
    {
        'kpi_id': 'KD-M11',
        'name': 'Текучесть персонала',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Персонал',
        'goal': 'Контроль текучести персонала коммерческой службы',
        'formula': r'\mathrm{KPI} = \dfrac{\text{Факт}}{\text{Норма}} \times 100\%',
        'unit': '%',
        'source': '1С ЗУП / HR',
        'green_threshold': '≤100%',
        'yellow_threshold': '100,1–110%',
        'red_threshold': '>110%',
        'weight_pct': 5.0,
    },
]

KOMDIR_ONLY_KPIS: list[dict] = []

CHART_KPIS = [
    {
        'kpi_id': 'KD-C1',
        'name': 'Динамика: Деньги, Отгрузки, Договоры',
        'block': 'график',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'chart_type': 'multi_line_plan_fact_monthly',
        'chart_type_label': 'Линейный тренд по месяцам (факт)',
    },
    {
        'kpi_id': 'KD-C2',
        'name': 'Круговые диаграммы',
        'block': 'график',
        'frequency': 'Ежемесячно',
        'perspective': 'Развитие',
        'chart_type': 'donut_multiple',
        'chart_type_label': 'Круговые диаграммы по направлениям',
    },
    {
        'kpi_id': 'KD-C3',
        'name': 'KPI за месяц: Деньги, Отгрузки, Договоры, Валовая прибыль',
        'block': 'график',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'chart_type': 'column_plan_fact_monthly',
        'chart_type_label': 'Столбцы KPI за месяц',
    },
]


class Command(BaseCommand):
    help = 'Замена KPI коммерческого директора и дочерних отделов (11 общих KPI + 3 графика)'

    def handle(self, *args, **options):
        deleted_total = 0
        for dept in ALL_OLD_DEPT_KEYS:
            cnt, _ = KpiDefinition.objects.filter(department=dept).delete()
            if cnt:
                self.stdout.write(f'  Удалено {cnt} записей для «{dept}»')
                deleted_total += cnt

        cnt, _ = KpiDefinition.objects.filter(department__icontains='коммерческ').delete()
        if cnt:
            self.stdout.write(f'  Удалено {cnt} записей по фильтру «коммерческ*»')
            deleted_total += cnt

        self.stdout.write(f'Итого удалено: {deleted_total}')

        all_depts = [KOMDIR_DEPT] + CHILD_DEPTS_DB_KEYS
        created = 0

        for dept in all_depts:
            tiles_for_dept = list(TILE_KPIS)
            if dept == KOMDIR_DEPT:
                tiles_for_dept += KOMDIR_ONLY_KPIS

            for pos, kpi in enumerate(tiles_for_dept):
                KpiDefinition.objects.update_or_create(
                    department=dept,
                    kpi_id=kpi['kpi_id'],
                    defaults={
                        'name': kpi['name'],
                        'block': kpi['block'],
                        'frequency': kpi['frequency'],
                        'perspective': kpi.get('perspective', ''),
                        'goal': kpi.get('goal', ''),
                        'formula': kpi.get('formula', ''),
                        'unit': kpi.get('unit', ''),
                        'source': kpi.get('source', ''),
                        'green_threshold': kpi.get('green_threshold'),
                        'yellow_threshold': kpi.get('yellow_threshold'),
                        'red_threshold': kpi.get('red_threshold'),
                        'weight_pct': kpi.get('weight_pct'),
                        'position': pos,
                    },
                )
                created += 1

            for pos, chart in enumerate(CHART_KPIS, start=len(tiles_for_dept)):
                KpiDefinition.objects.update_or_create(
                    department=dept,
                    kpi_id=chart['kpi_id'],
                    defaults={
                        'name': chart['name'],
                        'block': chart['block'],
                        'frequency': chart.get('frequency', ''),
                        'perspective': chart.get('perspective', ''),
                        'chart_type': chart.get('chart_type', ''),
                        'chart_type_label': chart.get('chart_type_label', ''),
                        'position': pos,
                    },
                )
                created += 1

        self.stdout.write(self.style.SUCCESS(
            f'Создано/обновлено {created} записей для {len(all_depts)} подразделений'
        ))
