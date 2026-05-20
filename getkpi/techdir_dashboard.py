"""Сборка графиков, таблиц и путей кэша для дашборда «Технический директор» (TD-*)."""
from __future__ import annotations

from pathlib import Path

from . import techdir_m3, techdir_m4, techdir_m5, techdir_projects, techdir_tekuchet
from .kpi_periods import pick_monthly_row_for_period

# Плитки с единицами «руб.» в универсальном билдере (часть KPI техдира в БД может дублироваться у QD).
TECHDIR_RUB_UNIT_KPI_IDS = frozenset({'TD-M5', 'TD-M6'})
# TD-C1 / TD-C2: бюджет контура, ФОТ и план ФОТ внешних заказов (БДДС, TD-M6); TD-M5 в графики не отдаём.
TECHDIR_CHART_TILE_IDS = frozenset({'TD-M3', 'TD-M4', 'TD-M6'})


def is_techdir_department(dept: str | None) -> bool:
    return (dept or '').strip().lower() == 'технический директор'


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    """Файлы кэша, по mtime которых показывается cache_updated_at плитки (только TD-*)."""
    mapping: dict[str, list[Path]] = {
        'TD-M1': [techdir_projects.CACHE_PATH],
        'TD-Q1': [techdir_projects.CACHE_PATH],
        'TD-M5': [
            techdir_projects.CACHE_PATH,
            techdir_m5.ytd_cache_path(ref_y, ref_m),
            techdir_m5.FACT_CACHE_DIR,
        ],
        'TD-M6': [techdir_projects.CACHE_PATH],
        'TD-M3': [techdir_m3.CACHE_DIR / f'techdir_m3_monthly_{ref_y}_{ref_m:02d}.json'],
        'TD-M4': [techdir_m4.CACHE_DIR / f'techdir_m4_monthly_{ref_y}_{ref_m:02d}.json'],
        'TD-Q2': [techdir_tekuchet.CACHE_DIR / f'techdir_tekuchet_{ref_y}_{ref_m:02d}.json'],
    }
    return list(mapping.get(kpi_id, []))


def build_charts(
    tiles_meta: list[dict],
    entries_by_id: dict[str, dict],
    tile_values_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    by_id = {k['kpi_id']: k for k in tiles_meta}
    techdir_chart_kpis = ('TD-M3', 'TD-M4', 'TD-M6')
    line_kpis = bar_kpis = techdir_chart_kpis
    display_names = {
        'TD-M3': 'Бюджет',
        'TD-M4': 'ФОТ',
        'TD-M6': 'План БДДС (внешние заказы)',
    }
    series: list[dict] = []

    for kid in line_kpis:
        kpi_meta = by_id.get(kid, {})
        entry = entries_by_id.get(kid) or {}
        monthly = entry.get('monthly_data') or []
        points = [
            {
                'month': row.get('month'),
                'month_name': row.get('month_name'),
                'year': row.get('year'),
                'plan': row.get('plan'),
                'fact': row.get('fact'),
            }
            for row in monthly
        ]
        if not points:
            continue
        if not any((p.get('plan') is not None or p.get('fact') is not None) for p in points):
            continue

        series.append({
            'kpi_id': kid,
            'name': display_names.get(kid, kpi_meta.get('name', kid)),
            'chart_type': 'line_plan_fact_monthly',
            'chart_type_label': f"План/Факт по месяцам: {display_names.get(kid, kpi_meta.get('name', kid))}",
            'points': points,
        })

    if not series:
        return {}

    bar_categories: list[str] = []
    bar_plan_values: list[float | None] = []
    bar_fact_values: list[float | None] = []
    bar_points: list[dict] = []
    for kid in bar_kpis:
        kpi_meta = by_id.get(kid, {})
        entry = entries_by_id.get(kid) or {}
        tile_vals = tile_values_by_id.get(kid) or {}
        point = {
            'plan': tile_vals.get('plan'),
            'fact': tile_vals.get('fact'),
            'kpi_pct': tile_vals.get('kpi_pct'),
        }
        if point['plan'] is None and point['fact'] is None:
            point = (
                entry.get('last_full_month_row')
                or pick_monthly_row_for_period(entry.get('monthly_data') or [], ref_y, ref_m)
                or {}
            )
        display_name = display_names.get(kid, kpi_meta.get('name', kid))
        bar_categories.append(display_name)
        bar_plan_values.append(point.get('plan'))
        bar_fact_values.append(point.get('fact'))
        bar_points.append({
            'kpi_id': kid,
            'name': display_name,
            'month': ref_m,
            'year': ref_y,
            'plan': point.get('plan'),
            'fact': point.get('fact'),
            'kpi_pct': point.get('kpi_pct'),
        })

    charts = {
        'TD-C1': {
            'kpi_id': 'TD-C1',
            'name': 'Динамика: бюджет, ФОТ и план по БДДС (внешние заказы)',
            'periodicity': 'ежемесячно',
            'chart_type': 'multi_line_plan_fact_monthly',
            'chart_type_label': 'Линейный тренд по месяцам (план/факт)',
            'series': series,
        }
    }

    if any(v is not None for v in bar_plan_values) or any(v is not None for v in bar_fact_values):
        charts['TD-C2'] = {
            'kpi_id': 'TD-C2',
            'name': 'KPI за месяц: бюджет, ФОТ и план БДДС (внешние заказы)',
            'periodicity': 'ежемесячно',
            'chart_type': 'column_plan_fact_monthly',
            'chart_type_label': 'Столбцы: план/факт за месяц',
            'series': [{
                'kpi_id': 'TD-C2',
                'name': 'План/факт за месяц',
                'chart_type': 'column_plan_fact_monthly',
                'chart_type_label': 'Столбцы',
                'categories': bar_categories,
                'plan': bar_plan_values,
                'fact': bar_fact_values,
                'points': bar_points,
            }],
        }

    return charts


def strip_external_orders_budget_from_grafiki(grafiki: dict[str, dict]) -> None:
    """Убрать из «Графиков» серии бюджета внешних заказов (TD-M5 / копии чартов техдира у qualdir)."""
    markers = (
        'внешние заказы',
        'внешних заказов',
        'внешним заказам',
        'по внешним заказ',
        'бюджет проектов по внешним',
    )

    def _is_external_orders_budget_series(s: dict) -> bool:
        kid = str(s.get('kpi_id') or '').strip().upper()
        if kid == 'TD-M5':
            return True
        n = str(s.get('name') or '').lower()
        return any(m in n for m in markers)

    for chart in grafiki.values():
        if not isinstance(chart, dict):
            continue
        ser = chart.get('series')
        if not isinstance(ser, list):
            continue
        new_series: list[dict] = []
        for block in ser:
            if not isinstance(block, dict):
                continue
            pts = block.get('points')
            cats = block.get('categories')
            if isinstance(pts, list) and isinstance(cats, list) and (
                'categories' in block or block.get('chart_type') == 'column_plan_fact_monthly'
            ):
                keep: list[int] = []
                for i, p in enumerate(pts):
                    if isinstance(p, dict) and not _is_external_orders_budget_series(p):
                        keep.append(i)
                if len(keep) == len(pts):
                    new_series.append(block)
                    continue
                nb = dict(block)
                nb['points'] = [pts[i] for i in keep]
                nb['categories'] = [cats[i] for i in keep if i < len(cats)]
                for key in ('plan', 'fact'):
                    arr = nb.get(key)
                    if isinstance(arr, list) and len(arr) == len(pts):
                        nb[key] = [arr[i] for i in keep]
                new_series.append(nb)
                continue
            if not _is_external_orders_budget_series(block):
                new_series.append(block)
        chart['series'] = new_series


def merge_deviation_tables(tablitsy: dict, ref_y: int, ref_m: int) -> None:
    try:
        techdir_tables = techdir_projects.get_td_deviation_tables(month=ref_m, year=ref_y)
    except Exception:
        techdir_tables = None
    if techdir_tables:
        tablitsy.update(techdir_tables)
