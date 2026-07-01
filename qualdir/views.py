"""Точки входа дашборда «директор по качеству»: сборка KPI для общего ``getkpi.views``.
"""
from __future__ import annotations

from typing import Any, Callable

from .mpp_tasks_report import get_qd_q1_ytd
from .qd_m1 import get_qd_m1_ytd
from .qd_m3 import get_qd_m3_ytd
from .qd_m4 import get_qd_m4_ytd
from .qd_m5 import get_qd_m5_ytd
from .qd_m6 import get_qd_m6_ytd
from .qd_m9 import get_qd_m9_ytd
from .qd_m10 import get_qd_m10_ytd
from .qd_m7 import get_qd_m7_ytd
from .qd_m8 import get_qd_m8_ytd
from .turnover import get_qd_q2_ytd

QUALDIR_TILE_KPI_IDS: frozenset[str] = frozenset({
    'QD-Q2', 'QD-Q1', 'QD-M1', 'QD-M4', 'QD-M3', 'QD-M5',
    'QD-M6', 'QD-M7', 'QD-M8', 'QD-M9', 'QD-M10',
})

KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset(
    {'QD-M1', 'QD-M3', 'QD-M4', 'QD-M5', 'QD-M6', 'QD-M8', 'QD-M9', 'QD-M10'}
)
RUB_UNIT_KPI_IDS: frozenset[str] = frozenset({'QD-M3', 'QD-M4'})
TILE_COLOR_TD_M4_LIMIT_IDS: frozenset[str] = frozenset({'QD-M3', 'QD-M4'})
TILE_COLOR_PLAN_FACT_IDS: frozenset[str] = frozenset(
    {'QD-M1', 'QD-M5', 'QD-M6', 'QD-M8', 'QD-M9', 'QD-M10', 'QD-Q1'}
)
TILE_FACT_ONLY_IDS: frozenset[str] = frozenset({'QD-M7'})
OTK_INCOMING_TILE_IDS: frozenset[str] = frozenset({'QD-M6', 'QD-M9', 'QD-M10'})


def kpi_pct_from_plan_fact(plan: Any, fact: Any) -> float | None:
    """Факт / план × 100 % для плиток qualdir с plan/fact."""
    if plan is None or fact is None:
        return None
    try:
        pv = float(plan)
        fv = float(fact)
    except (TypeError, ValueError):
        return None
    if pv <= 0 and fv <= 0:
        return 100.0
    from .turnover import _qd_q2_kpi_pct

    return _qd_q2_kpi_pct(plan, fact)


def rag_plan_fact_pct(pct: float | None) -> str:
    """QD-M1/M5/M6/M8/Q1: ≥90 % — зелёный, 80–89,9 % — жёлтый, <80 % — красный."""
    if pct is None:
        return 'unknown'
    if pct >= 90:
        return 'green'
    if pct >= 80:
        return 'yellow'
    return 'red'


def enrich_qualdir_plan_fact_row(row: dict[str, Any]) -> dict[str, Any]:
    """kpi_pct и RAG-цвет по порогам справочника (90/80)."""
    pct = row.get('kpi_pct')
    if pct is None:
        pct = kpi_pct_from_plan_fact(row.get('plan'), row.get('fact'))
    if pct is None:
        return row
    pct = float(pct)
    return {**row, 'kpi_pct': pct, 'color': rag_plan_fact_pct(pct)}


def sync_qualdir_plan_fact_tile_color(tile: dict[str, Any]) -> None:
    """Плитки QD-M1/M5/M6/M8/M9/M10/Q1: цвет и kpi_pct из plan/fact выбранного месяца."""
    kid = str(tile.get('kpi_id') or '').strip().upper()
    if kid not in TILE_COLOR_PLAN_FACT_IDS:
        return
    pct = kpi_pct_from_plan_fact(tile.get('plan'), tile.get('fact'))
    if pct is None:
        raw_pct = tile.get('kpi_pct')
        if raw_pct is not None:
            pct = float(raw_pct)
    else:
        tile['kpi_pct'] = pct
    if pct is not None:
        tile['color'] = rag_plan_fact_pct(float(pct))


def clear_qualdir_fact_only_tile_rag(tile: dict[str, Any]) -> None:
    """QD-M7: только счётчик документов, без KPI % и RAG."""
    kid = str(tile.get('kpi_id') or '').strip().upper()
    if kid not in TILE_FACT_ONLY_IDS:
        return
    tile.pop('kpi_pct', None)
    tile['color'] = None


def _merge_qd_q2(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_q2_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_q1(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_q1_ytd(year=year, month=month)
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m1(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m1_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['departments'] = qd.get('departments')
    entry['departments_by_month'] = qd.get('departments_by_month')
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m4(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m4_ytd(year=year, month=month)
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m3(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m3_ytd(year=year, month=month)
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m8(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m8_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['departments'] = qd.get('departments')
    entry['kinds'] = qd.get('kinds')
    entry['breakdown_by_month'] = qd.get('breakdown_by_month')
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m7(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m7_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m6(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m6_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m9(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m9_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m10(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m10_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['debug'] = qd.get('debug')
    return True


def _merge_qd_m5(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    qd = get_qd_m5_ytd(year=year, month=month)
    if qd is None:
        return False
    entry['data_granularity'] = qd['data_granularity']
    entry['monthly_data'] = qd['monthly_data']
    entry['last_full_month_row'] = qd.get('last_full_month_row')
    entry['ytd'] = qd['ytd']
    entry['kpi_period'] = qd['kpi_period']
    entry['departments'] = qd.get('departments')
    entry['departments_by_month'] = qd.get('departments_by_month')
    entry['debug'] = qd.get('debug')
    return True


_MERGE_BY_ID: dict[str, Callable[[dict[str, Any], int | None, int | None], bool]] = {
    'QD-Q2': _merge_qd_q2,
    'QD-Q1': _merge_qd_q1,
    'QD-M1': _merge_qd_m1,
    'QD-M4': _merge_qd_m4,
    'QD-M3': _merge_qd_m3,
    'QD-M5': _merge_qd_m5,
    'QD-M6': _merge_qd_m6,
    'QD-M9': _merge_qd_m9,
    'QD-M10': _merge_qd_m10,
    'QD-M7': _merge_qd_m7,
    'QD-M8': _merge_qd_m8,
}


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    """По ``kpi_id`` заполняет ``entry``. Возвращает True, если надо ``return entry`` в ``getkpi.views``."""
    kid = str(kpi_id or "").strip().upper()
    for cyr, lat in (("М", "M"), ("С", "C")):
        kid = kid.replace(cyr, lat)
    merger = _MERGE_BY_ID.get(kid)
    if merger is None:
        return False
    return merger(entry, year, month)
