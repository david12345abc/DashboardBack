"""Точки входа дашборда «директор по качеству»: сборка KPI для общего ``getkpi.views``.
"""
from __future__ import annotations

from typing import Any, Callable

from .mpp_tasks_report import get_qd_q1_ytd
from .qd_m1 import get_qd_m1_ytd
from .qd_m3 import get_qd_m3_ytd
from .qd_m4 import get_qd_m4_ytd
from .turnover import get_qd_q2_ytd

QUALDIR_TILE_KPI_IDS: frozenset[str] = frozenset({
    'QD-Q2', 'QD-Q1', 'QD-M1', 'QD-M4', 'QD-M3',
})

KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({'QD-M3', 'QD-M4'})
RUB_UNIT_KPI_IDS: frozenset[str] = frozenset({'QD-M3', 'QD-M4'})
TILE_COLOR_TD_M4_LIMIT_IDS: frozenset[str] = frozenset({'QD-M3', 'QD-M4'})


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
    entry['articles'] = qd.get('articles')
    entry['classifier'] = qd.get('classifier')
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


_MERGE_BY_ID: dict[str, Callable[[dict[str, Any], int | None, int | None], bool]] = {
    'QD-Q2': _merge_qd_q2,
    'QD-Q1': _merge_qd_q1,
    'QD-M1': _merge_qd_m1,
    'QD-M4': _merge_qd_m4,
    'QD-M3': _merge_qd_m3,
}


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    """По ``kpi_id`` заполняет ``entry``. Возвращает True, если надо ``return entry`` в ``getkpi.views``."""
    merger = _MERGE_BY_ID.get(kpi_id)
    if merger is None:
        return False
    return merger(entry, year, month)
