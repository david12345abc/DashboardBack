"""Сборка записи KPI дашборда «технический директор» (TD-*) — вызывается из ``getkpi.views``."""
from __future__ import annotations

from typing import Any, Callable

from . import techdir_m2, techdir_m3, techdir_m4, techdir_projects, techdir_tekuchet, techdir_y1

TECHDIR_TILE_KPI_IDS: frozenset[str] = frozenset({
    'TD-M1', 'TD-M2', 'TD-Q1', 'TD-M3', 'TD-M4', 'TD-M5', 'TD-Q2', 'TD-Y1',
})

TILE_COLOR_DZ_LOWER_IDS: frozenset[str] = frozenset({'TD-M3'})
TILE_COLOR_TD_M4_LIMIT_IDS: frozenset[str] = frozenset({'TD-M4', 'TD-M5'})


def _apply_monthly(entry: dict[str, Any], td: dict[str, Any]) -> None:
    entry['data_granularity'] = td['data_granularity']
    entry['monthly_data'] = td['monthly_data']
    entry['last_full_month_row'] = td.get('last_full_month_row')
    entry['ytd'] = td['ytd']
    entry['kpi_period'] = td['kpi_period']


def _merge_td_m1(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_projects.get_td_m1_ytd()
    if td is None:
        return False
    _apply_monthly(entry, td)
    return True


def _merge_td_m2(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_m2.get_td_m2_ytd()
    _apply_monthly(entry, td)
    return True


def _merge_td_q1(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_projects.get_td_q1_ytd()
    if td is None:
        return False
    entry['data_granularity'] = td['data_granularity']
    entry['monthly_data'] = td.get('monthly_data') or td.get('quarterly_data') or []
    entry['last_full_month_row'] = td.get('last_full_month_row')
    entry['ytd'] = td['ytd']
    entry['kpi_period'] = td['kpi_period']
    return True


def _merge_td_m3(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_m3.get_td_m3_ytd(year=year, month=month)
    if td is None:
        return False
    _apply_monthly(entry, td)
    return True


def _merge_td_m4(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_m4.get_td_m4_ytd(year=year, month=month)
    if td is None:
        return False
    _apply_monthly(entry, td)
    return True


def _merge_td_m5(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_projects.get_td_m5_ytd(year=year, month=month)
    if td is None:
        return False
    _apply_monthly(entry, td)
    entry['debug'] = td.get('debug')
    return True


def _merge_td_q2(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_tekuchet.get_td_q2_ytd(year=year, month=month)
    if td is None:
        return False
    _apply_monthly(entry, td)
    return True


def _merge_td_y1(entry: dict[str, Any], year: int | None, month: int | None) -> bool:
    td = techdir_y1.get_td_y1_ytd()
    entry['data_granularity'] = td['data_granularity']
    entry['yearly_data'] = td['yearly_data']
    entry['ytd'] = td['ytd']
    entry['kpi_period'] = td['kpi_period']
    return True


_MERGE_BY_ID: dict[str, Callable[[dict[str, Any], int | None, int | None], bool]] = {
    'TD-M1': _merge_td_m1,
    'TD-M2': _merge_td_m2,
    'TD-Q1': _merge_td_q1,
    'TD-M3': _merge_td_m3,
    'TD-M4': _merge_td_m4,
    'TD-M5': _merge_td_m5,
    'TD-Q2': _merge_td_q2,
    'TD-Y1': _merge_td_y1,
}


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    """По ``kpi_id`` заполняет ``entry``. Возвращает True, если надо ``return entry`` в views."""
    merger = _MERGE_BY_ID.get(kpi_id)
    if merger is None:
        return False
    return merger(entry, year, month)
