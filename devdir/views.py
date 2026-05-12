"""Точки входа дашборда «директор по развитию»: сборка KPI для общего ``getkpi.views``.

Расчёты данных — в пакете ``getkpi.devdir``; здесь только склейка полей записи KPI.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from getkpi.devdir import rd_m1_zpr, rd_m2_1, rd_m3_budget, rd_m4_fot, rd_q2_tekuchest
from getkpi.devdir.rd_monthly_period import MONTH_NAMES

DEVDIR_KPI_IDS: frozenset[str] = frozenset(
    {'RD-M1', 'RD-M2-1', 'RD-M3', 'RD-M4', 'RD-Q2'},
)


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    """Если ``kpi_id`` — KPI этого контура, заполняет ``entry`` и возвращает True."""
    if kpi_id == 'RD-M1':
        _fill_rd_m1_zpr_monthly_tile(entry, year=year, month=month)
        return True
    if kpi_id == 'RD-M2-1':
        _fill_rd_m2_1_monthly_tile(entry, year=year, month=month)
        return True
    if kpi_id == 'RD-M3':
        _fill_rd_m3_budget_monthly_tile(entry, year=year, month=month)
        return True
    if kpi_id == 'RD-M4':
        _fill_rd_m4_fot_monthly_tile(entry, year=year, month=month)
        return True
    if kpi_id == 'RD-Q2':
        _fill_rd_q2_tekuchest_monthly_tile(entry, year=year, month=month)
        return True
    return False


def _merge_monthly_dev_payload(entry: dict[str, Any], dev: dict[str, Any] | None) -> None:
    if dev is None:
        return
    entry['data_granularity'] = dev.get('data_granularity', 'monthly')
    entry['monthly_data'] = dev.get('monthly_data') or []
    entry['last_full_month_row'] = dev.get('last_full_month_row')
    entry['ytd'] = dev.get('ytd') or {}
    entry['kpi_period'] = dev.get('kpi_period')
    if dev.get('debug') is not None:
        entry['debug'] = dev['debug']


def _fill_rd_m1_zpr_monthly_tile(
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> None:
    payload = rd_m1_zpr.get_rd_m1_zpr_ytd(year=year, month=month)
    if payload is not None:
        _merge_monthly_dev_payload(entry, payload)
        return
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    entry['data_granularity'] = 'monthly'
    entry['monthly_data'] = []
    entry['last_full_month_row'] = None
    entry['ytd'] = {
        'total_plan': None,
        'total_fact': None,
        'kpi_pct': None,
        'months_with_data': 0,
        'months_total': 0,
        'values_unit': 'шт.',
    }
    entry['kpi_period'] = {
        'type': 'last_full_month',
        'year': ref_y,
        'month': ref_m,
        'month_name': MONTH_NAMES[ref_m],
    }


def _fill_rd_m2_1_monthly_tile(
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> None:
    payload = rd_m2_1.get_rd_m2_1_ytd(year=year, month=month)
    if payload is not None:
        _merge_monthly_dev_payload(entry, payload)
        return
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    entry['data_granularity'] = 'monthly'
    entry['monthly_data'] = []
    entry['last_full_month_row'] = None
    entry['ytd'] = {
        'total_plan': None,
        'total_fact': None,
        'kpi_pct': None,
        'months_with_data': 0,
        'months_total': 0,
        'values_unit': 'шт.',
    }
    entry['kpi_period'] = {
        'type': 'last_full_month',
        'year': ref_y,
        'month': ref_m,
        'month_name': MONTH_NAMES[ref_m],
    }


def _fill_rd_m3_budget_monthly_tile(
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> None:
    payload = rd_m3_budget.get_rd_m3_budget_ytd(year=year, month=month)
    if payload is not None:
        _merge_monthly_dev_payload(entry, payload)
        return
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    entry['data_granularity'] = 'monthly'
    entry['monthly_data'] = []
    entry['last_full_month_row'] = None
    entry['ytd'] = {
        'total_plan': None,
        'total_fact': None,
        'kpi_pct': None,
        'months_with_data': 0,
        'months_total': 0,
        'values_unit': 'руб.',
    }
    entry['kpi_period'] = {
        'type': 'last_full_month',
        'year': ref_y,
        'month': ref_m,
        'month_name': MONTH_NAMES[ref_m],
    }


def _fill_rd_m4_fot_monthly_tile(
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> None:
    payload = rd_m4_fot.get_rd_m4_fot_ytd(year=year, month=month)
    if payload is not None:
        _merge_monthly_dev_payload(entry, payload)
        return
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    entry['data_granularity'] = 'monthly'
    entry['monthly_data'] = []
    entry['last_full_month_row'] = None
    entry['ytd'] = {
        'total_plan': None,
        'total_fact': None,
        'kpi_pct': None,
        'months_with_data': 0,
        'months_total': 0,
        'values_unit': 'руб.',
    }
    entry['kpi_period'] = {
        'type': 'last_full_month',
        'year': ref_y,
        'month': ref_m,
        'month_name': MONTH_NAMES[ref_m],
    }


def _fill_rd_q2_tekuchest_monthly_tile(
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> None:
    payload = rd_q2_tekuchest.get_rd_q2_tekuchest_ytd(year=year, month=month)
    if payload is not None:
        _merge_monthly_dev_payload(entry, payload)
        return
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    entry['data_granularity'] = 'monthly'
    entry['monthly_data'] = []
    entry['last_full_month_row'] = None
    entry['ytd'] = {
        'total_plan': None,
        'total_fact': None,
        'kpi_pct': None,
        'months_with_data': 0,
        'months_total': 0,
        'values_unit': 'чел.',
    }
    entry['kpi_period'] = {
        'type': 'last_full_month',
        'year': ref_y,
        'month': ref_m,
        'month_name': MONTH_NAMES[ref_m],
    }
