"""Точки входа дашборда «директор по развитию»: сборка KPI для общего ``getkpi.views``.

Расчёты данных — в пакете ``devdir``; здесь только склейка полей записи KPI.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from . import (
    rd_m1_zpr,
    rd_m2_1,
    rd_m3_budget,
    rd_m4_fot,
    rd_q2_tekuchest,
    turboproject_projects_by_resources,
)
from .rd_monthly_period import MONTH_NAMES
from .turboproject_ope_projects import CACHE_PATH as OPE_PROJECTS_CACHE_PATH

DEVDIR_KPI_IDS: frozenset[str] = frozenset(
    {'RD-M1', 'RD-M2-1', 'RD-M3', 'RD-M3-1', 'RD-M4', 'RD-Q2'},
)
DEVDIR_PIECE_UNIT_KPI_IDS: frozenset[str] = frozenset({'RD-M1', 'RD-M2-1', 'RD-M3-1'})
DEVDIR_PLAN_FACT_COLOR_IDS: frozenset[str] = DEVDIR_PIECE_UNIT_KPI_IDS
DEVDIR_RUB_UNIT_KPI_IDS: frozenset[str] = frozenset({'RD-M3', 'RD-M4'})


def _normalize_devdir_kpi_id(kpi_id: str) -> str:
    kid = str(kpi_id or '').strip().upper()
    for cyr, lat in (('М', 'M'), ('С', 'C'), ('Р', 'P')):
        kid = kid.replace(cyr, lat)
    return kid


_DEVDIR_TILE_IDS_NORM = frozenset(_normalize_devdir_kpi_id(x) for x in DEVDIR_KPI_IDS)


def is_devdir_tile_kpi_id(kpi_id: str) -> bool:
    return _normalize_devdir_kpi_id(kpi_id) in _DEVDIR_TILE_IDS_NORM


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    """Файлы кэша, по mtime которых на плитке показывается ``cache_updated_at`` (как TD-* / GSPP)."""
    kid = _normalize_devdir_kpi_id(kpi_id)
    paths: list[Path] = []

    if kid == 'RD-M1':
        paths.append(rd_m1_zpr.cache_file_path_for_period(ref_y, ref_m))
    elif kid == 'RD-M2-1':
        paths.extend([
            rd_m2_1.cache_file_path_for_period(ref_y, ref_m),
            OPE_PROJECTS_CACHE_PATH,
        ])
    elif kid == 'RD-M3':
        paths.append(rd_m3_budget.cache_file_path_for_period(ref_y, ref_m))
    elif kid == 'RD-M3-1':
        paths.extend([
            turboproject_projects_by_resources.cache_file_path_for_period(ref_y, ref_m),
            turboproject_projects_by_resources.CACHE_PATH,
        ])
    elif kid == 'RD-M4':
        paths.append(rd_m4_fot.cache_file_path_for_period(ref_y, ref_m))
    elif kid == 'RD-Q2':
        paths.append(rd_q2_tekuchest.cache_file_path_for_period(ref_y, ref_m))

    return paths


def rag_devdir_plan_fact_pct(pct: float | None) -> str:
    """RD-M1 / RD-M2-1 / RD-M3-1: ≥90 % — зелёный, 80–89,9 % — жёлтый, <80 % — красный."""
    if pct is None:
        return 'unknown'
    if pct >= 90:
        return 'green'
    if pct >= 80:
        return 'yellow'
    return 'red'


def kpi_pct_from_plan_fact(plan: object, fact: object) -> float | None:
    try:
        p = float(plan)
        f = float(fact)
    except (TypeError, ValueError):
        return None
    if p <= 0:
        return None
    return round(f / p * 100, 1)


def sync_devdir_piece_tile_color(tile: dict[str, Any]) -> None:
    """Плитки RD-M1/M2-1/M3-1: цвет и kpi_pct строго из plan/fact выбранного месяца."""
    kid = str(tile.get('kpi_id') or '').strip().upper()
    if kid not in DEVDIR_PLAN_FACT_COLOR_IDS:
        return
    pct = kpi_pct_from_plan_fact(tile.get('plan'), tile.get('fact'))
    if pct is None:
        pct = tile.get('kpi_pct')
        if pct is not None:
            pct = float(pct)
    else:
        tile['kpi_pct'] = pct
    if pct is not None:
        color = rag_devdir_plan_fact_pct(float(pct))
        tile['color'] = color
        tile['status_color'] = color
    tile['pct_higher_is_better'] = True
    tile['rag_direction'] = 'higher_better'


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
    if kpi_id == 'RD-M3-1':
        _fill_rd_m3_1_projects_monthly_tile(entry, year=year, month=month)
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


def _fill_rd_m3_1_projects_monthly_tile(
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> None:
    payload = turboproject_projects_by_resources.get_rd_m3_1_ytd(year=year, month=month)
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
