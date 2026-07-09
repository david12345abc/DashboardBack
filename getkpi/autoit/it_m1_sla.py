"""KPI ИТ-M1 (SLA заявок в IT): план/факт из ``Document_ТД_ЗаявкаВСлужбуСопровождения``.

Кэш:
  • помесячно — ``getkpi/dashboard/autoit_it_m1_sla_monthly_<год>_<месяц>.json``;
  • YTD-плитка — ``getkpi/dashboard/autoit_it_m1_sla_<год>_<месяц>.json``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache

from .it_m1_sla_data import compute_it_m1_sla_monthly
from .it_monthly_period import MONTH_NAMES, normalize_it_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "autoit_it_m1_sla"
CACHE_SOURCE_TAG = "autoit_it_m1_sla_ytd"
CACHE_VERSION = 3

MONTHLY_CACHE_PREFIX = "autoit_it_m1_sla_monthly"
MONTHLY_SOURCE_TAG = "autoit_it_m1_sla_monthly_v1"
MONTHLY_CACHE_VERSION = 1


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    try:
        pv = float(plan)
        fv = float(fact)
    except (TypeError, ValueError):
        return None
    if pv <= 0:
        return None
    return round(fv / pv * 100, 1)


def monthly_cache_path(year: int, month: int) -> Path:
    return ytd_json_cache.cache_path(MONTHLY_CACHE_PREFIX, year, month)


def _monthly_cache_is_perpetual(year: int, month: int) -> bool:
    return ytd_json_cache.is_ref_period_fully_past(year, month)


def _load_monthly_cache(year: int, month: int) -> dict[str, Any] | None:
    return ytd_json_cache.load_payload(
        monthly_cache_path(year, month),
        source_tag=MONTHLY_SOURCE_TAG,
        version=MONTHLY_CACHE_VERSION,
        perpetual=_monthly_cache_is_perpetual(year, month),
    )


def _save_monthly_cache(year: int, month: int, payload: dict[str, Any]) -> None:
    ytd_json_cache.save_payload(
        monthly_cache_path(year, month),
        payload,
        source_tag=MONTHLY_SOURCE_TAG,
        version=MONTHLY_CACHE_VERSION,
    )


def get_it_m1_sla_monthly(year: int, month: int) -> dict[str, Any]:
    """План/факт SLA за один месяц с дисковым кэшем."""
    path = monthly_cache_path(year, month)
    perpetual = _monthly_cache_is_perpetual(year, month)

    def _compute_and_save() -> dict[str, Any]:
        payload = compute_it_m1_sla_monthly(year, month)
        _save_monthly_cache(year, month, payload)
        return payload

    return ytd_json_cache.resolve_payload(
        path,
        source_tag=MONTHLY_SOURCE_TAG,
        version=MONTHLY_CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"autoit_it_m1_sla_monthly_{year}_{month:02d}",
        compute_fn=_compute_and_save,
    )


def _month_row_from_snapshot(ref_y: int, m: int, snapshot: dict[str, Any]) -> dict[str, Any]:
    plan_raw = snapshot.get("plan")
    fact_raw = snapshot.get("fact")
    plan = float(plan_raw) if plan_raw is not None else None
    fact = float(fact_raw) if fact_raw is not None else None
    has_data = plan is not None and plan > 0
    return {
        "month": m,
        "year": ref_y,
        "month_name": MONTH_NAMES[m],
        "plan": plan,
        "fact": fact,
        "kpi_pct": _kpi_pct(plan, fact) if has_data else None,
        "has_data": has_data,
        "values_unit": "шт.",
    }


def _display_row_for_ref_period(
    monthly_rows: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> dict[str, Any] | None:
    """Строка для плитки: опорный месяц, если в нём есть данные, иначе последний полный."""
    ref_row: dict[str, Any] | None = None
    last_with_data: dict[str, Any] | None = None
    for row in monthly_rows:
        if not isinstance(row, dict):
            continue
        if row.get("month") == ref_m and row.get("year") == ref_y:
            ref_row = row
        if row.get("has_data"):
            last_with_data = row
    if ref_row and ref_row.get("has_data"):
        return ref_row
    return last_with_data


def _ytd_payload_matches_monthlies(
    cached: dict[str, Any],
    ref_y: int,
    ref_m: int,
) -> bool:
    """YTD-агрегат устарел, если строки monthly_data не совпадают с помесячными кэшами."""
    rows = cached.get("monthly_data") or []
    if not isinstance(rows, list):
        return False
    by_month = {
        int(row.get("month")): row
        for row in rows
        if isinstance(row, dict) and row.get("month") is not None
    }
    for m in range(1, ref_m + 1):
        cached_row = by_month.get(m)
        if cached_row is None:
            return False
        snapshot = _load_monthly_cache(ref_y, m)
        if snapshot is None:
            return False
        try:
            plan_ok = float(cached_row.get("plan") or -1) == float(snapshot.get("plan") or -2)
            fact_ok = float(cached_row.get("fact") or -1) == float(snapshot.get("fact") or -2)
        except (TypeError, ValueError):
            return False
        if not (plan_ok and fact_ok):
            return False
    return True


def _build_it_m1_sla_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    monthly_debug: list[dict[str, Any]] = []

    for m in range(1, ref_m + 1):
        snapshot = get_it_m1_sla_monthly(ref_y, m)
        row = _month_row_from_snapshot(ref_y, m, snapshot)
        monthly_rows.append(row)
        monthly_debug.append(
            {
                "month": m,
                "cache_file": str(monthly_cache_path(ref_y, m).name),
                "counts": snapshot.get("counts") or {},
            }
        )

    display_row = _display_row_for_ref_period(monthly_rows, ref_y, ref_m)
    with_data = [row for row in monthly_rows if row.get("has_data")]
    display_m = int(display_row["month"]) if display_row else ref_m
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(display_row) if display_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": display_m,
            "month_name": MONTH_NAMES[display_m],
        },
        "ytd": {
            "total_plan": display_row.get("plan") if display_row else None,
            "total_fact": display_row.get("fact") if display_row else None,
            "kpi_pct": display_row.get("kpi_pct") if display_row else None,
            "months_with_data": len(with_data),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "status": "ok" if with_data else "no_data",
            "kpi_id": "IT-M1",
            "plan_source": "getkpi/autoit/it_m1_sla_data.py (все заявки ОИТ за месяц)",
            "fact_source": "getkpi/autoit/it_m1_sla_data.py (подтверждение автора в месяце)",
            "monthly_cache_prefix": MONTHLY_CACHE_PREFIX,
            "monthly_cache_version": MONTHLY_CACHE_VERSION,
            "monthly_debug": monthly_debug,
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)


def get_it_m1_sla_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    cache_path = cache_file_path_for_period(year, month)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None and _ytd_payload_matches_monthlies(cached, ref_y, ref_m):
            return cached
        try:
            payload = _build_it_m1_sla_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте ИТ-M1 (SLA)")
            stale = ytd_json_cache.load_stale_payload(
                cache_path,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
            if stale is not None:
                return stale
            return None
        ytd_json_cache.save_payload(
            cache_path,
            payload,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        cache_path,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"autoit_it_m1_sla_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )
