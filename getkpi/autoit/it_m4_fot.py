"""KPI ИТ-M4 (ФОТ): план из it_m4_fot_plan, факт из it_m4_fot_fact.

Кэш:
  • помесячно — ``getkpi/dashboard/autoit_it_m4_fot_fact_monthly_<год>_<месяц>.json``;
  • YTD-плитка — ``getkpi/dashboard/autoit_it_m4_fot_<год>_<месяц>.json``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache

from .it_m4_fot_fact import compute_it_m4_fot_fact_monthly
from .it_m4_fot_plan import IT_M4_FOT_PLAN_BY_MONTH_2026
from .it_monthly_period import MONTH_NAMES, normalize_it_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "autoit_it_m4_fot"
CACHE_SOURCE_TAG = "autoit_it_m4_fot_ytd"
CACHE_VERSION = 2

MONTHLY_CACHE_PREFIX = "autoit_it_m4_fot_fact_monthly"
MONTHLY_SOURCE_TAG = "autoit_it_m4_fot_fact_monthly_v1"
MONTHLY_CACHE_VERSION = 1


def monthly_cache_path(year: int, month: int) -> Path:
    return ytd_json_cache.cache_path(MONTHLY_CACHE_PREFIX, year, month)


def _monthly_cache_is_perpetual(year: int, month: int) -> bool:
    return ytd_json_cache.is_ref_period_fully_past(year, month)


def get_it_m4_fot_fact_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт ФОТ за один месяц с дисковым кэшем."""
    path = monthly_cache_path(year, month)
    perpetual = _monthly_cache_is_perpetual(year, month)

    def _compute_and_save() -> dict[str, Any]:
        payload = compute_it_m4_fot_fact_monthly(year, month)
        ytd_json_cache.save_payload(
            path,
            payload,
            source_tag=MONTHLY_SOURCE_TAG,
            version=MONTHLY_CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        path,
        source_tag=MONTHLY_SOURCE_TAG,
        version=MONTHLY_CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"autoit_it_m4_fot_fact_monthly_{year}_{month:02d}",
        compute_fn=_compute_and_save,
    )


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in IT_M4_FOT_PLAN_BY_MONTH_2026:
        return float(IT_M4_FOT_PLAN_BY_MONTH_2026[month])
    return None


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _build_it_m4_fot_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for m in range(1, ref_m + 1):
        plan = _plan_for_month(ref_y, m)
        fact_payload = get_it_m4_fot_fact_monthly(ref_y, m)
        fact_raw = fact_payload.get("total_fact")
        fact_value = float(fact_raw) if fact_raw is not None else None
        has_data = plan is not None and fact_value is not None
        row = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": round(plan, 2) if plan is not None else None,
            "fact": round(fact_value, 2) if fact_value is not None else None,
            "kpi_pct": _kpi_pct(plan, fact_value) if has_data else None,
            "has_data": has_data,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        if m == ref_m:
            ref_row = row

    with_data = [row for row in monthly_rows if row.get("has_data")]
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": len(with_data),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok" if with_data else "no_data",
            "kpi_id": "IT-M4",
            "plan_source": "getkpi/autoit/it_m4_fot_plan.py",
            "fact_source": "getkpi/autoit/it_m4_fot_fact.py",
            "monthly_cache_prefix": MONTHLY_CACHE_PREFIX,
            "monthly_cache_version": MONTHLY_CACHE_VERSION,
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)


def get_it_m4_fot_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    cache_path = cache_file_path_for_period(year, month)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict | None:
        try:
            payload = _build_it_m4_fot_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте ИТ-M4 (ФОТ)")
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
        lock_key=f"autoit_it_m4_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )
