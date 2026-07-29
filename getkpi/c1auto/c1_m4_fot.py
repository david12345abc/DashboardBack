"""KPI 1С-M4 (ФОТ): план из c1_m4_fot_plan, факт из c1_m4_fot_fact.

Кэш: ``getkpi/dashboard/c1auto_c1_m4_fot_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache

from getkpi.autoit.it_monthly_period import (
    MONTH_NAMES,
    normalize_it_tile_period,
    pick_fot_display_row,
    trim_monthly_rows_to_display,
)

from .c1_m4_fot_fact import compute_c1_m4_fot_fact_monthly
from .c1_m4_fot_plan import C1_M4_FOT_PLAN_BY_MONTH_2026

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "c1auto_c1_m4_fot"
CACHE_SOURCE_TAG = "c1auto_c1_m4_fot_ytd_sql_v1"
CACHE_VERSION = 4


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in C1_M4_FOT_PLAN_BY_MONTH_2026:
        return float(C1_M4_FOT_PLAN_BY_MONTH_2026[month])
    return None


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _build_c1_m4_fot_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []

    for m in range(1, ref_m + 1):
        plan = _plan_for_month(ref_y, m)
        fact_payload = compute_c1_m4_fot_fact_monthly(ref_y, m)
        fact_raw = fact_payload.get("total_fact")
        fact_value = float(fact_raw) if fact_raw is not None else None
        has_data = plan is not None and fact_value is not None
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": round(plan, 2) if plan is not None else None,
                "fact": round(fact_value, 2) if fact_value is not None else None,
                "kpi_pct": _kpi_pct(plan, fact_value) if has_data else None,
                "has_data": has_data,
                "values_unit": "руб.",
            }
        )

    with_data = [row for row in monthly_rows if row.get("has_data")]
    display_row = pick_fot_display_row(monthly_rows, ref_m, ref_year=ref_y)
    monthly_rows = trim_monthly_rows_to_display(monthly_rows, display_row)
    display_m = int(display_row["month"]) if display_row and display_row.get("month") else ref_m
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
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok" if with_data else "no_data",
            "kpi_id": "1C-M4",
            "plan_source": "getkpi/c1auto/c1_m4_fot_plan.py",
            "fact_source": "getkpi/c1auto/c1_m4_core.py (SQL _AccRg2005 сч.26)",
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)


def get_c1_m4_fot_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    cache_path = cache_file_path_for_period(year, month)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict | None:
        try:
            payload = _build_c1_m4_fot_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте 1С-M4 (ФОТ)")
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
        lock_key=f"c1auto_c1_m4_fot_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )
