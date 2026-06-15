"""KPI ИТ-M3 / IT-M3 (бюджет): план из it_m3_plan, факт из it_m3_fact.

Кэш: ``getkpi/dashboard/autoit_it_m3_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from getkpi.cache_manager import locked_call
from devdir import ytd_json_cache
from qualdir.turnover import _qd_q2_kpi_pct

from .it_m3_fact import compute_it_m3_fact_monthly
from .it_m3_plan import IT_M3_PLAN_BY_MONTH_2026
from .it_monthly_period import MONTH_NAMES, normalize_it_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "autoit_it_m3"
CACHE_SOURCE_TAG = "autoit_it_m3_ytd"
CACHE_VERSION = 2


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in IT_M3_PLAN_BY_MONTH_2026:
        return float(IT_M3_PLAN_BY_MONTH_2026[month])
    return None


def _build_it_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for m in range(1, ref_m + 1):
        plan = _plan_for_month(ref_y, m)
        fact_payload = compute_it_m3_fact_monthly(ref_y, m)
        fact_raw = fact_payload.get("total_fact")
        fact_value = float(fact_raw) if fact_raw is not None else None
        has_data = plan is not None and fact_value is not None
        row = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": round(plan, 2) if plan is not None else None,
            "fact": round(fact_value, 2) if fact_value is not None else None,
            "kpi_pct": _qd_q2_kpi_pct(plan, fact_value) if has_data else None,
            "has_data": has_data,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        if m == ref_m:
            ref_row = row

    with_plan = [row for row in monthly_rows if row.get("has_data")]
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
            "months_with_data": len(with_plan),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok" if with_plan else "no_data",
            "kpi_id": "IT-M3",
            "plan_source": "getkpi/autoit/it_m3_plan.py (сумма 11 строк × месяц)",
            "fact_source": "getkpi/autoit/it_m3_fact.py",
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)


def get_it_m3_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    cache_path = cache_file_path_for_period(year, month)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_it_m3_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте ИТ-M3 (бюджет)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                cache_path,
                payload,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
        return payload

    return locked_call(f"autoit_it_m3_{ref_y}_{ref_m:02d}", _runner)
