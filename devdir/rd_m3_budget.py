"""KPI RD-M3 (бюджет): план из rd_m3_budget_plan; факт из calc_budj_dev_service_fact.

Кэш: ``getkpi/dashboard/devdir_rd_m3_budget_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from qualdir.turnover import _qd_q2_kpi_pct

from . import ytd_json_cache
from .calc_budj_dev_service_fact import (
    compute_dev_service_budget_fact_monthly,
    load_dev_service_department_keys,
    open_budget_fact_session,
)
from .rd_m3_budget_plan import RD_M3_BUDGET_PLAN_BY_MONTH
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_m3_budget"
CACHE_SOURCE_TAG = "devdir_rd_m3_budget_ytd"
CACHE_VERSION = 2


def _prior_monthly_rows_from_cache(ref_y: int, ref_m: int) -> list[dict[str, Any]]:
    if ref_m <= 1:
        return []
    prev_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m - 1)
    prev_payload = ytd_json_cache.load_payload(
        prev_path,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        perpetual=True,
    )
    if not prev_payload:
        return []
    rows = prev_payload.get("monthly_data") or []
    if not isinstance(rows, list) or len(rows) != ref_m - 1:
        return []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            return []
        if row.get("month") != idx or row.get("year") != ref_y:
            return []
    return [dict(row) for row in rows]


def _build_rd_m3_budget_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = _prior_monthly_rows_from_cache(ref_y, ref_m)
    first_month_to_compute = len(monthly_rows) + 1

    ref_row: dict[str, Any] | None = None
    ref_fact_debug: dict[str, Any] | None = None

    session = open_budget_fact_session()
    dept_keys = load_dev_service_department_keys(session)

    for m in range(first_month_to_compute, ref_m + 1):
        plan_value = RD_M3_BUDGET_PLAN_BY_MONTH.get(m)
        plan_float = float(plan_value) if plan_value is not None else None
        fact_payload = compute_dev_service_budget_fact_monthly(
            ref_y,
            m,
            session=session,
            dept_keys=dept_keys,
        )
        fact_raw = fact_payload.get("total_fact")
        fact_value = float(fact_raw) if fact_raw is not None else None
        has_data = plan_float is not None and fact_value is not None
        kpi_pct = _qd_q2_kpi_pct(plan_float, fact_value) if has_data else None

        row = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_float,
            "fact": round(fact_value, 2) if fact_value is not None else None,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        if m == ref_m:
            ref_row = row
            ref_fact_debug = fact_payload.get("debug")

    if not ref_row and monthly_rows:
        ref_row = monthly_rows[-1]

    plan_sum = sum(float(r["plan"]) for r in monthly_rows if r.get("plan") is not None)
    fact_sum = sum(float(r["fact"]) for r in monthly_rows if r.get("fact") is not None)

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
            "total_plan": round(plan_sum, 2) if monthly_rows else None,
            "total_fact": round(fact_sum, 2) if monthly_rows and plan_sum else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok" if ref_row and ref_row.get("has_data") else "partial_or_no_data",
            "kpi_id": "RD-M3",
            "plan_source": "devdir/rd_m3_budget_plan.py (БЮДЖЕТ ПЛАН)",
            "fact_source": "devdir/calc_budj_dev_service_fact.py (заявки, Служба развития + поддерево)",
            "last_month_fact_debug": ref_fact_debug,
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_FILE_PREFIX, year, month)


def get_rd_m3_budget_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ry, rm = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ry, rm)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ry, rm)

    def _compute_and_save() -> dict | None:
        try:
            payload = _build_rd_m3_budget_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M3 (бюджет)")
            stale = ytd_json_cache.load_stale_payload(
                c_path,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
            if stale is not None:
                return stale
            return None
        ytd_json_cache.save_payload(
            c_path,
            payload,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        c_path,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"devdir_rd_m3_budget_{ry}_{rm:02d}",
        compute_fn=_compute_and_save,
    )
