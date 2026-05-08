"""KPI RD-M3 (бюджет): план из rd_m3_budget_plan; факт временно синтетический (до подключения БДР / 1С).

Кэш: ``getkpi/dashboard/devdir_rd_m3_budget_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from ..cache_manager import locked_call
from . import ytd_json_cache
from .rd_m3_budget_plan import RD_M3_BUDGET_PLAN_BY_MONTH
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_m3_budget"
CACHE_SOURCE_TAG = "devdir_rd_m3_budget_ytd"
CACHE_VERSION = 1

# Доля плана → «факт»: детерминированно от (год, месяц), одинаково на всех воркерах.
_SYNTH_FACT_RATIO_MIN = 0.88
_SYNTH_FACT_RATIO_MAX = 1.06


def _synthetic_budget_fact_rub(plan: float, *, year: int, month: int) -> float:
    h = hashlib.sha256(f"RD-M3|synth_fact|{year}|{month}|v1".encode()).digest()
    u = int.from_bytes(h[:8], "big") / (2**64)
    ratio = _SYNTH_FACT_RATIO_MIN + u * (_SYNTH_FACT_RATIO_MAX - _SYNTH_FACT_RATIO_MIN)
    return round(float(plan) * ratio, 2)


def _build_rd_m3_budget_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        plan_value = RD_M3_BUDGET_PLAN_BY_MONTH.get(m)
        plan_float = float(plan_value) if plan_value is not None else None
        fact_value = (
            _synthetic_budget_fact_rub(plan_float, year=y, month=m)
            if plan_float is not None
            else None
        )
        has_data = plan_value is not None
        kpi_pct = (
            round(fact_value / plan_float * 100, 1)
            if plan_float and plan_float > 0 and fact_value is not None
            else None
        )

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_float,
            "fact": fact_value,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

    plan_sum = sum(float(r["plan"]) for r in monthly_rows if r.get("plan") is not None)
    fact_sum = sum(
        float(r["fact"])
        for r in monthly_rows
        if r.get("fact") is not None
    )

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
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_FILE_PREFIX, year, month)


def get_rd_m3_budget_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ry, rm = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ry, rm)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ry, rm)

    def _runner() -> dict | None:
        cached = ytd_json_cache.load_payload(
            c_path,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_rd_m3_budget_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M3 (бюджет)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                c_path,
                payload,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
        return payload

    return locked_call(f"devdir_rd_m3_budget_{ry}_{rm:02d}", _runner)
