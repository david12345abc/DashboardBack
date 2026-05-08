"""KPI RD-M2-1: пока заглушки — план и факт по месяцам нули (без OData / service_development_fot)."""

from __future__ import annotations

import logging
from typing import Any

from ..cache_manager import locked_call
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)


def _build_rd_m2_1_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        plan_value = 0.0
        fact_value = 0.0
        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_value,
            "fact": fact_value,
            "kpi_pct": 0.0,
            "has_data": True,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

    assert ref_row is not None

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": 0.0,
            "total_fact": 0.0,
            "kpi_pct": 0.0,
            "months_with_data": len(monthly_rows),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
    }


def get_rd_m2_1_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_rd_m2_1_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M2-1")
            return None

    return locked_call("devdir_rd_m2_1_stubs", _runner)
