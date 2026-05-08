"""KPI RD-M4 (ФОТ): план из rd_m4_fot_plan, факт ФОТ «Служба развития»."""
from __future__ import annotations

import logging
from typing import Any

from ..cache_manager import locked_call
from .rd_m4_fot_plan import RD_M4_FOT_PLAN_BY_MONTH
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from .service_development_fot import SERVICE_DEVELOPMENT_DEPARTMENT, service_development_fot_fact

logger = logging.getLogger(__name__)


def _build_rd_m4_fot_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        plan_value = RD_M4_FOT_PLAN_BY_MONTH.get(m)
        fact_value = service_development_fot_fact(y, m)
        has_data = plan_value is not None or fact_value is not None
        kpi_pct = round(fact_value / plan_value * 100, 1) if plan_value and fact_value is not None else None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_value,
            "fact": fact_value,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

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
            "total_plan": round(sum(float(row.get("plan") or 0) for row in monthly_rows if row.get("plan") is not None), 2) if monthly_rows else None,
            "total_fact": round(sum(float(row.get("fact") or 0) for row in monthly_rows if row.get("fact") is not None), 2) if monthly_rows else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "target_plan_source": "getkpi/devdir/rd_m4_fot_plan.py",
            "target_fact_source": "dashboard/fot_management_monthly_YYYY_MM.json",
            "target_department": SERVICE_DEVELOPMENT_DEPARTMENT,
            "rows_by_month": [
                {
                    "year": row["year"],
                    "month": row["month"],
                    "plan": row["plan"],
                    "fact": row["fact"],
                }
                for row in monthly_rows
            ],
        },
    }


def get_rd_m4_fot_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_rd_m4_fot_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M4 (ФОТ)")
            return None

    return locked_call("devdir_rd_m4", _runner)
