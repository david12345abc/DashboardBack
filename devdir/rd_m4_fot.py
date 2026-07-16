"""KPI RD-M4 (ФОТ) для devdir: план из rd_m4_fot_plan.

Факт — ``service_development_fot_fact`` (пять п/п контура развития в том же модуле).

Кэш: ``getkpi/dashboard/devdir_rd_m4_fot_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from . import ytd_json_cache
from .rd_m4_fot_plan import RD_M4_FOT_PLAN_BY_MONTH
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from .service_development_fot import (
    SERVICE_DEVELOPMENT_DEPARTMENTS,
    service_development_fot_fact,
)

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_m4_fot"
CACHE_SOURCE_TAG = "devdir_rd_m4_fot_ytd"
CACHE_VERSION = 1


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
            "target_fact_source": "getkpi/devdir/service_development_fot.py (service_development_fot_fact)",
            "kpi_route": "devdir_rd_m4",
            "fact_methodology": (
                "счёт 26, две статьи НПО АУП; сумма Дт по пяти подразделениям контура развития"
            ),
            "departments_in_fact_sum": list(SERVICE_DEVELOPMENT_DEPARTMENTS),
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


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_FILE_PREFIX, year, month)


def get_rd_m4_fot_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict | None:
        try:
            payload = _build_rd_m4_fot_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M4 (ФОТ)")
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
        lock_key=f"devdir_rd_m4_fot_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )
