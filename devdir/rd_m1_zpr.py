"""KPI RD-M1 (ЗПР): план и факт по месяцам из ``calc_zpr_plan`` / ``calc_zpr_fact``.

Кэш: ``getkpi/dashboard/devdir_rd_m1_zpr_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from devdir import calc_zpr_fact, calc_zpr_plan, ytd_json_cache
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_m1_zpr"
CACHE_SOURCE_TAG = "devdir_rd_m1_zpr_ytd"
CACHE_VERSION = 2


def _build_rd_m1_zpr_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    session = requests.Session()
    session.auth = calc_zpr_plan.AUTH

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        plan_v = float(calc_zpr_plan.count_zpr(session, y, m))
        fact_v = float(calc_zpr_fact.count_zpr_fact(session, y, m))
        has_data = plan_v > 0 or fact_v > 0
        kpi_pct = round(fact_v / plan_v * 100, 1) if plan_v > 0 else None

        row: dict[str, Any] = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_v,
            "fact": fact_v,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "values_unit": "шт.",
        }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

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
            "total_fact": round(fact_sum, 2) if monthly_rows else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "plan_source": "devdir/calc_zpr_plan.py",
            "fact_source": "devdir/calc_zpr_fact.py",
            "kpi_route": "devdir_rd_m1_zpr",
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_FILE_PREFIX, year, month)


def get_rd_m1_zpr_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict | None:
        try:
            payload = _build_rd_m1_zpr_monthly_payload(year=year, month=month)
        except Exception as exc:
            logger.exception("Ошибка при расчёте RD-M1 (ЗПР)")
            stale = ytd_json_cache.load_stale_payload(
                c_path,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
            if stale is not None:
                debug = dict(stale.get("debug") or {})
                debug.update(
                    {
                        "status": "stale_cache",
                        "odata_error": str(exc)[:500],
                        "cache_date_fallback": True,
                    }
                )
                stale["debug"] = debug
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
        lock_key=f"devdir_rd_m1_zpr_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )
