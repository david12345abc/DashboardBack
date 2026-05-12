"""KPI RD-Q2: текучесть «Служба развития» по месяцам из ``calc_tekuchest_dev_service``.

Кэш: ``getkpi/dashboard/devdir_rd_q2_tekuchest_<год>_<месяц>.json`` — см. ``ytd_json_cache``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from ..cache_manager import locked_call
from . import calc_tekuchest_dev_service, ytd_json_cache
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_q2_tekuchest"
CACHE_SOURCE_TAG = "devdir_rd_q2_tekuchest_ytd"
CACHE_VERSION = 2


def _build_rd_q2_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    session = requests.Session()
    session.auth = calc_tekuchest_dev_service.AUTH
    by_month = calc_tekuchest_dev_service.fetch_yearly_monthly_totals(session, ref_y)

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        cell = by_month.get(m) or {"plan": 0.0, "fact": 0.0}
        plan_v = float(cell["plan"])
        fact_v = float(cell["fact"])
        has_data = plan_v > 0 or fact_v > 0
        kpi_pct = round(fact_v, 2)

        row: dict[str, Any] = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_v,
            "fact": fact_v,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "values_unit": "чел.",
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
            "values_unit": "чел.",
        },
        "debug": {
            "source": "getkpi/devdir/calc_tekuchest_dev_service.py",
            "kpi_route": "devdir_rd_q2_tekuchest",
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_FILE_PREFIX, year, month)


def get_rd_q2_tekuchest_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

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
            payload = _build_rd_q2_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-Q2 (текучесть)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                c_path,
                payload,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
        return payload

    return locked_call(f"devdir_rd_q2_tekuchest_{ref_y}_{ref_m:02d}", _runner)
