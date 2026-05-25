"""KPI RD-Q2: текучесть «Служба развития» (devservice / директор по развитию).

План: утверждённая таблица на 2026 (``rd_q2_tekuchest_plan``); иначе top2 из 1С.
Факт: уволено / штат × 100 % по HR (как TD-Q2).

Кэш: ``getkpi/dashboard/devdir_rd_q2_tekuchest_<год>_<месяц>.json``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from getkpi.techdir_tekuchet import TURNOVER_VALUES_UNIT, build_turnover_month_payload
from getkpi.turnover_hr_scope import TurnoverHrScope

from ..cache_manager import locked_call
from . import ytd_json_cache
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from .rd_q2_tekuchest_plan import plan_for_month as rd_q2_plan_for_month

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_q2_tekuchest"
CACHE_SOURCE_TAG = "devdir_rd_q2_tekuchest_ytd"
CACHE_VERSION = 10

RD_Q2_GROUP_ALIASES: dict[str, list[str]] = {
    "Служба развития": [
        "служба развития",
        "директор по развитию",
    ],
}
RD_Q2_GROUP_ORDER = list(RD_Q2_GROUP_ALIASES.keys())
RD_Q2_HR_SCOPE = TurnoverHrScope(
    group_aliases=RD_Q2_GROUP_ALIASES,
    group_order=RD_Q2_GROUP_ORDER,
)


def _turnover_kpi_pct(_plan: Any, fact: Any) -> float | None:
    if fact is None:
        return None
    try:
        return round(float(fact), 1)
    except (TypeError, ValueError):
        return None


def _build_rd_q2_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for m in range(1, ref_m + 1):
        snapshot = build_turnover_month_payload(
            ref_y,
            m,
            group_aliases=RD_Q2_GROUP_ALIASES,
            group_order=RD_Q2_GROUP_ORDER,
            aggregate="top2",
            fact_from_hr=True,
            hr_scope=RD_Q2_HR_SCOPE,
        )
        plan = rd_q2_plan_for_month(ref_y, m)
        if plan is None:
            plan = snapshot.get("total_plan")
        fact = snapshot.get("total_fact")
        has_data = plan is not None and fact is not None
        row: dict[str, Any] = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": _turnover_kpi_pct(plan, fact),
            "has_data": has_data,
            "values_unit": TURNOVER_VALUES_UNIT,
        }
        monthly_rows.append(row)
        if m == ref_m:
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
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": TURNOVER_VALUES_UNIT,
        },
        "debug": {
            "source": "Document_ТД_ТекучестьПерсонала + HR staffing/dismissals",
            "kpi_route": "devdir_rd_q2_tekuchest",
            "plan_source": "getkpi/devdir/rd_q2_tekuchest_plan.py (2026); иначе group_max_top2_1c_tekuchet",
            "fact_source": "hr_staff_dismissals_turnover_pct",
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
