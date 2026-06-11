"""KPI ИТ-Q2: текучесть «Служба автоматизации» (autoit).

План: top2 из Document_ТД_ТекучестьПерсонала (одна группа → max по 1С).
Факт: уволено / штат × 100 % на конец месяца (HR, как TD-Q2 / RD-Q2).

Кэш: ``getkpi/dashboard/autoit_it_q2_tekuchest_<год>_<месяц>.json``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from getkpi.cache_manager import locked_call
from devdir import ytd_json_cache
from getkpi.techdir_tekuchet import TURNOVER_VALUES_UNIT, build_turnover_month_payload
from getkpi.turnover_hr_scope import TurnoverHrScope

from .it_monthly_period import MONTH_NAMES, normalize_it_tile_period

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "autoit_it_q2_tekuchest"
CACHE_SOURCE_TAG = "autoit_it_q2_tekuchest_ytd"
CACHE_VERSION = 4

IT_Q2_GROUP_ALIASES: dict[str, list[str]] = {
    "Служба автоматизации": [
        "служба автоматизации",
        "отдел автоматизации",
        "начальник отдела автоматизации",
        "автоматизации ит",
        "оит",
    ],
}
IT_Q2_GROUP_ORDER = list(IT_Q2_GROUP_ALIASES.keys())
IT_Q2_HR_SCOPE = TurnoverHrScope(
    group_aliases=IT_Q2_GROUP_ALIASES,
    group_order=IT_Q2_GROUP_ORDER,
)


def _turnover_kpi_pct(_plan: Any, fact: Any) -> float | None:
    """На плитке текучести KPI = факт (%), не факт/план."""
    if fact is None:
        return None
    try:
        return round(float(fact), 1)
    except (TypeError, ValueError):
        return None


def _build_it_q2_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for m in range(1, ref_m + 1):
        snapshot = build_turnover_month_payload(
            ref_y,
            m,
            group_aliases=IT_Q2_GROUP_ALIASES,
            group_order=IT_Q2_GROUP_ORDER,
            aggregate="top2",
            fact_from_hr=True,
            hr_scope=IT_Q2_HR_SCOPE,
        )
        plan = snapshot.get("total_plan")
        fact = snapshot.get("total_fact")
        plan_val = float(plan) if plan is not None else None
        fact_val = float(fact) if fact is not None else None
        has_data = fact_val is not None
        row: dict[str, Any] = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_val,
            "fact": fact_val,
            "kpi_pct": _turnover_kpi_pct(plan_val, fact_val),
            "has_data": has_data,
            "values_unit": TURNOVER_VALUES_UNIT,
        }
        monthly_rows.append(row)
        if m == ref_m:
            ref_row = row

    ref_row_out = dict(ref_row) if ref_row and ref_row.get("fact") is not None else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": ref_row_out,
        "kpi_period": {
            "type": "current_month",
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
            "kpi_route": "autoit_it_q2_tekuchest",
            "kpi_id": "IT-Q2",
            "plan_source": "group_max_top2_1c_tekuchet",
            "fact_source": "hr_staff_dismissals_turnover_pct",
            "department": IT_Q2_GROUP_ORDER[0],
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_it_tile_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)


def get_it_q2_tekuchest_ytd(year: int | None = None, month: int | None = None) -> dict | None:
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
            payload = _build_it_q2_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте ИТ-Q2 (текучесть)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                cache_path,
                payload,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
        return payload

    return locked_call(f"autoit_it_q2_tekuchest_{ref_y}_{ref_m:02d}", _runner)
