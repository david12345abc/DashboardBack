"""
TD-M5 — план бюджета внешних заказов техдира (TurboProject, byudzhet_plan).
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
from itertools import combinations
from typing import Any

from .techdir_projects import (
    MONTH_NAMES,
    TARGET_PROJECT_TYPE_TD_M1,
    _project_is_alive_in_month,
    _project_is_alive_in_range,
    _projects_for_filter,
    _safe_float,
)


def plan_fact_kpi_pct(plan: float, fact: float) -> float:
    if plan == 0:
        return 0.0
    return round(fact / plan * 100, 2)


def project_dedupe_key(project: dict[str, Any]) -> str:
    for key in ("project_code", "file_id"):
        value = project.get(key)
        if value is not None and str(value).strip():
            return f"{key}:{str(value).strip()}"
    from .techdir_projects import _project_date_bounds

    start, end = _project_date_bounds(project)
    return "|".join((
        str(project.get("project_name") or "").strip(),
        str(project.get("project_manager") or "").strip(),
        start.isoformat() if start else "",
        end.isoformat() if end else "",
    ))


def month_plan_total(
    target_projects: list[dict[str, Any]],
    year: int,
    month: int,
) -> float:
    """Сумма byudzhet_plan по проектам, живым в месяце."""
    plan_sum = 0.0
    for project in target_projects:
        if not _project_is_alive_in_month(project, year, month):
            continue
        pv = _safe_float(project.get("byudzhet_plan"))
        if pv is not None:
            plan_sum += pv
    return round(plan_sum, 2)


def period_plan_row(
    target_projects: list[dict[str, Any]],
    ranges: list[tuple[int, int, int]],
    *,
    period_type: str,
    label: str,
    year: int,
    start_month: int | None = None,
    end_month: int | None = None,
    selected_quarters: list[int] | None = None,
) -> dict[str, Any]:
    """План за период: каждый проект один раз, если жив хотя бы в одном месяце диапазона."""
    plan_sum = 0.0
    seen_projects: set[str] = set()

    for project in target_projects:
        is_alive = False
        for range_year, range_start_month, range_end_month in ranges:
            period_start = date(range_year, range_start_month, 1)
            period_end = date(
                range_year,
                range_end_month,
                monthrange(range_year, range_end_month)[1],
            )
            if _project_is_alive_in_range(project, period_start, period_end):
                is_alive = True
                break
        if not is_alive:
            continue
        dedupe_key = project_dedupe_key(project)
        if dedupe_key in seen_projects:
            continue
        seen_projects.add(dedupe_key)
        pv = _safe_float(project.get("byudzhet_plan"))
        if pv is not None:
            plan_sum += pv

    plan_sum = round(plan_sum, 2)
    has_data = bool(seen_projects)
    return {
        "period_type": period_type,
        "year": year,
        "start_month": start_month,
        "end_month": end_month,
        "ranges": [
            {"year": range_year, "start_month": range_start, "end_month": range_end}
            for range_year, range_start, range_end in ranges
        ],
        "selected_quarters": selected_quarters,
        "label": label,
        "plan": plan_sum if has_data else None,
        "fact": None,
        "kpi_pct": None,
        "has_data": has_data,
        "project_count": len(seen_projects),
        "values_unit": "руб.",
        "aggregation_strategy": "unique_projects_alive_in_period_plan",
    }


def _quarter_month_range(quarter: int) -> tuple[int, int]:
    quarter_start = 3 * (quarter - 1) + 1
    return quarter_start, quarter_start + 2


def quarter_combination_plan_aggregates(
    target_projects: list[dict[str, Any]],
    ref_y: int,
) -> dict[str, dict[str, Any]]:
    available_quarters = [1, 2, 3, 4]
    rows: dict[str, dict[str, Any]] = {}
    for size in range(1, len(available_quarters) + 1):
        for selected in combinations(available_quarters, size):
            selected_list = list(selected)
            ranges = [
                (ref_y, *_quarter_month_range(quarter))
                for quarter in selected_list
            ]
            key = ",".join(str(quarter) for quarter in selected_list)
            label = "+".join(f"Q{quarter}" for quarter in selected_list) + f" {ref_y}"
            rows[key] = period_plan_row(
                target_projects,
                ranges,
                period_type="selected_quarters",
                label=label,
                year=ref_y,
                selected_quarters=selected_list,
            )
    return rows


def build_period_plan_aggregates(
    target_projects: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> dict[str, dict[str, Any]]:
    quarter = (ref_m - 1) // 3 + 1
    quarter_start_month = 3 * (quarter - 1) + 1
    return {
        "month": period_plan_row(
            target_projects,
            [(ref_y, ref_m, ref_m)],
            period_type="month",
            label=f"{MONTH_NAMES[ref_m]} {ref_y}",
            year=ref_y,
            start_month=ref_m,
            end_month=ref_m,
        ),
        "quarter_to_date": period_plan_row(
            target_projects,
            [(ref_y, quarter_start_month, ref_m)],
            period_type="quarter_to_date",
            label=f"Q{quarter} {ref_y}",
            year=ref_y,
            start_month=quarter_start_month,
            end_month=ref_m,
        ),
        "year_to_date": period_plan_row(
            target_projects,
            [(ref_y, 1, ref_m)],
            period_type="year_to_date",
            label=f"Январь-{MONTH_NAMES[ref_m]} {ref_y}",
            year=ref_y,
            start_month=1,
            end_month=ref_m,
        ),
        "quarter_combinations": quarter_combination_plan_aggregates(target_projects, ref_y),
    }


def target_projects() -> list[dict[str, Any]]:
    return _projects_for_filter(TARGET_PROJECT_TYPE_TD_M1)
