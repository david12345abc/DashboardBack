"""
TD-M5 — факт бюджета внешних заказов техдира (оплаты по заявкам ДС, ext_budj_fact).
"""
from __future__ import annotations

import logging
from itertools import combinations
from typing import Any

import requests

from . import techdir_m5_fact_cache
from .techdir_projects import (
    MONTH_NAMES,
    TARGET_PROJECT_TYPE_TD_M1,
    _project_is_alive_in_month,
    _projects_for_filter,
)

logger = logging.getLogger(__name__)

FACT_CRITERION = "payment"


def alive_project_names(
    target_projects: list[dict[str, Any]],
    year: int,
    month: int,
) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for project in target_projects:
        if not _project_is_alive_in_month(project, year, month):
            continue
        name = str(project.get("project_name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def month_fact_total(
    session: requests.Session,
    target_projects: list[dict[str, Any]],
    year: int,
    month: int,
    *,
    cache_stats: dict[str, int] | None = None,
) -> tuple[float, list[dict[str, Any]]]:
    """Сумма факта по всем живым в месяце проектам (кэш по проекту/месяцу)."""
    names = alive_project_names(target_projects, year, month)
    if not names:
        return 0.0, []
    return techdir_m5_fact_cache.compute_fact_totals_for_projects_cached(
        session,
        names,
        year,
        month,
        criterion=FACT_CRITERION,
        stats=cache_stats,
    )


def fact_sum_for_months(
    session: requests.Session,
    target_projects: list[dict[str, Any]],
    year: int,
    month_numbers: list[int],
    *,
    cache_stats: dict[str, int] | None = None,
) -> tuple[float, list[dict[str, Any]]]:
    """Факт за период = сумма помесячных оплат (по живым в каждом месяце проектам)."""
    total = 0.0
    by_month: list[dict[str, Any]] = []
    for month in month_numbers:
        month_total, details = month_fact_total(
            session,
            target_projects,
            year,
            month,
            cache_stats=cache_stats,
        )
        total += month_total
        by_month.append({
            "month": month,
            "year": year,
            "fact": month_total,
            "projects": details,
        })
    return round(total, 2), by_month


def period_fact_row(
    session: requests.Session,
    target_projects: list[dict[str, Any]],
    year: int,
    month_numbers: list[int],
    *,
    period_type: str,
    label: str,
    start_month: int | None = None,
    end_month: int | None = None,
    selected_quarters: list[int] | None = None,
    cache_stats: dict[str, int] | None = None,
) -> dict[str, Any]:
    fact_sum, _details = fact_sum_for_months(
        session,
        target_projects,
        year,
        month_numbers,
        cache_stats=cache_stats,
    )
    has_alive = any(
        alive_project_names(target_projects, year, m) for m in month_numbers
    )
    has_data = has_alive or fact_sum > 0
    return {
        "period_type": period_type,
        "year": year,
        "start_month": start_month,
        "end_month": end_month,
        "ranges": [
            {"year": year, "start_month": m, "end_month": m}
            for m in month_numbers
        ],
        "selected_quarters": selected_quarters,
        "label": label,
        "plan": None,
        "fact": fact_sum if has_data else None,
        "kpi_pct": None,
        "has_data": has_data,
        "values_unit": "руб.",
        "aggregation_strategy": "sum_monthly_payments_ext_budj_fact",
    }


def _quarter_month_range(quarter: int) -> tuple[int, int]:
    quarter_start = 3 * (quarter - 1) + 1
    return quarter_start, quarter_start + 2


def quarter_combination_fact_aggregates(
    session: requests.Session,
    target_projects: list[dict[str, Any]],
    ref_y: int,
    *,
    cache_stats: dict[str, int] | None = None,
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for size in range(1, 5):
        for selected in combinations([1, 2, 3, 4], size):
            selected_list = list(selected)
            months: list[int] = []
            for quarter in selected_list:
                start_m, end_m = _quarter_month_range(quarter)
                months.extend(range(start_m, end_m + 1))
            months = sorted(set(months))
            key = ",".join(str(quarter) for quarter in selected_list)
            label = "+".join(f"Q{quarter}" for quarter in selected_list) + f" {ref_y}"
            rows[key] = period_fact_row(
                session,
                target_projects,
                ref_y,
                months,
                period_type="selected_quarters",
                label=label,
                selected_quarters=selected_list,
                cache_stats=cache_stats,
            )
    return rows


def build_period_fact_aggregates(
    session: requests.Session,
    target_projects: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
    *,
    cache_stats: dict[str, int] | None = None,
) -> dict[str, dict[str, Any]]:
    quarter = (ref_m - 1) // 3 + 1
    quarter_start_month = 3 * (quarter - 1) + 1
    return {
        "month": period_fact_row(
            session,
            target_projects,
            ref_y,
            [ref_m],
            period_type="month",
            label=f"{MONTH_NAMES[ref_m]} {ref_y}",
            start_month=ref_m,
            end_month=ref_m,
            cache_stats=cache_stats,
        ),
        "quarter_to_date": period_fact_row(
            session,
            target_projects,
            ref_y,
            list(range(quarter_start_month, ref_m + 1)),
            period_type="quarter_to_date",
            label=f"Q{quarter} {ref_y}",
            start_month=quarter_start_month,
            end_month=ref_m,
            cache_stats=cache_stats,
        ),
        "year_to_date": period_fact_row(
            session,
            target_projects,
            ref_y,
            list(range(1, ref_m + 1)),
            period_type="year_to_date",
            label=f"Январь-{MONTH_NAMES[ref_m]} {ref_y}",
            start_month=1,
            end_month=ref_m,
            cache_stats=cache_stats,
        ),
        "quarter_combinations": quarter_combination_fact_aggregates(
            session,
            target_projects,
            ref_y,
            cache_stats=cache_stats,
        ),
    }


def target_projects() -> list[dict[str, Any]]:
    return _projects_for_filter(TARGET_PROJECT_TYPE_TD_M1)
