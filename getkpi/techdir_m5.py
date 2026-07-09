"""
TD-M5 — бюджет проектов по внешним заказам техдира.

План: TurboProject (byudzhet_plan), модуль techdir_m5_plan.
Факт: оплаты по заявкам ДС (ext_budj_fact) по всем живым в месяце проектам.
Кэш: по проекту/месяцу (techdir_m5_fact_cache) и YTD-payload плитки (JSON).
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import requests

from . import cache_manager
from . import ext_budj_fact
from . import techdir_cache
from . import techdir_m5_fact_cache
from .techdir_m5_fact import (
    FACT_CRITERION,
    alive_project_names,
    build_period_fact_aggregates,
    month_fact_total,
)
from .techdir_m5_plan import (
    MONTH_NAMES,
    TARGET_PROJECT_TYPE_TD_M1,
    build_period_plan_aggregates,
    month_plan_total,
    plan_fact_kpi_pct,
    target_projects as plan_target_projects,
)
from .techdir_projects import (
    TARGET_ORGANIZATION,
    TECHDIR_OWNER_POSITION,
    _month_pairs_until,
    _normalize_ref_period,
    _project_is_alive_in_month,
)

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
FACT_CACHE_DIR = techdir_m5_fact_cache.CACHE_DIR
SOURCE_TAG = "techdir_m5_ytd_v1"
CACHE_VERSION = 1


def ytd_cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"techdir_m5_ytd_{year}_{month:02d}.json"


def _load_ytd_cache(path: Path, ref_y: int, ref_m: int) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("year") != ref_y or data.get("month") != ref_m:
        return None
    payload = data.get("payload")
    if not isinstance(payload, dict):
        return None
    if techdir_m5_fact_cache.is_period_fully_past(ref_y, ref_m):
        return payload
    if data.get("cache_date") == date.today().isoformat():
        return payload
    return None


def _load_stale_ytd_cache(path: Path, ref_y: int, ref_m: int) -> dict[str, Any] | None:
    data = techdir_cache.read_json(path)
    if data is None:
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("year") != ref_y or data.get("month") != ref_m:
        return None
    payload = data.get("payload")
    return payload if isinstance(payload, dict) else None


def _save_ytd_cache(path: Path, ref_y: int, ref_m: int, payload: dict[str, Any]) -> None:
    try:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "year": ref_y,
                    "month": ref_m,
                    "payload": payload,
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        logger.exception("Не удалось сохранить кэш TD-M5 YTD в %s", path)


def _odata_session() -> requests.Session:
    session = requests.Session()
    session.auth = ext_budj_fact.AUTH
    return session


def _merge_period_row(
    plan_row: dict[str, Any],
    fact_row: dict[str, Any],
) -> dict[str, Any]:
    plan = plan_row.get("plan")
    fact = fact_row.get("fact")
    has_data = bool(plan_row.get("has_data")) or bool(fact_row.get("has_data"))
    plan_val = float(plan) if plan is not None else 0.0
    fact_val = float(fact) if fact is not None else 0.0
    merged = dict(plan_row)
    merged["fact"] = fact if has_data else None
    merged["plan"] = plan if has_data else None
    merged["has_data"] = has_data
    merged["kpi_pct"] = (
        plan_fact_kpi_pct(plan_val, fact_val) if has_data else None
    )
    merged["aggregation_strategy"] = (
        f"{plan_row.get('aggregation_strategy', '')};"
        f"{fact_row.get('aggregation_strategy', '')}"
    )
    return merged


def _merge_period_aggregates(
    plan_agg: dict[str, dict[str, Any]],
    fact_agg: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    quarter_combinations: dict[str, dict[str, Any]] = {}
    plan_q = plan_agg.get("quarter_combinations") or {}
    fact_q = fact_agg.get("quarter_combinations") or {}
    for key in set(plan_q) | set(fact_q):
        quarter_combinations[key] = _merge_period_row(
            plan_q.get(key, {}),
            fact_q.get(key, {}),
        )

    return {
        "aggregation_strategy": "plan_unique_projects;fact_sum_monthly_payments",
        "dedupe_key": "project_code_or_file_id",
        "additive_across_months": False,
        "month": _merge_period_row(plan_agg["month"], fact_agg["month"]),
        "quarter_to_date": _merge_period_row(
            plan_agg["quarter_to_date"],
            fact_agg["quarter_to_date"],
        ),
        "year_to_date": _merge_period_row(
            plan_agg["year_to_date"],
            fact_agg["year_to_date"],
        ),
        "quarter_combinations": quarter_combinations,
    }


def build_td_m5_budget_payload(
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    target_projects = plan_target_projects()
    ref_y, ref_m = _normalize_ref_period(year, month)
    pairs = _month_pairs_until(ref_y, ref_m)
    session = _odata_session()
    cache_stats: dict[str, int] = {"hits": 0, "misses": 0}

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        plan_sum = month_plan_total(target_projects, y, m)
        fact_sum, _fact_details = month_fact_total(
            session,
            target_projects,
            y,
            m,
            cache_stats=cache_stats,
        )
        alive_count = sum(
            1
            for project in target_projects
            if _project_is_alive_in_month(project, y, m)
        )
        has_data = alive_count > 0
        if has_data:
            row = {
                "month": m,
                "year": y,
                "month_name": MONTH_NAMES[m],
                "plan": plan_sum,
                "fact": fact_sum,
                "kpi_pct": plan_fact_kpi_pct(plan_sum, fact_sum),
                "has_data": True,
                "values_unit": "руб.",
            }
        else:
            row = {
                "month": m,
                "year": y,
                "month_name": MONTH_NAMES[m],
                "plan": None,
                "fact": None,
                "kpi_pct": None,
                "has_data": False,
            }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

    ytd_block: dict[str, Any] = {
        "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
        "months_total": len(monthly_rows),
    }
    if ref_row and ref_row.get("has_data"):
        ytd_block.update({
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "values_unit": "руб.",
        })
    else:
        ytd_block.update({
            "total_plan": None,
            "total_fact": None,
            "kpi_pct": None,
        })

    plan_period = build_period_plan_aggregates(target_projects, ref_y, ref_m)
    fact_period = build_period_fact_aggregates(
        session,
        target_projects,
        ref_y,
        ref_m,
        cache_stats=cache_stats,
    )
    period_aggregates = _merge_period_aggregates(plan_period, fact_period)

    ref_names = alive_project_names(target_projects, ref_y, ref_m)

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
        "ytd": ytd_block,
        "period_aggregates": period_aggregates,
        "frontend_aggregation": {
            "additive_across_months": False,
            "use_period_aggregates_for_buttons": True,
            "selected_quarters_key_format": "comma_separated_quarter_numbers",
            "reason": (
                "TD-M5: план — бюджет проекта из TurboProject, один раз за период если проект жив; "
                "факт — сумма оплат по заявкам ДС (ext_budj_fact) по живым в каждом месяце проектам."
            ),
        },
        "debug": {
            "kpi_id": "TD-M5",
            "source": "TurboProject + ext_budj_fact",
            "filter": (
                f"tip_proekta={TARGET_PROJECT_TYPE_TD_M1!r}, "
                f"организация «{TARGET_ORGANIZATION}», куратор из должности «{TECHDIR_OWNER_POSITION}»"
            ),
            "plan_field": "byudzhet_plan",
            "fact_module": "ext_budj_fact",
            "fact_criterion": FACT_CRITERION,
            "fact_cache_dir": str(FACT_CACHE_DIR),
            "fact_cache_hits": cache_stats.get("hits", 0),
            "fact_cache_misses": cache_stats.get("misses", 0),
            "fact_project_names_ref_month": ref_names,
            "target_projects_count": len(target_projects),
        },
    }


def get_td_m5_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    """TD-M5: план из TurboProject, факт из оплат 1С по проектам внешних заказов."""

    ref_y, ref_m = _normalize_ref_period(year, month)
    cache_path = ytd_cache_path(ref_y, ref_m)
    lock_key = f"techdir_td_m5_{ref_y}_{ref_m:02d}"

    def _compute_and_save() -> dict | None:
        try:
            payload = build_td_m5_budget_payload(year=year, month=month)
            if payload is not None:
                _save_ytd_cache(cache_path, ref_y, ref_m, payload)
            return payload
        except Exception:
            logger.exception("Ошибка при расчёте TD-M5 (бюджет проектов по внешним заказам)")
            return None

    return cache_manager.stale_while_revalidate(
        lock_key,
        lambda: _load_ytd_cache(cache_path, ref_y, ref_m),
        lambda: _load_stale_ytd_cache(cache_path, ref_y, ref_m),
        _compute_and_save,
    )
