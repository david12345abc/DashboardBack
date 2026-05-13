"""ГСП-M3 — ФОТ ГСПП: план из утверждённой таблицы, факт из регистра бухучёта."""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import requests

from getkpi.cache_manager import locked_call
from getkpi.devdir import ytd_json_cache
from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from getkpi.fot_techdir_fact import (
    AUTH,
    FOT_SPEC_ARTICLES,
    MONTH_RU,
    calc_techdir_spec_reg_fact,
    load_fot_structure_map_for_spec,
)

logger = logging.getLogger(__name__)

GSPP_M3_CACHE_PREFIX = "gspp_m3_ytd"
GSPP_M3_DISK_TAG = "gspp_m3_fot_payload_v2"
GSPP_M3_DISK_VERSION = 2

GSPP_M3_PLAN_TARGET_2026: dict[int, int] = {
    1: 3_935_148,
    2: 4_007_859,
    3: 4_083_612,
    4: 4_349_149,
    5: 3_965_452,
    6: 4_123_100,
    7: 4_278_019,
    8: 4_293_323,
    9: 4_194_200,
    10: 4_313_961,
    11: 4_091_539,
    12: 4_124_167,
}

GSPP_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    ("ГСПП", ("гспп", "гсп")),
    (
        "Сектор сопровождения продаж",
        (
            "сектор сопровождения продаж",
            "сектор сопровожд продаж",
        ),
    ),
    (
        "Сектор сопровождения производства и продаж",
        (
            "сектор сопровождения производства и продаж",
            "сектор сопровождения производства продаж",
        ),
    ),
    (
        "Сектор доработки и улучшения продукции",
        (
            "сектор доработки и улучшения продукции",
            "сектор доработки улучшения продукции",
        ),
    ),
    (
        "Сектор постановки на производство несерийной продукции",
        (
            "сектор постановки на производство несерийной продукции",
            "сектор постановки производства несерийной продукции",
        ),
    ),
]
GSPP_GROUP_ORDER = [item[0] for item in GSPP_FOT_SPEC]


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _compute_gspp_fot_fact_month(year: int, month: int) -> dict[str, Any]:
    try:
        p_start, p_end = _month_period_bounds(year, month)
        session = requests.Session()
        session.auth = AUTH
        name_to_key, labels = load_fot_structure_map_for_spec(session, GSPP_FOT_SPEC)
        totals, article_totals = calc_techdir_spec_reg_fact(
            session, p_start, p_end, name_to_key,
        )
        total_fact = 0.0
        groups_out: dict[str, dict[str, float]] = {}
        for name in GSPP_GROUP_ORDER:
            row = totals.get(name, {})
            salary = float(row.get("fact_salary", 0) or 0)
            insurance = float(row.get("fact_insurance", 0) or 0)
            total = salary + insurance
            total_fact += total
            groups_out[name] = {
                "fact_salary": round(salary, 2),
                "fact_insurance": round(insurance, 2),
                "fact_total": round(total, 2),
            }
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": round(total_fact, 2),
            "groups": groups_out,
            "article_totals": dict(article_totals),
            "debug": {
                "status": "ok",
                "component": "gspp.m3._compute_gspp_fot_fact_month",
                "register": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
                "movement": "дебет счёта 26, поле Сумма (сторно инвертируется)",
                "cost_articles": list(FOT_SPEC_ARTICLES),
                "departments": list(GSPP_GROUP_ORDER),
                "department_filter_rule": "exact listed departments and their children via Catalog_СтруктураПредприятия",
                "structure_labels": labels,
            },
        }
    except Exception as exc:
        logger.exception("ГСП-M3: ошибка расчёта факта ФОТ за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": None,
            "groups": {},
            "article_totals": {},
            "debug": {
                "status": "error",
                "component": "gspp.m3._compute_gspp_fot_fact_month",
                "error": str(exc),
            },
        }


def _empty_month_row(year: int, month: int) -> dict[str, Any]:
    return {
        "month": month,
        "year": year,
        "month_name": MONTH_NAMES[month],
        "plan": None,
        "fact": None,
        "kpi_pct": None,
        "has_data": False,
        "values_unit": "руб.",
    }


def _build_gspp_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    month_debug: dict[int, dict[str, Any]] = {}

    for m in range(1, ref_m + 1):
        plan = (
            float(GSPP_M3_PLAN_TARGET_2026[m])
            if ref_y == 2026 and m in GSPP_M3_PLAN_TARGET_2026
            else None
        )
        if plan is None:
            monthly_rows.append(_empty_month_row(ref_y, m))
            continue

        fact_payload = _compute_gspp_fot_fact_month(ref_y, m)
        fact = fact_payload.get("total_fact")
        fact_value = float(fact) if fact is not None else None
        row = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": round(plan, 2),
            "fact": round(fact_value, 2) if fact_value is not None else None,
            "kpi_pct": _kpi_pct(plan, fact_value),
            "has_data": fact_value is not None,
            "values_unit": "руб.",
        }
        monthly_rows.append(row)
        month_debug[m] = fact_payload.get("debug") or {}

    ref_row = next(
        (row for row in reversed(monthly_rows) if row.get("month") == ref_m),
        monthly_rows[-1] if monthly_rows else _empty_month_row(ref_y, ref_m),
    )
    with_data = [row for row in monthly_rows if row.get("has_data")]
    ytd: dict[str, Any] = {
        "total_plan": ref_row.get("plan") if ref_row.get("has_data") else None,
        "total_fact": ref_row.get("fact") if ref_row.get("has_data") else None,
        "kpi_pct": ref_row.get("kpi_pct") if ref_row.get("has_data") else None,
        "months_with_data": len(with_data),
        "months_total": len(monthly_rows),
        "values_unit": "руб.",
    }
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": ytd,
        "debug": {
            "status": "ok" if with_data else "no_data",
            "kpi_id": "ГСП-M3",
            "plan_source": "monthly_constants_from_screenshot",
            "fact_source": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
            "fact_rule": "debit turnover of account 26 by department + cost article; no balances, no credit turnover",
            "plan_targets_2026": GSPP_M3_PLAN_TARGET_2026,
            "fact_departments": list(GSPP_GROUP_ORDER),
            "fact_cost_articles": list(FOT_SPEC_ARTICLES),
            "month_debug": month_debug,
        },
    }


def gspp_m3_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_M3_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_m3_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = gspp_m3_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any] | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=GSPP_M3_DISK_TAG,
            version=GSPP_M3_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        payload = _build_gspp_m3_payload(year=ref_y, month=ref_m)
        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                cache_path,
                payload,
                source_tag=GSPP_M3_DISK_TAG,
                version=GSPP_M3_DISK_VERSION,
            )
        return payload

    return locked_call(f"gspp_m3_fot_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    parser = argparse.ArgumentParser(description="Рассчитать ФОТ ГСП-M3.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_gspp_m3_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
