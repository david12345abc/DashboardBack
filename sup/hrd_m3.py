"""HRD-M3 — бюджет службы управления персоналом в пределах лимита.

План — сумма строк утверждённой таблицы ``hrd_m3_budget_plan``;
факт — оплаты по заявкам ДС (ЦФО «Служба управления персоналом»).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from qualdir.turnover import _qd_q2_kpi_pct

from sup.hrd_m3_budget_fact import compute_hrd_m3_budget_fact_monthly
from sup.hrd_m3_budget_plan import HRD_M3_BUDGET_PLAN_BY_MONTH_2026

logger = logging.getLogger(__name__)

KPI_ID = "HRD-M3"
CACHE_PREFIX = "sup_hrd_m3_budget"
CACHE_SOURCE_TAG = "sup_hrd_m3_budget_payload_v2"
CACHE_VERSION = 2

MONTHLY_CACHE_PREFIX = "sup_hrd_m3_budget_fact_monthly"
MONTHLY_SOURCE_TAG = "sup_hrd_m3_budget_fact_monthly_v2"
MONTHLY_CACHE_VERSION = 2


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in HRD_M3_BUDGET_PLAN_BY_MONTH_2026:
        return float(HRD_M3_BUDGET_PLAN_BY_MONTH_2026[month])
    return None


def monthly_cache_path(year: int, month: int) -> Path:
    return ytd_json_cache.cache_path(MONTHLY_CACHE_PREFIX, year, month)


def get_hrd_m3_fact_monthly(year: int, month: int) -> dict[str, Any]:
    path = monthly_cache_path(year, month)
    perpetual = ytd_json_cache.is_ref_period_fully_past(year, month)

    def _compute_and_save() -> dict[str, Any]:
        payload = compute_hrd_m3_budget_fact_monthly(year, month)
        ytd_json_cache.save_payload(
            path,
            payload,
            source_tag=MONTHLY_SOURCE_TAG,
            version=MONTHLY_CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        path,
        source_tag=MONTHLY_SOURCE_TAG,
        version=MONTHLY_CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"sup_hrd_m3_fact_monthly_{year}_{month:02d}",
        compute_fn=_compute_and_save,
    )


def _month_row(ref_y: int, m: int, snapshot: dict[str, Any]) -> dict[str, Any]:
    plan = _plan_for_month(ref_y, m)
    fact_raw = snapshot.get("total_fact")
    fact_value = float(fact_raw) if fact_raw is not None else None
    has_data = plan is not None and fact_value is not None
    return {
        "month": m,
        "year": ref_y,
        "month_name": MONTH_NAMES[m],
        "plan": round(plan, 2) if plan is not None else None,
        "fact": round(fact_value, 2) if fact_value is not None else None,
        "kpi_pct": _qd_q2_kpi_pct(plan, fact_value) if has_data else None,
        "has_data": has_data,
        "values_unit": "руб.",
    }


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for m in range(1, ref_m + 1):
        snapshot = get_hrd_m3_fact_monthly(ref_y, m)
        row = _month_row(ref_y, m, snapshot)
        monthly_rows.append(row)
        if m == ref_m:
            ref_row = row

    with_data = [row for row in monthly_rows if row.get("has_data")]
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
            "months_with_data": len(with_data),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok" if with_data else "no_data",
            "plan_source": "sup/hrd_m3_budget_plan.py (сумма 15 строк × месяц)",
            "fact_source": "sup/hrd_m3_budget_fact.py",
            "formula": "Факт / План × 100%",
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, year, month)


def get_hrd_m3_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("HRD-M3: ошибка расчёта бюджета")
            stale = ytd_json_cache.load_stale_payload(
                cache_path,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
            if stale is not None:
                return stale
            return None
        ytd_json_cache.save_payload(
            cache_path,
            payload,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        cache_path,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"sup_hrd_m3_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser(description="HRD-M3: бюджет службы управления персоналом.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_hrd_m3_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
