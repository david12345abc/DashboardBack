"""ГСП-Q5 — текучесть персонала ГСПП.

План: top2 по группам из Document_ТД_ТекучестьПерсонала (как TD-Q2).
Факт: уволено / штат × 100 % по HR (как TD-Q2).
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from getkpi.techdir_tekuchet import TURNOVER_VALUES_UNIT, build_turnover_month_payload
from getkpi.turnover_hr_scope import TurnoverHrScope

logger = logging.getLogger(__name__)

GSPP_Q5_CACHE_PREFIX = "gspp_q5_tekuchest"
GSPP_Q5_DISK_TAG = "gspp_q5_tekuchest_payload_v3"
GSPP_Q5_DISK_VERSION = 4

GSPP_TURNOVER_DEPARTMENTS = (
    "ГСПП",
    "Сектор сопровождения продаж",
    "Сектор сопровождения производства и продаж",
    "Сектор доработки и улучшения продукции",
    "Сектор постановки на производство несерийной продукции",
)

GSPP_Q5_GROUP_ALIASES: dict[str, list[str]] = {
    name: [name.lower().replace("ё", "е")]
    for name in GSPP_TURNOVER_DEPARTMENTS
}
GSPP_Q5_GROUP_ORDER = list(GSPP_Q5_GROUP_ALIASES.keys())
GSPP_Q5_HR_SCOPE = TurnoverHrScope(
    group_aliases=GSPP_Q5_GROUP_ALIASES,
    group_order=GSPP_Q5_GROUP_ORDER,
)


def _turnover_kpi_pct(_plan: Any, fact: Any) -> float | None:
    if fact is None:
        return None
    try:
        return round(float(fact), 1)
    except (TypeError, ValueError):
        return None


def _build_gspp_q5_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for m in range(1, ref_m + 1):
        snapshot = build_turnover_month_payload(
            ref_y,
            m,
            group_aliases=GSPP_Q5_GROUP_ALIASES,
            group_order=GSPP_Q5_GROUP_ORDER,
            aggregate="top2",
            fact_from_hr=True,
            hr_scope=GSPP_Q5_HR_SCOPE,
        )
        plan = snapshot.get("total_plan")
        fact = snapshot.get("total_fact")
        has_data = plan is not None and fact is not None
        row = {
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

    assert ref_row is not None
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
        "ytd": {
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": TURNOVER_VALUES_UNIT,
        },
        "debug": {
            "kpi_id": "ГСП-Q5",
            "source": "Document_ТД_ТекучестьПерсонала + HR staffing/dismissals",
            "plan_source": "group_max_top2_1c_tekuchet",
            "fact_source": "hr_staff_dismissals_turnover_pct",
            "target_departments": list(GSPP_TURNOVER_DEPARTMENTS),
        },
    }


def gspp_q5_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_Q5_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_q5_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = gspp_q5_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_gspp_q5_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("ГСП-Q5: ошибка расчёта текучести")
            return None
        ytd_json_cache.save_payload(
            cache_path,
            payload,
            source_tag=GSPP_Q5_DISK_TAG,
            version=GSPP_Q5_DISK_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        cache_path,
        source_tag=GSPP_Q5_DISK_TAG,
        version=GSPP_Q5_DISK_VERSION,
        perpetual=perpetual,
        lock_key=f"gspp_q5_tekuchest_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Рассчитать текучесть ГСП-Q5.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_gspp_q5_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
