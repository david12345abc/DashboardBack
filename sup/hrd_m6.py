"""HRD-M6 — просроченные задачи сотрудников СУП.

Временно: синтетические помесячные данные (методика по Task_ЗадачаИсполнителя
снята как неверная). Факт = 0 (нет просроченных) — зелёный RAG.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from qualdir.sql_tile_cache import get_ytd_via_cache

KPI_ID = "HRD-M6"
CACHE_PREFIX = "sup_hrd_m6_overdue"
CACHE_SOURCE_TAG = "sup_hrd_m6_overdue_payload_v2_synthetic"
CACHE_VERSION = 2
VALUES_UNIT = "шт."


def build_hrd_m6_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)

    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": None,
                "fact": 0,
                "kpi_pct": None,
                "has_data": True,
                "values_unit": VALUES_UNIT,
            }
        )

    ref_row = next((row for row in monthly_rows if row["month"] == ref_m), None)
    if ref_row is None:
        ref_row = {
            "month": ref_m,
            "year": ref_y,
            "month_name": MONTH_NAMES[ref_m],
            "plan": None,
            "fact": 0,
            "kpi_pct": None,
            "has_data": True,
            "values_unit": VALUES_UNIT,
        }

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": None,
            "total_fact": ref_row.get("fact"),
            "kpi_pct": None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": VALUES_UNIT,
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok",
            "source": "synthetic",
            "rule": "synthetic: fact=0 overdue tasks each month; RAG: 0 green, >=1 red",
            "rows_by_month": [
                {"month": row["month"], "fact": row["fact"]}
                for row in monthly_rows
            ],
            "overdue_total": 0,
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    from devdir import ytd_json_cache

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, ref_y, ref_m)


def get_hrd_m6_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="sup_hrd_m6_overdue",
        compute_fn=lambda y, m: build_hrd_m6_payload(y, m),
        kpi_id=KPI_ID,
    )


def main() -> int:
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    args = [a for a in sys.argv[1:] if a.strip()]
    year = int(args[0]) if args else None
    month = int(args[1]) if len(args) > 1 else None
    payload = build_hrd_m6_payload(year=year, month=month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
