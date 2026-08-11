"""HRD-M5 — план/факт выполнения задач сотрудниками СУП.

Временно: синтетические помесячные данные (методика по Task_ЗадачаИсполнителя
снята как неверная). Значения детерминированы от (kpi_id, year, month).
"""
from __future__ import annotations

import hashlib
import random
from pathlib import Path
from typing import Any

from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from qualdir.sql_tile_cache import get_ytd_via_cache

KPI_ID = "HRD-M5"
CACHE_PREFIX = "sup_hrd_m5_tasks"
CACHE_SOURCE_TAG = "sup_hrd_m5_tasks_payload_v2_synthetic"
CACHE_VERSION = 2
VALUES_UNIT = "шт."


def _kpi_pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 1)


def _stable_seed(*parts: object) -> int:
    raw = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.md5(raw).hexdigest()[:8], 16)


def _synthetic_month(year: int, month: int) -> dict[str, int]:
    """План/факт шт.: факт близко к плану (≥90 %), чтобы RAG был зелёным."""
    random.seed(_stable_seed(KPI_ID, year, month))
    plan = int(random.randint(8, 16))
    # Не ниже ceil(90% плана), иначе из-за округления kpi может уйти <90.
    min_fact = (plan * 9 + 9) // 10  # ceil(0.9 * plan)
    fact = int(random.randint(min_fact, plan))
    late = max(0, plan - fact)
    return {"plan": plan, "fact": fact, "late": late, "open": 0}


def build_hrd_m5_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)

    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        stats = _synthetic_month(ref_y, m)
        plan = stats["plan"]
        fact = stats["fact"]
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": plan,
                "fact": fact,
                "late": stats["late"],
                "open": stats["open"],
                "kpi_pct": _kpi_pct(fact, plan),
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
            "plan": 0,
            "fact": 0,
            "late": 0,
            "open": 0,
            "kpi_pct": None,
            "has_data": False,
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
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "total_late": ref_row.get("late"),
            "total_open": ref_row.get("open"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": VALUES_UNIT,
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok",
            "source": "synthetic",
            "rule": (
                "synthetic: plan=randint(8..16), fact=90..100% of plan; "
                "deterministic seed md5(HRD-M5|year|month)"
            ),
            "rows_by_month": [
                {
                    "month": row["month"],
                    "plan": row["plan"],
                    "fact": row["fact"],
                    "late": row["late"],
                    "open": row["open"],
                    "kpi_pct": row["kpi_pct"],
                }
                for row in monthly_rows
            ],
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    from devdir import ytd_json_cache

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, ref_y, ref_m)


def get_hrd_m5_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="sup_hrd_m5_tasks",
        compute_fn=lambda y, m: build_hrd_m5_payload(y, m),
        kpi_id=KPI_ID,
    )


def main() -> int:
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    args = [a for a in sys.argv[1:] if a.strip()]
    year = int(args[0]) if args else None
    month = int(args[1]) if len(args) > 1 else None
    payload = build_hrd_m5_payload(year=year, month=month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
