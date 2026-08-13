"""HRD-M7 — производительность труда (НПО + Алмаз).

Формула месяца:
  факт = выручка FND-T1 (факт) / (ССЧ НПО + ССЧ Алмаз)

Выручка — тот же билдер, что у плитки FND-T1 председателя:
  ``chairman_data._build_fnd_t1_revenue_rows`` → ``comdir.get_dengi_ytd``.

ССЧ — сумма среднесписочных НПО и Алмаз (выручка считается по обоим).

Плана нет; единица — руб./чел.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from getkpi.chairman_data import _build_fnd_t1_revenue_rows
from qualdir.sql_tile_cache import get_ytd_via_cache
from sup.npo_ssc import (
    ALMAZ_ORG_GUID,
    NPO_ORG_GUID,
    calc_org_ssc_by_month,
)

KPI_ID = "HRD-M7"
CACHE_PREFIX = "sup_hrd_m7_productivity"
CACHE_SOURCE_TAG = "sup_hrd_m7_productivity_payload_v5_kpi_pct_fact"
CACHE_VERSION = 5
VALUES_UNIT = "руб./чел."


def _fnd_t1_fact_by_month(year: int, through_month: int) -> dict[int, float | None]:
    """Факт выручки FND-T1 по месяцам 1..through_month (как на плитке председателя)."""
    rows = _build_fnd_t1_revenue_rows(list(range(1, through_month + 1)), year)
    out: dict[int, float | None] = {}
    for row in rows:
        m = int(row.get("month") or 0)
        if 1 <= m <= through_month:
            fact = row.get("fact")
            out[m] = float(fact) if fact is not None else None
    return out


def _productivity(revenue: float | None, ssc: float | None) -> float | None:
    if revenue is None or ssc is None or ssc <= 0:
        return None
    return round(float(revenue) / float(ssc), 2)


def build_hrd_m7_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    revenue_by_m = _fnd_t1_fact_by_month(ref_y, ref_m)

    npo_ssc = calc_org_ssc_by_month(ref_y, ref_m, org_guids=(NPO_ORG_GUID,))
    almaz_ssc = calc_org_ssc_by_month(ref_y, ref_m, org_guids=(ALMAZ_ORG_GUID,))
    npo_by_m: dict[int, float] = npo_ssc.get("months") or {}
    almaz_by_m: dict[int, float] = almaz_ssc.get("months") or {}

    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        revenue = revenue_by_m.get(m)
        ssc_npo = float(npo_by_m.get(m) or 0)
        ssc_almaz = float(almaz_by_m.get(m) or 0)
        ssc = round(ssc_npo + ssc_almaz, 1)
        fact = _productivity(revenue, ssc)
        has_data = revenue is not None and ssc > 0
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": None,
                "fact": fact,
                "revenue": revenue,
                "ssc": ssc,
                "ssc_npo": ssc_npo,
                "ssc_almaz": ssc_almaz,
                # Плана нет: в kpi_pct отдаём саму производительность (выручка/ССЧ),
                # чтобы карточка «KPI» на деталке не показывала «—%».
                "kpi_pct": fact,
                "has_data": has_data,
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
            "fact": None,
            "revenue": None,
            "ssc": None,
            "ssc_npo": None,
            "ssc_almaz": None,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": VALUES_UNIT,
        }

    ref_fact = ref_row.get("fact")
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
            "total_fact": ref_fact,
            "kpi_pct": ref_fact,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": VALUES_UNIT,
            "revenue": ref_row.get("revenue"),
            "ssc": ref_row.get("ssc"),
            "ssc_npo": ref_row.get("ssc_npo"),
            "ssc_almaz": ref_row.get("ssc_almaz"),
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok",
            "source": {
                "revenue": "FND-T1 via chairman_data._build_fnd_t1_revenue_rows / comdir.get_dengi_ytd",
                "ssc": "ССЧ НПО + ССЧ Алмаз (erp_pm HR history)",
            },
            "rule": (
                "fact = kpi_pct = FND-T1 revenue fact / (SSC NPO + SSC Almaz); no plan"
            ),
            "npo_ssc": {
                "departments_count": npo_ssc.get("departments_count"),
                "employees_tracked": npo_ssc.get("employees_tracked"),
                "organization_key": NPO_ORG_GUID,
            },
            "almaz_ssc": {
                "departments_count": almaz_ssc.get("departments_count"),
                "employees_tracked": almaz_ssc.get("employees_tracked"),
                "organization_key": ALMAZ_ORG_GUID,
            },
            "rows_by_month": [
                {
                    "month": row["month"],
                    "revenue": row["revenue"],
                    "ssc_npo": row["ssc_npo"],
                    "ssc_almaz": row["ssc_almaz"],
                    "ssc": row["ssc"],
                    "fact": row["fact"],
                }
                for row in monthly_rows
            ],
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    from devdir import ytd_json_cache

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, ref_y, ref_m)


def get_hrd_m7_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="sup_hrd_m7_productivity",
        compute_fn=lambda y, m: build_hrd_m7_payload(y, m),
        kpi_id=KPI_ID,
    )


def main() -> int:
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    args = [a for a in sys.argv[1:] if a.strip()]
    year = int(args[0]) if args else None
    month = int(args[1]) if len(args) > 1 else None
    payload = build_hrd_m7_payload(year=year, month=month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
