"""SH-M4 — обращения, исполненные в срок (претензии).

Источник: ``Catalog_Претензии`` (1С OData).
  • план — все обращения за месяц (по ``ДатаРегистрации``), без помеченных на удаление;
  • факт — обращения, где ``ДатаОкончания`` ≤ ``ТД_ДатаОкончанияПлан``;
  • KPI % — факт / план × 100.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import requests

from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from servhead.claims_common import (
    AUTH,
    CLAIMS_ENTITY,
    FIELD_DATE_FACT,
    FIELD_DATE_PLAN,
    build_kpi_period,
    empty_error_payload,
    fetch_claims_sla_counts_by_month,
    is_completed_on_time,
    kpi_pct,
)

logger = logging.getLogger(__name__)

CACHE_PREFIX = "servhead_sh_m4_claims"
CACHE_SOURCE_TAG = "servhead_sh_m4_claims_payload_v1"
CACHE_VERSION = 1

KPI_ID = "SH-M4"


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = requests.Session()
    session.auth = AUTH
    by_month = fetch_claims_sla_counts_by_month(
        session,
        year=ref_y,
        month_from=1,
        month_to=ref_m,
        log_label="SH-M4/Claims",
        fact_match=is_completed_on_time,
    )

    monthly_rows: list[dict[str, Any]] = []
    status_breakdown: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        plan = int(by_month.get(m, {}).get("plan", 0))
        fact = int(by_month.get(m, {}).get("fact", 0))
        monthly_rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": kpi_pct(plan, fact),
            "has_data": plan > 0,
            "values_unit": "шт.",
        })
        status_breakdown.append({"month": m, "plan": plan, "fact": fact})

    ref_row = monthly_rows[-1] if monthly_rows else {
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "plan": 0,
        "fact": 0,
        "kpi_pct": None,
        "has_data": False,
        "values_unit": "шт.",
    }

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": build_kpi_period(ref_y, ref_m, MONTH_NAMES),
        "ytd": {
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok",
            "source": "servhead.sh_m4",
            "odata_entity": CLAIMS_ENTITY,
            "date_fact_field": FIELD_DATE_FACT,
            "date_plan_field": FIELD_DATE_PLAN,
            "rule": (
                "plan = all claims in month by ДатаРегистрации; "
                f"fact = {FIELD_DATE_FACT} <= {FIELD_DATE_PLAN}"
            ),
            "rows_by_month": status_breakdown,
        },
    }


def cache_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, year, month)


def sh_m4_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    return cache_path_for_period(year, month)


def get_sh_m4_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = cache_path_for_period(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_payload(year=ref_y, month=ref_m)
        except Exception as exc:
            logger.exception("SH-M4: ошибка расчёта обращений в срок")
            stale = ytd_json_cache.load_stale_payload(
                cache_path,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
            if stale is not None:
                debug = dict(stale.get("debug") or {})
                debug.update({
                    "status": "stale_cache",
                    "odata_error": str(exc)[:500],
                    "cache_date_fallback": True,
                })
                stale["debug"] = debug
                return stale
            return empty_error_payload(
                kpi_id=KPI_ID,
                ref_y=ref_y,
                ref_m=ref_m,
                month_names=MONTH_NAMES,
                error=str(exc),
            )
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
        lock_key=f"servhead_sh_m4_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def build_sh_m4_json(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return _build_payload(year=year, month=month)


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="SH-M4: обращения в срок (факт) / все обращения за месяц (план).",
    )
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_sh_m4_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
