"""SH-T1 — таблица обращений по клиентам (в срок / не в срок).

SQL: ``servhead/claims_common.py``
(``_Reference389`` + ``_Reference328`` / Catalog_Партнеры).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from servhead.claims_common import (
    CLAIMS_TABLE,
    COL_DATE_FACT,
    COL_DATE_PLAN,
    COL_DATE_REG,
    COL_PARTNER,
    PARTNERS_TABLE,
    load_client_sla_rows,
)

logger = logging.getLogger(__name__)

TABLE_ID = "SH-T1"
CACHE_PREFIX = "servhead_sh_t1_clients"
CACHE_SOURCE_TAG = "servhead_sh_t1_clients_payload_v2_sql"
CACHE_VERSION = 2

TABLE_COLUMNS = [
    "Клиент",
    "Всего обращений",
    "В срок",
    "Не в срок",
]


def _build_table_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    rows = load_client_sla_rows(ref_y, ref_m)
    totals = {
        "total": sum(int(row["Всего обращений"]) for row in rows),
        "on_time": sum(int(row["В срок"]) for row in rows),
        "late": sum(int(row["Не в срок"]) for row in rows),
    }

    return {
        "kpi_id": TABLE_ID,
        "name": f"Обращения по клиентам — {MONTH_NAMES[ref_m].capitalize()} {ref_y}",
        "periodicity": "ежемесячно",
        "description": (
            "Агрегация обращений за месяц по клиенту: всего, в срок "
            "(ДатаОкончания ≤ ТД_ДатаОкончанияПлан), не в срок (факт > план). "
            f"SQL: {CLAIMS_TABLE} + {PARTNERS_TABLE}."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": list(TABLE_COLUMNS),
        "rows": rows,
        "totals": totals,
        "debug": {
            "kpi_id": TABLE_ID,
            "status": "ok",
            "source": "servhead.claims_common.load_client_sla_rows",
            "tables": {
                "claims": CLAIMS_TABLE,
                "partners": PARTNERS_TABLE,
                "partner_col": COL_PARTNER,
                "date_reg_col": COL_DATE_REG,
                "date_fact_col": COL_DATE_FACT,
                "date_plan_col": COL_DATE_PLAN,
            },
            "claims_count": totals["total"],
            "clients_count": len(rows),
        },
    }


def sh_t1_cache_path(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, year, month)


def get_sh_t1_table(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = sh_t1_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any]:
        try:
            payload = _build_table_payload(year=ref_y, month=ref_m)
        except Exception as exc:
            logger.exception("SH-T1: ошибка сборки таблицы по клиентам (SQL)")
            stale = ytd_json_cache.load_stale_payload(
                cache_path,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
            if stale is not None:
                debug = dict(stale.get("debug") or {})
                debug.update({
                    "status": "stale_cache",
                    "sql_error": str(exc)[:500],
                    "cache_date_fallback": True,
                })
                stale["debug"] = debug
                return stale
            return {
                "kpi_id": TABLE_ID,
                "name": f"Обращения по клиентам — {MONTH_NAMES[ref_m].capitalize()} {ref_y}",
                "periodicity": "ежемесячно",
                "period": {
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "columns": list(TABLE_COLUMNS),
                "rows": [],
                "totals": {"total": 0, "on_time": 0, "late": 0},
                "debug": {"kpi_id": TABLE_ID, "status": "error", "error": str(exc)},
            }
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
        lock_key=f"servhead_sh_t1_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser(description="SH-T1: таблица обращений по клиентам (SQL).")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_sh_t1_table(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
