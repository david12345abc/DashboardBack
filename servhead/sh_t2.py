"""SH-T2 — таблица анкет удовлетворённости клиентов (1С HTTP-сервис DataUK)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from servhead.get_customer_satisfaction import REPORT_COLUMNS, get_report_table

logger = logging.getLogger(__name__)

TABLE_ID = "SH-T2"
CACHE_PATH = ytd_json_cache.CACHE_DIR / "servhead_sh_t2_customer_satisfaction_all.json"
CACHE_SOURCE_TAG = "servhead_sh_t2_customer_satisfaction_all_payload_v1"
CACHE_VERSION = 5


def _build_table_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    rows = get_report_table()

    return {
        "kpi_id": TABLE_ID,
        "name": "Анкеты удовлетворённости клиентов",
        "periodicity": "за всё время",
        "description": (
            "Отчётная таблица всех анкет удовлетворённости клиентов из 1С "
            "(HTTP-сервис DataUK). Данные не зависят от выбранного месяца дашборда."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
            "scope": "all_time",
        },
        "columns": list(REPORT_COLUMNS),
        "rows": rows,
        "totals": {"count": len(rows)},
        "debug": {
            "kpi_id": TABLE_ID,
            "status": "ok",
            "source": "servhead.sh_t2",
            "rows_count": len(rows),
            "scope": "all_time",
        },
    }


def sh_t2_cache_path(year: int | None = None, month: int | None = None) -> Path:
    return CACHE_PATH


def get_sh_t2_table(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)

    def _compute_and_save() -> dict[str, Any]:
        try:
            payload = _build_table_payload(year=ref_y, month=ref_m)
        except Exception as exc:
            logger.exception("SH-T2: ошибка сборки таблицы удовлетворённости клиентов")
            stale = ytd_json_cache.load_stale_payload(
                CACHE_PATH,
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
            return {
                "kpi_id": TABLE_ID,
                "name": "Анкеты удовлетворённости клиентов",
                "periodicity": "за всё время",
                "period": {
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                    "scope": "all_time",
                },
                "columns": list(REPORT_COLUMNS),
                "rows": [],
                "totals": {"count": 0},
                "debug": {"kpi_id": TABLE_ID, "status": "error", "error": str(exc)},
            }
        ytd_json_cache.save_payload(
            CACHE_PATH,
            payload,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        CACHE_PATH,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        perpetual=False,
        lock_key="servhead_sh_t2_all",
        compute_fn=_compute_and_save,
    )


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="SH-T2: таблица анкет удовлетворённости клиентов."
    )
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_sh_t2_table(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
