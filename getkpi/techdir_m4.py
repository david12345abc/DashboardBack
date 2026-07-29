"""KPI TD-M4: ФОТ техдирекции — SQL-бэкап ``getkpi.td_m4``, кэш раз в день."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from qualdir.sql_tile_cache import get_ytd_via_cache

from . import td_m4

CACHE_FILE_PREFIX = "techdir_m4_ytd"
CACHE_SOURCE_TAG = "techdir_m4_ytd_sql_v1"
CACHE_VERSION = 4

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    def _fix_row(row: dict[str, Any] | None) -> None:
        if not isinstance(row, dict):
            return
        m = row.get("month")
        if isinstance(m, int) and m in MONTH_NAMES:
            row["month_name"] = MONTH_NAMES[m]

    for row in payload.get("monthly_data") or []:
        _fix_row(row if isinstance(row, dict) else None)
    _fix_row(payload.get("last_full_month_row"))
    period = payload.get("kpi_period")
    if isinstance(period, dict):
        m = period.get("month")
        if isinstance(m, int) and m in MONTH_NAMES:
            period["month_name"] = MONTH_NAMES[m]
    ref = payload.get("last_full_month_row") or {}
    ytd = payload.get("ytd")
    if isinstance(ytd, dict) and ref:
        ytd["total_plan"] = ref.get("plan")
        ytd["total_fact"] = ref.get("fact")
        ytd["kpi_pct"] = ref.get("kpi_pct")
    return payload


def _build_payload(year: int, month: int) -> dict[str, Any]:
    return _normalize_payload(td_m4.build_td_m4_payload(year=year, month=month))


def _cache_path(year: int, month: int) -> Path:
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, year, month)


def get_td_m4_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_FILE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="techdir_m4_sql",
        compute_fn=_build_payload,
        kpi_id="TD-M4",
    )
