"""
SH-M3 — зарегистрированные обращения (начальник службы качества).

На плитке одно число: сколько всего обращений за месяц по ДатаРегистрации.
Статус «Зарегистрирована» в это число не выделяется.
"""

from __future__ import annotations

import sys
from typing import Any

from servhead.claims_common import (
    STATUS_REGISTERED,
    build_claims_status_payload,
    run_cli,
)

FACT_STATUSES = frozenset({STATUS_REGISTERED})


def _as_total_count(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def present_sh_m3_count(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Одно число на плитке: все обращения месяца (бывший plan), без плана и KPI %."""
    if not isinstance(payload, dict):
        return {}
    out = dict(payload)

    def rewrite_row(row: Any) -> Any:
        if not isinstance(row, dict):
            return row
        total = row.get("plan")
        if total is None:
            total = row.get("fact")
        rewritten = dict(row)
        rewritten["plan"] = None
        rewritten["fact"] = _as_total_count(total)
        rewritten["kpi_pct"] = None
        rewritten.pop("color", None)
        return rewritten

    monthly = out.get("monthly_data")
    if isinstance(monthly, list):
        out["monthly_data"] = [rewrite_row(row) for row in monthly]
    if isinstance(out.get("last_full_month_row"), dict):
        out["last_full_month_row"] = rewrite_row(out["last_full_month_row"])
    ytd = out.get("ytd")
    if isinstance(ytd, dict):
        total = ytd.get("total_plan")
        if total is None:
            total = ytd.get("total_fact")
        ytd_out = dict(ytd)
        ytd_out["total_plan"] = None
        ytd_out["total_fact"] = _as_total_count(total)
        ytd_out["kpi_pct"] = None
        out["ytd"] = ytd_out
    debug = out.get("debug")
    if isinstance(debug, dict):
        debug_out = dict(debug)
        debug_out["rule"] = "tile value = all claims in month by ДатаРегистрации"
        out["debug"] = debug_out
    return out


def build_sh_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return present_sh_m3_count(
        build_claims_status_payload(
            kpi_id="SH-M3",
            fact_statuses=FACT_STATUSES,
            source_module="servhead.sh_m3.sql",
            year=year,
            month=month,
        )
    )


def build_sh_m3_json(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_sh_m3_payload(year=year, month=month)


def main() -> None:
    try:
        run_cli(
            kpi_id="SH-M3",
            file_prefix="sh_m3",
            title="Зарегистрированные обращения · Catalog_Претензии (SH-M3 / SQL)",
            fact_label=f"статус «{STATUS_REGISTERED}»",
            fact_statuses=FACT_STATUSES,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

SH_M3_CACHE_PREFIX = "servhead_sh_m3_claims"
SH_M3_DISK_TAG = "servhead_sh_m3_sql_payload_v1"
SH_M3_DISK_VERSION = 1


def cache_path_for_period(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(SH_M3_CACHE_PREFIX, ry, rm)


def sh_m3_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    return cache_path_for_period(year, month)


def get_sh_m3_ytd(year: int | None = None, month: int | None = None) -> dict:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=SH_M3_CACHE_PREFIX,
        source_tag=SH_M3_DISK_TAG,
        version=SH_M3_DISK_VERSION,
        lock_key_prefix="servhead_sh_m3_sql",
        compute_fn=lambda y, m: build_claims_status_payload(
            kpi_id="SH-M3",
            fact_statuses=FACT_STATUSES,
            source_module="servhead.sh_m3.sql",
            year=y,
            month=m,
        ),
        kpi_id="SH-M3",
    )
    return present_sh_m3_count(payload)
