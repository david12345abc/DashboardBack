"""
SH-M2 — обращения в обработке (начальник службы качества).

SQL: servhead/claims_common.py (_Reference389, _Enum1688).
  plan = все обращения за месяц по ДатаРегистрации
  fact = статус «Обрабатывается» / «В обработке»
  KPI % = fact / plan × 100
"""

from __future__ import annotations

import sys
from typing import Any

from servhead.claims_common import (
    STATUS_IN_PROCESS,
    build_claims_status_payload,
    run_cli,
)

FACT_STATUSES = STATUS_IN_PROCESS


def build_sh_m2_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_claims_status_payload(
        kpi_id="SH-M2",
        fact_statuses=FACT_STATUSES,
        source_module="servhead.sh_m2.sql",
        year=year,
        month=month,
    )


def build_sh_m2_json(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_sh_m2_payload(year=year, month=month)


def main() -> None:
    try:
        run_cli(
            kpi_id="SH-M2",
            file_prefix="sh_m2",
            title="Обращения в обработке · Catalog_Претензии (SH-M2 / SQL)",
            fact_label="статус «Обрабатывается» / «В обработке»",
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

SH_M2_CACHE_PREFIX = "servhead_sh_m2_claims"
SH_M2_DISK_TAG = "servhead_sh_m2_sql_payload_v1"
SH_M2_DISK_VERSION = 1


def cache_path_for_period(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(SH_M2_CACHE_PREFIX, ry, rm)


def sh_m2_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    return cache_path_for_period(year, month)


def get_sh_m2_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=SH_M2_CACHE_PREFIX,
        source_tag=SH_M2_DISK_TAG,
        version=SH_M2_DISK_VERSION,
        lock_key_prefix="servhead_sh_m2_sql",
        compute_fn=lambda y, m: build_sh_m2_payload(y, m),
        kpi_id="SH-M2",
    )
