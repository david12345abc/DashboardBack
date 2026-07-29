"""
SH-M5 — обращения, исполненные не в срок (начальник службы качества).

SQL: servhead/claims_common.py
  plan = все обращения за месяц по ДатаРегистрации
  fact = ДатаОкончания > ТД_ДатаОкончанияПлан
  KPI % = fact / plan × 100
"""

from __future__ import annotations

import sys
from typing import Any

from servhead.claims_common import (
    build_claims_sla_payload,
    run_sla_cli,
)


def build_sh_m5_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_claims_sla_payload(
        kpi_id="SH-M5",
        late=True,
        source_module="servhead.sh_m5.sql",
        year=year,
        month=month,
    )


def build_sh_m5_json(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_sh_m5_payload(year=year, month=month)


def main() -> None:
    try:
        run_sla_cli(
            kpi_id="SH-M5",
            file_prefix="sh_m5",
            title="Обращения не в срок · Catalog_Претензии (SH-M5 / SQL)",
            late=True,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

SH_M5_CACHE_PREFIX = "servhead_sh_m5_claims"
SH_M5_DISK_TAG = "servhead_sh_m5_sql_payload_v1"
SH_M5_DISK_VERSION = 1


def cache_path_for_period(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(SH_M5_CACHE_PREFIX, ry, rm)


def sh_m5_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    return cache_path_for_period(year, month)


def get_sh_m5_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=SH_M5_CACHE_PREFIX,
        source_tag=SH_M5_DISK_TAG,
        version=SH_M5_DISK_VERSION,
        lock_key_prefix="servhead_sh_m5_sql",
        compute_fn=lambda y, m: build_sh_m5_payload(y, m),
        kpi_id="SH-M5",
    )
