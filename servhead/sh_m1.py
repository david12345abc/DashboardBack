"""
SH-M1 — удовлетворённые обращения (начальник службы качества).

SQL: servhead/claims_common.py (_Reference389, _Enum1688).
  plan = все обращения за месяц по ДатаРегистрации (без помеченных на удаление)
  fact = статус «Удовлетворена»
  KPI % = fact / plan × 100
"""

from __future__ import annotations

import sys
from typing import Any

from servhead.claims_common import (
    STATUS_SATISFIED,
    build_claims_status_payload,
    run_cli,
)

FACT_STATUSES = frozenset({STATUS_SATISFIED})


def build_sh_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_claims_status_payload(
        kpi_id="SH-M1",
        fact_statuses=FACT_STATUSES,
        source_module="servhead.sh_m1.sql",
        year=year,
        month=month,
    )


def build_sh_m1_json(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Публичный JSON для плитки / API (без файлового кэша)."""
    return build_sh_m1_payload(year=year, month=month)


def main() -> None:
    try:
        run_cli(
            kpi_id="SH-M1",
            file_prefix="sh_m1",
            title="Удовлетворённые обращения · Catalog_Претензии (SH-M1 / SQL)",
            fact_label=f"статус «{STATUS_SATISFIED}»",
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

SH_M1_CACHE_PREFIX = "servhead_sh_m1_claims"
SH_M1_DISK_TAG = "servhead_sh_m1_sql_payload_v1"
SH_M1_DISK_VERSION = 1


def cache_path_for_period(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(SH_M1_CACHE_PREFIX, ry, rm)


def sh_m1_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    return cache_path_for_period(year, month)


def get_sh_m1_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=SH_M1_CACHE_PREFIX,
        source_tag=SH_M1_DISK_TAG,
        version=SH_M1_DISK_VERSION,
        lock_key_prefix="servhead_sh_m1_sql",
        compute_fn=lambda y, m: build_sh_m1_payload(y, m),
        kpi_id="SH-M1",
    )
