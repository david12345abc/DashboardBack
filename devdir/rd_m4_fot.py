"""KPI RD-M4 (ФОТ): план из констант; факт из SQL-бэкапа ``devdir.rd_m4``.

Кэш: ``getkpi/dashboard/devdir_rd_m4_fot_<год>_<месяц>.json`` — раз в день.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

from . import rd_m4, ytd_json_cache

CACHE_FILE_PREFIX = "devdir_rd_m4_fot"
CACHE_SOURCE_TAG = "devdir_rd_m4_fot_ytd_sql_v1"
CACHE_VERSION = 2


def _build_rd_m4_fot_monthly_payload(year: int, month: int) -> dict[str, Any]:
    return rd_m4.build_rd_m4_payload(year=year, month=month)


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ry, rm)


def get_rd_m4_fot_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_FILE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="devdir_rd_m4_fot_sql",
        compute_fn=_build_rd_m4_fot_monthly_payload,
        kpi_id="RD-M4",
    )
