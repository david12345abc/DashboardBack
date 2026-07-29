"""KPI RD-Q2: текучесть контура директора по развитию (SQL HR).

Факт: уволено / штат × 100 % (``devdir.rd_q2`` → hr_turnover_sql).
Кэш: ``getkpi/dashboard/devdir_rd_q2_tekuchest_<год>_<месяц>.json`` — раз в день.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from turnover_report import apply_plan_table_to_turnover_payload

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

from . import rd_q2, ytd_json_cache
from .rd_q2_tekuchest_plan import plan_for_month

CACHE_FILE_PREFIX = "devdir_rd_q2_tekuchest"
CACHE_SOURCE_TAG = "devdir_rd_q2_tekuchest_sql_v2"
CACHE_VERSION = 2


def _build_rd_q2_monthly_payload(year: int, month: int) -> dict[str, Any]:
    return rd_q2.build_rd_q2_payload(year=year, month=month)


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ry, rm)


def get_rd_q2_tekuchest_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_period(year, month)
    payload = get_ytd_via_cache(
        year=ref_y,
        month=ref_m,
        cache_prefix=CACHE_FILE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="devdir_rd_q2_sql",
        compute_fn=_build_rd_q2_monthly_payload,
        kpi_id="RD-Q2",
    )
    return apply_plan_table_to_turnover_payload(
        payload,
        ref_y,
        ref_m,
        plan_for_month=plan_for_month,
        plan_source="devdir.rd_q2_tekuchest_plan",
    )
