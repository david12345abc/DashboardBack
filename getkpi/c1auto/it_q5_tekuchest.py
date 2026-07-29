"""KPI 1С-Q5 / 1C-Q5: текучесть «Отдел сопровождения 1С» (SQL HR).

Факт: уволено / штат × 100 % (``c1_q5_core`` → hr_turnover_sql).
Кэш: ``getkpi/dashboard/c1auto_it_q5_tekuchest_<год>_<месяц>.json``.
"""

from __future__ import annotations

from pathlib import Path

from turnover_report import apply_plan_table_to_turnover_payload

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

from .c1_q5_core import build_c1_q5_payload
from .c1_q5_tekuchest_plan import plan_for_month

CACHE_FILE_PREFIX = "c1auto_it_q5_tekuchest"
CACHE_SOURCE_TAG = "c1auto_it_q5_tekuchest_sql_v2"
CACHE_VERSION = 2


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ry, rm)


def get_it_q5_tekuchest_ytd(year: int | None = None, month: int | None = None) -> dict:
    ref_y, ref_m = normalize_period(year, month)
    payload = get_ytd_via_cache(
        year=ref_y,
        month=ref_m,
        cache_prefix=CACHE_FILE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="c1auto_it_q5_sql",
        compute_fn=lambda y, m: build_c1_q5_payload(y, m),
        kpi_id="1С-Q5",
    )
    return apply_plan_table_to_turnover_payload(
        payload,
        ref_y,
        ref_m,
        plan_for_month=plan_for_month,
        plan_source="getkpi.c1auto.c1_q5_tekuchest_plan",
    )
