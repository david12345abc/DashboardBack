"""
QD-M10 — предъявления продукции на входной контроль (Алмаз).

Эталон (OData): DashboardBack/qualdir/qd_m10.py
  → та же логика, что QD-M6, фильтр Организация = АЛМАЗ ООО

SQL: см. qualdir/qd_m6.py (_Document122024, _Fld122026RRef).

Использование:
  python qualdir/qd_m10.py
  python qualdir/qd_m10.py 2026
  python qualdir/qd_m10.py 2026-01 2026-03
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from qualdir.qd_m6 import (
    ORG_ALMAZ_BIN,
    ORG_ALMAZ_KEY,
    ORG_ALMAZ_NAME,
    build_otk_payload,
    run_cli,
)


def build_qd_m10_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_otk_payload(
        year,
        month,
        kpi_id="QD-M10",
        organization_bin=ORG_ALMAZ_BIN,
        organization_key=ORG_ALMAZ_KEY,
        organization_name=ORG_ALMAZ_NAME,
        source_module="qualdir.qd_m10.sql",
    )


def main() -> None:
    try:
        run_cli(
            kpi_id="QD-M10",
            file_prefix="qd_m10",
            organization_bin=ORG_ALMAZ_BIN,
            organization_name=ORG_ALMAZ_NAME,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

QD_M10_YTD_CACHE_PREFIX = "qualdir_qd_m10_ytd"
QD_M10_YTD_DISK_TAG = "qualdir_qd_m10_ytd_payload_sql_v1"
QD_M10_YTD_DISK_VERSION = 13


def otk_predyavlenie_almaz_month_cache_path(year: int, month: int) -> _Path:
    return month_cache_path("qualdir_otk_predyavlenie_sql_almaz", year, month)


def qd_m10_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M10_YTD_CACHE_PREFIX, ry, rm)


def qd_m10_tile_cache_path(year: int, month: int) -> _Path:
    return qd_m10_ytd_cache_path(year, month)


def compute_qd_m10_month(year: int, month: int) -> dict:
    from qualdir.qd_m6 import calc_month

    return calc_month(year, month, organization_bin=ORG_ALMAZ_BIN)


def get_qd_m10_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M10_YTD_CACHE_PREFIX,
        source_tag=QD_M10_YTD_DISK_TAG,
        version=QD_M10_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m10_sql",
        compute_fn=lambda y, m: build_qd_m10_payload(y, m),
        kpi_id="QD-M10",
    )
