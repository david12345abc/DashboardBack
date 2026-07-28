"""Общий ежедневный YTD-кэш для qualdir-плиток на SQL-бэкапе / новых эталонах."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Callable

from devdir import ytd_json_cache

logger = logging.getLogger(__name__)

_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"


def normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    try:
        if date(ref_year, ref_month, 1) > date(today.year, today.month, 1):
            ref_year, ref_month = today.year, today.month
    except ValueError:
        pass
    return ref_year, ref_month


def month_cache_path(prefix: str, year: int, month: int) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"{prefix}_{year}_{month:02d}.json"


def get_ytd_via_cache(
    *,
    year: int | None,
    month: int | None,
    cache_prefix: str,
    source_tag: str,
    version: int,
    lock_key_prefix: str,
    compute_fn: Callable[[int, int], dict[str, Any]],
    kpi_id: str,
    error_factory: Callable[[int, int, Exception], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Stale-while-revalidate поверх ytd_json_cache (daily для текущего месяца)."""
    ref_y, ref_m = normalize_period(year, month)
    disk_path = ytd_json_cache.cache_path(cache_prefix, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any]:
        cached = ytd_json_cache.load_payload(
            disk_path,
            source_tag=source_tag,
            version=version,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = compute_fn(ref_y, ref_m)
        except Exception as exc:
            logger.exception("%s: ошибка расчёта YTD %d-%02d", kpi_id, ref_y, ref_m)
            if error_factory is not None:
                payload = error_factory(ref_y, ref_m, exc)
            else:
                payload = {
                    "data_granularity": "monthly",
                    "monthly_data": [],
                    "last_full_month_row": None,
                    "ytd": {
                        "total_plan": None,
                        "total_fact": None,
                        "kpi_pct": None,
                        "months_with_data": 0,
                        "months_total": 0,
                    },
                    "kpi_period": {
                        "type": "last_full_month",
                        "year": ref_y,
                        "month": ref_m,
                        "month_name": str(ref_m),
                        "data_complete": False,
                    },
                    "debug": {
                        "status": "error",
                        "kpi_id": kpi_id,
                        "error": str(exc),
                    },
                }
        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                disk_path,
                payload,
                source_tag=source_tag,
                version=version,
            )
        return payload

    return ytd_json_cache.resolve_payload(
        disk_path,
        source_tag=source_tag,
        version=version,
        perpetual=perpetual,
        lock_key=f"{lock_key_prefix}_{ref_y}_{ref_m:02d}",
        compute_fn=_runner,
    )
