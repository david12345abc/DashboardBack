"""
QD-M1 — внешний брак (директор по качеству / qualdir).

Источник: Document_ТД_Форма0319 через ``qualdir.brak_report`` (см. ``external_brak.py``).
На плитке: всего документов за месяц и разбивка по подразделениям (ОТК-1 / ОТК-2).

Кэш: помесячно ``qualdir_external_brak_<Y>_<MM>.json``; полный YTD —
``qualdir_qd_m1_ytd_<Y>_<MM>.json``.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import requests

from getkpi.cache_manager import locked_call
from getkpi.devdir import ytd_json_cache
from getkpi.techdir_tekuchet import MONTH_RU

from qualdir.brak_report import EXTERNAL_BRAK_ENTITY, compute_external_brak_month

logger = logging.getLogger(__name__)

_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
SOURCE_TAG = "qualdir_external_brak_month_v2"
CACHE_VERSION = 2

QD_M1_YTD_CACHE_PREFIX = "qualdir_qd_m1_ytd"
QD_M1_YTD_DISK_TAG = "qualdir_qd_m1_ytd_payload_v2"
QD_M1_YTD_DISK_VERSION = 2


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def external_brak_month_cache_path(year: int, month: int) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_external_brak_{year}_{month:02d}.json"


def qd_m1_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = _normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M1_YTD_CACHE_PREFIX, ref_y, ref_m)


def qd_m1_tile_cache_path(year: int, month: int) -> Path:
    """Алиас YTD-кэша (для cache_manager / warm)."""
    return qd_m1_ytd_cache_path(year, month)


def _month_row_cache_is_perpetual(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def _load_month_cache(year: int, month: int) -> dict[str, Any] | None:
    path = external_brak_month_cache_path(year, month)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if not _month_row_cache_is_perpetual(year, month):
        if data.get("cache_date") != date.today().isoformat():
            return None
    return data


def _save_month_cache(year: int, month: int, payload: dict[str, Any]) -> None:
    try:
        with external_brak_month_cache_path(year, month).open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    **payload,
                    "source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def compute_qd_m1_external_brak_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    cached = _load_month_cache(year, month)
    if cached is not None:
        return cached

    try:
        snapshot = compute_external_brak_month(year, month, session=session)
    except Exception as exc:
        logger.warning("QD-M1: нет данных за %d-%02d: %s", year, month, exc)
        snapshot = {
            "year": year,
            "month": month,
            "total": None,
            "departments": [],
            "has_data": False,
            "error": str(exc),
        }
    else:
        snapshot.setdefault("has_data", snapshot.get("total") is not None)

    _save_month_cache(year, month, snapshot)
    return snapshot


def _departments_by_month_section(monthly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "year": row["year"],
            "month": row["month"],
            "month_name": row["month_name"],
            "total": row.get("fact"),
            "has_data": row.get("has_data"),
            "departments": [dict(item) for item in row.get("departments") or []],
        }
        for row in monthly_rows
    ]


def _compute_qd_m1_tile(ref_y: int, ref_m: int) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    session = requests.Session()
    from qualdir.brak_report import AUTH

    session.auth = AUTH

    try:
        for y, m in _month_pairs(ref_y, ref_m):
            try:
                snapshot = compute_qd_m1_external_brak_month(y, m, session=session)
            except requests.RequestException as exc:
                logger.warning("QD-M1: OData error %d-%02d: %s", y, m, exc)
                snapshot = {
                    "year": y,
                    "month": m,
                    "total": None,
                    "departments": [],
                    "has_data": False,
                    "error": str(exc),
                }

            total = snapshot.get("total")
            has_data = bool(snapshot.get("has_data")) and total is not None
            row: dict[str, Any] = {
                "year": y,
                "month": m,
                "month_name": MONTH_RU[m].lower(),
                "plan": None,
                "fact": int(total) if has_data else None,
                "kpi_pct": None,
                "has_data": has_data,
                "departments": [dict(item) for item in snapshot.get("departments") or []],
            }
            if has_data:
                row["values_unit"] = "шт."
            monthly_rows.append(row)

        ref_row = next(
            (row for row in monthly_rows if row["year"] == ref_y and row["month"] == ref_m),
            monthly_rows[-1] if monthly_rows else None,
        )
        months_with_data = sum(1 for row in monthly_rows if row.get("has_data"))
        departments_out = [dict(item) for item in (ref_row or {}).get("departments") or []]

        return {
            "data_granularity": "monthly",
            "monthly_data": monthly_rows,
            "last_full_month_row": dict(ref_row) if ref_row else None,
            "departments": departments_out,
            "departments_by_month": _departments_by_month_section(monthly_rows),
            "ytd": {
                "total_plan": None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "kpi_pct": None,
                "months_with_data": months_with_data,
                "months_total": len(monthly_rows),
                **({"values_unit": "шт."} if ref_row and ref_row.get("has_data") else {}),
            },
            "kpi_period": {
                "type": "last_full_month",
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_RU[ref_m],
                "data_complete": bool(ref_row and ref_row.get("has_data")),
            },
            "debug": {
                "status": "ok",
                "kpi_id": "QD-M1",
                "source": EXTERNAL_BRAK_ENTITY,
                "logic": "qualdir.brak_report",
            },
        }
    finally:
        session.close()


def get_qd_m1_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """QD-M1: внешний брак — всего документов с разбивкой по подразделениям, помесячно."""
    ref_y, ref_m = _normalize_period(year, month)
    disk_path = qd_m1_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any]:
        cached = ytd_json_cache.load_payload(
            disk_path,
            source_tag=QD_M1_YTD_DISK_TAG,
            version=QD_M1_YTD_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached

        try:
            payload = _compute_qd_m1_tile(ref_y, ref_m)
        except Exception as exc:
            logger.exception("Ошибка при расчёте QD-M1 (внешний брак)")
            payload = {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "departments": [],
                "departments_by_month": [],
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
                    "month_name": MONTH_RU[ref_m],
                    "data_complete": False,
                },
                "debug": {
                    "status": "error",
                    "kpi_id": "QD-M1",
                    "source": EXTERNAL_BRAK_ENTITY,
                    "error": str(exc),
                },
            }

        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                disk_path,
                payload,
                source_tag=QD_M1_YTD_DISK_TAG,
                version=QD_M1_YTD_DISK_VERSION,
            )
        return payload

    return locked_call(f"qualdir_qd_m1_{ref_y}_{ref_m:02d}", _runner)
