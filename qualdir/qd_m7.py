"""
QD-M7 — предъявления продукции на выходной контроль (qualdir).

Источник: ``Document_ТД_ПредъявлениеПродукцииНаВыходнойКонтроль``.

На плитке за выбранный месяц:
  1) ``fact`` — число документов по реквизиту ``Date``;
  2) ``accepted_to_work_today`` — ``ДатаПринятоВРаботу`` = сегодня;
  3) ``checked_otk_today`` — ``ДатаПроверкиОТК`` = сегодня.

Кэш: помесячно ``qualdir_vyhod_kontrol_<Y>_<MM>.json``; YTD —
``qualdir_qd_m7_ytd_<Y>_<MM>.json``.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import requests

from devdir import ytd_json_cache
from getkpi.techdir_tekuchet import MONTH_RU

from qualdir.brak_report import AUTH
from qualdir.vyhod_kontrol_data import (
    DOC_ENTITY,
    compute_accepted_to_work_today,
    compute_checked_otk_today,
    compute_vyhod_kontrol_month,
)

logger = logging.getLogger(__name__)

_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
SOURCE_TAG = "qualdir_vyhod_kontrol_month_v1"
CACHE_VERSION = 1

QD_M7_YTD_CACHE_PREFIX = "qualdir_qd_m7_ytd"
QD_M7_YTD_DISK_TAG = "qualdir_qd_m7_ytd_payload_v1"
QD_M7_YTD_DISK_VERSION = 1

QD_M7_TILE_EXTRA_KEYS: tuple[str, ...] = (
    "accepted_to_work_today",
    "checked_otk_today",
)


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def vyhod_kontrol_month_cache_path(year: int, month: int) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_vyhod_kontrol_{year}_{month:02d}.json"


def qd_m7_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = _normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M7_YTD_CACHE_PREFIX, ref_y, ref_m)


def qd_m7_tile_cache_path(year: int, month: int) -> Path:
    return qd_m7_ytd_cache_path(year, month)


def _month_row_cache_is_perpetual(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def _load_month_cache(year: int, month: int) -> dict[str, Any] | None:
    path = vyhod_kontrol_month_cache_path(year, month)
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
        with vyhod_kontrol_month_cache_path(year, month).open("w", encoding="utf-8") as handle:
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


def compute_qd_m7_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    cached = _load_month_cache(year, month)
    if cached is not None:
        return cached

    snapshot = compute_vyhod_kontrol_month(year, month, session=session)
    _save_month_cache(year, month, snapshot)
    return snapshot


def _month_row_from_snapshot(snapshot: dict[str, Any], y: int, m: int) -> dict[str, Any]:
    docs_count = snapshot.get("docs_count")
    has_data = bool(snapshot.get("has_data")) and docs_count is not None
    row: dict[str, Any] = {
        "year": y,
        "month": m,
        "month_name": MONTH_RU[m].lower(),
        "plan": None,
        "fact": int(docs_count) if has_data else None,
        "kpi_pct": None,
        "has_data": has_data,
    }
    if has_data:
        row["values_unit"] = "шт."
    return row


def _compute_qd_m7_tile(ref_y: int, ref_m: int) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    session = requests.Session()
    session.auth = AUTH

    try:
        for y, m in _month_pairs(ref_y, ref_m):
            try:
                snapshot = compute_qd_m7_month(y, m, session=session)
            except requests.RequestException as exc:
                logger.warning("QD-M7: OData error %d-%02d: %s", y, m, exc)
                snapshot = {
                    "year": y,
                    "month": m,
                    "docs_count": None,
                    "has_data": False,
                    "error": str(exc),
                }
            monthly_rows.append(_month_row_from_snapshot(snapshot, y, m))

        ref_row = next(
            (row for row in monthly_rows if row["year"] == ref_y and row["month"] == ref_m),
            monthly_rows[-1] if monthly_rows else None,
        )
        accepted = compute_accepted_to_work_today(session=session)
        checked = compute_checked_otk_today(session=session)
        if ref_row is not None:
            ref_row = dict(ref_row)
            ref_row["accepted_to_work_today"] = accepted.get("accepted_to_work_today")
            ref_row["checked_otk_today"] = checked.get("checked_otk_today")

        months_with_data = sum(1 for row in monthly_rows if row.get("has_data"))
        return {
            "data_granularity": "monthly",
            "monthly_data": monthly_rows,
            "last_full_month_row": dict(ref_row) if ref_row else None,
            "ytd": {
                "total_plan": None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "accepted_to_work_today": accepted.get("accepted_to_work_today"),
                "checked_otk_today": checked.get("checked_otk_today"),
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
                "kpi_id": "QD-M7",
                "source": DOC_ENTITY,
                "logic": "qualdir.vyhod_kontrol_data",
                "accepted_to_work_today": accepted,
                "checked_otk_today": checked,
            },
        }
    finally:
        session.close()


def get_qd_m7_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = _normalize_period(year, month)
    disk_path = qd_m7_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any]:
        cached = ytd_json_cache.load_payload(
            disk_path,
            source_tag=QD_M7_YTD_DISK_TAG,
            version=QD_M7_YTD_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached

        try:
            payload = _compute_qd_m7_tile(ref_y, ref_m)
        except Exception as exc:
            logger.exception("Ошибка при расчёте QD-M7 (предъявления на выходной контроль)")
            payload = {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "accepted_to_work_today": None,
                    "checked_otk_today": None,
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
                    "kpi_id": "QD-M7",
                    "source": DOC_ENTITY,
                    "error": str(exc),
                },
            }

        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                disk_path,
                payload,
                source_tag=QD_M7_YTD_DISK_TAG,
                version=QD_M7_YTD_DISK_VERSION,
            )
        return payload

    return ytd_json_cache.resolve_payload(
        disk_path,
        source_tag=QD_M7_YTD_DISK_TAG,
        version=QD_M7_YTD_DISK_VERSION,
        perpetual=perpetual,
        lock_key=f"qualdir_qd_m7_{ref_y}_{ref_m:02d}",
        compute_fn=_runner,
    )
