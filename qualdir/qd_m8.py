"""
QD-M8 — документы ``Document_ТД_Форма0317`` (qualdir).

За выбранный месяц:
  1) ``plan`` — заявки по ``Date`` (исключены ``НеСогласовано``, ``Отменена``, ``НаСогласовании``);
  2) ``fact`` — исполненные (``Статус = Выполнено``);
  3) ``significant`` — значимые (``ФормаЯвляетсяЗначимой = Истина``);
  4) ``departments`` — ``ПодразделениеПоставщика``;
  5) ``kinds`` — ``ВидНесоответствия`` из ТЧ ``Несоответствия``.

Кэш OData: ``qualdir_forma0317_<Y>_<MM>.json``.
YTD-файл только для mtime / warm; данные плитки всегда собираются заново из помесячных снимков.
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

from qualdir.brak_report import AUTH, FORM_0317_ENTITY, compute_forma0317_month
from qualdir.turnover import _qd_q2_kpi_pct

logger = logging.getLogger(__name__)

_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
_MONTH_CACHE_META = frozenset({"source", "cache_version", "cache_date"})
SOURCE_TAG = "qualdir_forma0317_month_v13"
CACHE_VERSION = 13

QD_M8_YTD_CACHE_PREFIX = "qualdir_qd_m8_ytd"
QD_M8_YTD_DISK_TAG = "qualdir_qd_m8_ytd_payload_v15"
QD_M8_YTD_DISK_VERSION = 15


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def forma0317_month_cache_path(year: int, month: int) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_forma0317_{year}_{month:02d}.json"


def qd_m8_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = _normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M8_YTD_CACHE_PREFIX, ref_y, ref_m)


def qd_m8_tile_cache_path(year: int, month: int) -> Path:
    return qd_m8_ytd_cache_path(year, month)


def _month_row_cache_is_perpetual(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def _unwrap_month_snapshot(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if key not in _MONTH_CACHE_META}


def _month_snapshot_is_valid(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    if not snapshot.get("has_data"):
        return True
    if snapshot.get("total") is None:
        return False
    return "executed" in snapshot


def _month_row_has_plan_fact(row: dict[str, Any]) -> bool:
    if not row.get("has_data"):
        return True
    if row.get("plan") is None and row.get("fact") is not None:
        return False
    return row.get("plan") is not None and "kpi_pct" in row


def _cached_payload_is_valid(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    row = payload.get("last_full_month_row")
    if isinstance(row, dict) and not _month_row_has_plan_fact(row):
        return False
    for item in payload.get("monthly_data") or []:
        if isinstance(item, dict) and not _month_row_has_plan_fact(item):
            return False
    return True


def _purge_invalid_ytd_cache(path: Path) -> None:
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return
    payload = raw.get("payload") if isinstance(raw, dict) else None
    tag = raw.get("cache_source") if isinstance(raw, dict) else None
    version = raw.get("cache_version") if isinstance(raw, dict) else None
    if (
        tag != QD_M8_YTD_DISK_TAG
        or version != QD_M8_YTD_DISK_VERSION
        or not _cached_payload_is_valid(payload)
    ):
        try:
            path.unlink(missing_ok=True)
            logger.info("QD-M8: удалён устаревший YTD-кэш %s", path.name)
        except OSError:
            pass


def _load_month_cache(year: int, month: int) -> dict[str, Any] | None:
    path = forma0317_month_cache_path(year, month)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    source = data.get("source")
    version = data.get("cache_version")
    if source != SOURCE_TAG or version != CACHE_VERSION:
        return None
    if not _month_row_cache_is_perpetual(year, month):
        if data.get("cache_date") != date.today().isoformat():
            return None
    snapshot = _unwrap_month_snapshot(data)
    if not _month_snapshot_is_valid(snapshot):
        return None
    return snapshot


def _save_month_cache(year: int, month: int, payload: dict[str, Any]) -> None:
    try:
        with forma0317_month_cache_path(year, month).open("w", encoding="utf-8") as handle:
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


def compute_qd_m8_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    cached = _load_month_cache(year, month)
    if cached is not None:
        return cached

    snapshot = compute_forma0317_month(year, month, session=session)
    _save_month_cache(year, month, snapshot)
    return snapshot


def _month_row_from_snapshot(snapshot: dict[str, Any], y: int, m: int) -> dict[str, Any]:
    total = snapshot.get("total")
    executed = snapshot.get("executed")
    significant = snapshot.get("significant")
    has_data = bool(snapshot.get("has_data")) and total is not None
    plan = int(total) if has_data else None
    fact = int(executed) if has_data and executed is not None else None
    sig = int(significant) if has_data and significant is not None else None
    row: dict[str, Any] = {
        "year": y,
        "month": m,
        "month_name": MONTH_RU[m].lower(),
        "plan": plan,
        "fact": fact,
        "significant": sig,
        "kpi_pct": _qd_q2_kpi_pct(plan, fact),
        "has_data": has_data,
        "departments": [dict(item) for item in snapshot.get("departments") or []],
        "kinds": [dict(item) for item in snapshot.get("kinds") or []],
    }
    if has_data:
        row["values_unit"] = "шт."
    return row


def _breakdown_by_month_section(monthly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "year": row["year"],
            "month": row["month"],
            "month_name": row["month_name"],
            "total": row.get("plan"),
            "has_data": row.get("has_data"),
            "departments": [dict(item) for item in row.get("departments") or []],
            "kinds": [dict(item) for item in row.get("kinds") or []],
        }
        for row in monthly_rows
    ]


def _compute_qd_m8_tile(ref_y: int, ref_m: int) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    session = requests.Session()
    session.auth = AUTH

    try:
        for y, m in _month_pairs(ref_y, ref_m):
            try:
                snapshot = compute_qd_m8_month(y, m, session=session)
            except requests.RequestException as exc:
                logger.warning("QD-M8: OData error %d-%02d: %s", y, m, exc)
                snapshot = {
                    "year": y,
                    "month": m,
                    "total": None,
                    "executed": None,
                    "significant": None,
                    "departments": [],
                    "kinds": [],
                    "has_data": False,
                    "error": str(exc),
                }
            monthly_rows.append(_month_row_from_snapshot(snapshot, y, m))

        ref_row = next(
            (row for row in monthly_rows if row["year"] == ref_y and row["month"] == ref_m),
            monthly_rows[-1] if monthly_rows else None,
        )
        months_with_data = sum(1 for row in monthly_rows if row.get("has_data"))
        departments_out = [dict(item) for item in (ref_row or {}).get("departments") or []]
        kinds_out = [dict(item) for item in (ref_row or {}).get("kinds") or []]

        return {
            "data_granularity": "monthly",
            "monthly_data": monthly_rows,
            "last_full_month_row": dict(ref_row) if ref_row else None,
            "departments": departments_out,
            "kinds": kinds_out,
            "breakdown_by_month": _breakdown_by_month_section(monthly_rows),
            "ytd": {
                "total_plan": ref_row.get("plan") if ref_row else None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "total_significant": ref_row.get("significant") if ref_row else None,
                "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
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
                "kpi_id": "QD-M8",
                "source": FORM_0317_ENTITY,
                "logic": "qualdir.brak_report.compute_forma0317_month",
            },
        }
    finally:
        session.close()


def get_qd_m8_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = _normalize_period(year, month)
    disk_path = qd_m8_ytd_cache_path(ref_y, ref_m)

    def _runner() -> dict[str, Any]:
        _purge_invalid_ytd_cache(disk_path)

        try:
            payload = _compute_qd_m8_tile(ref_y, ref_m)
        except Exception as exc:
            logger.exception("Ошибка при расчёте QD-M8 (форма 0317)")
            payload = {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "departments": [],
                "kinds": [],
                "breakdown_by_month": [],
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
                    "kpi_id": "QD-M8",
                    "source": FORM_0317_ENTITY,
                    "error": str(exc),
                },
            }

        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                disk_path,
                payload,
                source_tag=QD_M8_YTD_DISK_TAG,
                version=QD_M8_YTD_DISK_VERSION,
            )
        return payload

    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)
    return ytd_json_cache.resolve_payload(
        disk_path,
        source_tag=QD_M8_YTD_DISK_TAG,
        version=QD_M8_YTD_DISK_VERSION,
        perpetual=perpetual,
        lock_key=f"qualdir_qd_m8_{ref_y}_{ref_m:02d}",
        compute_fn=_runner,
    )
