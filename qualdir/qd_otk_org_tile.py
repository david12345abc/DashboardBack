"""Общая сборка плиток предъявлений на ОТК (QD-M6 / QD-M9 / QD-M10)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable

import requests

from devdir import ytd_json_cache
from getkpi.cache_manager import locked_call
from getkpi.techdir_tekuchet import MONTH_RU

from qualdir.brak_report import AUTH
from qualdir.otk_predyavlenie_data import DOC_ENTITY, compute_in_work_today, compute_otk_predyavlenie_month
from qualdir.turnover import _qd_q2_kpi_pct

logger = logging.getLogger(__name__)

_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
_MONTH_CACHE_META = frozenset({"source", "cache_version", "cache_date"})
LEGACY_M6_SOURCE_TAG = "qualdir_otk_predyavlenie_month_v4"
LEGACY_M6_CACHE_VERSION = 4


@dataclass(frozen=True)
class OtkOrgTileConfig:
    kpi_id: str
    org_slug: str
    source_tag: str
    cache_version: int
    ytd_cache_prefix: str
    ytd_disk_tag: str
    ytd_disk_version: int
    organization_key: str | None = None
    organization_name: str | None = None
    log_prefix: str = "QD-M6"


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def month_cache_path(config: OtkOrgTileConfig, year: int, month: int) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_otk_predyavlenie_{config.org_slug}_{year}_{month:02d}.json"


def legacy_month_cache_path(config: OtkOrgTileConfig, year: int, month: int) -> Path | None:
    """Старый путь QD-M6 до разделения по организациям (``org_slug=all``)."""
    if config.org_slug != "all":
        return None
    return _CACHE_ROOT / f"qualdir_otk_predyavlenie_{year}_{month:02d}.json"


def ytd_cache_path(config: OtkOrgTileConfig, year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = _normalize_period(year, month)
    return ytd_json_cache.cache_path(config.ytd_cache_prefix, ref_y, ref_m)


def tile_cache_path(config: OtkOrgTileConfig, year: int, month: int) -> Path:
    return ytd_cache_path(config, year, month)


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
    return snapshot.get("docs_count") is not None


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


def _read_month_cache_file(
    path: Path,
    *,
    year: int,
    month: int,
    source_tag: str,
    cache_version: int,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != source_tag:
        return None
    if data.get("cache_version") != cache_version:
        return None
    if not _month_row_cache_is_perpetual(year, month):
        if data.get("cache_date") != date.today().isoformat():
            return None
    snapshot = _unwrap_month_snapshot(data)
    if not _month_snapshot_is_valid(snapshot):
        return None
    return snapshot


def _load_month_cache(config: OtkOrgTileConfig, year: int, month: int) -> dict[str, Any] | None:
    snapshot = _read_month_cache_file(
        month_cache_path(config, year, month),
        year=year,
        month=month,
        source_tag=config.source_tag,
        cache_version=config.cache_version,
    )
    if snapshot is not None:
        return snapshot

    legacy_path = legacy_month_cache_path(config, year, month)
    if legacy_path is None:
        return None
    return _read_month_cache_file(
        legacy_path,
        year=year,
        month=month,
        source_tag=LEGACY_M6_SOURCE_TAG,
        cache_version=LEGACY_M6_CACHE_VERSION,
    )


def _save_month_cache(
    config: OtkOrgTileConfig,
    year: int,
    month: int,
    payload: dict[str, Any],
) -> None:
    try:
        with month_cache_path(config, year, month).open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    **payload,
                    "source": config.source_tag,
                    "cache_version": config.cache_version,
                    "cache_date": date.today().isoformat(),
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def _purge_invalid_ytd_cache(config: OtkOrgTileConfig, path: Path) -> None:
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
        tag != config.ytd_disk_tag
        or version != config.ytd_disk_version
        or not _cached_payload_is_valid(payload)
    ):
        try:
            path.unlink(missing_ok=True)
            logger.info("%s: удалён устаревший YTD-кэш %s", config.kpi_id, path.name)
        except OSError:
            pass


def compute_month(
    config: OtkOrgTileConfig,
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    cached = _load_month_cache(config, year, month)
    if cached is not None:
        return cached

    try:
        snapshot = compute_otk_predyavlenie_month(
            year,
            month,
            session=session,
            organization_key=config.organization_key,
            organization_name=config.organization_name,
            kpi_id=config.kpi_id,
            log_prefix=config.log_prefix,
        )
    except requests.RequestException as exc:
        logger.warning("%s: OData error %d-%02d: %s", config.kpi_id, year, month, exc)
        snapshot = {
            "year": year,
            "month": month,
            "docs_count": None,
            "executed_count": None,
            "rejected_items_count": None,
            "has_data": False,
            "organization_key": config.organization_key,
            "organization_name": config.organization_name,
            "error": str(exc),
        }
    else:
        snapshot.setdefault("has_data", snapshot.get("docs_count") is not None)

    _save_month_cache(config, year, month, snapshot)
    return snapshot


def _month_row_from_snapshot(snapshot: dict[str, Any], y: int, m: int) -> dict[str, Any]:
    docs_count = snapshot.get("docs_count")
    executed_count = snapshot.get("executed_count")
    rejected_items_count = snapshot.get("rejected_items_count")
    has_data = bool(snapshot.get("has_data")) and docs_count is not None
    plan = int(docs_count) if has_data else None
    fact = int(executed_count) if has_data and executed_count is not None else None
    rejected = (
        int(rejected_items_count)
        if has_data and rejected_items_count is not None
        else None
    )
    row: dict[str, Any] = {
        "year": y,
        "month": m,
        "month_name": MONTH_RU[m].lower(),
        "plan": plan,
        "fact": fact,
        "docs_count": plan,
        "executed_count": fact,
        "rejected_items_count": rejected,
        "kpi_pct": _qd_q2_kpi_pct(plan, fact),
        "has_data": has_data,
    }
    if has_data:
        row["values_unit"] = "шт."
    return row


def _compute_tile(config: OtkOrgTileConfig, ref_y: int, ref_m: int) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    session = requests.Session()
    session.auth = AUTH

    try:
        for y, m in _month_pairs(ref_y, ref_m):
            try:
                snapshot = compute_month(config, y, m, session=session)
            except requests.RequestException as exc:
                logger.warning("%s: OData error %d-%02d: %s", config.kpi_id, y, m, exc)
                snapshot = {
                    "year": y,
                    "month": m,
                    "docs_count": None,
                    "executed_count": None,
                    "rejected_items_count": None,
                    "has_data": False,
                    "error": str(exc),
                }
            monthly_rows.append(_month_row_from_snapshot(snapshot, y, m))

        ref_row = next(
            (row for row in monthly_rows if row["year"] == ref_y and row["month"] == ref_m),
            monthly_rows[-1] if monthly_rows else None,
        )
        in_work = compute_in_work_today(
            session=session,
            organization_key=config.organization_key,
            log_tag=f"{config.log_prefix} in_work",
        )
        if ref_row is not None:
            ref_row = dict(ref_row)
            ref_row["in_work_today"] = in_work.get("in_work_today")

        months_with_data = sum(1 for row in monthly_rows if row.get("has_data"))
        return {
            "data_granularity": "monthly",
            "monthly_data": monthly_rows,
            "last_full_month_row": dict(ref_row) if ref_row else None,
            "ytd": {
                "total_plan": ref_row.get("plan") if ref_row else None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                "rejected_items_count": ref_row.get("rejected_items_count") if ref_row else None,
                "in_work_today": in_work.get("in_work_today"),
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
                "kpi_id": config.kpi_id,
                "source": DOC_ENTITY,
                "organization_key": config.organization_key,
                "organization_name": config.organization_name,
                "logic": "qualdir.otk_predyavlenie_data",
                "in_work_today": in_work,
            },
        }
    finally:
        session.close()


def get_ytd(
    config: OtkOrgTileConfig,
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    ref_y, ref_m = _normalize_period(year, month)
    disk_path = ytd_cache_path(config, ref_y, ref_m)
    lock_key = f"qualdir_{config.kpi_id.lower()}_{ref_y}_{ref_m:02d}"

    def _runner() -> dict[str, Any]:
        _purge_invalid_ytd_cache(config, disk_path)

        try:
            payload = _compute_tile(config, ref_y, ref_m)
        except Exception as exc:
            logger.exception("Ошибка при расчёте %s (предъявления на ОТК)", config.kpi_id)
            stale = ytd_json_cache.load_stale_payload(
                disk_path,
                source_tag=config.ytd_disk_tag,
                version=config.ytd_disk_version,
            )
            if stale is not None and _cached_payload_is_valid(stale):
                stale_payload = dict(stale)
                stale_debug = dict(stale_payload.get("debug") or {})
                stale_debug.update(
                    {
                        "status": "stale",
                        "kpi_id": config.kpi_id,
                        "error": str(exc),
                    }
                )
                stale_payload["debug"] = stale_debug
                return stale_payload

            payload = {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "rejected_items_count": None,
                    "in_work_today": None,
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
                    "kpi_id": config.kpi_id,
                    "source": DOC_ENTITY,
                    "organization_key": config.organization_key,
                    "organization_name": config.organization_name,
                    "error": str(exc),
                },
            }

        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                disk_path,
                payload,
                source_tag=config.ytd_disk_tag,
                version=config.ytd_disk_version,
            )
        return payload

    return locked_call(lock_key, _runner)


def build_tile_exports(config: OtkOrgTileConfig) -> dict[str, Callable[..., Any]]:
    return {
        "get_ytd": lambda year=None, month=None: get_ytd(config, year=year, month=month),
        "otk_predyavlenie_month_cache_path": lambda year, month: month_cache_path(config, year, month),
        "ytd_cache_path": lambda year=None, month=None: ytd_cache_path(config, year, month),
        "tile_cache_path": lambda year, month: tile_cache_path(config, year, month),
        "compute_month": lambda year, month, session=None: compute_month(
            config, year, month, session=session,
        ),
    }
