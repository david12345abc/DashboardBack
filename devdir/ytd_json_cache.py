"""Файловый кэш JSON для YTD-payload плиток devdir (RD-M*) и qualdir (QD-M3/M4/Q2).

Файлы: ``getkpi/dashboard/<prefix>_<год>_<месяц>.json``.

Прошлый опорный месяц — без срока годности после окончания месяца; снимок,
собранный до последнего дня месяца, считается устаревшим. Текущий календарный
месяц — валиден до смены ``cache_date``.
"""

from __future__ import annotations

import json
from calendar import monthrange
from datetime import date
from pathlib import Path
from typing import Any

from .rd_monthly_period import normalize_rd_tile_period

CACHE_DIR = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"


def is_ref_period_fully_past(ref_y: int, ref_m: int) -> bool:
    today = date.today()
    return (ref_y, ref_m) < (today.year, today.month)


def _period_end(ref_y: int, ref_m: int) -> date:
    return date(ref_y, ref_m, monthrange(ref_y, ref_m)[1])


def _cache_date(raw: dict) -> date | None:
    try:
        return date.fromisoformat(str(raw.get("cache_date") or "")[:10])
    except ValueError:
        return None


def _period_from_path(path: Path) -> tuple[int, int] | None:
    parts = path.stem.rsplit("_", 2)
    if len(parts) < 3:
        return None
    try:
        ref_y = int(parts[-2])
        ref_m = int(parts[-1])
    except ValueError:
        return None
    if 1 <= ref_m <= 12:
        return ref_y, ref_m
    return None


def _payload_period(raw: dict) -> tuple[int, int] | None:
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    try:
        ref_y = int(payload.get("year"))
        ref_m = int(payload.get("month"))
    except (TypeError, ValueError):
        return None
    if 1 <= ref_m <= 12:
        return ref_y, ref_m
    return None


def _resolve_period(raw: dict, path: Path) -> tuple[int, int] | None:
    return _payload_period(raw) or _period_from_path(path)


def cache_path(file_prefix: str, ref_y: int, ref_m: int) -> Path:
    return CACHE_DIR / f"{file_prefix}_{ref_y}_{ref_m:02d}.json"


def public_cache_path(file_prefix: str, year: int | None = None, month: int | None = None) -> Path:
    y, m = normalize_rd_tile_period(year, month)
    return cache_path(file_prefix, y, m)


def load_payload(
    path: Path,
    *,
    source_tag: str,
    version: int,
    perpetual: bool,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if raw.get("cache_source") != source_tag:
        return None
    if raw.get("cache_version") != version:
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    cache_day = _cache_date(raw)
    if cache_day is None:
        return None
    if perpetual:
        period = _resolve_period(raw, path)
        if period is None:
            return payload
        ref_y, ref_m = period
        if cache_day < _period_end(ref_y, ref_m):
            return None
        return payload
    if cache_day == date.today():
        return payload
    return None


def load_stale_payload(
    path: Path,
    *,
    source_tag: str,
    version: int,
) -> dict[str, Any] | None:
    """Payload из файла без проверки ``cache_date`` — fallback при ошибке OData."""
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if raw.get("cache_source") != source_tag:
        return None
    if raw.get("cache_version") != version:
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    return dict(payload)


def save_payload(
    path: Path,
    payload: dict[str, Any],
    *,
    source_tag: str,
    version: int,
) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "cache_source": source_tag,
                    "cache_version": version,
                    "cache_date": date.today().isoformat(),
                    "payload": payload,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def resolve_payload(
    path: Path,
    *,
    source_tag: str,
    version: int,
    perpetual: bool,
    lock_key: str,
    compute_fn,
) -> dict[str, Any] | None:
    """Stale-while-revalidate для YTD-payload (GSPP, devdir, qualdir)."""
    from getkpi.cache_manager import stale_while_revalidate

    return stale_while_revalidate(
        lock_key,
        lambda: load_payload(
            path,
            source_tag=source_tag,
            version=version,
            perpetual=perpetual,
        ),
        lambda: load_stale_payload(
            path,
            source_tag=source_tag,
            version=version,
        ),
        compute_fn,
    )
