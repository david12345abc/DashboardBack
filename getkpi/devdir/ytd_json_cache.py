"""Файловый кэш JSON для YTD-payload плиток devdir (RD-M*) и qualdir (QD-M3/M4/Q2).

Файлы: ``getkpi/dashboard/<prefix>_<год>_<месяц>.json``.

Прошлый опорный месяц — без срока годности; текущий календарный месяц —
валиден до смены ``cache_date``.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .rd_monthly_period import normalize_rd_tile_period

CACHE_DIR = Path(__file__).resolve().parent.parent / "dashboard"


def is_ref_period_fully_past(ref_y: int, ref_m: int) -> bool:
    today = date.today()
    return (ref_y, ref_m) < (today.year, today.month)


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
    if perpetual:
        return payload
    if raw.get("cache_date") == date.today().isoformat():
        return payload
    return None


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
