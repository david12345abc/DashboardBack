"""Stale-while-revalidate для файловых кэшей плиток «Технический директор» (TD-*)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Callable

from .cache_manager import stale_while_revalidate


def is_current_calendar_month(year: int, month: int) -> bool:
    today = date.today()
    return year == today.year and month == today.month


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def load_fresh_month_file(
    path: Path,
    *,
    source_tag: str,
    cache_version: int,
    year: int,
    month: int,
    source_field: str = "source",
) -> dict[str, Any] | None:
    """Свежий помесячный кэш: для текущего месяца — только cache_date == today."""
    data = read_json(path)
    if data is None:
        return None
    if data.get(source_field) != source_tag:
        return None
    if data.get("cache_version") != cache_version:
        return None
    if is_current_calendar_month(year, month):
        return data if data.get("cache_date") == date.today().isoformat() else None
    return data


def load_stale_month_file(
    path: Path,
    *,
    source_tag: str,
    cache_version: int,
    source_field: str = "source",
) -> dict[str, Any] | None:
    """Помесячный кэш без проверки cache_date."""
    data = read_json(path)
    if data is None:
        return None
    if data.get(source_field) != source_tag:
        return None
    if data.get("cache_version") != cache_version:
        return None
    return data


def resolve_month_file(
    key: str,
    path: Path,
    *,
    source_tag: str,
    cache_version: int,
    year: int,
    month: int,
    compute_fn: Callable[[], dict[str, Any]],
    source_field: str = "source",
) -> dict[str, Any]:
    """Отдать свежий / устаревший помесячный кэш или пересчитать синхронно."""
    return stale_while_revalidate(
        key,
        lambda: load_fresh_month_file(
            path,
            source_tag=source_tag,
            cache_version=cache_version,
            year=year,
            month=month,
            source_field=source_field,
        ),
        lambda: load_stale_month_file(
            path,
            source_tag=source_tag,
            cache_version=cache_version,
            source_field=source_field,
        ),
        compute_fn,
    )


def load_fresh_snapshot_file(
    path: Path,
    *,
    cache_version: int,
    extra_validators: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, Any] | None:
    """Снимок с cache_date == today (например techdir_projects_snapshot.json)."""
    data = read_json(path)
    if data is None:
        return None
    if data.get("cache_version") != cache_version:
        return None
    if extra_validators is not None and not extra_validators(data):
        return None
    return data if data.get("cache_date") == date.today().isoformat() else None


def load_stale_snapshot_file(
    path: Path,
    *,
    cache_version: int,
    extra_validators: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, Any] | None:
    """Снимок без проверки cache_date."""
    data = read_json(path)
    if data is None:
        return None
    if data.get("cache_version") != cache_version:
        return None
    if extra_validators is not None and not extra_validators(data):
        return None
    return data
