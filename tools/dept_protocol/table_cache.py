"""Файловый кэш помесячных блоков DEPT-T-PROTOCOL-OVERDUE (подразделение × год × месяц)."""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from getkpi.cache_manager import CACHE_DIR

from . import _bootstrap  # noqa: F401

from lookup_source_department import DEFAULT_DEPARTMENTS, load_departments  # type: ignore[import-untyped]
from list_enterprise_positions import normalize_text  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

CACHE_SUBDIR = CACHE_DIR / "dept_protocol_overdue"
SOURCE_TAG = "dept_protocol_overdue_month_v1"
CACHE_VERSION = 3
LEGACY_CACHE_VERSIONS = frozenset({2, 3})


def _digest_for_department(department: str) -> str | None:
    source_key = _source_key(department)
    if not source_key:
        return None
    return hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:20]


def _source_key(department: str) -> str | None:
    if not (department or "").strip():
        return None
    if not DEFAULT_DEPARTMENTS.is_file():
        return None
    departments = load_departments(DEFAULT_DEPARTMENTS)
    from lookup_source_department import resolve_source  # type: ignore[import-untyped]

    exact, partial = resolve_source(department.strip(), departments)
    entry = (exact or partial[:1] or [None])[0]
    if not entry:
        return None
    source = str(entry.get("source") or department).strip()
    return normalize_text(source) or None


def is_month_fully_past(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def cache_path(department: str, year: int, month: int) -> Path | None:
    digest = _digest_for_department(department)
    if digest is None:
        return None
    return CACHE_SUBDIR / f"{digest}_{year}_{month:02d}.json"


def warm_stamp_path(for_day: date | None = None) -> Path:
    day = for_day or date.today()
    return CACHE_SUBDIR / f"warm_{day.isoformat()}.stamp"


def lock_key(department: str) -> str:
    source_key = _source_key(department) or normalize_text(department) or "unknown"
    digest = hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:16]
    return f"dept_protocol_overdue_{digest}"


def warm_lock_key() -> str:
    return "dept_protocol_overdue_warm_all"


def _read_month_file(path: Path, *, require_today: bool) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if raw.get("cache_source") != SOURCE_TAG:
        return None
    if raw.get("cache_version") not in LEGACY_CACHE_VERSIONS:
        return None
    year = raw.get("year")
    month = raw.get("month")
    if not isinstance(year, int) or not isinstance(month, int):
        return None
    if require_today and not is_month_fully_past(year, month):
        if raw.get("cache_date") != date.today().isoformat():
            return None
    payload = raw.get("payload")
    return payload if isinstance(payload, dict) else None


def load_month_block(
    department: str,
    year: int,
    month: int,
    *,
    allow_stale: bool = False,
) -> dict[str, Any] | None:
    path = cache_path(department, year, month)
    if path is None or not path.exists():
        return None
    block = _read_month_file(path, require_today=not allow_stale)
    if block is None and allow_stale:
        block = _read_month_file(path, require_today=False)
        if block is not None:
            block = dict(block)
            block["cache_stale"] = True
    if block is not None:
        from tools.dept_protocol.dashboard_table import sanitize_month_block

        block = sanitize_month_block(block)
    return block


def save_month_block(
    department: str,
    year: int,
    month: int,
    block: dict[str, Any],
) -> None:
    path = cache_path(department, year, month)
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "cache_source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "year": year,
                    "month": month,
                    "department_query": (department or "").strip(),
                    "source_key": _source_key(department),
                    "payload": block,
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        logger.exception("dept_protocol: не удалось сохранить кэш %s", path)


def iter_configured_sources() -> list[str]:
    if not DEFAULT_DEPARTMENTS.is_file():
        return []
    return [
        str(row.get("source") or "").strip()
        for row in load_departments(DEFAULT_DEPARTMENTS)
        if str(row.get("source") or "").strip()
    ]


def all_sources_cached_for_months(month_pairs: list[tuple[int, int]]) -> bool:
    for source in iter_configured_sources():
        for year, month in month_pairs:
            if load_month_block(source, year, month) is None:
                return False
    return True


def mark_warm_complete() -> None:
    CACHE_SUBDIR.mkdir(parents=True, exist_ok=True)
    warm_stamp_path().write_text(date.today().isoformat(), encoding="utf-8")


def is_warm_complete_today() -> bool:
    stamp = warm_stamp_path()
    if not stamp.is_file():
        return False
    try:
        return stamp.read_text(encoding="utf-8").strip() == date.today().isoformat()
    except OSError:
        return False
