"""
Кэш факта TD-M5 по паре (проект, год, месяц).

Файлы: ``getkpi/dashboard/techdir_m5_fact/{YYYY}_{MM}/{hash}.json``.
Прошлые месяцы — без срока годности; текущий календарный месяц — до смены cache_date.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import requests

from . import ext_budj_fact

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / "dashboard" / "techdir_m5_fact"
SOURCE_TAG = "techdir_m5_fact_project_month_v1"
CACHE_VERSION = 1


def is_period_fully_past(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def _entry_path(project_name: str, year: int, month: int, criterion: str) -> Path:
    norm = ext_budj_fact.normalize_name(project_name)
    digest = hashlib.sha256(
        f"{norm}|{year}|{month:02d}|{criterion}".encode("utf-8"),
    ).hexdigest()[:24]
    return CACHE_DIR / f"{year}_{month:02d}" / f"{digest}.json"


def load_fact(
    project_name: str,
    year: int,
    month: int,
    *,
    criterion: str,
) -> float | None:
    path = _entry_path(project_name, year, month, criterion)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return None
    if raw.get("cache_source") != SOURCE_TAG:
        return None
    if raw.get("cache_version") != CACHE_VERSION:
        return None
    if raw.get("criterion") != criterion:
        return None
    if raw.get("year") != year or raw.get("month") != month:
        return None
    stored_name = ext_budj_fact.normalize_name(str(raw.get("project_name") or ""))
    if stored_name != ext_budj_fact.normalize_name(project_name):
        return None
    if not is_period_fully_past(year, month):
        if raw.get("cache_date") != date.today().isoformat():
            return None
    try:
        return round(float(raw["fact"]), 2)
    except (TypeError, ValueError, KeyError):
        return None


def save_fact(
    project_name: str,
    year: int,
    month: int,
    fact: float,
    *,
    criterion: str,
) -> None:
    path = _entry_path(project_name, year, month, criterion)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "cache_source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "project_name": (project_name or "").strip(),
                    "year": year,
                    "month": month,
                    "criterion": criterion,
                    "fact": round(float(fact), 2),
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        logger.exception("Не удалось сохранить кэш факта TD-M5: %s", path)


def compute_fact_total_rub_cached(
    session: requests.Session,
    project_full_name: str,
    year: int,
    month: int,
    *,
    criterion: str = "payment",
    property_row: dict | None = None,
    stats: dict[str, int] | None = None,
) -> float:
    """Факт за месяц с файловым кэшем; при промахе — OData через ext_budj_fact."""
    name = (project_full_name or "").strip()
    if not name:
        return 0.0

    cached = load_fact(name, year, month, criterion=criterion)
    if cached is not None:
        if stats is not None:
            stats["hits"] = stats.get("hits", 0) + 1
        return cached

    if stats is not None:
        stats["misses"] = stats.get("misses", 0) + 1

    amount = ext_budj_fact.compute_fact_total_rub(
        session,
        name,
        year,
        month,
        criterion=criterion,
        property_row=property_row,
    )
    save_fact(name, year, month, amount, criterion=criterion)
    return amount


def compute_fact_totals_for_projects_cached(
    session: requests.Session,
    project_full_names: list[str],
    year: int,
    month: int,
    *,
    criterion: str = "payment",
    stats: dict[str, int] | None = None,
) -> tuple[float, list[dict[str, Any]]]:
    """Сумма факта по списку проектов за месяц (с кэшем по каждому проекту)."""
    prop = ext_budj_fact.load_project_property(session, False)
    total = 0.0
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_name in project_full_names:
        name = (raw_name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        amount = compute_fact_total_rub_cached(
            session,
            name,
            year,
            month,
            criterion=criterion,
            property_row=prop,
            stats=stats,
        )
        details.append({"project_name": name, "fact": amount})
        total += amount
    return round(total, 2), details
