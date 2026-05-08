"""Факт ФОТ подразделения «Служба развития» из кэша / calc_fot_management."""
from __future__ import annotations

import io
import json
import logging
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SERVICE_DEVELOPMENT_DEPARTMENT = "Служба развития"


def _dashboard_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "dashboard"


def load_fot_management_monthly_cache(year: int, month: int) -> dict[str, Any] | None:
    path = _dashboard_dir() / f"fot_management_monthly_{year}_{month:02d}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    return data


def service_development_fot_fact(year: int, month: int) -> float | None:
    data = load_fot_management_monthly_cache(year, month)
    if data:
        months = data.get("months") or []
        for row in months:
            if not isinstance(row, dict):
                continue
            if row.get("year") != year or row.get("month") != month:
                continue
            fact_matrix = row.get("fact_matrix") or []
            for dept_row in fact_matrix:
                if not isinstance(dept_row, dict):
                    continue
                if dept_row.get("department") == SERVICE_DEVELOPMENT_DEPARTMENT:
                    try:
                        return float(dept_row.get("total") or 0)
                    except (TypeError, ValueError):
                        return None

    try:
        from .. import calc_fot_management
        with redirect_stdout(io.StringIO()):
            payload = calc_fot_management.get_fot_management_monthly(year=year, month=month)
    except Exception:
        logger.exception("Не удалось получить факт ФОТ для %s/%s", year, month)
        return None

    months = payload.get("months") or []
    for row in months:
        if not isinstance(row, dict):
            continue
        if row.get("year") != year or row.get("month") != month:
            continue
        fact_matrix = row.get("fact_matrix") or []
        for dept_row in fact_matrix:
            if not isinstance(dept_row, dict):
                continue
            if dept_row.get("department") == SERVICE_DEVELOPMENT_DEPARTMENT:
                try:
                    return float(dept_row.get("total") or 0)
                except (TypeError, ValueError):
                    return None
    return None
