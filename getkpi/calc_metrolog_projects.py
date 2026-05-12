"""TurboProject KPI for the chief metrologist dashboard."""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any

import requests

from . import cache_manager
from .calc_chief_constructor_projects import (
    MAX_ALLOWED_DELAY_WORKDAYS,
    MONTH_NAMES,
    _month_pairs_until,
    _month_start_end,
    _normalize_ref_period,
    _project_date_bounds,
    _project_delay_details,
)
from .calc_prod_deputy_projects import (
    _api_get,
    _api_overdue_milestones,
    _login,
    _overdue_milestone_rows,
    _project_progress_pct,
)
from .turboproject_config import TIMEOUT

logger = logging.getLogger(__name__)

CACHE_PATH = cache_manager.CACHE_DIR / "metrolog_projects_snapshot.json"
CACHE_VERSION = 1
TARGET_OWNER = "Хозуян"


def _load_cache() -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_date") == date.today().isoformat() and data.get("cache_version") == CACHE_VERSION:
        return data
    return None


def _save_cache(payload: dict) -> None:
    cache_manager.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
        "cache_version": CACHE_VERSION,
    }
    try:
        CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш проектов главного метролога")


def _normalize_person(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    raw = re.sub(r"[^0-9a-zа-я]+", " ", raw)
    return " ".join(raw.split())


def _contains_person(value: Any, target: str) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        normalized = _normalize_person(value)
        return normalized == target or target in normalized
    if isinstance(value, dict):
        return any(_contains_person(item, target) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_contains_person(item, target) for item in value)
    return False


def _is_target_owner_project(data_1c: dict[str, Any]) -> bool:
    target = _normalize_person(TARGET_OWNER)
    return (
        _contains_person(data_1c.get("kurator"), target)
        or _contains_person(data_1c.get("rukovoditel"), target)
    )


def _project_summary(summary_item: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    data_1c = details.get("data_1c") or {}
    project_meta = details.get("project") or {}
    tasks = details.get("tasks") or []
    overdue_milestones = _overdue_milestone_rows(_api_overdue_milestones(details), tasks)
    return {
        "file_id": summary_item.get("id"),
        "project_name": project_meta.get("name") or summary_item.get("original_name"),
        "project_manager": data_1c.get("rukovoditel"),
        "project_code": data_1c.get("nomer_proekta"),
        "rukovoditel": data_1c.get("rukovoditel"),
        "kurator": data_1c.get("kurator"),
        "podrazdelenie": data_1c.get("podrazdelenie"),
        "tip_proekta": data_1c.get("tip_proekta"),
        "status_proekta": data_1c.get("status_proekta"),
        "start_date": project_meta.get("start_date"),
        "finish_date": project_meta.get("finish_date"),
        "baseline_start": project_meta.get("baseline_start"),
        "baseline_finish": project_meta.get("baseline_finish"),
        "data_nachala": data_1c.get("data_nachala"),
        "planovaya_data_nachala": data_1c.get("planovaya_data_nachala"),
        "planovaya_data_okonchaniya": data_1c.get("planovaya_data_okonchaniya"),
        "data_okonchaniya": data_1c.get("data_okonchaniya"),
        "project_progress_pct": _project_progress_pct(tasks),
        "overdue_milestones": overdue_milestones,
    }


def _compute_projects_snapshot() -> dict:
    cached = _load_cache()
    if cached is not None:
        return cached

    session = requests.Session()
    token = _login(session)
    summary = _api_get(session, "/api/projects/files", token)
    items = summary.get("items") or []
    projects: list[dict[str, Any]] = []

    for item in items:
        file_id = item.get("id")
        if not file_id:
            continue
        details = _api_get(session, f"/api/projects/files/{file_id}", token)
        data_1c = details.get("data_1c") or {}
        if not _is_target_owner_project(data_1c):
            continue
        projects.append(_project_summary(item, details))

    payload = {
        "projects": projects,
        "debug": {
            "source": "metrolog_projects",
            "target_owner": TARGET_OWNER,
            "owner_fields": ["kurator", "rukovoditel"],
            "all_projects_count": len(items),
            "target_projects_count": len(projects),
            "timeout": TIMEOUT,
        },
    }
    _save_cache(payload)
    return payload


def _project_is_alive_in_month(project: dict[str, Any], year: int, month: int) -> bool:
    month_start, month_end = _month_start_end(year, month)
    start, end = _project_date_bounds(project)
    if start is not None and start > month_end:
        return False
    if end is not None and end < month_start:
        return False
    return True


def get_metrolog_projects_without_major_deviation_monthly(
    year: int | None = None,
    month: int | None = None,
) -> dict | None:
    """Plan/fact for projects where Hozuyan is curator or project manager."""
    try:
        snapshot = _compute_projects_snapshot()
        projects = list(snapshot.get("projects") or [])
        ref_y, ref_m = _normalize_ref_period(year, month)
        rows: list[dict[str, Any]] = []
        ref_row: dict[str, Any] | None = None

        for y, m in _month_pairs_until(ref_y, ref_m):
            _month_start, month_end = _month_start_end(y, m)
            as_of_date = min(month_end, date.today())
            month_projects = [project for project in projects if _project_is_alive_in_month(project, y, m)]
            ok_projects = []
            for project in month_projects:
                max_delay, _details = _project_delay_details(project, as_of_date)
                if max_delay < MAX_ALLOWED_DELAY_WORKDAYS:
                    ok_projects.append(project)
            plan_count = len(month_projects)
            fact_count = len(ok_projects)
            row = {
                "month": m,
                "year": y,
                "month_name": MONTH_NAMES[m],
                "plan": plan_count,
                "fact": fact_count,
                "kpi_pct": round(fact_count / plan_count * 100, 1) if plan_count else None,
                "has_data": plan_count > 0,
                "values_unit": "шт.",
                "target_owner": TARGET_OWNER,
                "max_allowed_delay_workdays": MAX_ALLOWED_DELAY_WORKDAYS,
            }
            rows.append(row)
            if (y, m) == (ref_y, ref_m):
                ref_row = row

        return {
            "data_granularity": "monthly",
            "monthly_data": rows,
            "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
            "kpi_period": {
                "type": "last_full_month",
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_NAMES[ref_m],
            },
            "ytd": {
                "total_plan": ref_row.get("plan") if ref_row else None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                "months_with_data": sum(1 for row in rows if row.get("has_data")),
                "months_total": len(rows),
                "values_unit": "шт.",
            },
            "debug": snapshot.get("debug") or {},
        }
    except Exception:
        logger.exception("Ошибка при расчёте проектов главного метролога")
        return None
