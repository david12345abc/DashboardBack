from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from .cache_manager import locked_call

logger = logging.getLogger(__name__)

TARGET_ORGANIZATION = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
TARGET_PROJECT_TYPE_TD_M1 = "ВнешнийЗаказ"
TARGET_PROJECT_TYPE_TD_Q1 = "РазвитияИУлучшений"
TIMEOUT = 60

ROOT_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_PATH = CACHE_DIR / "techdir_projects_snapshot.json"
_CREDENTIAL_FILES = (
    "API для dashboard.py",
    "api все проекты 3.py",
)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _extract_py_string_constant(text: str, name: str) -> str | None:
    match = re.search(rf"^{name}\s*=\s*['\"]([^'\"]+)['\"]", text, re.MULTILINE)
    return match.group(1) if match else None


def _load_credentials_from_example_files() -> tuple[str, str, str]:
    for file_name in _CREDENTIAL_FILES:
        path = ROOT_DIR / file_name
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        api_base = _extract_py_string_constant(text, "API_BASE")
        email = _extract_py_string_constant(text, "EMAIL")
        password = _extract_py_string_constant(text, "PASSWORD")
        if api_base and email and password:
            return api_base, email, password
    raise RuntimeError("Не удалось найти настройки TurboProject в файлах примеров.")


def _get_credentials() -> tuple[str, str, str]:
    api_base = (
        os.getenv("TURBOPROJECT_API_BASE")
        or os.getenv("TURBO_API_BASE")
    )
    email = (
        os.getenv("TURBOPROJECT_EMAIL")
        or os.getenv("TURBO_EMAIL")
    )
    password = (
        os.getenv("TURBOPROJECT_PASSWORD")
        or os.getenv("TURBO_PASSWORD")
    )
    if api_base and email and password:
        return api_base.rstrip("/"), email, password
    file_api_base, file_email, file_password = _load_credentials_from_example_files()
    return file_api_base.rstrip("/"), file_email, file_password


def _load_cache() -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_date") == date.today().isoformat():
        return data
    return None


def _save_cache(payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
    }
    try:
        CACHE_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        logger.exception("Не удалось сохранить кэш проектов техдирекции в %s", CACHE_PATH)


def _parse_iso_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _login(session: requests.Session) -> str:
    api_base, email, password = _get_credentials()
    resp = session.post(
        f"{api_base}/api/auth/login",
        json={"email": email, "password": password},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def _api_get(
    session: requests.Session,
    path: str,
    token: str,
    params: dict[str, Any] | None = None,
) -> Any:
    api_base, _, _ = _get_credentials()
    resp = session.get(
        f"{api_base}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _count_overdue_milestones(tasks: list[dict[str, Any]]) -> int:
    today = datetime.now().date()
    overdue_count = 0
    for task in tasks:
        if not task.get("is_milestone"):
            continue
        percent_complete = float(task.get("percent_complete") or 0.0)
        if percent_complete >= 1.0:
            continue
        finish_dt = _parse_iso_date(task.get("finish_date"))
        if finish_dt is None:
            continue
        if finish_dt.date() < today:
            overdue_count += 1
    return overdue_count


def _is_target_project(data_1c: dict[str, Any], project_type: str | None = None) -> bool:
    if data_1c.get("organizatsiya") != TARGET_ORGANIZATION:
        return False
    if project_type is None:
        return True
    return data_1c.get("tip_proekta") == project_type


def _project_summary(
    summary_item: dict[str, Any],
    details: dict[str, Any],
    overdue_milestones_count: int,
) -> dict[str, Any]:
    data_1c = details.get("data_1c") or {}
    project_meta = details.get("project") or {}
    return {
        "file_id": summary_item.get("id"),
        "project_name": project_meta.get("name") or summary_item.get("original_name"),
        "organizatsiya": data_1c.get("organizatsiya"),
        "tip_proekta": data_1c.get("tip_proekta"),
        "podrazdelenie": data_1c.get("podrazdelenie"),
        "status_proekta": data_1c.get("status_proekta"),
        "finish_date": project_meta.get("finish_date"),
        "baseline_finish": project_meta.get("baseline_finish"),
        "planovaya_data_okonchaniya": data_1c.get("planovaya_data_okonchaniya"),
        "data_okonchaniya": data_1c.get("data_okonchaniya"),
        "overdue_milestones_count": overdue_milestones_count,
    }


def _compute_projects_snapshot() -> dict:
    cached = _load_cache()
    if cached is not None:
        return cached

    session = requests.Session()
    token = _login(session)

    summary = _api_get(session, "/api/projects/files", token)
    items = summary.get("items") or []
    projects_with_1c = [item for item in items if item.get("has_1c")]

    target_projects: list[dict[str, Any]] = []

    for item in projects_with_1c:
        file_id = item.get("id")
        if not file_id:
            continue
        details = _api_get(session, f"/api/projects/files/{file_id}", token)
        data_1c = details.get("data_1c") or {}
        if not _is_target_project(data_1c):
            continue

        overdue_milestones_count = _count_overdue_milestones(details.get("tasks") or [])
        project_row = _project_summary(item, details, overdue_milestones_count)
        target_projects.append(project_row)

    payload = {
        "projects": target_projects,
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_projects": target_projects,
        },
    }
    _save_cache(payload)
    return payload


def _projects_for_type(project_type: str) -> list[dict[str, Any]]:
    snapshot = _compute_projects_snapshot()
    return [
        project
        for project in (snapshot.get("projects") or [])
        if project.get("tip_proekta") == project_type
    ]


def _current_quarter() -> tuple[int, int]:
    today = date.today()
    return today.year, (today.month - 1) // 3 + 1


def _build_monthly_payload(project_type: str) -> dict:
    target_projects = _projects_for_type(project_type)
    delayed_projects = [
        project for project in target_projects
        if (project.get("overdue_milestones_count") or 0) != 0
    ]
    plan_count = len(target_projects)
    fact_count = len(delayed_projects)
    on_time_count = max(plan_count - fact_count, 0)
    kpi_pct = round(on_time_count / plan_count * 100, 1) if plan_count else None

    today = date.today()
    snapshot_row = {
        "month": today.month,
        "year": today.year,
        "month_name": MONTH_NAMES[today.month],
        "plan": plan_count,
        "fact": fact_count,
        "kpi_pct": kpi_pct,
        "has_data": True,
        "projects_on_time": on_time_count,
        "projects_with_overdue_milestones": fact_count,
        "values_unit": "шт.",
    }

    return {
        "data_granularity": "monthly",
        "monthly_data": [snapshot_row],
        "last_full_month_row": snapshot_row,
        "kpi_period": {
            "type": "snapshot",
            "year": today.year,
            "month": today.month,
            "month_name": MONTH_NAMES[today.month],
            "snapshot_date": today.isoformat(),
        },
        "ytd": {
            "total_plan": plan_count,
            "total_fact": fact_count,
            "kpi_pct": kpi_pct,
            "months_with_data": 1 if plan_count or fact_count else 0,
            "months_total": 1 if plan_count or fact_count else 0,
            "values_unit": "шт.",
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": project_type,
            "target_projects_count": plan_count,
            "delayed_projects_count": fact_count,
            "target_projects": target_projects,
            "delayed_projects": delayed_projects,
        },
    }


def _build_quarterly_payload(project_type: str) -> dict:
    target_projects = _projects_for_type(project_type)
    delayed_projects = [
        project for project in target_projects
        if (project.get("overdue_milestones_count") or 0) != 0
    ]
    plan_count = len(target_projects)
    fact_count = len(delayed_projects)
    on_time_count = max(plan_count - fact_count, 0)
    kpi_pct = round(on_time_count / plan_count * 100, 1) if plan_count else None

    year, quarter = _current_quarter()
    quarter_row = {
        "quarter": quarter,
        "year": year,
        "label": f"Q{quarter} {year}",
        "plan": plan_count,
        "fact": fact_count,
        "kpi_pct": kpi_pct,
        "has_data": True,
        "projects_on_time": on_time_count,
        "projects_with_overdue_milestones": fact_count,
        "values_unit": "шт.",
    }

    return {
        "data_granularity": "quarterly",
        "quarterly_data": [quarter_row],
        "kpi_period": {
            "type": "snapshot_quarter",
            "year": year,
            "quarter": quarter,
            "label": f"Q{quarter} {year}",
            "snapshot_date": date.today().isoformat(),
        },
        "ytd": {
            "total_plan": plan_count,
            "total_fact": fact_count,
            "kpi_pct": kpi_pct,
            "quarters_with_data": 1 if plan_count or fact_count else 0,
            "quarters_total": 1 if plan_count or fact_count else 0,
            "values_unit": "шт.",
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": project_type,
            "target_projects_count": plan_count,
            "delayed_projects_count": fact_count,
            "target_projects": target_projects,
            "delayed_projects": delayed_projects,
        },
    }


def get_td_m1_ytd() -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_monthly_payload(TARGET_PROJECT_TYPE_TD_M1)
        except Exception:
            logger.exception("Ошибка при расчёте TD-M1 из TurboProject")
            return None

    return locked_call("techdir_td_m1", _runner)


def get_td_q1_ytd() -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_quarterly_payload(TARGET_PROJECT_TYPE_TD_Q1)
        except Exception:
            logger.exception("Ошибка при расчёте TD-Q1 из TurboProject")
            return None

    return locked_call("techdir_td_q1", _runner)
