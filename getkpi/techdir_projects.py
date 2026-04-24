from __future__ import annotations

import json
import logging
import os
import re
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from . import cache_manager
from .kpi_periods import last_full_quarter, quarter_month_tuples

logger = logging.getLogger(__name__)

TARGET_ORGANIZATION = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
TARGET_PROJECT_TYPE_TD_M1 = "ВнешнийЗаказ"
TARGET_PROJECT_TYPE_TD_Q1 = "РазвитияИУлучшений"
TIMEOUT = 60

ROOT_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = cache_manager.CACHE_DIR
CACHE_PATH = CACHE_DIR / "techdir_projects_snapshot.json"
CACHE_VERSION = 3
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
    if data.get("cache_date") == date.today().isoformat() and data.get("cache_version") == CACHE_VERSION:
        return data
    return None


def _save_cache(payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
        "cache_version": CACHE_VERSION,
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


def _parse_project_date(value: Any) -> date | None:
    dt = _parse_iso_date(value)
    if dt is not None:
        return dt.date()
    if isinstance(value, str):
        raw = value.strip()
        if len(raw) >= 10:
            try:
                return datetime.fromisoformat(raw[:10]).date()
            except ValueError:
                return None
    return None


def _parse_real_project_date(value: Any) -> date | None:
    dt = _parse_iso_date(value)
    if not _is_real_date(dt):
        return None
    return dt.date()


def _is_real_date(dt: datetime | None) -> bool:
    return dt is not None and dt.year > 1900


def _month_start_end(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _milestone_month_keys(tasks: list[dict[str, Any]]) -> list[str]:
    months: set[str] = set()
    for task in tasks:
        if task.get("is_summary"):
            continue
        if "is_milestone" in task and not task.get("is_milestone"):
            continue
        fin = _parse_iso_date(task.get("finish_date"))
        if not _is_real_date(fin):
            continue
        months.add(f"{fin.year:04d}-{fin.month:02d}")
    return sorted(months)


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


def _overdue_milestone_month_keys(tasks: list[dict[str, Any]]) -> list[str]:
    months: set[str] = set()
    for task in tasks:
        if task.get("is_summary"):
            continue
        if not task.get("is_milestone"):
            continue
        percent_complete = float(task.get("percent_complete") or 0.0)
        if percent_complete >= 1.0:
            continue
        finish_dt = _parse_iso_date(task.get("finish_date"))
        if not _is_real_date(finish_dt):
            continue
        months.add(f"{finish_dt.year:04d}-{finish_dt.month:02d}")
    return sorted(months)


def _overdue_milestone_rows(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for task in tasks:
        if task.get("is_summary"):
            continue
        if not task.get("is_milestone"):
            continue
        percent_complete = float(task.get("percent_complete") or 0.0)
        if percent_complete >= 1.0:
            continue
        finish_dt = _parse_iso_date(task.get("finish_date"))
        if not _is_real_date(finish_dt):
            continue
        rows.append({
            "name": task.get("name") or "",
            "start_date": task.get("start_date"),
            "finish_date": task.get("finish_date"),
            "percent_complete": percent_complete,
        })
    return rows


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
    tasks = details.get("tasks") or []
    return {
        "file_id": summary_item.get("id"),
        "project_name": project_meta.get("name") or summary_item.get("original_name"),
        "project_manager": data_1c.get("rukovoditel"),
        "project_code": data_1c.get("nomer_proekta"),
        "organizatsiya": data_1c.get("organizatsiya"),
        "tip_proekta": data_1c.get("tip_proekta"),
        "podrazdelenie": data_1c.get("podrazdelenie"),
        "status_proekta": data_1c.get("status_proekta"),
        "start_date": project_meta.get("start_date"),
        "finish_date": project_meta.get("finish_date"),
        "baseline_start": project_meta.get("baseline_start"),
        "baseline_finish": project_meta.get("baseline_finish"),
        "data_nachala": data_1c.get("data_nachala"),
        "planovaya_data_nachala": data_1c.get("planovaya_data_nachala"),
        "planovaya_data_okonchaniya": data_1c.get("planovaya_data_okonchaniya"),
        "data_okonchaniya": data_1c.get("data_okonchaniya"),
        "overdue_milestones_count": overdue_milestones_count,
        "milestone_months": _milestone_month_keys(tasks),
        "overdue_milestone_months": _overdue_milestone_month_keys(tasks),
        "overdue_milestones": _overdue_milestone_rows(tasks),
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


def get_projects_snapshot() -> dict:
    return _compute_projects_snapshot()


def _projects_for_type(project_type: str) -> list[dict[str, Any]]:
    snapshot = _compute_projects_snapshot()
    return [
        project
        for project in (snapshot.get("projects") or [])
        if project.get("tip_proekta") == project_type
    ]


def _project_date_bounds(project: dict[str, Any]) -> tuple[date | None, date | None]:
    start = (
        _parse_real_project_date(project.get("data_nachala"))
        or _parse_real_project_date(project.get("planovaya_data_nachala"))
        or _parse_real_project_date(project.get("start_date"))
        or _parse_real_project_date(project.get("baseline_start"))
    )
    end = (
        _parse_real_project_date(project.get("data_okonchaniya"))
        or _parse_real_project_date(project.get("planovaya_data_okonchaniya"))
        or _parse_real_project_date(project.get("finish_date"))
        or _parse_real_project_date(project.get("baseline_finish"))
    )
    return start, end


def _project_is_alive_in_month(project: dict[str, Any], year: int, month: int) -> bool:
    month_start, month_end = _month_start_end(year, month)
    return _project_is_alive_in_range(project, month_start, month_end)


def _project_is_alive_in_range(
    project: dict[str, Any],
    period_start: date,
    period_end: date,
) -> bool:
    start, end = _project_date_bounds(project)
    if start is not None and start > period_end:
        return False
    if end is not None and end < period_start:
        return False
    return True


def _project_has_overdue_milestone_in_month(project: dict[str, Any], year: int, month: int) -> bool:
    month_key = f"{year:04d}-{month:02d}"
    return month_key in (project.get("overdue_milestone_months") or [])


def _month_pairs_from_january() -> tuple[list[tuple[int, int]], tuple[int, int]]:
    today = date.today()
    return [(today.year, mm) for mm in range(1, today.month + 1)], (today.year, today.month)


def _build_monthly_payload(project_type: str) -> dict:
    target_projects = _projects_for_type(project_type)
    pairs, (ref_y, ref_m) = _month_pairs_from_january()

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        month_projects = [
            project for project in target_projects
            if _project_is_alive_in_month(project, y, m)
        ]
        delayed_projects = [
            project for project in month_projects
            if _project_has_overdue_milestone_in_month(project, y, m)
        ]
        plan_count = len(month_projects)
        fact_count = len(delayed_projects)
        on_time_count = max(plan_count - fact_count, 0)
        has_data = plan_count > 0 or fact_count > 0
        kpi_pct = round(on_time_count / plan_count * 100, 1) if plan_count else None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_count,
            "fact": fact_count,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "projects_on_time": on_time_count,
            "projects_with_overdue_milestones": fact_count,
            "values_unit": "шт.",
        }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
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
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": project_type,
            "target_projects_count": len(target_projects),
            "rows_by_month": [
                {
                    "year": row["year"],
                    "month": row["month"],
                    "plan": row["plan"],
                    "fact": row["fact"],
                }
                for row in monthly_rows
            ],
            "target_projects": target_projects,
        },
    }


def _build_quarterly_payload(project_type: str) -> dict:
    target_projects = _projects_for_type(project_type)
    year, quarter = last_full_quarter(date.today())
    q_months = {f"{yy:04d}-{mm:02d}" for yy, mm in quarter_month_tuples(year, quarter)}
    quarter_month_dates = quarter_month_tuples(year, quarter)
    quarter_start = date(quarter_month_dates[0][0], quarter_month_dates[0][1], 1)
    quarter_end_year, quarter_end_month = quarter_month_dates[-1]
    quarter_end = date(quarter_end_year, quarter_end_month, monthrange(quarter_end_year, quarter_end_month)[1])

    quarter_projects = [
        project for project in target_projects
        if _project_is_alive_in_range(project, quarter_start, quarter_end)
    ]
    delayed_projects = [
        project for project in quarter_projects
        if q_months.intersection(project.get("overdue_milestone_months") or [])
    ]
    plan_count = len(quarter_projects)
    fact_count = len(delayed_projects)
    on_time_count = max(plan_count - fact_count, 0)
    has_data = plan_count > 0 or fact_count > 0
    kpi_pct = round(on_time_count / plan_count * 100, 1) if plan_count else None
    quarter_row = {
        "quarter": quarter,
        "year": year,
        "label": f"Q{quarter} {year}",
        "plan": plan_count,
        "fact": fact_count,
        "kpi_pct": kpi_pct,
        "has_data": has_data,
        "projects_on_time": on_time_count,
        "projects_with_overdue_milestones": fact_count,
        "values_unit": "шт.",
    }

    return {
        "data_granularity": "quarterly",
        "quarterly_data": [quarter_row],
        "kpi_period": {
            "type": "last_full_quarter",
            "year": year,
            "quarter": quarter,
            "label": f"Q{quarter} {year}",
        },
        "ytd": {
            "total_plan": plan_count,
            "total_fact": fact_count,
            "kpi_pct": kpi_pct,
            "quarters_with_data": 1 if has_data else 0,
            "quarters_total": 1 if plan_count or fact_count else 0,
            "values_unit": "шт.",
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": project_type,
            "target_projects_count": plan_count,
            "delayed_projects_count": fact_count,
            "target_projects": quarter_projects,
            "delayed_projects": delayed_projects,
        },
    }


def _build_deviation_table(
    project_type: str,
    ref_y: int,
    ref_m: int,
) -> dict[str, Any]:
    project_type_label = {
        TARGET_PROJECT_TYPE_TD_M1: "Внешний Заказ",
        TARGET_PROJECT_TYPE_TD_Q1: "Улучшение и развитие",
    }.get(project_type, project_type)
    target_projects = _projects_for_type(project_type)
    month_start, month_end = _month_start_end(ref_y, ref_m)
    rows: list[dict[str, Any]] = []

    for project in target_projects:
        if not _project_is_alive_in_month(project, ref_y, ref_m):
            continue
        for milestone in project.get("overdue_milestones") or []:
            finish_dt = _parse_real_project_date(milestone.get("finish_date"))
            if finish_dt is None:
                continue
            if finish_dt < month_start or finish_dt > month_end:
                continue
            rows.append({
                "project_name": project.get("project_name"),
                "project_manager": project.get("project_manager"),
                "milestone_name": milestone.get("name"),
                "milestone_planned_finish_date": milestone.get("finish_date"),
                "milestone_start_date": milestone.get("start_date"),
                "deviation_date": month_end.isoformat(),
                "delay_days": max((month_end - finish_dt).days, 0),
                "percent_complete": milestone.get("percent_complete"),
            })

    rows.sort(key=lambda row: (row.get("project_name") or "", row.get("milestone_planned_finish_date") or ""))
    return {
        "name": f"Отклонения по вехам: {project_type_label}",
        "periodicity": "ежемесячно",
        "description": (
            "Проекты с просроченными вехами в выбранном месяце. "
            "Одна строка = одна веха с отклонением."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": [
            "Название проекта",
            "Руководитель проекта",
            "Название вехи",
            "Плановая дата вехи",
            "Дата отклонения",
            "Дней отклонения",
            "Процент выполнения",
        ],
        "rows": rows,
    }


def get_td_deviation_tables(month: int | None = None, year: int | None = None) -> dict[str, Any] | None:
    def _runner() -> dict[str, Any] | None:
        try:
            if month is None or year is None:
                ref_y, ref_m = date.today().year, date.today().month
            else:
                ref_y, ref_m = year, month
            return {
                "TD-T-M1-DEVIATIONS": _build_deviation_table(TARGET_PROJECT_TYPE_TD_M1, ref_y, ref_m),
                "TD-T-Q1-DEVIATIONS": _build_deviation_table(TARGET_PROJECT_TYPE_TD_Q1, ref_y, ref_m),
            }
        except Exception:
            logger.exception("Ошибка при расчёте таблиц техдирекции из TurboProject")
            return None

    return cache_manager.locked_call("techdir_td_tables", _runner)


def get_td_m1_ytd() -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_monthly_payload(TARGET_PROJECT_TYPE_TD_M1)
        except Exception:
            logger.exception("Ошибка при расчёте TD-M1 из TurboProject")
            return None

    return cache_manager.locked_call("techdir_td_m1", _runner)


def get_td_q1_ytd() -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_quarterly_payload(TARGET_PROJECT_TYPE_TD_Q1)
        except Exception:
            logger.exception("Ошибка при расчёте TD-Q1 из TurboProject")
            return None

    return cache_manager.locked_call("techdir_td_q1", _runner)
