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
from .turboproject_config import API_BASE as TURBO_CFG_API_BASE, EMAIL as TURBO_CFG_EMAIL, PASSWORD as TURBO_CFG_PASSWORD

logger = logging.getLogger(__name__)

TARGET_ORGANIZATION = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
TARGET_PROJECT_TYPE_TD_M1 = "ВнешнийЗаказ"
TARGET_PROJECT_TYPE_TD_Q1 = "РазвитияИУлучшений"
TARGET_PROJECT_TYPE_OD_Q1 = None
PRODUCTION_DEPUTY_PROJECT_DEPARTMENTS = {
    "Производственный цех №1",
    "Производственный цех №2",
}
TIMEOUT = 60

ROOT_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = cache_manager.CACHE_DIR
CACHE_PATH = CACHE_DIR / "techdir_projects_snapshot.json"
CACHE_VERSION = 7
OD_OVERDUE_MILESTONES_SCHEMA = "zero_duration_milestones_v1"
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
    if TURBO_CFG_API_BASE and TURBO_CFG_EMAIL and TURBO_CFG_PASSWORD:
        return TURBO_CFG_API_BASE.rstrip("/"), TURBO_CFG_EMAIL, TURBO_CFG_PASSWORD
    file_api_base, file_email, file_password = _load_credentials_from_example_files()
    return file_api_base.rstrip("/"), file_email, file_password


def _load_cache() -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        data.get("cache_date") == date.today().isoformat()
        and data.get("cache_version") == CACHE_VERSION
        and data.get("od_overdue_milestones_schema") == OD_OVERDUE_MILESTONES_SCHEMA
    ):
        return data
    return None


def _save_cache(payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
        "cache_version": CACHE_VERSION,
        "od_overdue_milestones_schema": OD_OVERDUE_MILESTONES_SCHEMA,
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


def _safe_float(value: Any) -> float | None:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if num == num else None


def _milestone_progress_as_fraction(raw: Any) -> float | None:
    """Доля выполнения вехи: API отдаёт 0..1 или проценты 0..100."""
    num = _safe_float(raw)
    if num is None:
        return None
    if abs(num) <= 1.0:
        return num
    return num / 100.0


def _milestone_is_zero_percent_complete(raw: Any) -> bool:
    """Только полностью невыполненные вехи (0%%), частичный прогресс отсекаем."""
    frac = _milestone_progress_as_fraction(raw)
    if frac is None:
        return False
    return abs(frac) < 1e-9


def _bool_is_true(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == value and value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "да"}
    return False


def _row_ref_ids(row: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for key in ("id", "uid", "task_id", "taskId", "ref", "ref_key", "Ref_Key"):
        value = row.get(key)
        if value is not None and str(value).strip():
            ids.add(str(value).strip())
    return ids


def _row_milestone_match_key(row: dict[str, Any]) -> tuple[str, str, str]:
    start_dt = _parse_iso_date(row.get("start_date"))
    finish_dt = _parse_iso_date(row.get("finish_date"))
    return (
        " ".join(str(row.get("name") or "").split()).lower(),
        start_dt.date().isoformat() if _is_real_date(start_dt) else str(row.get("start_date") or "")[:10],
        finish_dt.date().isoformat() if _is_real_date(finish_dt) else str(row.get("finish_date") or "")[:10],
    )


def _is_zero_duration_milestone(row: dict[str, Any]) -> bool:
    if row.get("is_summary"):
        return False
    if "is_milestone" in row and not _bool_is_true(row.get("is_milestone")):
        return False
    start_dt = _parse_iso_date(row.get("start_date"))
    finish_dt = _parse_iso_date(row.get("finish_date"))
    if not _is_real_date(start_dt) or not _is_real_date(finish_dt):
        return False
    return start_dt == finish_dt


def _actual_milestone_indexes(tasks: list[dict[str, Any]]) -> tuple[set[str], set[tuple[str, str, str]]]:
    ids: set[str] = set()
    keys: set[tuple[str, str, str]] = set()
    for task in tasks:
        if not _is_zero_duration_milestone(task):
            continue
        ids.update(_row_ref_ids(task))
        keys.add(_row_milestone_match_key(task))
    return ids, keys


def _is_actual_milestone(
    row: dict[str, Any],
    milestone_ids: set[str],
    milestone_keys: set[tuple[str, str, str]],
) -> bool:
    row_ids = _row_ref_ids(row)
    if row_ids and row_ids.intersection(milestone_ids):
        return True
    key = _row_milestone_match_key(row)
    return bool(key[0] and key in milestone_keys)


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


def _api_overdue_milestones(details: dict[str, Any]) -> list[dict[str, Any]]:
    """Готовый список просроченных вех из API проекта, без смешивания с tasks."""
    candidates = details.get("overdue_milestones")
    if candidates is None:
        candidates = (details.get("project") or {}).get("overdue_milestones")
    return candidates if isinstance(candidates, list) else []


def _overdue_milestone_month_keys(milestones: list[dict[str, Any]]) -> list[str]:
    today = datetime.now().date()
    months: set[str] = set()
    for milestone in milestones:
        if not _milestone_is_zero_percent_complete(milestone.get("percent_complete")):
            continue
        finish_dt = _parse_iso_date(milestone.get("finish_date"))
        if not _is_real_date(finish_dt):
            continue
        if finish_dt.date() >= today:
            continue
        months.add(f"{finish_dt.year:04d}-{finish_dt.month:02d}")
    return sorted(months)


def _overdue_milestone_rows(
    milestones: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    today = datetime.now().date()
    source_rows = milestones or tasks
    milestone_ids, milestone_keys = _actual_milestone_indexes(tasks)
    rows: list[dict[str, Any]] = []
    for milestone in source_rows:
        if not _is_actual_milestone(milestone, milestone_ids, milestone_keys):
            continue
        if not _milestone_is_zero_percent_complete(milestone.get("percent_complete")):
            continue
        finish_dt = _parse_iso_date(milestone.get("finish_date"))
        if not _is_real_date(finish_dt):
            continue
        if finish_dt.date() >= today:
            continue
        frac = _milestone_progress_as_fraction(milestone.get("percent_complete"))
        stored_pct = frac if frac is not None else 0.0
        rows.append({
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "percent_complete": stored_pct,
        })
    rows.sort(key=lambda row: (row.get("finish_date") or "", row.get("name") or ""))
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
    overdue_milestones: list[dict[str, Any]],
) -> dict[str, Any]:
    data_1c = details.get("data_1c") or {}
    project_meta = details.get("project") or {}
    tasks = details.get("tasks") or []
    project_progress_pct = _project_progress_pct(project_meta, tasks)
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
        "overdue_milestones_count": len(overdue_milestones),
        "project_progress_pct": project_progress_pct,
        "milestone_months": _milestone_month_keys(tasks),
        "overdue_milestone_months": _overdue_milestone_month_keys(overdue_milestones),
        "overdue_milestones": overdue_milestones,
    }


def _project_progress_pct(project_meta: dict[str, Any], tasks: list[dict[str, Any]]) -> float | None:
    task_total = 0
    task_done = 0
    for task in tasks or []:
        if task.get("is_summary"):
            continue
        pct = _safe_float(task.get("percent_complete"))
        if pct is None:
            continue
        task_total += 1
        pct_value = pct * 100 if abs(pct) <= 1 else pct
        if pct_value >= 100:
            task_done += 1
    if not task_total:
        return None
    return round(task_done / task_total * 100, 1)


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

        overdue_milestones = _overdue_milestone_rows(
            _api_overdue_milestones(details),
            details.get("tasks") or [],
        )
        project_row = _project_summary(item, details, overdue_milestones)
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


def _projects_for_filter(
    project_type: str | None,
    *,
    departments: set[str] | None = None,
) -> list[dict[str, Any]]:
    snapshot = _compute_projects_snapshot()
    projects = list(snapshot.get("projects") or [])
    if project_type is not None:
        projects = [
            project
            for project in projects
            if project.get("tip_proekta") == project_type
        ]
    if departments is not None:
        projects = [
            project
            for project in projects
            if project.get("podrazdelenie") in departments
        ]
    return projects


def _projects_for_type(project_type: str | None) -> list[dict[str, Any]]:
    return _projects_for_filter(project_type)


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


def _normalize_ref_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    return ref_y, ref_m


def _month_pairs_until(ref_y: int, ref_m: int) -> list[tuple[int, int]]:
    return [(ref_y, mm) for mm in range(1, ref_m + 1)]


def _build_monthly_payload(
    project_type: str | None,
    year: int | None = None,
    month: int | None = None,
    *,
    departments: set[str] | None = None,
) -> dict:
    target_projects = _projects_for_filter(project_type, departments=departments)
    ref_y, ref_m = _normalize_ref_period(year, month)
    pairs = _month_pairs_until(ref_y, ref_m)
    values_unit = "шт." if project_type is None or project_type in {TARGET_PROJECT_TYPE_TD_M1, TARGET_PROJECT_TYPE_TD_Q1} else "%"

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
        overdue_count = len(delayed_projects)
        on_time_count = max(plan_count - overdue_count, 0)
        has_data = plan_count > 0 or overdue_count > 0
        kpi_pct = round(on_time_count / plan_count * 100, 1) if plan_count else None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_count,
            "fact": overdue_count,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "projects_on_time": on_time_count,
            "projects_with_overdue_milestones": overdue_count,
            "values_unit": values_unit,
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
            "values_unit": values_unit,
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": project_type,
            "target_departments": sorted(departments) if departments else None,
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


def _project_overdue_milestones_in_month(
    project: dict[str, Any],
    ref_y: int,
    ref_m: int,
) -> list[dict[str, Any]]:
    month_start, month_end = _month_start_end(ref_y, ref_m)
    rows: list[dict[str, Any]] = []
    for milestone in project.get("overdue_milestones") or []:
        finish_dt = _parse_real_project_date(milestone.get("finish_date"))
        if finish_dt is None:
            continue
        if finish_dt < month_start or finish_dt > month_end:
            continue
        rows.append(milestone)
    return rows


def _build_milestone_deviation_details(
    overdue_rows: list[dict[str, Any]],
    as_of_date: date,
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for index, milestone in enumerate(overdue_rows, start=1):
        finish_dt = _parse_real_project_date(milestone.get("finish_date"))
        delay_days = max((as_of_date - finish_dt).days, 0) if finish_dt else 0
        details.append({
            "number": index,
            "id": milestone.get("id"),
            "uid": milestone.get("uid"),
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "delay_days": delay_days,
            "percent_complete": milestone.get("percent_complete"),
        })
    return details


def _project_timeline_label(project: dict[str, Any]) -> str:
    start_dt, end_dt = _project_date_bounds(project)
    if start_dt and end_dt:
        return f"{start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"
    if start_dt:
        return f"с {start_dt.strftime('%d.%m.%Y')}"
    if end_dt:
        return f"до {end_dt.strftime('%d.%m.%Y')}"
    return ""


def _project_status_label(project: dict[str, Any]) -> str:
    raw = str(project.get("status_proekta") or "").strip()
    status_map = {
        "ВРаботе": "В работе",
        "Завершен": "Завершен",
        "Закрыт": "Закрыт",
        "НаПаузе": "На паузе",
        "Отменен": "Отменен",
    }
    return status_map.get(raw, raw)


def _build_project_deviation_table(
    project_type: str | None,
    ref_y: int,
    ref_m: int,
    *,
    table_name: str,
    departments: set[str] | None = None,
) -> dict[str, Any]:
    target_projects = _projects_for_filter(project_type, departments=departments)
    month_end = _month_start_end(ref_y, ref_m)[1]
    as_of_date = min(month_end, date.today())
    rows: list[dict[str, Any]] = []

    for project in target_projects:
        if not _project_is_alive_in_month(project, ref_y, ref_m):
            continue
        overdue_rows = _project_overdue_milestones_in_month(project, ref_y, ref_m)
        if not overdue_rows:
            continue

        milestone_details = _build_milestone_deviation_details(overdue_rows, as_of_date)
        max_delay_days = max(
            (int(milestone.get("delay_days") or 0) for milestone in milestone_details),
            default=0,
        )

        rows.append({
            "number": len(rows) + 1,
            "project_code": project.get("project_code") or "",
            "project_name": project.get("project_name") or "",
            "project_manager": project.get("project_manager") or "",
            "timeline": _project_timeline_label(project),
            "deviation": f"{len(overdue_rows)} вех., {max_delay_days} дн.",
            "delay_days": max_delay_days,
            "status": _project_status_label(project),
            "progress_pct": project.get("project_progress_pct"),
            "overdue_milestones_count": len(overdue_rows),
            "milestone_deviations": milestone_details,
        })

    rows.sort(
        key=lambda row: (
            -(int(row.get("delay_days") or 0)),
            -(int(row.get("overdue_milestones_count") or 0)),
            str(row.get("project_name") or ""),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["number"] = index

    return {
        "name": table_name,
        "periodicity": "ежемесячно",
        "description": (
            "Проекты выбранного периода, у которых есть отклонения по вехам. "
            "Одна строка = один проект."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
        "rows": rows,
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
    overdue_count = len(delayed_projects)
    on_time_count = max(plan_count - overdue_count, 0)
    has_data = plan_count > 0 or overdue_count > 0
    kpi_pct = round(on_time_count / plan_count * 100, 1) if plan_count else None
    quarter_row = {
        "quarter": quarter,
        "year": year,
        "label": f"Q{quarter} {year}",
        "plan": plan_count,
        "fact": on_time_count,
        "kpi_pct": kpi_pct,
        "has_data": has_data,
        "projects_on_time": on_time_count,
        "projects_with_overdue_milestones": overdue_count,
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
            "total_fact": on_time_count,
            "kpi_pct": kpi_pct,
            "quarters_with_data": 1 if has_data else 0,
            "quarters_total": 1 if plan_count or overdue_count else 0,
            "values_unit": "шт.",
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": project_type,
            "target_projects_count": plan_count,
            "delayed_projects_count": overdue_count,
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
    month_end = _month_start_end(ref_y, ref_m)[1]
    as_of_date = min(month_end, date.today())
    rows: list[dict[str, Any]] = []

    for project in target_projects:
        if not _project_is_alive_in_month(project, ref_y, ref_m):
            continue

        overdue_rows = _project_overdue_milestones_in_month(project, ref_y, ref_m)
        if not overdue_rows:
            continue

        milestone_details = _build_milestone_deviation_details(overdue_rows, as_of_date)
        max_delay_days = max(
            (int(milestone.get("delay_days") or 0) for milestone in milestone_details),
            default=0,
        )

        rows.append({
            "number": len(rows) + 1,
            "project_code": project.get("project_code") or "",
            "project_name": project.get("project_name") or "",
            "project_manager": project.get("project_manager") or "",
            "timeline": _project_timeline_label(project),
            "deviation": f"{len(overdue_rows)} вех., {max_delay_days} дн.",
            "delay_days": max_delay_days,
            "status": _project_status_label(project),
            "progress_pct": project.get("project_progress_pct"),
            "overdue_milestones_count": len(overdue_rows),
            "milestone_deviations": milestone_details,
        })

    rows.sort(
        key=lambda row: (
            -(int(row.get("delay_days") or 0)),
            -(int(row.get("overdue_milestones_count") or 0)),
            str(row.get("project_name") or ""),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["number"] = index

    return {
        "name": f"Отклонения по вехам: {project_type_label}",
        "periodicity": "ежемесячно",
        "description": (
            "Проекты выбранного периода, у которых есть отклонения по вехам. "
            "Одна строка = один проект."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
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
            payload = _build_monthly_payload(TARGET_PROJECT_TYPE_TD_Q1)
            # Backward compatibility: older callers still expect quarterly_data.
            payload["quarterly_data"] = payload.get("monthly_data") or []
            return payload
        except Exception:
            logger.exception("Ошибка при расчёте TD-Q1 из TurboProject")
            return None

    return cache_manager.locked_call("techdir_td_q1", _runner)


def get_od_q1_monthly(year: int | None = None, month: int | None = None) -> dict | None:
    try:
        return _build_monthly_payload(TARGET_PROJECT_TYPE_OD_Q1, year=year, month=month)
    except Exception:
        logger.exception("Ошибка при расчёте OD-Q1 из TurboProject")
        return None


def get_od_q1_deviation_table(month: int | None = None, year: int | None = None) -> dict[str, Any] | None:
    try:
        ref_y, ref_m = _normalize_ref_period(year, month)
        return _build_project_deviation_table(
            TARGET_PROJECT_TYPE_OD_Q1,
            ref_y,
            ref_m,
            table_name="Ключевые инициативы и проекты с отклонениями по вехам",
        )
    except Exception:
        logger.exception("Ошибка при построении таблицы OD-Q1 из TurboProject")
        return None


def get_pd_q1_monthly(year: int | None = None, month: int | None = None) -> dict | None:
    try:
        return _build_monthly_payload(
            TARGET_PROJECT_TYPE_OD_Q1,
            year=year,
            month=month,
            departments=PRODUCTION_DEPUTY_PROJECT_DEPARTMENTS,
        )
    except Exception:
        logger.exception("Ошибка при расчёте PD-Q1 из TurboProject")
        return None


def get_pd_q1_deviation_table(month: int | None = None, year: int | None = None) -> dict[str, Any] | None:
    try:
        ref_y, ref_m = _normalize_ref_period(year, month)
        return _build_project_deviation_table(
            TARGET_PROJECT_TYPE_OD_Q1,
            ref_y,
            ref_m,
            table_name="Проекты улучшений / сокращения потерь с отклонениями по вехам",
            departments=PRODUCTION_DEPUTY_PROJECT_DEPARTMENTS,
        )
    except Exception:
        logger.exception("Ошибка при построении таблицы PD-Q1 из TurboProject")
        return None
