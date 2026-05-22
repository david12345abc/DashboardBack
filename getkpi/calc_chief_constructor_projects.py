"""
TurboProject KPI for the chief constructor dashboard.

GK-M1: share of design bureau projects without milestone deviations over 10 working days.
"""
from __future__ import annotations

import json
import logging
from calendar import monthrange
from datetime import date, timedelta
from typing import Any

import requests

from . import cache_manager
from .calc_prod_deputy_projects import (
    _api_get,
    _api_overdue_milestones,
    _bool_is_true,
    _login,
    _milestone_progress_as_fraction,
    _overdue_milestone_rows,
    _parse_real_project_date,
    _project_progress_pct,
)
from .turboproject_config import TIMEOUT

logger = logging.getLogger(__name__)

CACHE_PATH = cache_manager.CACHE_DIR / "chief_constructor_projects_snapshot.json"
CACHE_VERSION = 4
TARGET_DEPARTMENT = "Конструкторское бюро"
MAX_ALLOWED_DELAY_WORKDAYS = 10
RND_PROJECT_TYPE_MARKERS = ("ОКР", "НИР", "НИОКР")
PLAN_FINISH_KEYS = (
    "baseline_finish",
    "BaselineFinish",
    "baselineFinish",
    "plan_finish",
    "planned_finish",
    "planned_finish_date",
    "plan_finish_date",
    "constraint_date",
    "finish_date",
)
FACT_FINISH_KEYS = (
    "actual_finish",
    "actual_finish_date",
    "fact_finish",
    "fact_finish_date",
    "finish_actual",
    "data_fakticheskogo_okonchaniya",
)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


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
        logger.exception("Не удалось сохранить кэш проектов главного конструктора")


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _month_start_end(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _normalize_ref_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    return ref_y, max(1, min(12, ref_m))


def _month_pairs_until(ref_y: int, ref_m: int) -> list[tuple[int, int]]:
    return [(ref_y, mm) for mm in range(1, ref_m + 1)]


def _last_full_quarter_for_period(year: int, month: int) -> tuple[int, int]:
    if month <= 3:
        return year - 1, 4
    if month <= 6:
        return year, 1
    if month <= 9:
        return year, 2
    return year, 3


def _quarter_start_end(year: int, quarter: int) -> tuple[date, date]:
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    return date(year, start_month, 1), date(year, end_month, monthrange(year, end_month)[1])


def _business_days_between_exclusive(start: date, end: date) -> int:
    """Working days after start through end inclusive."""
    if end <= start:
        return 0
    current = start + timedelta(days=1)
    count = 0
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count


def _first_present_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _task_date(row: dict[str, Any], keys: tuple[str, ...]) -> date | None:
    return _parse_real_project_date(_first_present_value(row, keys))


def _flatten_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def walk(items: list[Any]) -> None:
        for item in items or []:
            if not isinstance(item, dict):
                continue
            task_id = str(item.get("id") or item.get("task_id") or item.get("uid") or "").strip()
            if task_id:
                if task_id in seen:
                    continue
                seen.add(task_id)
            rows.append(item)
            for key in ("children", "childTasks", "subTasks", "tasks"):
                children = item.get(key)
                if isinstance(children, list):
                    walk(children)

    walk(tasks)
    return rows


def _is_milestone_task(task: dict[str, Any]) -> bool:
    if task.get("is_summary"):
        return False
    if _bool_is_true(task.get("is_milestone")):
        return True
    start_dt = _parse_real_project_date(task.get("start_date"))
    finish_dt = _parse_real_project_date(task.get("finish_date"))
    return bool(start_dt and finish_dt and start_dt == finish_dt)


def _milestone_plan_finish_date(task: dict[str, Any]) -> date | None:
    return _task_date(task, PLAN_FINISH_KEYS)


def _milestone_fact_finish_date(task: dict[str, Any]) -> date | None:
    fact_dt = _task_date(task, FACT_FINISH_KEYS)
    if fact_dt is not None:
        return fact_dt
    progress = _milestone_progress_as_fraction(task.get("percent_complete"))
    if progress is not None and progress >= 1.0 - 1e-9:
        return _parse_real_project_date(task.get("finish_date"))
    return None


def _project_milestone_rows(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for task in _flatten_tasks(tasks):
        if not _is_milestone_task(task):
            continue
        plan_finish = _milestone_plan_finish_date(task)
        fact_finish = _milestone_fact_finish_date(task)
        rows.append({
            "task_id": task.get("id") or task.get("task_id") or task.get("uid"),
            "name": task.get("name") or "",
            "start_date": task.get("start_date"),
            "finish_date": task.get("finish_date"),
            "plan_finish_date": plan_finish.isoformat() if plan_finish else None,
            "fact_finish_date": fact_finish.isoformat() if fact_finish else None,
            "percent_complete": task.get("percent_complete"),
            "outline_level": task.get("outline_level"),
        })
    rows.sort(key=lambda row: (row.get("plan_finish_date") or "", row.get("name") or ""))
    return rows


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
        "milestones": _project_milestone_rows(tasks),
    }


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
    start, end = _project_date_bounds(project)
    if start is not None and start > month_end:
        return False
    if end is not None and end < month_start:
        return False
    return True


def _project_is_active_on_month_end(project: dict[str, Any], year: int, month: int) -> bool:
    _month_start, month_end = _month_start_end(year, month)
    start, end = _project_date_bounds(project)
    return bool(start and end and start <= month_end <= end)


def _project_is_alive_in_period(project: dict[str, Any], period_start: date, period_end: date) -> bool:
    start, end = _project_date_bounds(project)
    if start is not None and start > period_end:
        return False
    if end is not None and end < period_start:
        return False
    return True


def _is_rnd_project_type(value: Any) -> bool:
    normalized = _normalize_text(value).replace("ё", "е").upper()
    compact = "".join(ch for ch in normalized if ch.isalnum())
    return any(marker in normalized or marker in compact for marker in RND_PROJECT_TYPE_MARKERS)


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
        "Планируется": "Планируется",
        "Завершен": "Завершен",
        "Закрыт": "Закрыт",
        "НаПаузе": "На паузе",
        "Отменен": "Отменен",
    }
    return status_map.get(raw, raw)


def _milestone_delay_workdays(milestone: dict[str, Any], as_of_date: date) -> int:
    finish_dt = _parse_real_project_date(milestone.get("finish_date"))
    if finish_dt is None or finish_dt > as_of_date:
        return 0
    return _business_days_between_exclusive(finish_dt, as_of_date)


def _project_delay_details(project: dict[str, Any], as_of_date: date) -> tuple[int, list[dict[str, Any]]]:
    details: list[dict[str, Any]] = []
    for milestone in project.get("overdue_milestones") or []:
        delay_workdays = _milestone_delay_workdays(milestone, as_of_date)
        if delay_workdays <= 0:
            continue
        details.append({
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "delay_workdays": delay_workdays,
            "percent_complete": milestone.get("percent_complete"),
        })
    details.sort(key=lambda row: (-(int(row.get("delay_workdays") or 0)), row.get("finish_date") or ""))
    max_delay = max((int(row.get("delay_workdays") or 0) for row in details), default=0)
    return max_delay, details


def _milestone_in_month(milestone: dict[str, Any], year: int, month: int) -> bool:
    plan_finish = _parse_real_project_date(milestone.get("plan_finish_date"))
    if plan_finish is None:
        return False
    return plan_finish.year == year and plan_finish.month == month


def _project_milestone_deviation_details(
    project: dict[str, Any],
    year: int,
    month: int,
) -> tuple[int, list[dict[str, Any]]]:
    details: list[dict[str, Any]] = []
    for milestone in project.get("milestones") or []:
        if not _milestone_in_month(milestone, year, month):
            continue
        plan_finish = _parse_real_project_date(milestone.get("plan_finish_date"))
        fact_finish = _parse_real_project_date(milestone.get("fact_finish_date"))
        if plan_finish is None or fact_finish is None:
            continue
        delay_workdays = _business_days_between_exclusive(plan_finish, fact_finish)
        if delay_workdays <= MAX_ALLOWED_DELAY_WORKDAYS:
            continue
        details.append({
            "name": milestone.get("name") or "",
            "task_id": milestone.get("task_id"),
            "plan_finish_date": milestone.get("plan_finish_date"),
            "fact_finish_date": milestone.get("fact_finish_date"),
            "delay_workdays": delay_workdays,
            "percent_complete": milestone.get("percent_complete"),
        })
    details.sort(key=lambda row: (-(int(row.get("delay_workdays") or 0)), row.get("plan_finish_date") or ""))
    max_delay = max((int(row.get("delay_workdays") or 0) for row in details), default=0)
    return max_delay, details


def _project_deviation_card_rows(projects: list[dict[str, Any]], year: int, month: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for project in projects:
        max_delay, milestone_details = _project_milestone_deviation_details(project, year, month)
        rows.append({
            "project_name": project.get("project_name") or "",
            "project_manager": project.get("project_manager") or "",
            "delay_workdays": max_delay,
            "milestones_count": len(milestone_details),
            "is_deviated": bool(milestone_details),
        })
    rows.sort(
        key=lambda row: (
            0 if row.get("is_deviated") else 1,
            -(int(row.get("delay_workdays") or 0)),
            str(row.get("project_name") or ""),
        )
    )
    return rows


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
        if _normalize_text(data_1c.get("podrazdelenie")) != TARGET_DEPARTMENT:
            continue
        projects.append(_project_summary(item, details))

    payload = {
        "projects": projects,
        "debug": {
            "source": "chief_constructor_projects",
            "target_department": TARGET_DEPARTMENT,
            "all_projects_count": len(items),
            "target_projects_count": len(projects),
            "timeout": TIMEOUT,
        },
    }
    _save_cache(payload)
    return payload


def get_gk_m1_monthly(year: int | None = None, month: int | None = None) -> dict | None:
    try:
        snapshot = _compute_projects_snapshot()
        projects = list(snapshot.get("projects") or [])
        ref_y, ref_m = _normalize_ref_period(year, month)
        rows: list[dict[str, Any]] = []
        ref_row: dict[str, Any] | None = None

        for y, m in _month_pairs_until(ref_y, ref_m):
            month_projects = [project for project in projects if _project_is_active_on_month_end(project, y, m)]
            deviated_projects = []
            for project in month_projects:
                _max_delay, details = _project_milestone_deviation_details(project, y, m)
                if details:
                    deviated_projects.append(project)
            plan_count = len(month_projects)
            deviated_count = len(deviated_projects)
            fact_count = max(plan_count - deviated_count, 0)
            row = {
                "month": m,
                "year": y,
                "month_name": MONTH_NAMES[m],
                "plan": plan_count,
                "fact": fact_count,
                "fact_deviated_over_10_workdays": deviated_count,
                "kpi_pct": round(fact_count / plan_count * 100, 1) if plan_count else None,
                "has_data": plan_count > 0,
                "values_unit": "шт.",
                "max_allowed_delay_workdays": MAX_ALLOWED_DELAY_WORKDAYS,
                "project_deviation_rows": _project_deviation_card_rows(month_projects, y, m),
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
        logger.exception("Ошибка при расчёте GK-M1 проектов главного конструктора")
        return None


def get_gk_q1_rnd_quarterly(year: int | None = None, month: int | None = None) -> dict | None:
    try:
        snapshot = _compute_projects_snapshot()
        projects = list(snapshot.get("projects") or [])
        ref_y, ref_m = _normalize_ref_period(year, month)
        rows: list[dict[str, Any]] = []
        ref_row: dict[str, Any] | None = None

        for y, m in _month_pairs_until(ref_y, ref_m):
            rnd_projects = [
                project for project in projects
                if _is_rnd_project_type(project.get("tip_proekta"))
                and _project_is_active_on_month_end(project, y, m)
            ]
            deviated_projects = []
            for project in rnd_projects:
                _max_delay, details = _project_milestone_deviation_details(project, y, m)
                if details:
                    deviated_projects.append(project)

            plan_count = len(rnd_projects)
            deviated_count = len(deviated_projects)
            fact_count = max(plan_count - deviated_count, 0)
            row = {
                "month": m,
                "year": y,
                "month_name": MONTH_NAMES[m],
                "plan": plan_count,
                "fact": fact_count,
                "fact_deviated_over_10_workdays": deviated_count,
                "kpi_pct": round(fact_count / plan_count * 100, 1) if plan_count else None,
                "has_data": plan_count > 0,
                "values_unit": "шт.",
                "project_type_markers": list(RND_PROJECT_TYPE_MARKERS),
                "max_allowed_delay_workdays": MAX_ALLOWED_DELAY_WORKDAYS,
                "project_deviation_rows": _project_deviation_card_rows(rnd_projects, y, m),
            }
            rows.append(row)
            if (y, m) == (ref_y, ref_m):
                ref_row = row

        type_counts: dict[str, int] = {}
        ref_projects = [
            project for project in projects
            if _is_rnd_project_type(project.get("tip_proekta"))
            and _project_is_active_on_month_end(project, ref_y, ref_m)
        ]
        for project in ref_projects:
            key = str(project.get("tip_proekta") or "").strip() or "без типа"
            type_counts[key] = type_counts.get(key, 0) + 1

        return {
            "data_granularity": "monthly",
            "monthly_data": rows,
            "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
            "ytd": {
                "total_plan": ref_row.get("plan") if ref_row else None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "total_fact_deviated_over_10_workdays": (
                    ref_row.get("fact_deviated_over_10_workdays") if ref_row else None
                ),
                "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                "months_with_data": sum(1 for row in rows if row.get("has_data")),
                "months_total": len(rows),
                "values_unit": "шт.",
            },
            "kpi_period": {
                "type": "last_full_month",
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_NAMES[ref_m],
            },
            "debug": {
                **(snapshot.get("debug") or {}),
                "filter": (
                    f"podrazdelenie={TARGET_DEPARTMENT!r}; "
                    "tip_proekta contains ОКР/НИР; active on selected month end"
                ),
                "project_type_counts": type_counts,
            },
        }
    except Exception:
        logger.exception("Ошибка при расчёте GK-Q1 проектов НИОКР главного конструктора")
        return None


def get_gk_m1_deviation_table(month: int | None = None, year: int | None = None) -> dict[str, Any] | None:
    try:
        snapshot = _compute_projects_snapshot()
        projects = list(snapshot.get("projects") or [])
        ref_y, ref_m = _normalize_ref_period(year, month)
        rows: list[dict[str, Any]] = []

        for project in projects:
            if not _project_is_active_on_month_end(project, ref_y, ref_m):
                continue
            max_delay, milestone_details = _project_milestone_deviation_details(project, ref_y, ref_m)
            if not milestone_details:
                continue
            rows.append({
                "number": len(rows) + 1,
                "project_code": project.get("project_code") or "",
                "project_name": project.get("project_name") or "",
                "project_manager": project.get("project_manager") or "",
                "timeline": _project_timeline_label(project),
                "deviation": f"{len(milestone_details)} вех., {max_delay} р.д.",
                "delay_workdays": max_delay,
                "status": _project_status_label(project),
                "progress_pct": project.get("project_progress_pct"),
                "milestone_deviations": milestone_details,
            })

        rows.sort(
            key=lambda row: (
                -(int(row.get("delay_workdays") or 0)),
                str(row.get("project_name") or ""),
            )
        )
        for index, row in enumerate(rows, start=1):
            row["number"] = index

        return {
            "name": "Проекты КБ с отклонениями по вехам >10 рабочих дней",
            "periodicity": "ежемесячно",
            "description": (
                "Проекты TurboProject с podrazdelenie=Конструкторское бюро, у которых есть "
                "вехи выбранного месяца с фактическим выполнением позже плана более чем на 10 рабочих дней."
            ),
            "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
            "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
            "rows": rows,
        }
    except Exception:
        logger.exception("Ошибка при построении таблицы GK-M1 проектов главного конструктора")
        return None
