"""Turboproject snapshot for projects where selected resources participate.

The module mirrors the techdir-style snapshot flow:
  - fetch project list;
  - inspect each project details payload;
  - keep only projects that match the target resources;
  - build a cached snapshot with plan/fact counters.

Plan = total matching projects.
Fact = projects without milestone deviations.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

from ..cache_manager import locked_call
from ..list_enterprise_positions import employees_by_department
from . import ytd_json_cache
from .rd_monthly_period import MONTH_NAMES
from .rd_monthly_period import normalize_rd_tile_period

logger = logging.getLogger(__name__)

API_BASE = os.getenv("TURBOPROJECT_API_BASE", "http://192.168.1.236:8000")
EMAIL = os.getenv("TURBOPROJECT_EMAIL", "sktb_razvitie6@turbo-don.ru")
PASSWORD = os.getenv("TURBOPROJECT_PASSWORD", "Ruslandavletov28")
TIMEOUT = 60

TARGET_ORGANIZATION = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
TARGET_RESOURCES_DEPARTMENT = (
    "Председатель Совета Директоров / ОПЕРАЦИОННЫЙ ДИРЕКТОР / "
    "Зам.операционного директора - директор по производству / "
    "ДИРЕКТОР ПО РАЗВИТИЮ / Служба развития / "
    "Сектор по внедрению искусственного интеллекта"
)

CACHE_DIR = Path(__file__).resolve().parent.parent / "dashboard"
CACHE_PATH = CACHE_DIR / "devdir_turboproject_projects_by_resources_snapshot.json"
CACHE_VERSION = 4
TABLE_CACHE_PREFIX = "devdir_turboproject_projects_by_resources_deviations"
TABLE_CACHE_VERSION = 8
TILE_CACHE_PREFIX = "devdir_rd_m3_1_turboproject_projects_by_resources"
TILE_CACHE_SOURCE_TAG = "devdir_rd_m3_1_turboproject_projects_by_resources_ytd"
TILE_CACHE_VERSION = 7

EMPTY = "00000000-0000-0000-0000-000000000000"


def normalize_name(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().lower().replace("ё", "е").split())


@lru_cache(maxsize=1)
def target_resources() -> tuple[str, ...]:
    """Актуальные сотрудники сектора ИИ из 1С."""
    return tuple(employees_by_department(TARGET_RESOURCES_DEPARTMENT))


def target_resources_normalized() -> set[str]:
    return {
        normalized
        for resource in target_resources()
        if (normalized := normalize_name(resource))
    }


def unique_resource_names(names: list[Any]) -> list[str]:
    resources_by_key: dict[str, str] = {}
    for name in names:
        if not isinstance(name, str):
            continue
        normalized = name.strip()
        if not normalized:
            continue
        resources_by_key.setdefault(normalize_name(normalized), normalized)
    return sorted(resources_by_key.values(), key=str.lower)


def build_project_resources(details: dict[str, Any]) -> list[str]:
    resources = details.get("resources") or []
    if resources:
        return unique_resource_names(resources)

    assignment_resource_names: list[Any] = []
    for task in details.get("tasks") or []:
        for assignment in task.get("assignments") or []:
            assignment_resource_names.append(assignment.get("resource_name"))

    return unique_resource_names(assignment_resource_names)


def matched_target_resources(resources: list[str]) -> list[str]:
    normalized = {normalize_name(resource) for resource in resources}
    targets = target_resources_normalized()
    return sorted(
        {
            resource
            for resource in resources
            if normalize_name(resource) in targets
        },
        key=str.lower,
    ) if normalized & targets else []


def has_target_resource(resources: list[str]) -> bool:
    return bool(matched_target_resources(resources))


def login(session: requests.Session) -> str:
    response = session.post(
        f"{API_BASE}/api/auth/login",
        json={"email": EMAIL, "password": PASSWORD},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["token"]


def api_get(
    session: requests.Session,
    path: str,
    token: str,
    params: dict[str, Any] | None = None,
) -> Any:
    response = session.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


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


def _is_real_date(dt: datetime | None) -> bool:
    return dt is not None and dt.year > 1900


def _parse_real_project_date(value: Any) -> date | None:
    dt = _parse_iso_date(value)
    if not _is_real_date(dt):
        return None
    return dt.date()


def _month_start_end(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


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


def _project_is_alive_in_range(project: dict[str, Any], period_start: date, period_end: date) -> bool:
    start, end = _project_date_bounds(project)
    if start is not None and start > period_end:
        return False
    if end is not None and end < period_start:
        return False
    return True


def _project_is_alive_in_month(project: dict[str, Any], year: int, month: int) -> bool:
    month_start, month_end = _month_start_end(year, month)
    return _project_is_alive_in_range(project, month_start, month_end)


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
    if "is_milestone" in row and not row.get("is_milestone"):
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
    return _row_milestone_match_key(row) in milestone_keys


def _milestone_progress_as_fraction(raw: Any) -> float | None:
    try:
        num = float(raw)
    except (TypeError, ValueError):
        return None
    if num != num:
        return None
    if abs(num) <= 1.0:
        return num
    return num / 100.0


def _milestone_is_zero_percent_complete(raw: Any) -> bool:
    frac = _milestone_progress_as_fraction(raw)
    if frac is None:
        return False
    return abs(frac) < 1e-9


def _working_days_between(start: date, end: date) -> int:
    """Считает рабочие дни между датами, исключая выходные."""
    if start >= end:
        return 0
    days = 0
    current = start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def _api_overdue_milestones(details: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = details.get("overdue_milestones")
    if candidates is None:
        candidates = (details.get("project") or {}).get("overdue_milestones")
    return candidates if isinstance(candidates, list) else []


def _overdue_milestone_rows(
    milestones: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    *,
    as_of_date: date,
) -> list[dict[str, Any]]:
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
        finish_date = finish_dt.date()
        if finish_date >= as_of_date:
            continue
        delay_workdays = _working_days_between(finish_date, as_of_date)
        frac = _milestone_progress_as_fraction(milestone.get("percent_complete"))
        rows.append({
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "month": finish_date.month,
            "month_name": MONTH_NAMES[finish_date.month],
            "percent_complete": frac if frac is not None else 0.0,
            "delay_workdays": delay_workdays,
        })
    rows.sort(key=lambda row: (row.get("finish_date") or "", row.get("name") or ""))
    return rows


def _project_progress_pct(
    tasks: list[dict[str, Any]],
    project_meta: dict[str, Any] | None = None,
) -> float | None:
    """Прогресс как в TurboProject UI: корневая summary-задача / project.percent_complete."""
    for raw in (
        (project_meta or {}).get("percent_complete"),
        next((task.get("percent_complete") for task in tasks or [] if task.get("is_summary")), None),
    ):
        frac = _milestone_progress_as_fraction(raw)
        if frac is not None:
            return round(frac * 100, 1)

    task_total = 0
    task_done = 0
    for task in tasks or []:
        if task.get("is_summary"):
            continue
        frac = _milestone_progress_as_fraction(task.get("percent_complete"))
        if frac is None:
            continue
        task_total += 1
        if frac >= 1.0 - 1e-9:
            task_done += 1
    if not task_total:
        return None
    return round(task_done / task_total * 100, 1)


def project_name(summary_item: dict[str, Any], details: dict[str, Any]) -> str:
    project_meta = details.get("project") or {}
    return (project_meta.get("name") or summary_item.get("original_name") or "").strip()


def _project_summary(
    summary_item: dict[str, Any],
    details: dict[str, Any],
    overdue_milestones: list[dict[str, Any]],
    resources: list[str],
) -> dict[str, Any]:
    project_meta = details.get("project") or {}
    data_1c = details.get("data_1c") or {}
    tasks = details.get("tasks") or []
    project_progress_pct = _project_progress_pct(tasks, project_meta)
    max_delay_workdays = max((int(row.get("delay_workdays") or 0) for row in overdue_milestones), default=0)
    is_fact = not overdue_milestones
    return {
        "file_id": summary_item.get("id"),
        "project_name": project_name(summary_item, details),
        "resources": resources,
        "matched_resources": matched_target_resources(resources),
        "project_manager": data_1c.get("rukovoditel"),
        "kurator": data_1c.get("kurator"),
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
        "max_delay_workdays": max_delay_workdays,
        "is_fact": is_fact,
        "project_progress_pct": project_progress_pct,
        "milestone_deviations": overdue_milestones,
    }


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


def _project_actual_completion_date(project: dict[str, Any]) -> date | None:
    """Фактическая дата закрытия проекта из 1С (data_okonchaniya)."""
    completion = _parse_real_project_date(project.get("data_okonchaniya"))
    if completion is not None:
        return completion
    status = str(project.get("status_proekta") or "").strip()
    if status in {"Завершен", "Закрыт"}:
        return (
            _parse_real_project_date(project.get("finish_date"))
            or _parse_real_project_date(project.get("planovaya_data_okonchaniya"))
        )
    return None


def _project_is_fully_complete(project: dict[str, Any]) -> bool:
    progress = project.get("project_progress_pct")
    if progress is not None and float(progress) >= 100.0 - 1e-9:
        return True
    status = str(project.get("status_proekta") or "").strip()
    return status in {"Завершен", "Закрыт"}


def _project_exempt_from_month_deviations(
    project: dict[str, Any],
    ref_y: int,
    ref_m: int,
) -> bool:
    """100% выполнение на конец месяца — отклонения по вехам не показываем."""
    if not _project_is_fully_complete(project):
        return False
    completion = _project_actual_completion_date(project)
    if completion is None:
        return True
    _, month_end = _month_start_end(ref_y, ref_m)
    return completion <= month_end


def _project_overdue_milestones_in_month(
    project: dict[str, Any],
    ref_y: int,
    ref_m: int,
    *,
    as_of_date: date,
) -> list[dict[str, Any]]:
    if _project_exempt_from_month_deviations(project, ref_y, ref_m):
        return []

    month_start, month_end = _month_start_end(ref_y, ref_m)
    rows: list[dict[str, Any]] = []
    for milestone in project.get("milestone_deviations") or []:
        finish_dt = _parse_iso_date(milestone.get("finish_date"))
        if not _is_real_date(finish_dt):
            continue
        finish_date = finish_dt.date()
        if finish_date < month_start or finish_date > month_end:
            continue
        if finish_date >= as_of_date:
            continue
        delay_workdays = _working_days_between(finish_date, as_of_date)
        rows.append({
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "percent_complete": milestone.get("percent_complete"),
            "delay_workdays": delay_workdays,
        })
    rows.sort(key=lambda row: (row.get("finish_date") or "", row.get("name") or ""))
    return rows


def _build_projects_deviation_table(
    ref_y: int,
    ref_m: int,
    *,
    projects: list[dict[str, Any]] | None = None,
    table_name: str = "Проекты с отклонениями по вехам",
) -> dict[str, Any]:
    if projects is None:
        snapshot = get_projects_snapshot()
        projects = list(snapshot.get("projects") or [])
    month_end = _month_start_end(ref_y, ref_m)[1]
    as_of_date = min(month_end, date.today())
    month_projects = [
        project
        for project in projects
        if _project_is_alive_in_month(project, ref_y, ref_m)
    ]
    rows: list[dict[str, Any]] = []

    for project in month_projects:
        milestone_rows = _project_overdue_milestones_in_month(
            project,
            ref_y,
            ref_m,
            as_of_date=as_of_date,
        )
        if not milestone_rows:
            continue
        max_delay_workdays = max(
            (int(row.get("delay_workdays") or 0) for row in milestone_rows),
            default=0,
        )
        rows.append({
            "number": len(rows) + 1,
            "project_code": project.get("project_code") or "",
            "project_name": project.get("project_name") or "",
            "project_manager": project.get("project_manager") or "",
            "timeline": _project_timeline_label(project),
            "deviation": f"{len(milestone_rows)} вех., {max_delay_workdays} р.д.",
            "delay_days": max_delay_workdays,
            "status": _project_status_label(project),
            "progress_pct": project.get("project_progress_pct"),
            "overdue_milestones_count": len(milestone_rows),
            "milestone_deviations": milestone_rows,
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
            "Проекты, в которых есть отклонения по вехам. "
            "Одна строка = один проект."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
        "rows": rows,
        "summary": {
            "plan": len(month_projects),
            "fact": len(month_projects) - len(rows),
        },
    }


def _build_projects_monthly_payload(
    year: int | None = None,
    month: int | None = None,
    *,
    projects: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]
    if projects is None:
        snapshot = get_projects_snapshot()
        projects = list(snapshot.get("projects") or [])

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        month_end = _month_start_end(y, m)[1]
        as_of_date = min(month_end, date.today())
        month_projects = [
            project for project in projects
            if _project_is_alive_in_month(project, y, m)
        ]
        fact_projects = 0
        deviation_projects = 0
        for project in month_projects:
            milestone_rows = _project_overdue_milestones_in_month(
                project,
                y,
                m,
                as_of_date=as_of_date,
            )
            max_delay_workdays = max(
                (int(row.get("delay_workdays") or 0) for row in milestone_rows),
                default=0,
            )
            if milestone_rows:
                deviation_projects += 1
            else:
                fact_projects += 1
        plan_count = len(month_projects)
        kpi_pct = round(fact_projects / plan_count * 100, 1) if plan_count else None
        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_count,
            "fact": fact_projects,
            "kpi_pct": kpi_pct,
            "has_data": plan_count > 0,
            "projects_with_resources": plan_count,
            "projects_without_deviations": fact_projects,
            "projects_with_deviations": deviation_projects,
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
            "target_resources_department": TARGET_RESOURCES_DEPARTMENT,
            "target_resources": list(target_resources()),
            "target_projects_count": len(projects),
            "kpi_route": TILE_CACHE_PREFIX,
        },
    }


def get_rd_m3_1_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(TILE_CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict | None:
        cached = ytd_json_cache.load_payload(
            c_path,
            source_tag=TILE_CACHE_SOURCE_TAG,
            version=TILE_CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_projects_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M3-1 (TurboProject по ресурсам)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                c_path,
                payload,
                source_tag=TILE_CACHE_SOURCE_TAG,
                version=TILE_CACHE_VERSION,
            )
        return payload

    return locked_call(f"{TILE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}", _runner)


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    y, m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(TILE_CACHE_PREFIX, y, m)


def _load_cache() -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        data.get("cache_version") == CACHE_VERSION
        and data.get("cache_date") == date.today().isoformat()
    ):
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
        CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш проектов Turboproject в %s", CACHE_PATH)


def _table_cache_path(ref_y: int, ref_m: int) -> Path:
    return CACHE_DIR / f"{TABLE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}.json"


def _load_table_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") != TABLE_CACHE_VERSION:
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    payload = data.get("payload")
    return payload if isinstance(payload, dict) else None


def _save_table_cache(path: Path, payload: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "cache_date": date.today().isoformat(),
        "cache_version": TABLE_CACHE_VERSION,
        "payload": payload,
    }
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш таблицы отклонений в %s", path)


def _compute_projects_snapshot() -> dict:
    cached = _load_cache()
    if cached is not None:
        return cached

    session = requests.Session()
    token = login(session)

    summary = api_get(session, "/api/projects/files", token)
    items = summary.get("items") or []
    as_of_date = datetime.now().date()

    target_projects: list[dict[str, Any]] = []
    for item in items:
        file_id = item.get("id")
        if not file_id:
            continue
        try:
            details = api_get(session, f"/api/projects/files/{file_id}", token)
        except Exception:
            logger.exception("Не удалось получить детали проекта %s", file_id)
            continue

        resources = build_project_resources(details)
        if not has_target_resource(resources):
            continue

        tasks = details.get("tasks") or []
        project_meta = details.get("project") or {}
        overdue_milestones = _overdue_milestone_rows(
            _api_overdue_milestones(details),
            tasks,
            as_of_date=as_of_date,
        )
        summary_row = _project_summary(item, details, overdue_milestones, resources)
        if _project_is_fully_complete(summary_row):
            summary_row["milestone_deviations"] = []
            summary_row["overdue_milestones_count"] = 0
            summary_row["max_delay_workdays"] = 0
            summary_row["is_fact"] = True
        target_projects.append(summary_row)

    fact_projects = [project for project in target_projects if project.get("is_fact")]
    payload = {
        "projects": target_projects,
        "summary": {
            "plan": len(target_projects),
            "fact": len(fact_projects),
            "kpi_pct": (
                round(len(fact_projects) / len(target_projects) * 100, 1)
                if target_projects
                else None
            ),
            "selection_scope": "resources_only",
            "organization": TARGET_ORGANIZATION,
            "target_resources_department": TARGET_RESOURCES_DEPARTMENT,
            "target_resources": list(target_resources()),
        },
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_resources_department": TARGET_RESOURCES_DEPARTMENT,
            "target_resources": list(target_resources()),
            "target_projects_count": len(target_projects),
            "fact_projects_count": len(fact_projects),
            "cache_path": str(CACHE_PATH),
            "projects": target_projects,
        },
    }
    _save_cache(payload)
    return payload


def get_projects_snapshot() -> dict:
    return _compute_projects_snapshot()


def get_projects_deviation_table(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y = int(year) if year is not None else date.today().year
    ref_m = int(month) if month is not None else date.today().month
    ref_m = max(1, min(12, ref_m))
    cache_path = _table_cache_path(ref_y, ref_m)

    def _runner() -> dict[str, Any]:
        cached = _load_table_cache(cache_path)
        if cached is not None:
            return cached
        payload = _build_projects_deviation_table(ref_y, ref_m)
        _save_table_cache(cache_path, payload)
        return payload

    return locked_call(f"{TABLE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    try:
        snapshot = get_projects_snapshot()
        if "--json" in sys.argv[1:]:
            print(json.dumps(snapshot, ensure_ascii=False, indent=2))
            return

        summary = snapshot.get("summary") or {}
        print(f"План: {summary.get('plan', 0)}")
        print(f"Факт: {summary.get('fact', 0)}")
        for project in snapshot.get("projects") or []:
            name = str(project.get("project_name") or "").strip()
            if name:
                print(name)
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
