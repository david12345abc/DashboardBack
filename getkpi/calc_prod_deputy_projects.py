"""
Проекты улучшений / сокращения потерь для заместителя операционного директора.

Роль не связана с техническим директором: модуль отдельно ходит в TurboProject,
берёт все проекты и оставляет только те, где в executors / kurator / rukovoditel
указан Целищев Павел Сергеевич.
"""
from __future__ import annotations

import json
import logging
import re
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from . import cache_manager
from .turboproject_config import API_BASE, EMAIL, PASSWORD, TIMEOUT

logger = logging.getLogger(__name__)

TARGET_PERSON = "Целищев Павел Сергеевич"
CACHE_DIR = cache_manager.CACHE_DIR
CACHE_PATH = CACHE_DIR / "prod_deputy_projects_snapshot.json"
CACHE_VERSION = 1

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
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
        "cache_version": CACHE_VERSION,
    }
    try:
        CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш проектов заместителя операционного директора")


def _parse_iso_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_real_project_date(value: Any) -> date | None:
    dt = _parse_iso_date(value)
    if dt is not None and dt.year > 1900:
        return dt.date()
    return None


def _safe_float(value: Any) -> float | None:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if num == num else None


def _milestone_progress_as_fraction(raw: Any) -> float | None:
    num = _safe_float(raw)
    if num is None:
        return None
    return num if abs(num) <= 1.0 else num / 100.0


def _milestone_is_zero_percent_complete(raw: Any) -> bool:
    frac = _milestone_progress_as_fraction(raw)
    return frac is not None and abs(frac) < 1e-9


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
        start_dt.date().isoformat() if start_dt and start_dt.year > 1900 else str(row.get("start_date") or "")[:10],
        finish_dt.date().isoformat() if finish_dt and finish_dt.year > 1900 else str(row.get("finish_date") or "")[:10],
    )


def _is_zero_duration_milestone(row: dict[str, Any]) -> bool:
    if row.get("is_summary"):
        return False
    if "is_milestone" in row and not _bool_is_true(row.get("is_milestone")):
        return False
    start_dt = _parse_iso_date(row.get("start_date"))
    finish_dt = _parse_iso_date(row.get("finish_date"))
    if not start_dt or not finish_dt or start_dt.year <= 1900 or finish_dt.year <= 1900:
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


def _api_overdue_milestones(details: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = details.get("overdue_milestones")
    if candidates is None:
        candidates = (details.get("project") or {}).get("overdue_milestones")
    return candidates if isinstance(candidates, list) else []


def _overdue_milestone_rows(milestones: list[dict[str, Any]], tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        if not finish_dt or finish_dt.year <= 1900 or finish_dt.date() >= today:
            continue
        frac = _milestone_progress_as_fraction(milestone.get("percent_complete"))
        rows.append({
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "percent_complete": frac if frac is not None else 0.0,
        })
    rows.sort(key=lambda row: (row.get("finish_date") or "", row.get("name") or ""))
    return rows


def _overdue_milestone_month_keys(milestones: list[dict[str, Any]]) -> list[str]:
    months: set[str] = set()
    for milestone in milestones:
        finish_dt = _parse_iso_date(milestone.get("finish_date"))
        if finish_dt and finish_dt.year > 1900:
            months.add(f"{finish_dt.year:04d}-{finish_dt.month:02d}")
    return sorted(months)


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
        return any(_contains_person(v, target) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_contains_person(item, target) for item in value)
    return False


def _is_target_person_project(data_1c: dict[str, Any]) -> bool:
    target = _normalize_person(TARGET_PERSON)
    return (
        _contains_person(data_1c.get("executors"), target)
        or _contains_person(data_1c.get("kurator"), target)
        or _contains_person(data_1c.get("rukovoditel"), target)
    )


def _login(session: requests.Session) -> str:
    resp = session.post(
        f"{API_BASE}/api/auth/login",
        json={"email": EMAIL, "password": PASSWORD},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def _api_get(session: requests.Session, path: str, token: str, params: dict[str, Any] | None = None) -> Any:
    resp = session.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _project_progress_pct(tasks: list[dict[str, Any]]) -> float | None:
    total = 0
    done = 0
    for task in tasks:
        if task.get("is_summary"):
            continue
        pct = _safe_float(task.get("percent_complete"))
        if pct is None:
            continue
        total += 1
        pct_value = pct * 100 if abs(pct) <= 1 else pct
        if pct_value >= 100:
            done += 1
    if not total:
        return None
    return round(done / total * 100, 1)


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
        "executors": data_1c.get("executors"),
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
        "project_progress_pct": _project_progress_pct(tasks),
        "overdue_milestone_months": _overdue_milestone_month_keys(overdue_milestones),
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

    target_projects: list[dict[str, Any]] = []
    for item in items:
        file_id = item.get("id")
        if not file_id:
            continue
        details = _api_get(session, f"/api/projects/files/{file_id}", token)
        data_1c = details.get("data_1c") or {}
        if not _is_target_person_project(data_1c):
            continue
        target_projects.append(_project_summary(item, details))

    payload = {
        "projects": target_projects,
        "debug": {
            "source": "prod_deputy_projects",
            "target_person": TARGET_PERSON,
            "all_projects_count": len(items),
            "target_projects_count": len(target_projects),
        },
    }
    _save_cache(payload)
    return payload


def _month_start_end(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _normalize_ref_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    return ref_y, max(1, min(12, ref_m))


def _month_pairs_until(ref_y: int, ref_m: int) -> list[tuple[int, int]]:
    return [(ref_y, mm) for mm in range(1, ref_m + 1)]


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


def _project_has_overdue_milestone_in_month(project: dict[str, Any], year: int, month: int) -> bool:
    return f"{year:04d}-{month:02d}" in (project.get("overdue_milestone_months") or [])


def get_pd_q1_monthly(year: int | None = None, month: int | None = None) -> dict | None:
    try:
        target_projects = list((_compute_projects_snapshot().get("projects") or []))
        ref_y, ref_m = _normalize_ref_period(year, month)
        rows: list[dict[str, Any]] = []
        ref_row: dict[str, Any] | None = None

        for y, m in _month_pairs_until(ref_y, ref_m):
            month_projects = [project for project in target_projects if _project_is_alive_in_month(project, y, m)]
            delayed_projects = [
                project for project in month_projects
                if _project_has_overdue_milestone_in_month(project, y, m)
            ]
            plan_count = len(month_projects)
            overdue_count = len(delayed_projects)
            on_time_count = max(plan_count - overdue_count, 0)
            row = {
                "month": m,
                "year": y,
                "month_name": MONTH_NAMES[m],
                "plan": plan_count,
                "fact": on_time_count,
                "fact_overdue": overdue_count,
                "kpi_pct": round(on_time_count / plan_count * 100, 1) if plan_count else None,
                "has_data": plan_count > 0 or overdue_count > 0,
                "projects_on_time": on_time_count,
                "projects_with_overdue_milestones": overdue_count,
                "values_unit": "шт.",
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
                "total_fact_overdue": ref_row.get("fact_overdue") if ref_row else None,
                "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                "months_with_data": sum(1 for row in rows if row.get("has_data")),
                "months_total": len(rows),
                "values_unit": "шт.",
            },
            "debug": {
                **((_compute_projects_snapshot().get("debug") or {})),
                "target_projects": target_projects,
            },
        }
    except Exception:
        logger.exception("Ошибка при расчёте PD-Q1 заместителя операционного директора")
        return None


def _project_overdue_milestones_in_month(project: dict[str, Any], ref_y: int, ref_m: int) -> list[dict[str, Any]]:
    month_start, month_end = _month_start_end(ref_y, ref_m)
    rows: list[dict[str, Any]] = []
    for milestone in project.get("overdue_milestones") or []:
        finish_dt = _parse_real_project_date(milestone.get("finish_date"))
        if finish_dt is not None and month_start <= finish_dt <= month_end:
            rows.append(milestone)
    return rows


def _build_milestone_deviation_details(overdue_rows: list[dict[str, Any]], as_of_date: date) -> list[dict[str, Any]]:
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


def get_pd_q1_deviation_table(month: int | None = None, year: int | None = None) -> dict[str, Any] | None:
    try:
        target_projects = list((_compute_projects_snapshot().get("projects") or []))
        ref_y, ref_m = _normalize_ref_period(year, month)
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
            "name": "Проекты улучшений / сокращения потерь с отклонениями по вехам",
            "periodicity": "ежемесячно",
            "description": (
                "Проекты, где Целищев Павел Сергеевич указан в executors, kurator или rukovoditel, "
                "и у которых есть отклонения по вехам. Одна строка = один проект."
            ),
            "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
            "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
            "rows": rows,
        }
    except Exception:
        logger.exception("Ошибка при построении таблицы PD-Q1 заместителя операционного директора")
        return None
