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
    _normalize_ref_period,
    _project_deviation_card_rows,
    _project_is_active_on_month_end,
    _project_milestone_deviation_details,
    _project_milestone_rows,
    _project_status_label,
    _project_timeline_label,
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
CACHE_VERSION = 3
YTD_CACHE_VERSION = 2
TARGET_OWNER = "Хозуян"
CERTIFICATION_DEPARTMENT_MARKERS = ("сертификац", "омис")


def _load_cache(allow_stale: bool = False) -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if allow_stale or data.get("cache_date") == date.today().isoformat():
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


def metrolog_projects_ytd_cache_path(year: int, month: int) -> Any:
    return cache_manager.CACHE_DIR / f"metrolog_projects_ytd_{int(year)}_{int(month):02d}.json"


def certification_projects_ytd_cache_path(year: int, month: int) -> Any:
    return cache_manager.CACHE_DIR / f"metrolog_certification_projects_ytd_{int(year)}_{int(month):02d}.json"


def _load_period_cache(path: Any, source: str) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    if data.get("cache_version") != YTD_CACHE_VERSION:
        return None
    if data.get("cache_source") != source:
        return None
    return data


def _save_period_cache(path: Any, source: str, payload: dict) -> None:
    cache_manager.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
        "cache_version": YTD_CACHE_VERSION,
        "cache_source": source,
    }
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить периодный кэш проектов главного метролога")


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


def _normalize_project_marker(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    raw = re.sub(r"[^0-9a-zа-я]+", " ", raw)
    return " ".join(raw.split())


def _is_certification_department_project(data_1c: dict[str, Any]) -> bool:
    values = (
        data_1c.get("podrazdelenie"),
        data_1c.get("nomer_proekta"),
        data_1c.get("tip_proekta"),
    )
    haystack = " ".join(_normalize_project_marker(value) for value in values)
    return any(marker in haystack for marker in CERTIFICATION_DEPARTMENT_MARKERS)


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
        "milestones": _project_milestone_rows(tasks),
    }


def _compute_projects_snapshot() -> dict:
    cached = _load_cache()
    if cached is not None:
        return cached

    session = requests.Session()
    try:
        token = _login(session)
        summary = _api_get(session, "/api/projects/files", token)
    except requests.RequestException:
        stale = _load_cache(allow_stale=True)
        if stale is not None:
            logger.warning("TurboProject недоступен для проектов метролога, используем устаревший кэш")
            return stale
        raise

    items = summary.get("items") or []
    projects: list[dict[str, Any]] = []
    certification_projects: list[dict[str, Any]] = []
    failed_projects: list[Any] = []

    for item in items:
        file_id = item.get("id")
        if not file_id:
            continue
        try:
            details = _api_get(session, f"/api/projects/files/{file_id}", token)
        except requests.RequestException as exc:
            failed_projects.append(file_id)
            logger.warning(
                "Пропускаем проект метролога %s: TurboProject не ответил (%s)",
                file_id,
                exc,
            )
            continue
        data_1c = details.get("data_1c") or {}
        is_owner_project = _is_target_owner_project(data_1c)
        is_certification_project = _is_certification_department_project(data_1c)
        if not is_owner_project and not is_certification_project:
            continue
        summary_row = _project_summary(item, details)
        if is_owner_project:
            projects.append(summary_row)
        if is_certification_project:
            certification_projects.append(summary_row)

    payload = {
        "projects": projects,
        "certification_projects": certification_projects,
        "debug": {
            "source": "metrolog_projects",
            "target_owner": TARGET_OWNER,
            "owner_fields": ["kurator", "rukovoditel"],
            "certification_markers": list(CERTIFICATION_DEPARTMENT_MARKERS),
            "all_projects_count": len(items),
            "target_projects_count": len(projects),
            "certification_projects_count": len(certification_projects),
            "failed_projects_count": len(failed_projects),
            "failed_project_ids": failed_projects[:20],
            "timeout": TIMEOUT,
        },
    }
    if not failed_projects:
        _save_cache(payload)
    return payload


def get_metrolog_projects_without_major_deviation_monthly(
    year: int | None = None,
    month: int | None = None,
) -> dict | None:
    """Plan/fact for projects where Hozuyan is curator or project manager."""
    try:
        ref_y, ref_m = _normalize_ref_period(year, month)
        cache_path = metrolog_projects_ytd_cache_path(ref_y, ref_m)
        cached = _load_period_cache(cache_path, "metrolog_projects_ytd")
        if cached is not None:
            return cached
        snapshot = _compute_projects_snapshot()
        projects = list(snapshot.get("projects") or [])
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
                "target_owner": TARGET_OWNER,
                "max_allowed_delay_workdays": MAX_ALLOWED_DELAY_WORKDAYS,
                "project_deviation_rows": _project_deviation_card_rows(month_projects, y, m),
            }
            rows.append(row)
            if (y, m) == (ref_y, ref_m):
                ref_row = row

        payload = {
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
        _save_period_cache(cache_path, "metrolog_projects_ytd", payload)
        return payload
    except Exception:
        logger.exception("Ошибка при расчёте проектов главного метролога")
        return None


def _build_projects_without_major_deviation_monthly(
    projects: list[dict[str, Any]],
    *,
    year: int | None,
    month: int | None,
    debug: dict[str, Any] | None = None,
) -> dict:
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
        "debug": debug or {},
    }


def get_certification_projects_without_major_deviation_monthly(
    year: int | None = None,
    month: int | None = None,
) -> dict | None:
    """Plan/fact for certification department projects without >10 workday milestone deviations."""
    try:
        ref_y, ref_m = _normalize_ref_period(year, month)
        cache_path = certification_projects_ytd_cache_path(ref_y, ref_m)
        cached = _load_period_cache(cache_path, "metrolog_certification_projects_ytd")
        if cached is not None:
            return cached
        snapshot = _compute_projects_snapshot()
        projects = list(snapshot.get("certification_projects") or [])
        payload = _build_projects_without_major_deviation_monthly(
            projects,
            year=ref_y,
            month=ref_m,
            debug={
                **(snapshot.get("debug") or {}),
                "filter": "certification department markers",
            },
        )
        _save_period_cache(cache_path, "metrolog_certification_projects_ytd", payload)
        return payload
    except Exception:
        logger.exception("Ошибка при расчёте проектов отдела сертификации главного метролога")
        return None


def _public_milestone_details(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in details:
        delay = int(item.get("delay_workdays") or 0)
        rows.append({
            **item,
            "delay_days": delay,
        })
    return rows


def get_metrolog_project_deviation_table(month: int | None = None, year: int | None = None) -> dict[str, Any] | None:
    """Projects where Hozuyan is owner and milestone deviation is over 10 working days."""
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
            public_details = _public_milestone_details(milestone_details)
            rows.append({
                "number": len(rows) + 1,
                "project_code": project.get("project_code") or "",
                "project_name": project.get("project_name") or "",
                "project_manager": project.get("project_manager") or "",
                "timeline": _project_timeline_label(project),
                "deviation": f"{len(public_details)} вех., {max_delay} р.д.",
                "delay_days": max_delay,
                "delay_workdays": max_delay,
                "status": _project_status_label(project),
                "progress_pct": project.get("project_progress_pct"),
                "kurator": project.get("kurator") or "",
                "milestone_deviations": public_details,
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
            "name": "Проекты метрологической службы с отклонениями >10 р.д.",
            "periodicity": "ежемесячно",
            "description": (
                "Проекты TurboProject, где Хозуян указан куратором или руководителем проекта, "
                "и вехи выбранного месяца выполнены позже плана более чем на 10 рабочих дней."
            ),
            "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
            "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
            "rows": rows,
        }
    except Exception:
        logger.exception("Ошибка при построении таблицы проектов главного метролога")
        return None
