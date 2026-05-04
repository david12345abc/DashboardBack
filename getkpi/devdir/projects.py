from __future__ import annotations

import json
import logging
from calendar import monthrange
from datetime import date, datetime
from contextlib import redirect_stdout
import io
from pathlib import Path
from typing import Any

import requests

from ..cache_manager import locked_call
from ..techdir_projects import _api_get, _get_credentials, _login

logger = logging.getLogger(__name__)

TARGET_ORGANIZATION = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
TARGET_PROJECT_TYPE = "ОПЭ"
TARGET_PROJECT_STATUSES = {"ВРаботе", "Приостановлен"}

RD_M2_1_PLAN_BY_MONTH = {
    1: 1_474_715,
    2: 1_589_363,
    3: 1_557_220,
    4: 1_618_194,
    5: 1_561_051,
    6: 1_584_279,
    7: 1_555_304,
    8: 1_505_029,
    9: 1_648_114,
    10: 1_557_221,
    11: 1_564_826,
    12: 1_571_386,
}

CACHE_DIR = Path(__file__).resolve().parents[1] / "dashboard"
CACHE_PATH = CACHE_DIR / "devdir_projects_snapshot.json"
CACHE_VERSION = 1

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

SERVICE_DEVELOPMENT_DEPARTMENT = "Служба развития"


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("ё", "е").strip().lower().split())


def _status_allowed(raw_status: Any) -> bool:
    status = _normalize_text(raw_status)
    return status in {_normalize_text(item) for item in TARGET_PROJECT_STATUSES}


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


def _api_overdue_milestones(details: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = details.get("overdue_milestones")
    if candidates is None:
        candidates = (details.get("project") or {}).get("overdue_milestones")
    return candidates if isinstance(candidates, list) else []


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
        rows.append({
            "name": milestone.get("name") or "",
            "start_date": milestone.get("start_date"),
            "finish_date": milestone.get("finish_date"),
            "percent_complete": frac if frac is not None else 0.0,
        })
    rows.sort(key=lambda row: (row.get("finish_date") or "", row.get("name") or ""))
    return rows


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


def _project_progress_pct(tasks: list[dict[str, Any]]) -> float | None:
    task_total = 0
    task_done = 0
    for task in tasks or []:
        if task.get("is_summary"):
            continue
        try:
            pct = float(task.get("percent_complete"))
        except (TypeError, ValueError):
            continue
        if pct != pct:
            continue
        task_total += 1
        pct_value = pct * 100 if abs(pct) <= 1 else pct
        if pct_value >= 100:
            task_done += 1
    if not task_total:
        return None
    return round(task_done / task_total * 100, 1)


def _project_summary(
    summary_item: dict[str, Any],
    details: dict[str, Any],
    overdue_milestones: list[dict[str, Any]],
) -> dict[str, Any]:
    data_1c = details.get("data_1c") or {}
    project_meta = details.get("project") or {}
    tasks = details.get("tasks") or []
    project_progress_pct = _project_progress_pct(tasks)
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


def _load_cache() -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("cache_date") == date.today().isoformat():
        return data
    return None


def _save_cache(payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {**payload, "cache_date": date.today().isoformat(), "cache_version": CACHE_VERSION}
    try:
        CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш проектов devdir в %s", CACHE_PATH)


def _load_fot_management_cache(year: int, month: int) -> dict | None:
    path = Path(__file__).resolve().parents[1] / "dashboard" / f"fot_management_monthly_{year}_{month:02d}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    return data


def _service_development_fact_from_cache(year: int, month: int) -> float | None:
    data = _load_fot_management_cache(year, month)
    if not data:
        data = None
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
        if not item.get("has_1c"):
            continue
        file_id = item.get("id")
        if not file_id:
            continue
        details = _api_get(session, f"/api/projects/files/{file_id}", token)
        data_1c = details.get("data_1c") or {}
        if data_1c.get("organizatsiya") != TARGET_ORGANIZATION:
            continue
        if _normalize_text(data_1c.get("tip_proekta")) != _normalize_text(TARGET_PROJECT_TYPE):
            continue
        if not _status_allowed(data_1c.get("status_proekta")):
            continue

        overdue_milestones = _overdue_milestone_rows(
            _api_overdue_milestones(details),
            details.get("tasks") or [],
        )
        target_projects.append(_project_summary(item, details, overdue_milestones))

    payload = {
        "projects": target_projects,
        "debug": {
            "target_organization": TARGET_ORGANIZATION,
            "target_project_type": TARGET_PROJECT_TYPE,
            "target_project_statuses": sorted(TARGET_PROJECT_STATUSES),
            "target_projects_count": len(target_projects),
        },
    }
    _save_cache(payload)
    return payload


def get_devdir_projects_snapshot() -> dict:
    return _compute_projects_snapshot()


def _normalize_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    if ref_year == today.year:
        ref_month = min(ref_month, today.month)
    return ref_year, ref_month


def _build_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = _normalize_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    for y, m in pairs:
        plan_value = RD_M2_1_PLAN_BY_MONTH.get(m)
        fact_value = _service_development_fact_from_cache(y, m)
        has_data = plan_value is not None or fact_value is not None
        kpi_pct = round(fact_value / plan_value * 100, 1) if plan_value and fact_value is not None else None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan_value,
            "fact": fact_value,
            "kpi_pct": kpi_pct,
            "has_data": has_data,
            "values_unit": "руб.",
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
            "total_plan": round(sum(float(row.get("plan") or 0) for row in monthly_rows if row.get("plan") is not None), 2) if monthly_rows else None,
            "total_fact": round(sum(float(row.get("fact") or 0) for row in monthly_rows if row.get("fact") is not None), 2) if monthly_rows else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "target_plan_source": "devdir.xlsx",
            "target_fact_source": "dashboard/fot_management_monthly_YYYY_MM.json",
            "target_department": SERVICE_DEVELOPMENT_DEPARTMENT,
            "rows_by_month": [
                {
                    "year": row["year"],
                    "month": row["month"],
                    "plan": row["plan"],
                    "fact": row["fact"],
                }
                for row in monthly_rows
            ],
        },
    }


def get_rd_m2_1_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    def _runner() -> dict | None:
        try:
            return _build_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M2-1 из devdir")
            return None

    return locked_call("devdir_rd_m2_1", _runner)
