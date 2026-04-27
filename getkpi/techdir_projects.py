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
from .kpi_periods import current_calendar_quarter, quarter_month_tuples

logger = logging.getLogger(__name__)

TARGET_ORGANIZATION = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
TARGET_PROJECT_TYPE_TD_M1 = "ВнешнийЗаказ"
TARGET_PROJECT_TYPE_TD_Q1 = "РазвитияИУлучшений"
TIMEOUT = 60

ROOT_DIR = Path(__file__).resolve().parents[2]
CACHE_DIR = cache_manager.CACHE_DIR
CACHE_PATH = CACHE_DIR / "techdir_projects_snapshot.json"
CACHE_VERSION = 11
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


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(round(val))
    s = str(val).strip()
    if not s:
        return None
    try:
        return int(float(s.replace(",", ".")))
    except (ValueError, TypeError):
        return None


def _task_stat_get(stats: dict[str, Any], *candidates: str) -> Any:
    for key in candidates:
        if key in stats and stats[key] is not None:
            return stats[key]
    lower = {str(k).lower(): v for k, v in stats.items()}
    for key in candidates:
        v = lower.get(key.lower())
        if v is not None:
            return v
    return None


def _task_percent_weighted(percent_complete: Any) -> float:
    """Доля выполнения задачи в процентах (0–100): TurboProject даёт 0–1 или 0–100."""
    try:
        pc = float(percent_complete or 0)
    except (TypeError, ValueError):
        return 0.0
    if pc <= 1.0 + 1e-6:
        return pc * 100.0
    return min(pc, 100.0)


def _compute_project_progress_pct(details: dict[str, Any]) -> float | None:
    """
    Доля выполнения проекта, %.
    Сначала task_stats (разные регистры ключей), иначе — по дереву tasks.
    """
    ts = details.get("task_stats")
    if not isinstance(ts, dict):
        ts = {}
    ct = _safe_int(_task_stat_get(ts, "completed_tasks", "CompletedTasks"))
    tt = _safe_int(_task_stat_get(ts, "total_tasks", "TotalTasks"))
    ns = _safe_int(_task_stat_get(ts, "non_summary_tasks", "NonSummaryTasks"))
    cti = 0 if ct is None else max(ct, 0)
    if tt is not None and tt > 0:
        return round(cti / tt * 100, 1)
    if ns is not None and ns > 0:
        return round(cti / ns * 100, 1)

    tasks = details.get("tasks") or []
    if not isinstance(tasks, list) or not tasks:
        return None
    eligible: list[dict[str, Any]] = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        if t.get("is_summary"):
            continue
        eligible.append(t)
    if not eligible:
        return None
    # Среднее по % завершения рабочих задач (учитывает частичный прогресс 0–1 от MS Project).
    return round(
        sum(_task_percent_weighted(t.get("percent_complete")) for t in eligible) / len(eligible),
        1,
    )


def _project_summary(
    summary_item: dict[str, Any],
    details: dict[str, Any],
    overdue_milestones_count: int,
) -> dict[str, Any]:
    data_1c = details.get("data_1c") or {}
    dates = details.get("dates") or {}
    task_stats = details.get("task_stats") or {}
    if not isinstance(task_stats, dict):
        task_stats = {}
    project_progress_pct = _compute_project_progress_pct(details)
    name_tp = (details.get("project_name") or "").strip()
    name_orig = (summary_item.get("original_name") or "").strip()
    return {
        "file_id": summary_item.get("id"),
        "project_name": name_tp or name_orig,
        "project_manager": data_1c.get("rukovoditel"),
        "project_code": data_1c.get("nomer_proekta"),
        "organizatsiya": data_1c.get("organizatsiya"),
        "tip_proekta": data_1c.get("tip_proekta"),
        "podrazdelenie": data_1c.get("podrazdelenie"),
        "status_proekta": data_1c.get("status_proekta"),
        "start_date": dates.get("start_date"),
        "finish_date": dates.get("finish_date"),
        "actual_finish_date": dates.get("actual_finish_date"),
        "baseline_start": dates.get("baseline_start"),
        "baseline_finish": dates.get("baseline_finish"),
        "plan_finish_1c": dates.get("plan_finish_1c"),
        "data_nachala": data_1c.get("data_nachala"),
        "planovaya_data_nachala": data_1c.get("planovaya_data_nachala"),
        "planovaya_data_okonchaniya": data_1c.get("planovaya_data_okonchaniya"),
        "data_okonchaniya": data_1c.get("data_okonchaniya"),
        "task_stats": task_stats,
        "project_progress_pct": project_progress_pct,
        "overdue_milestones_count": overdue_milestones_count,
        "milestone_months": _milestone_month_keys(details.get("tasks") or []),
        "overdue_milestone_months": _overdue_milestone_month_keys(details.get("tasks") or []),
        "overdue_milestones": _overdue_milestone_rows(details.get("tasks") or []),
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


def _format_ru_date(value: Any) -> str:
    dt = _parse_project_date(value)
    if dt is None:
        return "—"
    return dt.strftime("%d.%m.%Y")


def _format_date_range(start_value: Any, end_value: Any) -> str:
    start = _format_ru_date(start_value)
    end = _format_ru_date(end_value)
    if start == "—" and end == "—":
        return "—"
    if start == "—":
        return end
    if end == "—":
        return start
    return f"{start} - {end}"


def _format_progress(project: dict[str, Any]) -> str:
    pct = project.get("project_progress_pct")
    if pct is None:
        return "—"
    return f"{pct}%"


def _project_sroki(project: dict[str, Any]) -> str:
    """Сроки: приоритет baseline_start — baseline_finish (реальные даты); иначе даты из графика / 1С."""
    bs, bf = project.get("baseline_start"), project.get("baseline_finish")
    bs_ok = bs if _parse_real_project_date(bs) is not None else None
    bf_ok = bf if _parse_real_project_date(bf) is not None else None
    if bs_ok is not None or bf_ok is not None:
        return _format_date_range(bs_ok, bf_ok)
    return _format_date_range(
        project.get("start_date")
        or project.get("planovaya_data_nachala")
        or project.get("data_nachala"),
        project.get("finish_date")
        or project.get("planovaya_data_okonchaniya")
        or project.get("data_okonchaniya"),
    )


def _project_schedule_bounds_for_overlap(project: dict[str, Any]) -> tuple[date | None, date | None]:
    """Границы сроков для пересечения с периодом — те же поля и порядок, что в колонке «Сроки»."""
    bs, bf = project.get("baseline_start"), project.get("baseline_finish")
    bs_ok = bs if _parse_real_project_date(bs) is not None else None
    bf_ok = bf if _parse_real_project_date(bf) is not None else None
    if bs_ok is not None or bf_ok is not None:
        return (
            _parse_real_project_date(bs_ok) if bs_ok else None,
            _parse_real_project_date(bf_ok) if bf_ok else None,
        )
    start_val = (
        project.get("start_date")
        or project.get("planovaya_data_nachala")
        or project.get("data_nachala")
    )
    end_val = (
        project.get("finish_date")
        or project.get("planovaya_data_okonchaniya")
        or project.get("data_okonchaniya")
    )
    return _parse_real_project_date(start_val), _parse_real_project_date(end_val)


def _project_overlaps_period_by_schedule(
    project: dict[str, Any],
    period_start: date,
    period_end: date,
) -> bool:
    """Есть ли пересечение календарного интервала проекта с [period_start, period_end]."""
    start, end = _project_schedule_bounds_for_overlap(project)
    if start is not None and start > period_end:
        return False
    if end is not None and end < period_start:
        return False
    return True


def _project_is_alive_in_month(project: dict[str, Any], year: int, month: int) -> bool:
    month_start, month_end = _month_start_end(year, month)
    return _project_overlaps_period_by_schedule(project, month_start, month_end)


def _project_is_alive_in_range(
    project: dict[str, Any],
    period_start: date,
    period_end: date,
) -> bool:
    return _project_overlaps_period_by_schedule(project, period_start, period_end)


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
    year, quarter = current_calendar_quarter(date.today())
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
            "type": "current_quarter",
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
    """Таблица отклонений по типу «Внешний заказ» (TD-M1): период — выбранный месяц."""
    project_type_label = {
        TARGET_PROJECT_TYPE_TD_M1: "Внешний Заказ",
        TARGET_PROJECT_TYPE_TD_Q1: "Улучшение и развитие",
    }.get(project_type, project_type)
    target_projects = _projects_for_type(project_type)
    month_start, month_end = _month_start_end(ref_y, ref_m)
    rows_with_sort: list[tuple[int, int, str, dict[str, Any]]] = []

    for project in target_projects:
        if not _project_is_alive_in_month(project, ref_y, ref_m):
            continue
        total_delay_days = 0
        for milestone in project.get("overdue_milestones") or []:
            finish_dt = _parse_real_project_date(milestone.get("finish_date"))
            if finish_dt is None:
                continue
            if finish_dt < month_start or finish_dt > month_end:
                continue
            delay_days = max((month_end - finish_dt).days, 0)
            total_delay_days += delay_days

        if total_delay_days <= 0:
            continue

        nomer = project.get("project_code")
        pname = project.get("project_name")
        row = {
            "nomer_proekta": nomer if nomer not in (None, "") else "—",
            "project_name": pname if pname not in (None, "") else "—",
            "rp": project.get("project_manager") or "—",
            "sroki": _project_sroki(project),
            "otklonenie_summarnoe": total_delay_days,
            "status": project.get("status_proekta") or "—",
            "progress": _format_progress(project),
        }
        rows_with_sort.append((0, -total_delay_days, row["project_name"], row))

    rows = [row for _, _, _, row in sorted(rows_with_sort)]
    return {
        "name": f"Проекты с отклонениями: {project_type_label}",
        "periodicity": "ежемесячно",
        "description": (
            "Проекты техдирекции, активные в выбранном месяце, с просроченными незавершёнными вехами. "
            "Одна строка = один проект; «Отклонение (суммарное)» — сумма дней просрочки по всем таким вехам в месяце."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": [
            "№",
            "Название",
            "РП",
            "Сроки",
            "Отклонение (суммарное)",
            "Статус",
            "Прогресс",
        ],
        "rows": rows,
    }


def _build_deviation_table_q1_quarter() -> dict[str, Any]:
    """Таблица отклонений TD-Q1: тот же период и состав проектов, что на плитке (текущий календарный квартал)."""
    project_type = TARGET_PROJECT_TYPE_TD_Q1
    project_type_label = "Улучшение и развитие"
    target_projects = _projects_for_type(project_type)
    year, quarter = current_calendar_quarter(date.today())
    q_months = {f"{yy:04d}-{mm:02d}" for yy, mm in quarter_month_tuples(year, quarter)}
    quarter_month_dates = quarter_month_tuples(year, quarter)
    quarter_start = date(quarter_month_dates[0][0], quarter_month_dates[0][1], 1)
    quarter_end_year, quarter_end_month = quarter_month_dates[-1]
    quarter_end = date(
        quarter_end_year, quarter_end_month,
        monthrange(quarter_end_year, quarter_end_month)[1],
    )

    quarter_projects = [
        project for project in target_projects
        if _project_is_alive_in_range(project, quarter_start, quarter_end)
    ]
    delayed_projects = [
        project for project in quarter_projects
        if q_months.intersection(project.get("overdue_milestone_months") or [])
    ]

    rows_with_sort: list[tuple[int, int, str, dict[str, Any]]] = []
    for project in delayed_projects:
        total_delay_days = 0
        for milestone in project.get("overdue_milestones") or []:
            finish_dt = _parse_real_project_date(milestone.get("finish_date"))
            if finish_dt is None:
                continue
            if finish_dt < quarter_start or finish_dt > quarter_end:
                continue
            delay_days = max((quarter_end - finish_dt).days, 0)
            total_delay_days += delay_days

        nomer = project.get("project_code")
        pname = project.get("project_name")
        row = {
            "nomer_proekta": nomer if nomer not in (None, "") else "—",
            "project_name": pname if pname not in (None, "") else "—",
            "rp": project.get("project_manager") or "—",
            "sroki": _project_sroki(project),
            "otklonenie_summarnoe": total_delay_days,
            "status": project.get("status_proekta") or "—",
            "progress": _format_progress(project),
        }
        rows_with_sort.append((0, -total_delay_days, row["project_name"], row))

    rows = [row for _, _, _, row in sorted(rows_with_sort)]
    return {
        "name": f"Проекты с отклонениями: {project_type_label}",
        "periodicity": "ежеквартально",
        "description": (
            "Те же проекты и период, что на плитке TD-Q1: текущий календарный квартал. "
            "Состав строк = проекты с отклонениями по вехам в квартале (как fact на плитке). "
            "«Отклонение (суммарное)» — сумма дней от плановой даты вехи до конца квартала по всем таким вехам в квартале."
        ),
        "period": {
            "type": "current_quarter",
            "year": year,
            "quarter": quarter,
            "label": f"Q{quarter} {year}",
            "quarter_start": quarter_start.isoformat(),
            "quarter_end": quarter_end.isoformat(),
        },
        "columns": [
            "№",
            "Название",
            "РП",
            "Сроки",
            "Отклонение (суммарное)",
            "Статус",
            "Прогресс",
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
                "TD-T-Q1-DEVIATIONS": _build_deviation_table_q1_quarter(),
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
