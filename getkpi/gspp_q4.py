"""
ГСП-Q4 — вехи проектов TurboProject, где РП совпадает с актуальным «Руководителем отдела» ГСПП.

**Когорта месяца (план):** все «живые» вехи опорного месяца — плановая дата (baseline и др.)
попадает в месяц **или** дата окончания в графике (``finish_date``) попадает в месяц.

**Отклонение:** веха из когорты, у которой к дате расчёта
(конец месяца для закрытых периодов, сегодня для текущего месяца):
  - не выполнена (``percent_complete`` < 100%%), а срок не позже даты расчёта
    (берётся baseline, иначе ``finish_date`` как текущий срок); или
  - выполнена, но фактическое окончание позже baseline (если baseline нет —
    завершённая веха не считается отклонившейся, см. ``debug.milestones_without_baseline``).

**Факт:** план минус число отклонившихся (остальные — без отклонения за месяц).
План/факт по плитке — **сумма по всем** подходящим проектам.

**Отбор проектов:** ``has_1c``, РП = актуальный «Руководитель отдела» ГСПП.
Для каждого **опорного месяца** отдельно: «Планирование» не учитывается; «В работе» —
если проект пересекается с месяцем; «Завершен»/«Закрыт» — месяцы до даты фактического
окончания включительно (исторически были «в работе»).

Веха — задача с флагом ``is_milestone`` и нулевой длительностью по **календарным** датам
(``start_date``/``finish_date`` в т.ч. ``дд.мм.гггг`` из Turbo), плюс обход вложенных
``children``/``subTasks`` в ответе API.

Файловый кэш: ``getkpi/dashboard/gspp_q4_ytd_<год>_<месяц>.json`` (``ytd_json_cache``) —
прошлый опорный месяц без срока годности, текущий календарный месяц — по ``cache_date``.
При ``debug.status == "error"`` на диск не пишем.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

from .cache_manager import locked_call, stale_while_revalidate
from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from .list_enterprise_positions import employees_by_position
from .techdir_projects import (
    _api_get,
    _bool_is_true,
    _is_real_date,
    _is_zero_duration_milestone,
    _login,
    _milestone_progress_as_fraction,
    _month_start_end,
    _normalize_person_label,
    _parse_iso_date,
    _parse_real_project_date,
    _project_progress_pct,
    _project_status_label,
    _project_timeline_label,
)

logger = logging.getLogger(__name__)

TARGET_MANAGER_DEPARTMENT = (
    "Обособленное подразделение / Производство №1 / "
    "Группа сопровождения продаж и производства"
)
TARGET_MANAGER_POSITION = "Руководитель отдела"
TARGET_PROJECT_STATUSES = frozenset({"ВРаботе", "В работе"})
STATUS_PLANNING = "планирование"
STATUS_COMPLETED = frozenset({"завершен", "закрыт"})
PROJECT_NAME_SUBSTR = "номенклатур"

GSPP_Q4_CACHE_PREFIX = "gspp_q4_ytd"
GSPP_Q4_DISK_TAG = "gspp_q4_ytd_payload_v12"
GSPP_Q4_DISK_VERSION = 12

GSPP_Q4_DEVIATION_CACHE_PREFIX = "gspp_q4_deviation_tables"
GSPP_Q4_DEVIATION_DISK_TAG = "gspp_q4_deviation_tables_v7"
GSPP_Q4_DEVIATION_DISK_VERSION = 7

_MANAGER_PROJECTS_TTL = 3600
_MANAGER_PROJECTS_DISK_TAG = "gspp_manager_projects_v1"
_MANAGER_PROJECTS_DISK_VERSION = 1
_MANAGER_PROJECTS_DISK_PATH = ytd_json_cache.CACHE_DIR / "gspp_manager_projects_snapshot.json"
_manager_projects_cache: tuple[
    float,
    list[tuple[dict[str, Any], dict[str, Any]]],
    str | None,
] | None = None
_manager_projects_lock = threading.Lock()


def _project_display_name(details: dict[str, Any], summary_item: dict[str, Any]) -> str:
    meta = details.get("project") or {}
    return str(meta.get("name") or summary_item.get("original_name") or "").strip()


def _name_matches_nomenclature(name: str) -> bool:
    return PROJECT_NAME_SUBSTR in name.lower()


@lru_cache(maxsize=1)
def _target_manager_names() -> tuple[str, ...]:
    return tuple(employees_by_position(
        TARGET_MANAGER_POSITION,
        department_path=TARGET_MANAGER_DEPARTMENT,
    ))


def _target_manager_labels() -> set[str]:
    return {
        normalized
        for name in _target_manager_names()
        if (normalized := _normalize_person_label(name))
    }


def _manager_matches(data_1c: dict[str, Any]) -> bool:
    lead = _normalize_person_label(data_1c.get("rukovoditel"))
    return bool(lead) and lead in _target_manager_labels()


def _normalize_project_status(value: Any) -> str:
    return " ".join(str(value or "").replace("ё", "е").strip().lower().split())


def _allowed_project_statuses_normalized() -> frozenset[str]:
    return frozenset(_normalize_project_status(status) for status in TARGET_PROJECT_STATUSES)


def _project_status_in_work(data_1c: dict[str, Any]) -> bool:
    status = _normalize_project_status(data_1c.get("status_proekta"))
    return status in _allowed_project_statuses_normalized()


def _project_date_bounds(details: dict[str, Any]) -> tuple[date | None, date | None]:
    data_1c = details.get("data_1c") or {}
    meta = details.get("project") or {}
    start = (
        _parse_real_project_date(data_1c.get("data_nachala"))
        or _parse_real_project_date(data_1c.get("planovaya_data_nachala"))
        or _parse_real_project_date(meta.get("start_date"))
        or _parse_real_project_date(meta.get("baseline_start"))
    )
    finish = (
        _parse_real_project_date(data_1c.get("data_okonchaniya"))
        or _parse_real_project_date(data_1c.get("planovaya_data_okonchaniya"))
        or _parse_real_project_date(meta.get("finish_date"))
        or _parse_real_project_date(meta.get("baseline_finish"))
    )
    return start, finish


def _project_alive_in_month(details: dict[str, Any], year: int, month: int) -> bool:
    period_start, period_end = _month_start_end(year, month)
    start, finish = _project_date_bounds(details)
    if start is not None and start > period_end:
        return False
    if finish is not None and finish < period_start:
        return False
    return True


def _project_completion_date(details: dict[str, Any]) -> date | None:
    data_1c = details.get("data_1c") or {}
    meta = details.get("project") or {}
    for key in ("data_okonchaniya", "planovaya_data_okonchaniya"):
        dt = _parse_real_project_date(data_1c.get(key))
        if dt is not None:
            return dt
    for key in ("finish_date", "baseline_finish"):
        dt = _parse_real_project_date(meta.get(key))
        if dt is not None:
            return dt
    return None


def _project_in_work_in_month(details: dict[str, Any], ref_y: int, ref_m: int) -> bool:
    """Проект учитывается в опорном месяце по статусу 1С (текущий статус + дата закрытия)."""
    data_1c = details.get("data_1c") or {}
    status = _normalize_project_status(data_1c.get("status_proekta"))
    period_start, _period_end = _month_start_end(ref_y, ref_m)

    if status == STATUS_PLANNING:
        return False
    if status in _allowed_project_statuses_normalized():
        return _project_alive_in_month(details, ref_y, ref_m)
    if status in STATUS_COMPLETED:
        completion = _project_completion_date(details)
        if completion is not None and period_start > completion:
            return False
        return True
    return False


def _pairs_in_work_for_month(
    project_pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    ref_y: int,
    ref_m: int,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [
        pair for pair in project_pairs
        if _project_in_work_in_month(pair[1], ref_y, ref_m)
    ]


def _task_baseline_finish(task: dict[str, Any]) -> Any:
    for key in (
        "baseline_finish",
        "BaselineFinish",
        "baselineFinish",
        "plan_finish",
        "planned_finish",
        "baseline_end",
        "constraint_date",
    ):
        if task.get(key):
            return task.get(key)
    return None


def _calendar_date_from_field(value: Any) -> date | None:
    """Дата задачи для календарного месяца (API может отличаться от чистого ISO-8601)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        dt = _parse_iso_date(raw)
        if _is_real_date(dt):
            return dt.date()
        if len(raw) >= 10:
            head = raw[:10]
            try:
                return datetime.fromisoformat(head).date()
            except ValueError:
                pass
            if raw[2:3] == "." and raw[5:6] == ".":
                try:
                    return datetime.strptime(head, "%d.%m.%Y").date()
                except ValueError:
                    pass
    return None


def _flatten_tasks_tree(nodes: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Turbo может отдавать иерархию (children / subTasks); плиточный список — плоский."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def walk(items: list[Any]) -> None:
        for t in items or []:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("id") or t.get("task_id") or t.get("taskId") or "").strip()
            if tid:
                if tid in seen:
                    continue
                seen.add(tid)
            out.append(t)
            for key in ("children", "childTasks", "subTasks", "tasks"):
                ch = t.get(key)
                if isinstance(ch, list):
                    walk(ch)

    walk(list(nodes or []))
    return out


def _gspp_q4_counts_as_milestone(task: dict[str, Any]) -> bool:
    """Веха: ``is_milestone`` (если поле есть) и нулевая длительность по календарным датам.

    Флаг ``is_milestone`` в API TurboProject часто стоит и у обычных многодневных задач;
    в UI вехой считается только zero-duration — поэтому одной галочки недостаточно.
    """
    if task.get("is_summary"):
        return False
    if "is_milestone" in task and not _bool_is_true(task.get("is_milestone")):
        return False
    sd = _calendar_date_from_field(task.get("start_date"))
    fd = _calendar_date_from_field(task.get("finish_date"))
    if sd is not None and fd is not None and sd == fd:
        return True
    return _is_zero_duration_milestone(task)


def _milestone_completed(task: dict[str, Any]) -> bool:
    frac = _milestone_progress_as_fraction(task.get("percent_complete"))
    if frac is None:
        return False
    return frac >= 1.0 - 1e-9


def _deviation_as_of_date(ref_y: int, ref_m: int) -> date:
    """Закрытые месяцы считаем на конец месяца, текущий месяц — только на сегодня."""
    _, month_end = _month_start_end(ref_y, ref_m)
    return min(month_end, date.today())


def _milestone_deviated(
    task: dict[str, Any],
    ref_y: int,
    ref_m: int,
    *,
    without_baseline: list[str],
) -> bool:
    """Веха когорты месяца считается отклонившейся — см. модульный docstring."""
    as_of_date = _deviation_as_of_date(ref_y, ref_m)
    completed = _milestone_completed(task)
    act_d = _calendar_date_from_field(task.get("finish_date"))
    base_raw = _task_baseline_finish(task)
    base_d = _calendar_date_from_field(base_raw) if base_raw else None

    if completed:
        if act_d is None:
            return True
        if base_d is None:
            name = str(task.get("name") or "").strip()
            if name:
                without_baseline.append(name)
            return False
        return act_d > base_d

    due_d = base_d if base_d is not None else act_d
    if due_d is None:
        return False
    return due_d <= as_of_date


def _gspp_delay_days_for_deviated(
    task: dict[str, Any], ref_y: int, ref_m: int, as_of_date: date,
) -> int:
    """Дни отклонения для строки таблицы: просрочка к ``as_of_date`` или срыв относительно baseline."""
    completed = _milestone_completed(task)
    act_d = _calendar_date_from_field(task.get("finish_date"))
    base_raw = _task_baseline_finish(task)
    base_d = _calendar_date_from_field(base_raw) if base_raw else None

    if completed:
        if base_d and act_d:
            return max(0, (act_d - base_d).days)
        return 0

    due_d = base_d if base_d is not None else act_d
    if due_d is None:
        return 0
    if due_d > as_of_date:
        return 0
    return max(0, (as_of_date - due_d).days)


def _collect_gspp_q4_deviated_milestones(
    tasks: list[dict[str, Any]], ref_y: int, ref_m: int,
) -> list[dict[str, Any]]:
    """Все вехи когорты месяца с отклонением (та же логика, что у плитки ГСП-Q4)."""
    out: list[dict[str, Any]] = []
    for task in tasks:
        if task.get("is_summary"):
            continue
        if "is_milestone" in task and not _bool_is_true(task.get("is_milestone")):
            continue
        if not _gspp_q4_counts_as_milestone(task):
            continue
        if not _milestone_in_reference_month(task, ref_y, ref_m):
            continue
        if not _milestone_deviated(task, ref_y, ref_m, without_baseline=[]):
            continue
        out.append(task)
    return out


def _display_dates_for_month(
    task: dict[str, Any], ref_y: int, ref_m: int,
) -> tuple[Any, Any, Any]:
    """Даты для колонок таблицы за опорный месяц.

    Если веха попала в месяц по baseline, а график уже уехал (часто в текущий
    август), в «Окончание» показываем плановую дату месяца — иначе во всех
    прошлых месяцах светятся одни и те же августовские даты графика.
    """
    base_raw = _task_baseline_finish(task)
    base_d = _calendar_date_from_field(base_raw)
    start_raw = task.get("start_date")
    finish_raw = task.get("finish_date")
    if base_d is not None and base_d.year == ref_y and base_d.month == ref_m:
        # zero-duration: начало = окончание = плановая дата месяца
        return base_raw, base_raw, base_raw
    return start_raw, finish_raw, base_raw


def _gspp_milestone_deviation_details(
    deviated_tasks: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
    as_of_date: date,
) -> list[dict[str, Any]]:
    """Структура элементов как у ``_build_milestone_deviation_details`` в ``techdir_projects``.

    В список попадают только zero-duration вехи (см. ``_gspp_q4_counts_as_milestone``).
    """
    details: list[dict[str, Any]] = []
    for index, task in enumerate(deviated_tasks, start=1):
        delay_days = _gspp_delay_days_for_deviated(task, ref_y, ref_m, as_of_date)
        start_d, finish_d, baseline_d = _display_dates_for_month(task, ref_y, ref_m)
        details.append({
            "number": index,
            "id": task.get("id"),
            "uid": task.get("uid"),
            "name": str(task.get("name") or ""),
            "start_date": start_d,
            "finish_date": finish_d,
            "baseline_finish": baseline_d,
            "schedule_finish": task.get("finish_date"),
            "delay_days": delay_days,
            "percent_complete": task.get("percent_complete"),
        })
    return details


def _milestone_in_reference_month(task: dict[str, Any], ref_y: int, ref_m: int) -> bool:
    """Веха в опорном месяце: плановая дата (baseline) в месяце **или** фактическое окончание в месяце.

    Иначе при пустом/нестандартном baseline в JSON все вехи выпадали из месяца → план и факт 0.
    """
    base_d = _calendar_date_from_field(_task_baseline_finish(task))
    if base_d is not None and base_d.year == ref_y and base_d.month == ref_m:
        return True
    finish_d = _calendar_date_from_field(task.get("finish_date"))
    if finish_d is not None and finish_d.year == ref_y and finish_d.month == ref_m:
        return True
    return False


def _count_zero_duration_milestones(tasks: list[dict[str, Any]]) -> int:
    n = 0
    for task in tasks:
        if task.get("is_summary"):
            continue
        if "is_milestone" in task and not _bool_is_true(task.get("is_milestone")):
            continue
        if not _gspp_q4_counts_as_milestone(task):
            continue
        n += 1
    return n


def _load_manager_project_pairs(
    session: requests.Session,
    token: str,
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], list[str]]:
    """Проекты TurboProject (``has_1c``): РП из оргструктуры ГСПП (статус — помесячно при расчёте)."""
    summary = _api_get(session, "/api/projects/files", token)
    items = summary.get("items") or []
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    errors: list[str] = []
    for item in items:
        if not item.get("has_1c"):
            continue
        fid = item.get("id")
        if not fid:
            continue
        try:
            details = _api_get(session, f"/api/projects/files/{fid}", token)
        except Exception as exc:
            errors.append(f"id={fid}: {exc}")
            continue
        data_1c = details.get("data_1c") or {}
        if not _manager_matches(data_1c):
            continue
        pairs.append((item, details))
    return pairs, errors


def _serialize_manager_project_pairs(
    pairs: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[list[dict[str, Any]]]:
    return [[item, details] for item, details in pairs]


def _deserialize_manager_project_pairs(
    raw_pairs: object,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    if not isinstance(raw_pairs, list):
        return []
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for entry in raw_pairs:
        if isinstance(entry, list) and len(entry) == 2 and all(isinstance(x, dict) for x in entry):
            out.append((entry[0], entry[1]))
    return out


def _unpack_manager_projects_payload(payload: dict[str, Any]) -> tuple[
    list[tuple[dict[str, Any], dict[str, Any]]], str | None,
]:
    pairs = _deserialize_manager_project_pairs(payload.get("pairs"))
    err = payload.get("error")
    return pairs, err if isinstance(err, str) or err is None else str(err)


def _store_manager_projects_cache(
    pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    err: str | None,
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None]:
    global _manager_projects_cache
    ytd_json_cache.save_payload(
        _MANAGER_PROJECTS_DISK_PATH,
        {"pairs": _serialize_manager_project_pairs(pairs), "error": err},
        source_tag=_MANAGER_PROJECTS_DISK_TAG,
        version=_MANAGER_PROJECTS_DISK_VERSION,
    )
    with _manager_projects_lock:
        _manager_projects_cache = (time.monotonic() + _MANAGER_PROJECTS_TTL, pairs, err)
    return pairs, err


def _fetch_manager_project_pairs_from_api() -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None]:
    session = requests.Session()
    token = _login(session)
    return _find_manager_projects(session, token)


def get_manager_project_pairs() -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None]:
    """Список проектов TurboProject для Q4/M5/таблицы — RAM TTL + файловый stale-while-revalidate."""
    now = time.monotonic()
    with _manager_projects_lock:
        entry = _manager_projects_cache
        if entry is not None and now < entry[0]:
            return entry[1], entry[2]

    perpetual = False

    def _load_fresh() -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None] | None:
        payload = ytd_json_cache.load_payload(
            _MANAGER_PROJECTS_DISK_PATH,
            source_tag=_MANAGER_PROJECTS_DISK_TAG,
            version=_MANAGER_PROJECTS_DISK_VERSION,
            perpetual=perpetual,
        )
        if payload is None:
            return None
        return _unpack_manager_projects_payload(payload)

    def _load_stale() -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None] | None:
        payload = ytd_json_cache.load_stale_payload(
            _MANAGER_PROJECTS_DISK_PATH,
            source_tag=_MANAGER_PROJECTS_DISK_TAG,
            version=_MANAGER_PROJECTS_DISK_VERSION,
        )
        if payload is None:
            return None
        return _unpack_manager_projects_payload(payload)

    def _compute_and_save() -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None]:
        with _manager_projects_lock:
            entry = _manager_projects_cache
            if entry is not None and time.monotonic() < entry[0]:
                return entry[1], entry[2]
        pairs, err = _fetch_manager_project_pairs_from_api()
        return _store_manager_projects_cache(pairs, err)

    return stale_while_revalidate(
        "gspp_turbo_manager_projects",
        _load_fresh,
        _load_stale,
        _compute_and_save,
    )


def _find_manager_projects(
    session: requests.Session,
    token: str,
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], str | None]:
    pairs, errors = _load_manager_project_pairs(session, token)
    if pairs:
        return pairs, None
    if errors:
        return [], "; ".join(errors[:3])
    return [], "нет проектов has_1c с подходящим руководителем"


def _find_target_project(
    session: requests.Session,
    token: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Первый проект «номенклатур*» с подходящим РП — для ГСП-M5 (логика M5 не менялась)."""
    pairs, err = _find_manager_projects(session, token)
    for item, details in pairs:
        pname = _project_display_name(details, item)
        if _name_matches_nomenclature(pname):
            return item, details, None
    hint = err or "нет проектов has_1c с подходящим руководителем и названием «номенклатур*»"
    return None, None, hint


def _count_milestones_for_projects(
    project_pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    ref_y: int,
    ref_m: int,
) -> tuple[int, int, int, list[str]]:
    """Сумма plan/fact/deviated по всем проектам."""
    without_baseline: list[str] = []
    plan_n = 0
    deviated_n = 0
    for _item, details in project_pairs:
        if not _project_in_work_in_month(details, ref_y, ref_m):
            continue
        tasks = _flatten_tasks_tree(details.get("tasks") or [])
        pn, _fn, dn, wob_m = _count_milestones_for_month(tasks, ref_y, ref_m)
        plan_n += pn
        deviated_n += dn
        without_baseline.extend(wob_m)
    fact_n = plan_n - deviated_n
    return plan_n, fact_n, deviated_n, without_baseline


def _count_milestones_for_month(
    tasks: list[dict[str, Any]], ref_y: int, ref_m: int,
) -> tuple[int, int, int, list[str]]:
    """(план, факт, отклонившиеся, имена завершённых вех без baseline для debug).

    Факт = план − отклонившиеся.
    """
    without_baseline: list[str] = []
    plan_n = 0
    deviated_n = 0
    for task in tasks:
        if task.get("is_summary"):
            continue
        if "is_milestone" in task and not _bool_is_true(task.get("is_milestone")):
            continue
        if not _gspp_q4_counts_as_milestone(task):
            continue
        if not _milestone_in_reference_month(task, ref_y, ref_m):
            continue
        plan_n += 1
        if _milestone_deviated(task, ref_y, ref_m, without_baseline=without_baseline):
            deviated_n += 1
    fact_n = plan_n - deviated_n
    return plan_n, fact_n, deviated_n, without_baseline


def _gspp_q4_stub_monthly_rows(ref_y: int, ref_m: int) -> list[dict[str, Any]]:
    """Пустые точки янв. — опорный месяц (как у ``_generate_monthly_data`` для текущего года)."""
    rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": 0.0,
                "fact": 0.0,
                "kpi_pct": None,
                "has_data": False,
                "values_unit": "шт.",
            },
        )
    return rows


def _build_gspp_q4_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    plan_val: float = 0.0
    fact_val: float = 0.0
    monthly_rows: list[dict[str, Any]] = _gspp_q4_stub_monthly_rows(ref_y, ref_m)
    dbg: dict[str, Any] = {
        "kpi_id": "ГСП-Q4",
        "source": "getkpi/gspp_q4.py (TurboProject)",
        "target_manager_department": TARGET_MANAGER_DEPARTMENT,
        "target_manager_position": TARGET_MANAGER_POSITION,
        "target_managers": list(_target_manager_names()),
        "target_project_statuses": sorted(TARGET_PROJECT_STATUSES),
        "project_filter": (
            "has_1c + rukovoditel matches org structure; "
            "per month: in work, or completed through completion date; planning excluded"
        ),
        "status": "no_project",
    }
    try:
        project_pairs, err = get_manager_project_pairs()
        if not project_pairs:
            dbg["status"] = "no_project"
            dbg["hint"] = err
        else:
            monthly_rows = []
            plan_n = fact_n = deviated_n = 0
            wob: list[str] = []
            milestones_total = 0
            for m in range(1, ref_m + 1):
                pn, fn, dn, wob_m = _count_milestones_for_projects(project_pairs, ref_y, m)
                if m == ref_m:
                    plan_n, fact_n, deviated_n, wob = pn, fn, dn, wob_m
                pct_m = round(fn / pn * 100, 1) if pn > 0 else None
                row_m = {
                    "month": m,
                    "year": ref_y,
                    "month_name": MONTH_NAMES[m],
                    "plan": float(pn),
                    "fact": float(fn),
                    "kpi_pct": pct_m,
                    "has_data": bool(pn or fn or dn),
                    "values_unit": "шт.",
                }
                monthly_rows.append(row_m)
            for _item, details in project_pairs:
                tasks = _flatten_tasks_tree(details.get("tasks") or [])
                milestones_total += _count_zero_duration_milestones(tasks)
            ref_pairs = _pairs_in_work_for_month(project_pairs, ref_y, ref_m)
            dbg = {
                **dbg,
                "status": "ok" if any(row.get("has_data") for row in monthly_rows) else "no_project",
                "projects_count": len(project_pairs),
                "projects_in_ref_month": len(ref_pairs),
                "projects": [
                    {
                        "file_id": item.get("id"),
                        "project_name": _project_display_name(details, item),
                        "project_code": (details.get("data_1c") or {}).get("nomer_proekta"),
                        "status_proekta": (details.get("data_1c") or {}).get("status_proekta"),
                        "data_okonchaniya": (details.get("data_1c") or {}).get("data_okonchaniya"),
                        "in_ref_month": _project_in_work_in_month(details, ref_y, ref_m),
                    }
                    for item, details in project_pairs
                ],
                "milestones_zero_duration_in_projects": milestones_total,
                "milestones_in_month_plan": plan_n,
                "milestones_in_month_deviated": deviated_n,
                "milestones_in_month_without_deviation": fact_n,
                "milestones_without_baseline": wob[:20],
                "milestones_without_baseline_truncated": len(wob) > 20,
            }
    except Exception as exc:
        logger.exception("ГСП-Q4: сбой TurboProject")
        dbg["status"] = "error"
        dbg["error"] = str(exc)

    last_row = next(
        (r for r in reversed(monthly_rows) if r.get("month") == ref_m),
        monthly_rows[-1] if monthly_rows else {},
    )
    plan_val = float(last_row.get("plan") or 0.0)
    fact_val = float(last_row.get("fact") or 0.0)
    has_data = plan_val > 0 or fact_val > 0 or dbg.get("status") == "ok"
    kpi_pct = round(fact_val / plan_val * 100, 1) if plan_val > 0 else None
    last_row = {
        **last_row,
        "plan": plan_val,
        "fact": fact_val,
        "kpi_pct": kpi_pct,
        "has_data": has_data,
    }
    if monthly_rows:
        monthly_rows[-1] = dict(last_row)

    with_data = [r for r in monthly_rows if r.get("has_data")]
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(last_row),
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": plan_val,
            "total_fact": fact_val,
            "kpi_pct": kpi_pct,
            "months_with_data": len(with_data),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": dbg,
    }


def gspp_q4_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ry, rm = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_Q4_CACHE_PREFIX, ry, rm)


def gspp_q4_deviation_tables_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ry, rm = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_Q4_DEVIATION_CACHE_PREFIX, ry, rm)


def get_gspp_q4_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(GSPP_Q4_CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_gspp_q4_payload(year=year, month=month)
        except Exception:
            logger.exception("ГСП-Q4: ошибка сборки payload")
            return None
        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                c_path,
                payload,
                source_tag=GSPP_Q4_DISK_TAG,
                version=GSPP_Q4_DISK_VERSION,
            )
        return payload

    return ytd_json_cache.resolve_payload(
        c_path,
        source_tag=GSPP_Q4_DISK_TAG,
        version=GSPP_Q4_DISK_VERSION,
        perpetual=perpetual,
        lock_key=f"gspp_q4_turbo_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def _gspp_project_row_for_deviation(
    item: dict[str, Any],
    details: dict[str, Any],
    tasks_flat: list[dict[str, Any]],
) -> dict[str, Any]:
    """Метаданные проекта для строки таблицы (вехи отклонений считаются отдельно, как у плитки)."""
    data_1c = details.get("data_1c") or {}
    meta = details.get("project") or {}
    return {
        "project_code": data_1c.get("nomer_proekta"),
        "project_name": meta.get("name") or item.get("original_name"),
        "project_manager": data_1c.get("rukovoditel"),
        "status_proekta": data_1c.get("status_proekta"),
        "tip_proekta": data_1c.get("tip_proekta"),
        "podrazdelenie": data_1c.get("podrazdelenie"),
        "start_date": meta.get("start_date"),
        "finish_date": meta.get("finish_date"),
        "baseline_start": meta.get("baseline_start"),
        "baseline_finish": meta.get("baseline_finish"),
        "data_nachala": data_1c.get("data_nachala"),
        "planovaya_data_nachala": data_1c.get("planovaya_data_nachala"),
        "planovaya_data_okonchaniya": data_1c.get("planovaya_data_okonchaniya"),
        "data_okonchaniya": data_1c.get("data_okonchaniya"),
        "overdue_milestones": [],
        "project_progress_pct": _project_progress_pct(meta, tasks_flat),
    }


def _empty_gspp_deviation_table(ref_y: int, ref_m: int, *, hint: str | None = None) -> dict[str, Any]:
    desc = (
        "Проекты TurboProject с РП из оргструктуры ГСПП — вехи с отклонением "
        f"за {MONTH_NAMES[ref_m]} {ref_y} (структура как у TD-T-*-DEVIATIONS)."
    )
    if hint:
        desc = f"{desc} {hint}"
    return {
        "name": "Отклонения по вехам: ГСП-Q4",
        "periodicity": "ежемесячно",
        "description": desc,
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
        "rows": [],
    }


def _deviation_row_for_project(
    ref_y: int,
    ref_m: int,
    item: dict[str, Any],
    details: dict[str, Any],
) -> dict[str, Any] | None:
    tasks_flat = _flatten_tasks_tree(details.get("tasks") or [])
    project_row = _gspp_project_row_for_deviation(item, details, tasks_flat)
    as_of_date = _deviation_as_of_date(ref_y, ref_m)
    deviated_tasks = _collect_gspp_q4_deviated_milestones(tasks_flat, ref_y, ref_m)
    if not deviated_tasks:
        return None
    deviated_tasks.sort(
        key=lambda t: (
            -_gspp_delay_days_for_deviated(t, ref_y, ref_m, as_of_date),
            str(t.get("name") or "").lower(),
        ),
    )
    milestone_details = _gspp_milestone_deviation_details(
        deviated_tasks, ref_y, ref_m, as_of_date,
    )
    max_delay_days = max(
        (int(m.get("delay_days") or 0) for m in milestone_details),
        default=0,
    )
    return {
        "project_code": str(project_row.get("project_code") or ""),
        "project_name": str(project_row.get("project_name") or ""),
        "project_manager": str(project_row.get("project_manager") or ""),
        "timeline": _project_timeline_label(project_row),
        "deviation": f"{len(milestone_details)} вех., {max_delay_days} дн.",
        "delay_days": max_delay_days,
        "status": _project_status_label(project_row),
        "progress_pct": project_row.get("project_progress_pct"),
        "overdue_milestones_count": len(milestone_details),
        "milestone_deviations": milestone_details,
    }


def _build_gspp_q4_deviation_table_payload(
    ref_y: int,
    ref_m: int,
    project_pairs: list[tuple[dict[str, Any], dict[str, Any]]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for item, details in project_pairs:
        row = _deviation_row_for_project(ref_y, ref_m, item, details)
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda r: (-int(r.get("delay_days") or 0), str(r.get("project_name") or "").lower()))
    for index, row in enumerate(rows, start=1):
        row["number"] = index
    return {
        "name": "Отклонения по вехам: ГСП-Q4",
        "periodicity": "ежемесячно",
        "description": (
            "Проекты TurboProject (has_1c, РП ГСПП; «в работе» или завершённые до даты закрытия) — "
            "только вехи с нулевой календарной длительностью и отклонением "
            f"за {MONTH_NAMES[ref_m]} {ref_y} (та же логика, что у плитки ГСП-Q4; "
            "флага is_milestone без zero-duration недостаточно). "
            "Структура вложенности — как у технического директора "
            "(TD-T-M1-DEVIATIONS / TD-T-Q1-DEVIATIONS)."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": ["№ 1С", "Название", "РП", "Сроки", "Отклонение", "Статус", "Прогресс"],
        "rows": rows,
    }


def _compute_gspp_q4_deviation_tables(ref_y: int, ref_m: int) -> dict[str, Any]:
    fallback = {
        "GSPP-T-Q4-DEVIATIONS": _empty_gspp_deviation_table(
            ref_y, ref_m, hint="(внутренняя ошибка расчёта таблицы)",
        ),
    }
    try:
        project_pairs, err = get_manager_project_pairs()
        if not project_pairs:
            return {
                "GSPP-T-Q4-DEVIATIONS": _empty_gspp_deviation_table(
                    ref_y, ref_m, hint=f"({err or 'проекты не найдены'})",
                ),
            }
        tbl = _build_gspp_q4_deviation_table_payload(
            ref_y, ref_m, _pairs_in_work_for_month(project_pairs, ref_y, ref_m),
        )
        return {"GSPP-T-Q4-DEVIATIONS": tbl}
    except Exception:
        logger.exception("ГСП-Q4: ошибка таблицы отклонений по вехам")
        return dict(fallback)


def get_gspp_q4_deviation_tables(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Таблица всех отклонившихся вех для дашборда ГСПП (ключ ``GSPP-T-Q4-DEVIATIONS``).

    Всегда возвращает словарь с этим ключом (при ошибке — пустые ``rows`` и пояснение в ``description``),
    чтобы блок попадал в JSON ``Таблицы`` универсального билдера.

    Файловый кэш: ``getkpi/dashboard/gspp_q4_deviation_tables_<год>_<месяц>.json``.
    """
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    fallback = {
        "GSPP-T-Q4-DEVIATIONS": _empty_gspp_deviation_table(
            ref_y, ref_m, hint="(внутренняя ошибка расчёта таблицы)",
        ),
    }
    cache_path = gspp_q4_deviation_tables_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any]:
        result = _compute_gspp_q4_deviation_tables(ref_y, ref_m)
        if isinstance(result, dict):
            ytd_json_cache.save_payload(
                cache_path,
                result,
                source_tag=GSPP_Q4_DEVIATION_DISK_TAG,
                version=GSPP_Q4_DEVIATION_DISK_VERSION,
            )
        return result

    try:
        out = ytd_json_cache.resolve_payload(
            cache_path,
            source_tag=GSPP_Q4_DEVIATION_DISK_TAG,
            version=GSPP_Q4_DEVIATION_DISK_VERSION,
            perpetual=perpetual,
            lock_key=f"gspp_q4_deviation_tables_{ref_y}_{ref_m:02d}",
            compute_fn=_compute_and_save,
        )
    except Exception:
        logger.exception("ГСП-Q4: сбой resolve таблицы отклонений")
        return dict(fallback)
    return out if isinstance(out, dict) else dict(fallback)


def gspp_q4_kpi_id_matches(kpi_id: str) -> bool:
    """Совпадение кода KPI после ``_normalize_dashboard_kpi_id`` (С→C и т.д.) и исходных строк."""
    raw = (kpi_id or "").strip()
    if not raw:
        return False
    compact = raw.replace(" ", "").upper()
    for d in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u2212"):
        compact = compact.replace(d, "-")
    return compact in {
        "GSP-Q4",
        "GSPP-Q4",
        "ГСП-Q4",
        "ГСПП-Q4",
        "ГCP-Q4",
        "ГCП-Q4",
    }
