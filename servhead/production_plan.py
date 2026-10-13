"""План производства сервисной службы из квартальных файлов MS Project.

SH-M6 — сводные задачи прямо под месяцем: план = все, факт = % завершения 100.
SH-M8 — обычные задачи под этими сводными: план = все,
факт = срок «Окончание» уже прошёл, а процент завершения не 100.
SH-T3 — таблица этих отставаний.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from servhead.claims_common import kpi_pct

logger = logging.getLogger(__name__)

DEFAULT_MPP_ROOT = Path(
    r"\\192.168.1.198\Files\11.НПО\Сервисная служба\СМР ПНР\Планы"
)
WORK_TYPE_COLUMN = "вид работ"

SNAPSHOT_TAG = "servhead_sh_plan_month_v1"
SNAPSHOT_VERSION = 3
TILE_TAG = "servhead_sh_plan_tile_v1"
TILE_VERSION = 2
TABLE_ID = "SH-T3"
TABLE_COLUMNS = [
    "Суммарная задача",
    "Задача",
    "Вид работ",
    "Начало",
    "Окончание",
    "% завершения",
    "Ресурс",
]

_CACHE_DIR = ytd_json_cache.CACHE_DIR


def plan_mpp_root() -> Path:
    raw = (os.environ.get("SH_PLAN_MPP_ROOT") or "").strip()
    return Path(raw) if raw else DEFAULT_MPP_ROOT


def quarter_for_month(month: int) -> int:
    return (int(month) - 1) // 3 + 1


def quarter_months(quarter: int) -> tuple[int, int, int]:
    start = (int(quarter) - 1) * 3 + 1
    return start, start + 1, start + 2


def resolve_mpp_path(year: int, month: int) -> Path:
    """Папка года и файл ``{квартал} кв. {год}.mpp``."""
    root = plan_mpp_root()
    year_dir = _year_dir(root, year)
    if year_dir is None:
        raise FileNotFoundError(f"Нет папки {year} в {root}")
    quarter = quarter_for_month(month)
    exact = year_dir / f"{quarter} кв. {year}.mpp"
    if exact.exists():
        return exact
    hits = [
        path
        for path in year_dir.glob("*.mpp")
        if _mpp_name_matches_quarter(path.name, year, quarter)
    ]
    if len(hits) == 1:
        return hits[0]
    raise FileNotFoundError(
        f"Нет файла {quarter} квартала {year} в {year_dir}"
    )


def sh_m6_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return _CACHE_DIR / f"servhead_sh_m6_plan_{ref_y}_{ref_m:02d}.json"


def sh_m8_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return _CACHE_DIR / f"servhead_sh_m8_stages_{ref_y}_{ref_m:02d}.json"


def sh_t3_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return _CACHE_DIR / f"servhead_sh_t3_open_{ref_y}_{ref_m:02d}.json"


def get_sh_m6_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return _cached_tile(
        kpi_id="SH-M6",
        cache_path_fn=sh_m6_ytd_cache_path,
        build_fn=_build_m6_payload,
        year=year,
        month=month,
    )


def get_sh_m8_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return _cached_tile(
        kpi_id="SH-M8",
        cache_path_fn=sh_m8_ytd_cache_path,
        build_fn=_build_m8_payload,
        year=year,
        month=month,
    )


def get_sh_t3_table(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    path = sh_t3_cache_path(ref_y, ref_m)

    def build() -> dict[str, Any]:
        snapshots = ensure_snapshots(ref_y, [ref_m])
        table = _table_from_snapshot(snapshots[ref_m])
        _save_wrapper(path, table, _mtimes_of([snapshots[ref_m]]))
        return table

    return _cached_value(f"servhead_sh_t3_{ref_y}_{ref_m:02d}", path, ref_y, ref_m, build)


def ensure_snapshots(year: int, months: list[int]) -> dict[int, dict[str, Any]]:
    """Снимки месяцев. Файл квартала читается один раз на все недостающие месяцы."""
    result: dict[int, dict[str, Any]] = {}
    pending: list[int] = []
    for month in months:
        cached = _load_snapshot(year, month)
        if cached is not None:
            result[month] = cached
        else:
            pending.append(month)
    if not pending:
        return result

    by_quarter: dict[int, list[int]] = {}
    for month in pending:
        by_quarter.setdefault(quarter_for_month(month), []).append(month)

    for quarter, quarter_pending in by_quarter.items():
        filled = _load_quarter(year, quarter, quarter_pending)
        result.update(filled)
    return result


def _load_quarter(
    year: int,
    quarter: int,
    needed: list[int],
) -> dict[int, dict[str, Any]]:
    from getkpi.cache_manager import locked_call

    def _compute() -> dict[int, dict[str, Any]]:
        ready: dict[int, dict[str, Any]] = {}
        still: list[int] = []
        for month in needed:
            cached = _load_snapshot(year, month)
            if cached is not None:
                ready[month] = cached
            else:
                still.append(month)
        if not still:
            return ready
        try:
            path = resolve_mpp_path(year, still[0])
        except FileNotFoundError as exc:
            for month in still:
                snap = _empty_snapshot(year, month, error=str(exc))
                _save_snapshot(snap, mpp_path=None, mpp_mtime=None)
                ready[month] = snap
            return ready
        mtime = _file_mtime(path)
        try:
            from qualdir.mpp_tasks_report import apply_license, read_project

            apply_license()
            with _local_mpp_copy(path) as local_path:
                project = read_project(local_path)
                parsed = {
                    month: _snapshot_from_project(project, year, month, path)
                    for month in quarter_months(quarter)
                }
        except Exception as exc:
            logger.exception("SH plan: не прочитан %s", path)
            for month in still:
                ready[month] = _empty_snapshot(year, month, error=str(exc), mpp_path=path)
            return ready
        for month, snap in parsed.items():
            if _load_snapshot(year, month) is None:
                _save_snapshot(snap, mpp_path=path, mpp_mtime=mtime)
            ready.setdefault(month, _load_snapshot(year, month) or snap)
        return ready

    return locked_call(f"servhead_sh_plan_q_{year}_{quarter}", _compute)


def _snapshot_from_project(project: Any, year: int, month: int, mpp_path: Path) -> dict[str, Any]:
    month_task = _find_month_task(project, month)
    if month_task is None:
        return _empty_snapshot(year, month, mpp_path=mpp_path, found=False)
    summaries = [child for child in month_task.children if bool(getattr(child, "is_summary", False))]
    work_type_field = _field_id_by_title(project, WORK_TYPE_COLUMN)
    as_of = _delay_as_of(year, month)
    unfinished: list[dict[str, Any]] = []
    stage_total = 0
    stage_open = 0
    for summary in summaries:
        for task in _leaf_tasks(summary):
            stage_total += 1
            if not _is_behind_plan(task, as_of):
                continue
            stage_open += 1
            unfinished.append(_task_row(summary, task, work_type_field))
    plan_total = len(summaries)
    plan_done = sum(1 for summary in summaries if _is_done(summary))
    return {
        "year": year,
        "month": month,
        "found": True,
        "plan_total": plan_total,
        "plan_done": plan_done,
        "stage_total": stage_total,
        "stage_open": stage_open,
        "delay_as_of": as_of.isoformat(),
        "unfinished": unfinished,
        "mpp_path": str(mpp_path),
        "error": None,
    }


def _find_month_task(project: Any, month: int) -> Any | None:
    from qualdir.mpp_tasks_report import iter_child_tasks

    target = MONTH_NAMES[month].casefold()
    best = None
    best_score = -1
    for task, _level in iter_child_tasks(project.root_task):
        if not bool(getattr(task, "is_summary", False)):
            continue
        name = (getattr(task, "name", None) or "").strip().casefold()
        if name != target:
            continue
        score = sum(1 for child in task.children if bool(getattr(child, "is_summary", False)))
        if score > best_score:
            best = task
            best_score = score
    return best


def _leaf_tasks(summary: Any) -> list[Any]:
    from qualdir.mpp_tasks_report import iter_child_tasks

    leaves = [
        task
        for task, _level in iter_child_tasks(summary)
        if not bool(getattr(task, "is_summary", False))
    ]
    leaves.sort(key=lambda task: int(getattr(task, "id", 0) or 0))
    return leaves


def _custom_text(task: Any, field_id: int | None) -> str:
    if field_id is None:
        return ""
    try:
        from qualdir.mpp_tasks_report import get_table_field_value

        return get_table_field_value(task, field_id).strip()
    except Exception:
        return ""


def _task_row(summary: Any, task: Any, work_type_field: int | None) -> dict[str, Any]:
    percent = _percent(task)
    work_type = _custom_text(task, work_type_field) or _custom_text(summary, work_type_field)
    return {
        "Суммарная задача": (getattr(summary, "name", None) or "").strip(),
        "Задача": (getattr(task, "name", None) or "").strip(),
        "Вид работ": work_type,
        "Начало": _format_date(getattr(task, "start", None)),
        "Окончание": _format_date(getattr(task, "finish", None)),
        "% завершения": _format_percent(percent),
        "Ресурс": _resource_names(task),
    }


def _build_m6_payload(year: int, month: int) -> dict[str, Any]:
    snapshots = ensure_snapshots(year, list(range(1, month + 1)))
    return _tile_payload(
        kpi_id="SH-M6",
        year=year,
        month=month,
        snapshots=snapshots,
        plan_key="plan_total",
        fact_key="plan_done",
        rule="plan = summary tasks directly under the month; fact = percent complete 100",
    )


def _build_m8_payload(year: int, month: int) -> dict[str, Any]:
    snapshots = ensure_snapshots(year, list(range(1, month + 1)))
    return _tile_payload(
        kpi_id="SH-M8",
        year=year,
        month=month,
        snapshots=snapshots,
        plan_key="stage_total",
        fact_key="stage_open",
        rule=(
            "plan = leaf tasks under month summaries; "
            "fact = planned finish is on or before the as-of date and percent complete is not 100"
        ),
    )


def _tile_payload(
    *,
    kpi_id: str,
    year: int,
    month: int,
    snapshots: dict[int, dict[str, Any]],
    plan_key: str,
    fact_key: str,
    rule: str,
) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    for month_num in range(1, month + 1):
        snap = snapshots.get(month_num) or _empty_snapshot(year, month_num)
        plan = snap.get(plan_key)
        fact = snap.get(fact_key)
        has_data = bool(snap.get("found")) and plan is not None
        row: dict[str, Any] = {
            "month": month_num,
            "year": year,
            "month_name": MONTH_NAMES[month_num],
            "plan": plan if has_data else None,
            "fact": fact if has_data else None,
            "kpi_pct": kpi_pct(int(plan), int(fact)) if has_data else None,
            "has_data": has_data,
        }
        if has_data:
            row["values_unit"] = "шт."
        monthly_rows.append(row)
    ref_row = next((item for item in monthly_rows if item["month"] == month), None)
    today = date.today()
    period_type = (
        "current_month"
        if (year, month) >= (today.year, today.month)
        else "last_full_month"
    )
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "kpi_period": {
            "type": period_type,
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": kpi_id,
            "status": "ok",
            "source": "servhead.production_plan",
            "rule": rule,
            "mpp_paths": sorted({
                str(snap.get("mpp_path"))
                for snap in snapshots.values()
                if snap.get("mpp_path")
            }),
        },
        "_mpp_mtimes": _mtimes_of(list(snapshots.values())),
    }


def _table_from_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    year = int(snap["year"])
    month = int(snap["month"])
    rows = list(snap.get("unfinished") or [])
    month_title = MONTH_NAMES[month].capitalize()
    debug: dict[str, Any] = {
        "kpi_id": TABLE_ID,
        "status": "error" if snap.get("error") else "ok",
        "source": "servhead.production_plan",
        "mpp_path": snap.get("mpp_path"),
        "found_month": bool(snap.get("found")),
    }
    if snap.get("error"):
        debug["error"] = snap["error"]
    return {
        "kpi_id": TABLE_ID,
        "name": f"Незавершённые этапы — {month_title} {year}",
        "periodicity": "ежемесячно",
        "description": (
            "Обычные задачи под сводными задачами месяца, "
            "у которых дата окончания по плану уже прошла, "
            "а процент завершения не 100."
        ),
        "period": {"year": year, "month": month, "month_name": MONTH_NAMES[month]},
        "columns": list(TABLE_COLUMNS),
        "rows": rows,
        "totals": {
            "total": int(snap.get("stage_total") or 0) if snap.get("found") else 0,
            "open": len(rows),
        },
        "debug": debug,
    }


def _cached_tile(
    *,
    kpi_id: str,
    cache_path_fn,
    build_fn,
    year: int | None,
    month: int | None,
) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    path = cache_path_fn(ref_y, ref_m)

    def build() -> dict[str, Any]:
        payload = build_fn(ref_y, ref_m)
        mtimes = payload.pop("_mpp_mtimes", {})
        _save_wrapper(path, payload, mtimes)
        return payload

    return _cached_value(f"servhead_{kpi_id.lower()}_{ref_y}_{ref_m:02d}", path, ref_y, ref_m, build)


def _cached_value(lock_key: str, path: Path, year: int, month: int, build) -> dict[str, Any]:
    from getkpi.cache_manager import stale_while_revalidate

    def load_fresh() -> dict[str, Any] | None:
        wrapper = _read_wrapper(path)
        if wrapper is None:
            return None
        if ytd_json_cache.is_ref_period_fully_past(year, month):
            return wrapper.get("payload")
        if wrapper.get("cache_date") != date.today().isoformat():
            return None
        if not _mtimes_match(wrapper.get("mpp_mtimes") or {}):
            return None
        payload = wrapper.get("payload")
        return payload if isinstance(payload, dict) else None

    def load_stale() -> dict[str, Any] | None:
        wrapper = _read_wrapper(path)
        if wrapper is None:
            return None
        payload = wrapper.get("payload")
        return payload if isinstance(payload, dict) else None

    def compute() -> dict[str, Any]:
        fresh = load_fresh()
        if fresh is not None:
            return fresh
        return build()

    return stale_while_revalidate(lock_key, load_fresh, load_stale, compute)


def _snapshot_path(year: int, month: int) -> Path:
    return _CACHE_DIR / f"servhead_sh_plan_month_{year}_{month:02d}.json"


def _load_snapshot(year: int, month: int) -> dict[str, Any] | None:
    wrapper = _read_wrapper(_snapshot_path(year, month), source=SNAPSHOT_TAG, version=SNAPSHOT_VERSION)
    if wrapper is None:
        return None
    snap = wrapper.get("snapshot")
    if not isinstance(snap, dict):
        return None
    if ytd_json_cache.is_ref_period_fully_past(year, month):
        return snap
    if wrapper.get("cache_date") != date.today().isoformat():
        return None
    stored_path = wrapper.get("mpp_path")
    stored_mtime = wrapper.get("mpp_mtime")
    if stored_path:
        current = _file_mtime(Path(stored_path))
        if current != stored_mtime:
            return None
    elif _try_resolve(year, month) is not None:
        return None
    return snap


def _save_snapshot(snap: dict[str, Any], *, mpp_path: Path | None, mpp_mtime: float | None) -> None:
    path = _snapshot_path(int(snap["year"]), int(snap["month"]))
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    body = {
        "source": SNAPSHOT_TAG,
        "cache_version": SNAPSHOT_VERSION,
        "cache_date": date.today().isoformat(),
        "mpp_path": str(mpp_path) if mpp_path else snap.get("mpp_path"),
        "mpp_mtime": mpp_mtime,
        "snapshot": snap,
    }
    _write_json(path, body)


def _empty_snapshot(
    year: int,
    month: int,
    *,
    error: str | None = None,
    mpp_path: Path | None = None,
    found: bool = False,
) -> dict[str, Any]:
    return {
        "year": year,
        "month": month,
        "found": found,
        "plan_total": None,
        "plan_done": None,
        "stage_total": None,
        "stage_open": None,
        "delay_as_of": None,
        "unfinished": [],
        "mpp_path": str(mpp_path) if mpp_path else None,
        "error": error,
    }


def _save_wrapper(path: Path, payload: dict[str, Any], mtimes: dict[str, float | None]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(
        path,
        {
            "source": TILE_TAG,
            "cache_version": TILE_VERSION,
            "cache_date": date.today().isoformat(),
            "mpp_mtimes": mtimes,
            "payload": payload,
        },
    )


def _read_wrapper(
    path: Path,
    *,
    source: str = TILE_TAG,
    version: int = TILE_VERSION,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    if raw.get("source") != source or raw.get("cache_version") != version:
        return None
    return raw


def _write_json(path: Path, body: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(body, handle, ensure_ascii=False)
    temporary.replace(path)


def _mtimes_of(snapshots: list[dict[str, Any]]) -> dict[str, float | None]:
    mtimes: dict[str, float | None] = {}
    for snap in snapshots:
        raw = snap.get("mpp_path")
        if not raw or raw in mtimes:
            continue
        mtimes[str(raw)] = _file_mtime(Path(str(raw)))
    return mtimes


def _mtimes_match(stored: dict[str, Any]) -> bool:
    if not isinstance(stored, dict):
        return False
    for raw, saved in stored.items():
        if _file_mtime(Path(str(raw))) != saved:
            return False
    return True


def _try_resolve(year: int, month: int) -> Path | None:
    try:
        return resolve_mpp_path(year, month)
    except FileNotFoundError:
        return None


def _file_mtime(path: Path) -> float | None:
    try:
        if path.exists():
            return path.stat().st_mtime
    except OSError:
        return None
    return None


@contextmanager
def _local_mpp_copy(source_path: Path) -> Iterator[Path]:
    """Копия MPP потоковым чтением: CopyFile2 на этой шаре падает с WinError 1450."""
    if not source_path.exists():
        raise FileNotFoundError(f"MPP file was not found: {source_path}")
    with tempfile.TemporaryDirectory(prefix="mpp_sh_plan_") as temp_dir:
        temp_path = Path(temp_dir) / source_path.name
        with source_path.open("rb") as source, temp_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        yield temp_path


def _year_dir(root: Path, year: int) -> Path | None:
    if not root.is_dir():
        return None
    token = str(year)
    exact: list[Path] = []
    partial: list[Path] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if child.name == token:
            exact.append(child)
        elif token in child.name:
            partial.append(child)
    candidates = exact or partial
    if not candidates:
        return None
    candidates.sort(key=lambda item: (len(item.name), item.name.casefold()))
    return candidates[0]


def _mpp_name_matches_quarter(name: str, year: int, quarter: int) -> bool:
    folded = name.casefold().replace("ё", "е")
    if not folded.endswith(".mpp") or str(year) not in folded:
        return False
    return (
        folded.startswith(f"{quarter} кв")
        or folded.startswith(f"{quarter}кв")
        or f"{quarter} кв" in folded
    )


def _field_id_by_title(project: Any, title: str) -> int | None:
    wanted = title.strip().casefold()
    for table in project.tables.to_list():
        for table_field in table.table_fields:
            if (table_field.title or "").strip().casefold() == wanted:
                return int(table_field.field)
    for definition in project.extended_attributes.to_list():
        alias = (getattr(definition, "alias", None) or "").strip().casefold()
        if alias == wanted:
            return int(definition.field_id)
    return None


def _percent(task: Any) -> float | None:
    value = getattr(task, "percent_complete", None)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _delay_as_of(year: int, month: int, today: date | None = None) -> date:
    """День, к которому срок по плану уже должен был наступить.

    Закрытый месяц считается на последний день месяца. Текущий и будущий —
    на вчера, чтобы задача со сроком «сегодня» ещё не была отставанием.
    """
    today = today or date.today()
    last = _month_end(year, month)
    if (year, month) < (today.year, today.month):
        return last
    return min(last, today - timedelta(days=1))


def _task_finish_date(task: Any) -> date | None:
    value = getattr(task, "finish", None)
    if isinstance(value, datetime):
        moment = value.date()
    elif isinstance(value, date):
        moment = value
    else:
        return None
    if moment.year < 2000 or moment.year > 2100:
        return None
    return moment


def _is_done(task: Any) -> bool:
    percent = _percent(task)
    return percent is not None and percent >= 100


def _is_behind_plan(task: Any, as_of: date) -> bool:
    """Отставание: по плану задача уже должна быть закрыта, но процент не 100."""
    if _is_done(task):
        return False
    finish = _task_finish_date(task)
    if finish is None:
        return False
    return finish <= as_of


def _format_percent(percent: float | None) -> int | None:
    if percent is None:
        return None
    return int(percent) if float(percent).is_integer() else round(percent, 1)


def _format_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, date):
        moment = datetime(value.year, value.month, value.day)
    else:
        return ""
    if moment.year < 2000 or moment.year > 2100:
        return ""
    return f"{moment.day:02d}.{moment.month:02d}.{moment.year}"


def _resource_names(task: Any) -> str:
    names: list[str] = []
    try:
        assignments = task.assignments
    except Exception:
        return ""
    for assignment in assignments:
        resource = getattr(assignment, "resource", None)
        name = (getattr(resource, "name", None) or "").strip()
        if name and name not in names:
            names.append(name)
    return ", ".join(names)
