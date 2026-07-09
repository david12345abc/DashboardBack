"""Поиск в MS Project (.mpp) сводной строки месяца и JSON для плитки QD-Q1.

- Помесячный снимок: ``read_qd_q1_plan_fact_for_month``.
- Тело плитки: ``build_qd_q1_tile_json`` (чистый расчёт из MPP).
- Для API: ``get_qd_q1_ytd`` — дисковый кэш в ``getkpi/dashboard/qualdir_qd_q1_tile_<Y>_<MM>.json``
  (тот же календарный день + совпадение mtime файла MPP) и ``locked_call`` при промахе.

Переменные окружения: ``QD_Q1_MPP_PATH``, ``QD_Q1_MPP_YEAR_COLUMN``, ``QD_Q1_MPP_START_ROW``,
``ASPOSE_TASKS_LICENSE``.

CLI::

    python qualdir/mpp_tasks_report.py --year 2026 --month 3
    python qualdir/mpp_tasks_report.py --year 2026 --month март
    python qualdir/mpp_tasks_report.py --tile --year 2026 --month 3

Install: ``python -m pip install -r requirements.txt``
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Iterator

import aspose.tasks as tasks

from getkpi.cache_manager import stale_while_revalidate
from devdir import ytd_json_cache
from getkpi.techdir_tekuchet import MONTH_RU

from qualdir.turnover import _qd_q2_kpi_pct

logger = logging.getLogger(__name__)

# Тег источника в debug плитки и в JSON-кэше (getkpi/dashboard).
SOURCE_TAG = "qualdir_qd_q1_mpp_v1"
QD_Q1_TILE_CACHE_VERSION = 1
_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"


DEFAULT_MPP_PATH = Path(
    r"\\192.168.1.198\Files\Обмен\Управление несоответствиями\!!! Корректирующие мероприятия по подразделениям\ОТК-1\проект устранения несоответствий промышленных приборов ОТК-1!.mpp"
)
DEFAULT_YEAR_COLUMN_TITLE = "Зав. №"
PLAN_COLUMN_TITLE = "Кол-во планируемых задач"
FACT_COLUMN_TITLE = "Кол-во выполненных задач"
DEFAULT_YEAR_START_ROW = 117
YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
MONTH_ALIASES = {
    "1": "январь",
    "01": "январь",
    "январь": "январь",
    "января": "январь",
    "2": "февраль",
    "02": "февраль",
    "февраль": "февраль",
    "февраля": "февраль",
    "3": "март",
    "03": "март",
    "март": "март",
    "марта": "март",
    "4": "апрель",
    "04": "апрель",
    "апрель": "апрель",
    "апреля": "апрель",
    "5": "май",
    "05": "май",
    "май": "май",
    "мая": "май",
    "6": "июнь",
    "06": "июнь",
    "июнь": "июнь",
    "июня": "июнь",
    "7": "июль",
    "07": "июль",
    "июль": "июль",
    "июля": "июль",
    "8": "август",
    "08": "август",
    "август": "август",
    "августа": "август",
    "9": "сентябрь",
    "09": "сентябрь",
    "сентябрь": "сентябрь",
    "сентября": "сентябрь",
    "10": "октябрь",
    "октябрь": "октябрь",
    "октября": "октябрь",
    "11": "ноябрь",
    "ноябрь": "ноябрь",
    "ноября": "ноябрь",
    "12": "декабрь",
    "декабрь": "декабрь",
    "декабря": "декабрь",
}

FIELD_ID_ATTRIBUTE_NAMES = {
    tasks.Field.TASK_ID: "id",
    tasks.Field.TASK_UNIQUE_ID: "uid",
    tasks.Field.TASK_WBS: "wbs",
    tasks.Field.TASK_NAME: "name",
    tasks.Field.TASK_START: "start",
    tasks.Field.TASK_FINISH: "finish",
    tasks.Field.TASK_DURATION: "duration",
    tasks.Field.TASK_PERCENT_COMPLETE: "percent_complete",
    tasks.Field.TASK_MILESTONE: "is_milestone",
}


def apply_license() -> None:
    license_path = os.environ.get("ASPOSE_TASKS_LICENSE")
    if not license_path:
        return

    license_file = Path(license_path)
    if not license_file.exists():
        raise FileNotFoundError(f"License file was not found: {license_file}")

    license = tasks.License()
    license.set_license(str(license_file))


def read_project(path: Path) -> tasks.Project:
    if not path.exists():
        raise FileNotFoundError(f"MPP file was not found: {path}")

    return tasks.Project(str(path))


@contextmanager
def local_project_copy(source_path: Path, enabled: bool = True) -> Iterator[Path]:
    """Копия MPP во временный файл, чтобы не держать сетевой файл открытым."""
    if not enabled:
        yield source_path
        return

    if not source_path.exists():
        raise FileNotFoundError(f"MPP file was not found: {source_path}")

    with tempfile.TemporaryDirectory(prefix="mpp_qd_q1_") as temp_dir:
        temp_path = Path(temp_dir) / source_path.name
        shutil.copy2(source_path, temp_path)
        yield temp_path


def iter_child_tasks(task: tasks.Task, level: int = 0) -> Iterable[tuple[tasks.Task, int]]:
    for child in task.children:
        yield child, level
        yield from iter_child_tasks(child, level + 1)


def normalize_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def find_project_field_id(project: tasks.Project, column_title: str) -> int:
    normalized_column_title = column_title.strip()

    for table in project.tables.to_list():
        for table_field in table.table_fields:
            if table_field.title == normalized_column_title:
                return int(table_field.field)

    for definition in project.extended_attributes.to_list():
        if definition.alias == normalized_column_title:
            return int(definition.field_id)

    raise ValueError(f"Column was not found in project tables or custom fields: {column_title}")


def get_table_field_value(task: tasks.Task, field_id: int) -> str:
    attribute_name = FIELD_ID_ATTRIBUTE_NAMES.get(field_id)
    if attribute_name is not None:
        return normalize_value(getattr(task, attribute_name))

    for attribute in task.extended_attributes:
        if int(attribute.field_id) == field_id:
            return get_extended_attribute_value(attribute)

    return ""


def get_extended_attribute_value(attribute: tasks.ExtendedAttribute) -> str:
    for value_attribute_name in (
        "text_value",
        "numeric_value",
        "date_value",
        "duration_value",
        "flag_value",
    ):
        try:
            value = getattr(attribute, value_attribute_name)
        except Exception:
            continue

        if value not in (None, ""):
            return normalize_value(value)

    return ""


def normalize_number_value(value: str) -> str:
    stripped_value = value.strip()
    if not stripped_value:
        return ""

    try:
        number = float(stripped_value.replace(",", "."))
    except ValueError:
        return stripped_value

    if number.is_integer():
        return str(int(number))

    return str(number)


def normalize_month(month: str) -> str:
    normalized = month.strip().lower().rstrip(".")
    if normalized not in MONTH_ALIASES:
        supported_months = ", ".join(
            [
                "январь",
                "февраль",
                "март",
                "апрель",
                "май",
                "июнь",
                "июль",
                "август",
                "сентябрь",
                "октябрь",
                "ноябрь",
                "декабрь",
            ]
        )
        raise ValueError(f"Unknown month: {month}. Supported values: 1-12 or {supported_months}")

    return MONTH_ALIASES[normalized]


def is_year_value(value: str, year: int) -> bool:
    return str(year) in YEAR_PATTERN.findall(value)


def build_summary_task_row(
    task: tasks.Task,
    row_number: int,
    plan_field_id: int,
    fact_field_id: int,
) -> dict[str, str | int]:
    plan = normalize_number_value(get_table_field_value(task, plan_field_id))
    fact = normalize_number_value(get_table_field_value(task, fact_field_id))
    return {
        "row": row_number,
        "plan": plan,
        "fact": fact,
    }


def find_month_summary_by_year_month(
    project: tasks.Project,
    column_title: str,
    year: int,
    month: str,
    start_row: int,
) -> tuple[dict[str, str | int] | None, int]:
    """
    Найти сводную задачу **месяца** внутри блока **целевого года** и вернуть план/факт из её полей.

    Алгоритм (после ``start_row`` по порядку обхода ``iter_child_tasks``):

    1. В колонке ``column_title`` ищется текст, содержащий четырехзначный ``year``;
       такая **сводная** задача открывает «блок года».
    2. Блок года закрывается, как только встречается задача с ``level`` не глубже, чем у
       сводной строки года.
    3. Внутри блока ищется **сводная** задача, у которой значение той же колонки (в нижнем регистре)
       совпадает с нормализованным именем месяца (``normalize_month``).
    4. У найденной строки читаются поля ``PLAN_COLUMN_TITLE`` / ``FACT_COLUMN_TITLE``.

    Отдельные (несводные) задачи в план/факт **не** входят — только эта сводная строка месяца.
    """
    field_id = find_project_field_id(project, column_title)
    plan_field_id = find_project_field_id(project, PLAN_COLUMN_TITLE)
    fact_field_id = find_project_field_id(project, FACT_COLUMN_TITLE)
    target_month = normalize_month(month)
    rows = list(iter_child_tasks(project.root_task))
    current_year_level: int | None = None
    inside_target_year = False

    for row_number, (task, level) in enumerate(rows, start=1):
        if row_number < start_row:
            continue

        value = get_table_field_value(task, field_id).strip()
        normalized_value = value.lower()

        if task.is_summary and is_year_value(value, year):
            inside_target_year = True
            current_year_level = level
            continue

        if inside_target_year and current_year_level is not None and level <= current_year_level:
            inside_target_year = False
            current_year_level = None

        if not inside_target_year:
            continue

        if task.is_summary and normalized_value == target_month:
            return (
                build_summary_task_row(
                    task,
                    row_number,
                    plan_field_id,
                    fact_field_id,
                ),
                field_id,
            )

    return None, field_id


def month_token_for_mpp(month: int | str) -> str:
    if isinstance(month, int):
        return str(month)
    return str(month).strip()


def parse_plan_fact_counts(plan: str, fact: str) -> tuple[float | None, float | None]:
    pv = normalize_number_value(plan).strip()
    fv = normalize_number_value(fact).strip()
    try:
        out_p = float(pv.replace(",", ".")) if pv else None
    except ValueError:
        out_p = None
    try:
        out_f = float(fv.replace(",", ".")) if fv else None
    except ValueError:
        out_f = None
    return out_p, out_f


def resolve_qd_q1_mpp_settings() -> tuple[Path, str, int]:
    raw = os.environ.get("QD_Q1_MPP_PATH")
    path = Path(raw) if raw else DEFAULT_MPP_PATH
    col = os.environ.get("QD_Q1_MPP_YEAR_COLUMN", DEFAULT_YEAR_COLUMN_TITLE).strip()
    start = int(os.environ.get("QD_Q1_MPP_START_ROW", str(DEFAULT_YEAR_START_ROW)))
    return path, col, start


def read_qd_q1_plan_fact_for_month(
    year: int,
    month: int | str,
    *,
    mpp_path: Path | None = None,
    year_column_title: str | None = None,
    start_row: int | None = None,
    use_local_copy: bool = True,
) -> dict[str, Any]:
    """
    План и факт (количества из пользовательских полей MPP) за календарный месяц —
    одна сводная строка по правилам ``find_month_summary_by_year_month``.

    ``use_local_copy``: при True MPP копируется во временный файл (удобно для сетевых путей).
    """
    path, default_col, default_start = resolve_qd_q1_mpp_settings()
    path = mpp_path or path
    year_column_title = year_column_title or default_col
    sr = start_row if start_row is not None else default_start
    month_arg = month_token_for_mpp(month)
    apply_license()
    try:
        with local_project_copy(path, enabled=use_local_copy) as project_path:
            project = read_project(project_path)
            summary, field_id = find_month_summary_by_year_month(
                project,
                column_title=year_column_title,
                year=year,
                month=month_arg,
                start_row=sr,
            )
            if summary is None:
                return {
                    "year": year,
                    "month": month,
                    "plan": None,
                    "fact": None,
                    "has_data": False,
                    "debug": {
                        "status": "no_month_row",
                        "mpp_path": str(path),
                        "year_column": year_column_title,
                        "start_row": sr,
                        "field_id": field_id,
                    },
                }
            plan_s = str(summary.get("plan") or "")
            fact_s = str(summary.get("fact") or "")
            plan, fact = parse_plan_fact_counts(plan_s, fact_s)
            has_data = plan is not None and fact is not None
            kpi_pct = (
                round(fact / plan * 100.0, 1)
                if has_data and plan and plan > 0
                else None
            )
            return {
                "year": year,
                "month": month,
                "plan": plan,
                "fact": fact,
                "has_data": has_data,
                "kpi_pct": kpi_pct,
                "debug": {
                    "status": "ok",
                    "mpp_path": str(path),
                    "year_column": year_column_title,
                    "start_row": sr,
                    "field_id": field_id,
                    "summary_row": summary.get("row"),
                },
            }
    except Exception as exc:
        return {
            "year": year,
            "month": month,
            "plan": None,
            "fact": None,
            "has_data": False,
            "kpi_pct": None,
            "debug": {
                "status": "error",
                "mpp_path": str(path),
                "error": str(exc),
            },
        }


def normalize_qd_q1_tile_period(year: int | None, month: int | None) -> tuple[int, int]:
    """Год и месяц для плитки: по умолчанию текущие; для текущего года — не позже текущего месяца."""
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    if ref_year == today.year:
        ref_month = min(ref_month, today.month)
    return ref_year, ref_month


def qd_q1_tile_month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def build_qd_q1_tile_json(
    year: int | None = None,
    month: int | None = None,
    *,
    mpp_path: Path | None = None,
    year_column_title: str | None = None,
    start_row: int | None = None,
    use_local_copy: bool = True,
) -> dict[str, Any]:
    """
    JSON плитки QD-Q1 для API: ``data_granularity``, ``monthly_data``, ``last_full_month_row``,
    ``kpi_period``, ``ytd``, ``debug`` — в том же виде, что ожидает ``getkpi.views`` для KPI ``QD-Q1``.

    Параметры ``mpp_path`` / ``year_column_title`` / ``start_row`` / ``use_local_copy``
    пробрасываются в каждый вызов ``read_qd_q1_plan_fact_for_month`` (как в одиночном месяце).
    """
    try:
        ref_y, ref_m = normalize_qd_q1_tile_period(year, month)
        pairs = qd_q1_tile_month_pairs(ref_y, ref_m)
        monthly_rows: list[dict[str, Any]] = []
        ref_row: dict[str, Any] | None = None
        last_debug: dict[str, Any] = {}

        for y, m in pairs:
            snap = read_qd_q1_plan_fact_for_month(
                y,
                m,
                mpp_path=mpp_path,
                year_column_title=year_column_title,
                start_row=start_row,
                use_local_copy=use_local_copy,
            )
            last_debug = snap.get("debug") or {}
            plan = snap.get("plan")
            fact = snap.get("fact")
            has_data = plan is not None and fact is not None
            kpi_pct = _qd_q2_kpi_pct(plan, fact) if has_data else None
            row: dict[str, Any] = {
                "year": y,
                "month": m,
                "month_name": MONTH_RU[m].lower(),
                "plan": plan,
                "fact": fact,
                "kpi_pct": kpi_pct,
                "has_data": has_data,
            }
            if has_data:
                row["values_unit"] = "шт."
            monthly_rows.append(row)
            if (y, m) == (ref_y, ref_m):
                ref_row = row

        return {
            "data_granularity": "monthly",
            "monthly_data": monthly_rows,
            "last_full_month_row": dict(ref_row)
            if ref_row
            and (
                ref_row.get("plan") is not None or ref_row.get("fact") is not None
            )
            else None,
            "kpi_period": {
                "type": "last_full_month",
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_RU[ref_m],
            },
            "ytd": {
                "total_plan": ref_row.get("plan") if ref_row else None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
                "months_total": len(monthly_rows),
                **(
                    {"values_unit": "шт."}
                    if ref_row
                    and (
                        ref_row.get("plan") is not None
                        or ref_row.get("fact") is not None
                    )
                    else {}
                ),
            },
            "debug": {
                "status": "ok",
                "kpi_id": "QD-Q1",
                "source": SOURCE_TAG,
                "mpp": last_debug,
            },
        }
    except Exception as exc:
        logger.exception("Ошибка при расчёте QD-Q1 (MPP)")
        ref_y, ref_m = normalize_qd_q1_tile_period(year, month)
        return {
            "data_granularity": "monthly",
            "monthly_data": [],
            "last_full_month_row": None,
            "kpi_period": {
                "type": "last_full_month",
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_RU[ref_m],
            },
            "ytd": {
                "total_plan": None,
                "total_fact": None,
                "kpi_pct": None,
                "months_with_data": 0,
                "months_total": 0,
            },
            "debug": {
                "status": "error",
                "kpi_id": "QD-Q1",
                "source": SOURCE_TAG,
                "error": str(exc),
            },
        }


def qd_q1_mpp_path_for_stamp() -> Path | None:
    path, _, _ = resolve_qd_q1_mpp_settings()
    return path if path.exists() else None


def qd_q1_tile_cache_path(year: int, month: int) -> Path:
    """Путь к JSON-кэшу плитки QD-Q1 за нормализованную пару (год, месяц)."""
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_qd_q1_tile_{year}_{month:02d}.json"


def _qd_q1_mpp_mtime_for_cache() -> float | None:
    path, _, _ = resolve_qd_q1_mpp_settings()
    try:
        if path.exists():
            return path.stat().st_mtime
    except OSError:
        return None
    return None


def _load_qd_q1_tile_cache(ref_y: int, ref_m: int) -> dict[str, Any] | None:
    path = qd_q1_tile_cache_path(ref_y, ref_m)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != QD_Q1_TILE_CACHE_VERSION:
        return None
    if not ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m):
        if data.get("cache_date") != date.today().isoformat():
            return None
    if data.get("mpp_mtime") != _qd_q1_mpp_mtime_for_cache():
        return None
    tile = data.get("tile")
    return tile if isinstance(tile, dict) else None


def _save_qd_q1_tile_cache(ref_y: int, ref_m: int, tile: dict[str, Any]) -> None:
    try:
        path = qd_q1_tile_cache_path(ref_y, ref_m)
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "tile": tile,
                    "source": SOURCE_TAG,
                    "cache_version": QD_Q1_TILE_CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "mpp_mtime": _qd_q1_mpp_mtime_for_cache(),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def _load_stale_qd_q1_tile_cache(ref_y: int, ref_m: int) -> dict[str, Any] | None:
    path = qd_q1_tile_cache_path(ref_y, ref_m)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != QD_Q1_TILE_CACHE_VERSION:
        return None
    tile = data.get("tile")
    return tile if isinstance(tile, dict) else None


def get_qd_q1_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """
    Плитка QD-Q1 для API: stale-while-revalidate по JSON-кэшу (mtime MPP учитывается только в «свежем» кэше).
    """
    from getkpi.cache_manager import stale_while_revalidate

    lock_y, lock_m = normalize_qd_q1_tile_period(year, month)

    def _compute() -> dict[str, Any]:
        payload = build_qd_q1_tile_json(year=year, month=month)
        _save_qd_q1_tile_cache(lock_y, lock_m, payload)
        return payload

    return stale_while_revalidate(
        f"qualdir_qd_q1_{lock_y}_{lock_m:02d}",
        lambda: _load_qd_q1_tile_cache(lock_y, lock_m),
        lambda: _load_stale_qd_q1_tile_cache(lock_y, lock_m),
        _compute,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MPP: план и факт за месяц (QD-Q1).",
    )
    p.add_argument(
        "--mpp",
        type=Path,
        default=DEFAULT_MPP_PATH,
        help=f"Путь к .mpp. По умолчанию: {DEFAULT_MPP_PATH}",
    )
    p.add_argument(
        "--no-local-copy",
        action="store_true",
        help="Читать MPP с сети без копии во временный файл.",
    )
    p.add_argument(
        "--column-title",
        default=DEFAULT_YEAR_COLUMN_TITLE,
        help=f"Колонка года/месяца. По умолчанию: {DEFAULT_YEAR_COLUMN_TITLE}",
    )
    p.add_argument(
        "--start-row",
        type=int,
        default=DEFAULT_YEAR_START_ROW,
        help=f"Первая строка разбора. По умолчанию: {DEFAULT_YEAR_START_ROW}",
    )
    p.add_argument("--year", type=int, required=True, help="Год, например 2026")
    p.add_argument(
        "--month",
        required=True,
        help="Месяц: 1..12 или январь, март…",
    )
    p.add_argument(
        "--tile",
        action="store_true",
        help="Вывести JSON всей плитки QD-Q1 до выбранного месяца (как в API).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.tile:
        ms = str(args.month).strip()
        if not ms.isdigit():
            raise SystemExit("С ключом --tile укажите месяц числом 1..12 (как в API плитки).")
        payload = build_qd_q1_tile_json(
            year=args.year,
            month=int(ms),
            mpp_path=args.mpp,
            year_column_title=args.column_title,
            start_row=args.start_row,
            use_local_copy=not args.no_local_copy,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    payload = read_qd_q1_plan_fact_for_month(
        args.year,
        args.month,
        mpp_path=args.mpp,
        year_column_title=args.column_title,
        start_row=args.start_row,
        use_local_copy=not args.no_local_copy,
    )
    slim = {
        "year": payload.get("year"),
        "month": payload.get("month"),
        "plan": payload.get("plan"),
        "fact": payload.get("fact"),
        "has_data": payload.get("has_data"),
        "kpi_pct": payload.get("kpi_pct"),
        "debug": payload.get("debug"),
    }
    print(json.dumps(slim, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
