"""HRD-M4 — текучесть персонала по компании.

Источник: ``HC_сводный_{year}_{Месяц}.xls[x]`` из каталога HR-отчётов, лист ``Текучесть``.

Важно: накопительные значения (строка «С накоплением») в старых помесячных файлах
устаревают, когда HR обновляет сетку в более новом файле. Поэтому все месяцы 1..m
читаются из **последнего доступного** HC-файла года — даже если выбран более ранний
месяц фильтра (колонка июля из августовского файла, а не из июльского).

Факт — строка «С накоплением» (row 10), план — «План» (row 11),
колонка месяца *m* (H, K, N…, шаг 3).
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import xlrd

from getkpi.cache_manager import stale_while_revalidate
from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from sup.hc_reports import HC_REPORTS_DIR, hc_report_path, open_hc_workbook, reports_mtime_ns

logger = logging.getLogger(__name__)

SHEET_NAME = "Текучесть"
CACHE_PREFIX = "sup_hrd_m4_turnover"
CACHE_SOURCE_TAG = "sup_hrd_m4_turnover_payload_v9_latest_year_file"
CACHE_VERSION = 10

FACT_ROW = 10
PLAN_ROW = 11
FIRST_MONTH_COLUMN = 8  # H
MONTH_COLUMN_STEP = 3


def _month_column(month: int) -> int:
    return FIRST_MONTH_COLUMN + (month - 1) * MONTH_COLUMN_STEP


def _column_letter(column: int) -> str:
    letters = ""
    while column:
        column, rem = divmod(column - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _safe_percent(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        raw = value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if not raw or raw.startswith("#"):
            return None
        has_percent_sign = raw.endswith("%")
        if has_percent_sign:
            raw = raw[:-1]
        try:
            num = float(raw)
        except ValueError:
            return None
        if has_percent_sign:
            return round(num, 2)
    else:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None

    if num != num:
        return None
    if abs(num) <= 1:
        num *= 100
    return round(num, 2)


def _kpi_pct_from_plan_fact(plan: float | None, fact: float | None) -> float | None:
    """KPI плитки: факт / план × 100 (% выполнения порога)."""
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 1)


def _open_tekuchest_sheet(book: xlrd.Book):
    target = SHEET_NAME.strip().lower().replace("ё", "е")
    for name in book.sheet_names():
        if name.strip().lower().replace("ё", "е") == target:
            return book.sheet_by_name(name)
    raise KeyError(f"Лист {SHEET_NAME!r} не найден")


def _latest_hc_report(ref_y: int, ref_m: int | None = None) -> tuple[Path | None, int | None]:
    """Последний существующий HC_сводный за год (актуальная накопительная сетка).

    ``ref_m`` не ограничивает поиск: июль в фильтре всё равно читается из
    августовского (или более нового) файла, если он уже есть.
    """
    del ref_m  # период влияет на набор колонок, не на выбор файла-источника
    for month in range(12, 0, -1):
        path = hc_report_path(ref_y, month)
        try:
            if path.exists():
                return path, month
        except OSError:
            continue
    return None, None


def _read_turnover_cells_from_sheet(
    sheet,
    report_month: int,
    *,
    source_path: Path,
    source_file_month: int,
) -> tuple[float | None, float | None, dict[str, Any]]:
    column = _month_column(report_month)
    coord = f"{_column_letter(column)}{FACT_ROW}"
    plan_coord = f"{_column_letter(column)}{PLAN_ROW}"
    debug: dict[str, Any] = {
        "month": report_month,
        "source_file": str(source_path),
        "source_file_month": source_file_month,
        "sheet": SHEET_NAME,
        "fact_row": FACT_ROW,
        "plan_row": PLAN_ROW,
        "month_column": _column_letter(column),
        "fact_cell": coord,
        "plan_cell": plan_coord,
    }
    try:
        raw_fact = sheet.cell_value(FACT_ROW - 1, column - 1)
        raw_plan = sheet.cell_value(PLAN_ROW - 1, column - 1)
    except Exception as exc:
        logger.warning("HRD-M4: не удалось прочитать ячейки %s: %s", source_path, exc)
        debug["status"] = "read_error"
        debug["error"] = str(exc)
        return None, None, debug

    fact = _safe_percent(raw_fact)
    plan = _safe_percent(raw_plan)
    debug.update({
        "status": "ok",
        "raw_fact": raw_fact,
        "raw_plan": raw_plan,
        "fact": fact,
        "plan": plan,
    })
    return fact, plan, debug


def _load_turnover_months(ref_y: int, ref_m: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Все месяцы 1..ref_m — из последнего HC-файла (актуальная накопительная сетка)."""
    monthly_rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    path, source_file_month = _latest_hc_report(ref_y, ref_m)

    if path is None or source_file_month is None:
        for month in range(1, ref_m + 1):
            monthly_rows.append({
                "month": month,
                "year": ref_y,
                "month_name": MONTH_NAMES[month],
                "plan": None,
                "fact": None,
                "kpi_pct": None,
                "has_data": False,
                "values_unit": "%",
            })
            sources.append({"month": month, "status": "missing_file"})
        return monthly_rows, {
            "reports_dir": str(HC_REPORTS_DIR),
            "source_file": None,
            "source_file_month": None,
            "first_month_column": "H",
            "month_column_step": MONTH_COLUMN_STEP,
            "report_sources": sources,
        }

    book = None
    try:
        book = open_hc_workbook(path)
        sheet = _open_tekuchest_sheet(book)
        for month in range(1, ref_m + 1):
            fact, plan, source_debug = _read_turnover_cells_from_sheet(
                sheet,
                month,
                source_path=path,
                source_file_month=source_file_month,
            )
            monthly_rows.append({
                "month": month,
                "year": ref_y,
                "month_name": MONTH_NAMES[month],
                "plan": plan,
                "fact": fact,
                "kpi_pct": _kpi_pct_from_plan_fact(plan, fact),
                # План без факта (незакрытый месяц / #DIV/0!) — не «есть данные».
                "has_data": fact is not None,
                "values_unit": "%",
            })
            sources.append(source_debug)
    except Exception as exc:
        logger.exception("HRD-M4: ошибка чтения %s", path)
        monthly_rows = []
        sources = [{"status": "read_error", "error": str(exc), "source_file": str(path)}]
        for month in range(1, ref_m + 1):
            monthly_rows.append({
                "month": month,
                "year": ref_y,
                "month_name": MONTH_NAMES[month],
                "plan": None,
                "fact": None,
                "kpi_pct": None,
                "has_data": False,
                "values_unit": "%",
            })
    finally:
        if book is not None and hasattr(book, "close"):
            book.close()

    return monthly_rows, {
        "reports_dir": str(HC_REPORTS_DIR),
        "source_file": str(path),
        "source_file_month": source_file_month,
        "first_month_column": "H",
        "month_column_step": MONTH_COLUMN_STEP,
        "report_sources": sources,
    }


def _pick_display_row(monthly_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Для плитки: если у опорного месяца нет факта — план и факт за прошлый месяц."""
    if not monthly_rows:
        return None
    ref_row = monthly_rows[-1]
    if ref_row.get("fact") is not None:
        return ref_row
    if len(monthly_rows) >= 2:
        return monthly_rows[-2]
    return ref_row


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    from getkpi.autoit.it_monthly_period import trim_monthly_rows_to_display

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows, debug = _load_turnover_months(ref_y, ref_m)
    # Незакрытый месяц (fact = null / #DIV/0!): план и факт — строго за прошлый месяц.
    display_row = _pick_display_row(monthly_rows)
    # Не отдаём на фронт месяц без факта: иначе клиент подменяет опору пустым августом.
    monthly_rows = trim_monthly_rows_to_display(monthly_rows, display_row)
    display_y = int(display_row["year"]) if display_row else ref_y
    display_m = int(display_row["month"]) if display_row else ref_m

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(display_row) if display_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": display_y,
            "month": display_m,
            "month_name": MONTH_NAMES[display_m],
        },
        "ytd": {
            "total_plan": display_row.get("plan") if display_row else None,
            "total_fact": display_row.get("fact") if display_row else None,
            "kpi_pct": display_row.get("kpi_pct") if display_row else None,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "%",
        },
        "debug": {
            "kpi_id": "HRD-M4",
            "status": "ok",
            "requested_year": ref_y,
            "requested_month": ref_m,
            "display_year": display_y,
            "display_month": display_m,
            "rule": (
                "HC_сводный_{year}_{Month}.xls[x] / sheet Текучесть; "
                "all months 1..m from latest available HC file of the year "
                "(not limited by filter month); "
                "fact = row «С накоплением» (10), plan = row «План» (11), "
                "column H/K/N... (step 3); Excel percent → percent points; "
                "if requested month has no fact → tile plan/fact = previous month"
            ),
            **debug,
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, year, month)


def _load_cache(path: Path, *, source_mtime_ns: int, perpetual: bool) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if raw.get("cache_source") != CACHE_SOURCE_TAG:
        return None
    if raw.get("cache_version") != CACHE_VERSION:
        return None
    if raw.get("source_mtime_ns") != source_mtime_ns:
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    if perpetual or raw.get("cache_date") == date.today().isoformat():
        return payload
    return None


def _save_cache(path: Path, payload: dict[str, Any], *, source_mtime_ns: int) -> None:
    ytd_json_cache.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "cache_source": CACHE_SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "source_mtime_ns": source_mtime_ns,
                    "payload": payload,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        logger.exception("HRD-M4: не удалось сохранить кэш")


def _load_stale_cache(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if raw.get("cache_source") != CACHE_SOURCE_TAG:
        return None
    if raw.get("cache_version") != CACHE_VERSION:
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    return payload


def get_hrd_m4_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)
    lock_key = f"sup_hrd_m4_{ref_y}_{ref_m:02d}"

    def _load_fresh() -> dict[str, Any] | None:
        # mtime по всем месяцам года: новый августовский файл инвалидирует июльский кэш.
        source_mtime_ns = reports_mtime_ns(ref_y, 12)
        return _load_cache(cache_path, source_mtime_ns=source_mtime_ns, perpetual=perpetual)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("HRD-M4: ошибка расчёта текучести")
            stale = _load_stale_cache(cache_path)
            if stale is not None:
                return stale
            return None
        source_mtime_ns = reports_mtime_ns(ref_y, 12)
        _save_cache(cache_path, payload, source_mtime_ns=source_mtime_ns)
        return payload

    return stale_while_revalidate(
        lock_key,
        _load_fresh,
        lambda: _load_stale_cache(cache_path),
        _compute_and_save,
    )
