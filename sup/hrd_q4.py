"""HRD-Q4 — успешность адаптации / удержания в период адаптации.

План — накопительно с начала года: ``номер_месяца × 1,5`` (январь = 1,5 %, июнь = 9 %).
Факт — лист «Текучесть», столбец D, последняя непустая ячейка в файле
``HC_сводный_{year}_{Месяц}.xls`` из каталога отчётов HR.

В незакрытом календарном месяце факт из файла ещё «живой» (меняется при каждом
сохранении HC) — на плитке показываем план и факт за предыдущий месяц.
KPI плитки — ``факт / план × 100`` (%).
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
from sup.hrd_m4 import _kpi_pct_from_plan_fact, _safe_percent

logger = logging.getLogger(__name__)

HC_SHEET_NAME = "Текучесть"
HC_FACT_COLUMN = 3  # D

CACHE_PREFIX = "sup_hrd_q4_adaptation"
CACHE_SOURCE_TAG = "sup_hrd_q4_adaptation_payload_v8_prev_month"
CACHE_VERSION = 8

# Накопительный план с января: месяц × 1,5 п.п.
PLAN_PCT_PER_MONTH = 1.5


def _cumulative_plan_for_month(month: int) -> float:
    """Накопительный план за год: январь 1,5 %, февраль 3 %, …, декабрь 18 %."""
    return round(month * PLAN_PCT_PER_MONTH, 2)


def _fact_from_cell(raw: Any) -> float | None:
    """Ячейка столбца D; не число / ошибка Excel → None (месяц ещё без факта)."""
    return _safe_percent(raw)


def _is_unclosed_calendar_month(year: int, month: int) -> bool:
    today = date.today()
    return int(year) == today.year and int(month) == today.month


def _open_tekuchest_sheet(book: xlrd.Book):
    target = HC_SHEET_NAME.strip().lower().replace("ё", "е")
    for name in book.sheet_names():
        if name.strip().lower().replace("ё", "е") == target:
            return book.sheet_by_name(name)
    raise KeyError(f"Лист {HC_SHEET_NAME!r} не найден")


def _read_fact_from_hc_report(path: Path) -> tuple[float | None, dict[str, Any]]:
    debug: dict[str, Any] = {
        "source_file": str(path),
        "sheet": HC_SHEET_NAME,
        "column": "D",
    }
    try:
        if not path.exists():
            debug["status"] = "missing_file"
            return None, debug
    except OSError as exc:
        debug["status"] = "missing_file"
        debug["error"] = str(exc)
        return None, debug

    book = None
    try:
        book = open_hc_workbook(path)
        sheet = _open_tekuchest_sheet(book)
        raw_value: Any = None
        row_idx: int | None = None
        for row in range(sheet.nrows - 1, -1, -1):
            value = sheet.cell_value(row, HC_FACT_COLUMN)
            if value in (None, ""):
                continue
            if isinstance(value, str) and value.strip().startswith("#"):
                continue
            raw_value = value
            row_idx = row
            break
        fact = _fact_from_cell(raw_value)
        debug.update({
            "status": "ok" if fact is not None else "no_numeric_fact",
            "row": (row_idx + 1) if row_idx is not None else None,
            "raw_fact": raw_value,
            "fact": fact,
        })
        return fact, debug
    except Exception as exc:
        logger.warning("HRD-Q4: не удалось прочитать %s: %s", path, exc)
        debug["status"] = "read_error"
        debug["error"] = str(exc)
        return None, debug
    finally:
        if book is not None and hasattr(book, "close"):
            book.close()


def _read_monthly_rows(ref_y: int, ref_m: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    monthly_rows: list[dict[str, Any]] = []
    fact_sources: list[dict[str, Any]] = []

    for month in range(1, ref_m + 1):
        plan = _cumulative_plan_for_month(month)
        report_path = hc_report_path(ref_y, month)
        fact, source_debug = _read_fact_from_hc_report(report_path)
        # Незакрытый месяц: не берём «живой» черновик из HC — иначе плитка скачет.
        if _is_unclosed_calendar_month(ref_y, month):
            source_debug = {
                **source_debug,
                "display_fact_suppressed": True,
                "raw_file_fact": fact,
            }
            fact = None
        monthly_rows.append({
            "month": month,
            "year": ref_y,
            "month_name": MONTH_NAMES[month],
            "plan": plan,
            "fact": fact,
            "kpi_pct": _kpi_pct_from_plan_fact(plan, fact),
            "has_data": fact is not None or plan is not None,
            "values_unit": "%",
        })
        fact_sources.append({"month": month, **source_debug})

    return monthly_rows, {
        "fact_source": "HC_сводный_{year}_{month}.xls / Текучесть / column D last numeric row",
        "reports_dir": str(HC_REPORTS_DIR),
        "fact_sources": fact_sources,
    }


def _pick_display_row(monthly_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Если у опорного месяца нет факта — план и факт за прошлый месяц."""
    if not monthly_rows:
        return None
    ref_row = monthly_rows[-1]
    if ref_row.get("fact") is not None:
        return ref_row
    if len(monthly_rows) >= 2:
        return monthly_rows[-2]
    return ref_row


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows, debug = _read_monthly_rows(ref_y, ref_m)
    display_row = _pick_display_row(monthly_rows)
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
            "kpi_id": "HRD-Q4",
            "status": "ok",
            "requested_year": ref_y,
            "requested_month": ref_m,
            "display_year": display_y,
            "display_month": display_m,
            "rule": (
                "plan = month * 1.5 (cumulative from year start); "
                "fact = last numeric value in column D on sheet Текучесть "
                "from HC_сводный_{year}_{Month}.xls; "
                "unclosed calendar month → fact suppressed; "
                "tile plan/fact = previous month if requested month has no fact; "
                "kpi_pct = fact / plan * 100"
            ),
            "plan_pct_per_month": PLAN_PCT_PER_MONTH,
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
        logger.exception("HRD-Q4: не удалось сохранить кэш")


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


def get_hrd_q4_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)
    lock_key = f"sup_hrd_q4_{ref_y}_{ref_m:02d}"

    def _load_fresh() -> dict[str, Any] | None:
        source_mtime_ns = reports_mtime_ns(ref_y, ref_m)
        return _load_cache(cache_path, source_mtime_ns=source_mtime_ns, perpetual=perpetual)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("HRD-Q4: ошибка расчёта адаптации")
            stale = _load_stale_cache(cache_path)
            if stale is not None:
                return stale
            return None
        source_mtime_ns = reports_mtime_ns(ref_y, ref_m)
        _save_cache(cache_path, payload, source_mtime_ns=source_mtime_ns)
        return payload

    return stale_while_revalidate(
        lock_key,
        _load_fresh,
        lambda: _load_stale_cache(cache_path),
        _compute_and_save,
    )
