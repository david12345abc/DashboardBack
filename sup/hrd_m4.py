"""HRD-M4 — текучесть персонала по компании.

Источник: ``HC_сводный_{year}_{Месяц}.xls`` из каталога HR-отчётов, лист ``Текучесть``.
Для месяца *m* открывается файл этого месяца; факт — строка 10, план — строка 11,
колонка месяца *m* (H, K, N…, шаг 3) — как в ``SUP_data.xlsx``.
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
CACHE_SOURCE_TAG = "sup_hrd_m4_turnover_payload_v5_hc_xlsx"
CACHE_VERSION = 5

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


def _read_turnover_for_month(
    ref_y: int,
    report_month: int,
) -> tuple[float | None, float | None, dict[str, Any]]:
    path = hc_report_path(ref_y, report_month)
    column = _month_column(report_month)
    coord = f"{_column_letter(column)}{FACT_ROW}"
    plan_coord = f"{_column_letter(column)}{PLAN_ROW}"
    debug: dict[str, Any] = {
        "month": report_month,
        "source_file": str(path),
        "sheet": SHEET_NAME,
        "fact_row": FACT_ROW,
        "plan_row": PLAN_ROW,
        "month_column": _column_letter(column),
        "fact_cell": coord,
        "plan_cell": plan_coord,
    }

    if not path.exists():
        debug["status"] = "missing_file"
        return None, None, debug

    book = None
    try:
        book = open_hc_workbook(path)
        sheet = _open_tekuchest_sheet(book)
        raw_fact = sheet.cell_value(FACT_ROW - 1, column - 1)
        raw_plan = sheet.cell_value(PLAN_ROW - 1, column - 1)
    except Exception as exc:
        logger.warning("HRD-M4: не удалось прочитать %s: %s", path, exc)
        debug["status"] = "read_error"
        debug["error"] = str(exc)
        return None, None, debug
    finally:
        if book is not None and hasattr(book, "close"):
            book.close()

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
    monthly_rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []

    for month in range(1, ref_m + 1):
        fact, plan, source_debug = _read_turnover_for_month(ref_y, month)
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
        sources.append(source_debug)

    return monthly_rows, {
        "reports_dir": str(HC_REPORTS_DIR),
        "first_month_column": "H",
        "month_column_step": MONTH_COLUMN_STEP,
        "report_sources": sources,
    }


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows, debug = _load_turnover_months(ref_y, ref_m)
    ref_row = monthly_rows[-1] if monthly_rows else None

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
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
            "values_unit": "%",
        },
        "debug": {
            "kpi_id": "HRD-M4",
            "status": "ok",
            "rule": (
                "HC_сводный_{year}_{Month}.xls / sheet Текучесть; "
                "for month m read file m, fact row 10, plan row 11, "
                "column H/K/N... (step 3); Excel percent values converted to percent points"
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
        source_mtime_ns = reports_mtime_ns(ref_y, ref_m)
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
        source_mtime_ns = reports_mtime_ns(ref_y, ref_m)
        _save_cache(cache_path, payload, source_mtime_ns=source_mtime_ns)
        return payload

    return stale_while_revalidate(
        lock_key,
        _load_fresh,
        lambda: _load_stale_cache(cache_path),
        _compute_and_save,
    )
