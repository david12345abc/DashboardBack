"""HRD-Q4 — успешность адаптации / удержания в период адаптации.

Источник: ``sup/SUP_data.xlsx``, лист ``Текучесть``.
Для каждого месяца в временной копии Excel-файла проставляется номер месяца
в селектор, книга пересчитывается Excel, затем читаются ``C179`` (план) и
``D179`` (факт).
"""
from __future__ import annotations

import json
import logging
import re
import shutil
import tempfile
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from getkpi.cache_manager import locked_call
from getkpi.devdir import ytd_json_cache
from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

SOURCE_FILE = Path(__file__).resolve().parent / "SUP_data.xlsx"
SHEET_NAME = "Текучесть"
CACHE_PREFIX = "sup_hrd_q4_adaptation"
CACHE_SOURCE_TAG = "sup_hrd_q4_adaptation_payload_v1"
CACHE_VERSION = 1

MONTH_SELECTOR_CELLS = ("D14", "C14")
PLAN_CELL = "C179"
FACT_CELL = "D179"
PLAN_FALLBACK_TITLE_CELL = "A98"


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


def _fallback_plan_from_title(title: Any, month: int) -> float | None:
    """Если C179 пустая, берём годовой план из заголовка вида ``... 18%``."""
    match = re.search(r"(\d+(?:[,.]\d+)?)\s*%", str(title or ""))
    if not match:
        return None
    annual_plan = float(match.group(1).replace(",", "."))
    return round(annual_plan / 12 * month, 2)


def _read_monthly_rows_via_excel(ref_y: int, ref_m: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        import pythoncom
        import win32com.client
    except ImportError as exc:
        raise RuntimeError("Для HRD-Q4 нужен pywin32/win32com для пересчёта Excel") from exc

    temp_path = Path(tempfile.gettempdir()) / f"sup_hrd_q4_{uuid.uuid4().hex}.xlsx"
    shutil.copy2(SOURCE_FILE, temp_path)

    pythoncom.CoInitialize()
    app = None
    wb = None
    try:
        app = win32com.client.DispatchEx("Excel.Application")
        app.Visible = False
        app.DisplayAlerts = False
        wb = app.Workbooks.Open(str(temp_path))
        ws = wb.Worksheets(SHEET_NAME)

        monthly_rows: list[dict[str, Any]] = []
        raw_cells: list[dict[str, Any]] = []
        for month in range(1, ref_m + 1):
            for selector_cell in MONTH_SELECTOR_CELLS:
                ws.Range(selector_cell).Value = month
            app.CalculateFullRebuild()

            raw_plan = ws.Range(PLAN_CELL).Value
            raw_fact = ws.Range(FACT_CELL).Value
            plan = _safe_percent(raw_plan)
            if plan is None:
                plan = _fallback_plan_from_title(ws.Range(PLAN_FALLBACK_TITLE_CELL).Value, month)
            fact = _safe_percent(raw_fact)

            monthly_rows.append({
                "month": month,
                "year": ref_y,
                "month_name": MONTH_NAMES[month],
                "plan": plan,
                "fact": fact,
                "kpi_pct": fact,
                "has_data": fact is not None or plan is not None,
                "values_unit": "%",
            })
            raw_cells.append({
                "month": month,
                "selector_cells": list(MONTH_SELECTOR_CELLS),
                "plan_cell": PLAN_CELL,
                "fact_cell": FACT_CELL,
                "raw_plan": raw_plan,
                "raw_fact": raw_fact,
                "plan": plan,
                "fact": fact,
                "plan_used_fallback": raw_plan is None or raw_plan == "",
            })

        return monthly_rows, {
            "source_file": str(SOURCE_FILE),
            "sheet": SHEET_NAME,
            "selector_cells": list(MONTH_SELECTOR_CELLS),
            "plan_cell": PLAN_CELL,
            "fact_cell": FACT_CELL,
            "plan_fallback_title_cell": PLAN_FALLBACK_TITLE_CELL,
            "raw_cells": raw_cells,
        }
    finally:
        if wb is not None:
            wb.Close(False)
        if app is not None:
            app.Quit()
        pythoncom.CoUninitialize()
        try:
            temp_path.unlink()
        except OSError:
            pass


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows, debug = _read_monthly_rows_via_excel(ref_y, ref_m)
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
            "kpi_id": "HRD-Q4",
            "status": "ok",
            "rule": "set month selector, recalculate Excel, read plan C179 and fact D179",
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


def get_hrd_q4_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any] | None:
        try:
            source_mtime_ns = SOURCE_FILE.stat().st_mtime_ns
        except OSError:
            logger.exception("HRD-Q4: не найден источник %s", SOURCE_FILE)
            return None

        cached = _load_cache(cache_path, source_mtime_ns=source_mtime_ns, perpetual=perpetual)
        if cached is not None:
            return cached

        try:
            payload = _build_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("HRD-Q4: ошибка расчёта адаптации")
            return None
        _save_cache(cache_path, payload, source_mtime_ns=source_mtime_ns)
        return payload

    return locked_call(f"sup_hrd_q4_{ref_y}_{ref_m:02d}", _runner)
