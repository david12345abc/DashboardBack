"""HRD-Q4 — успешность адаптации / удержания в период адаптации.

Источник: ``sup/SUP_data.xlsx``, лист ``Текучесть``.
План считается от годового норматива секции адаптации, факт — по накопительным
итогам строки 179: пары колонок ``F/G``, ``I/J``, ``L/M`` ... с января.
"""
from __future__ import annotations

import json
import logging
import posixpath
import re
import xml.etree.ElementTree as ET
import zipfile
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
CACHE_SOURCE_TAG = "sup_hrd_q4_adaptation_payload_v2"
CACHE_VERSION = 2

PLAN_FALLBACK_TITLE_CELL = "A98"
TOTAL_ROW = 179
FIRST_MONTH_HEADCOUNT_COLUMN = 6  # F
MONTH_COLUMN_STEP = 3


def _safe_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        raw = value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if not raw or raw.startswith("#"):
            return None
        try:
            num = float(raw)
        except ValueError:
            return None
    else:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
    return num if num == num else None


def _annual_plan_from_title(title: Any) -> float | None:
    """Годовой план из заголовка вида ``... 18%``."""
    match = re.search(r"(\d+(?:[,.]\d+)?)\s*%", str(title or ""))
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _month_columns(month: int) -> tuple[int, int]:
    headcount_col = FIRST_MONTH_HEADCOUNT_COLUMN + (month - 1) * MONTH_COLUMN_STEP
    dismissal_col = headcount_col + 1
    return headcount_col, dismissal_col


def _column_letters(index: int) -> str:
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _cell_ref(row: int, column: int) -> str:
    return f"{_column_letters(column)}{row}"


def _xml_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return "".join(node.itertext())


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        raw = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(raw)
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    return [_xml_text(si) for si in root.findall("x:si", ns)]


def _sheet_xml_path(zf: zipfile.ZipFile, sheet_name: str) -> str:
    wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
    rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    wb_ns = {
        "x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    rel_ns = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
    rel_targets = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("r:Relationship", rel_ns)
    }
    for sheet in wb_root.findall("x:sheets/x:sheet", wb_ns):
        if sheet.attrib.get("name") != sheet_name:
            continue
        rel_id = sheet.attrib.get(f"{{{wb_ns['r']}}}id")
        target = rel_targets.get(rel_id or "")
        if not target:
            break
        return posixpath.normpath(posixpath.join("xl", target))
    raise RuntimeError(f"Не найден лист {sheet_name!r} в {SOURCE_FILE}")


def _parse_xlsx_cell_value(cell: ET.Element, shared_strings: list[str]) -> Any:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return _xml_text(cell.find("x:is", ns))
    value_node = cell.find("x:v", ns)
    if value_node is None or value_node.text is None:
        return None
    raw = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (ValueError, IndexError):
            return ""
    if cell_type == "str":
        return raw
    if cell_type == "b":
        return raw == "1"
    try:
        num = float(raw)
    except ValueError:
        return raw
    return int(num) if num.is_integer() else num


def _read_sheet_cells(cell_refs: set[str]) -> dict[str, Any]:
    with zipfile.ZipFile(SOURCE_FILE) as zf:
        shared_strings = _load_shared_strings(zf)
        sheet_path = _sheet_xml_path(zf, SHEET_NAME)
        root = ET.fromstring(zf.read(sheet_path))
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    values: dict[str, Any] = {}
    for cell in root.findall(".//x:c", ns):
        ref = cell.attrib.get("r")
        if ref in cell_refs:
            values[ref] = _parse_xlsx_cell_value(cell, shared_strings)
    return values


def _read_monthly_rows(ref_y: int, ref_m: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cell_refs = {PLAN_FALLBACK_TITLE_CELL}
    for month in range(1, ref_m + 1):
        headcount_col, dismissal_col = _month_columns(month)
        cell_refs.add(_cell_ref(TOTAL_ROW, headcount_col))
        cell_refs.add(_cell_ref(TOTAL_ROW, dismissal_col))
    sheet_values = _read_sheet_cells(cell_refs)
    annual_plan = _annual_plan_from_title(sheet_values.get(PLAN_FALLBACK_TITLE_CELL))

    monthly_rows: list[dict[str, Any]] = []
    raw_cells: list[dict[str, Any]] = []
    cumulative_headcount = 0.0
    cumulative_dismissals = 0.0

    for month in range(1, ref_m + 1):
        headcount_col, dismissal_col = _month_columns(month)
        headcount_ref = _cell_ref(TOTAL_ROW, headcount_col)
        dismissal_ref = _cell_ref(TOTAL_ROW, dismissal_col)
        raw_headcount = sheet_values.get(headcount_ref)
        raw_dismissals = sheet_values.get(dismissal_ref)
        headcount = _safe_number(raw_headcount)
        dismissals = _safe_number(raw_dismissals)

        if headcount is not None:
            cumulative_headcount += headcount
        if dismissals is not None:
            cumulative_dismissals += dismissals

        plan = round(annual_plan / 12 * month, 2) if annual_plan is not None else None
        fact = (
            round(cumulative_dismissals / cumulative_headcount * 100, 2)
            if cumulative_headcount > 0
            else None
        )

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
            "headcount_cell": headcount_ref,
            "dismissal_cell": dismissal_ref,
            "raw_headcount": raw_headcount,
            "raw_dismissals": raw_dismissals,
            "cumulative_headcount": cumulative_headcount,
            "cumulative_dismissals": cumulative_dismissals,
            "plan": plan,
            "fact": fact,
        })

    return monthly_rows, {
        "source_file": str(SOURCE_FILE),
        "sheet": SHEET_NAME,
        "plan_title_cell": PLAN_FALLBACK_TITLE_CELL,
        "annual_plan_pct": annual_plan,
        "total_row": TOTAL_ROW,
        "first_month_columns": "F/G",
        "month_column_step": MONTH_COLUMN_STEP,
        "raw_cells": raw_cells,
    }


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows, debug = _read_monthly_rows(ref_y, ref_m)
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
            "rule": "plan = annual adaptation norm / 12 * month; fact = cumulative dismissals / cumulative headcount from row 179",
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
