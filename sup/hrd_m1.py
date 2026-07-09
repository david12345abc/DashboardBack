"""HRD-M1 — критические вакансии типа A, закрытые в срок.

Источник: ``HC_сводный_{year}_{Месяц}.xls`` из каталога HR-отчётов, лист ``Вакансии``.
Для месяца *m* читается файл этого месяца; в расчёт попадают строки, у которых
«Месяц закрытия план» совпадает с месяцем файла.
План месяца — число таких вакансий типа A (дата закрытия факт может быть пустой).
Факт — в срок: «Месяц закрытия факт» заполнен и не позже «Месяц закрытия план».
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import xlrd

from getkpi.cache_manager import stale_while_revalidate
from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from sup.hc_reports import HC_REPORTS_DIR, hc_report_path, reports_mtime_ns

logger = logging.getLogger(__name__)

SHEET_NAME = "Вакансии"
CACHE_PREFIX = "sup_hrd_m1_vacancies"
CACHE_SOURCE_TAG = "sup_hrd_m1_vacancies_payload_v5_hc_plan_month"
CACHE_VERSION = 7

MONTH_NAME_TO_NUM: dict[str, int] = {
    "январь": 1,
    "февраль": 2,
    "март": 3,
    "апрель": 4,
    "май": 5,
    "июнь": 6,
    "июль": 7,
    "август": 8,
    "сентябрь": 9,
    "октябрь": 10,
    "ноябрь": 11,
    "декабрь": 12,
}

EXCLUSION_PHRASES = (
    "снята заказчиком",
    "снят заказчиком",
    "снято заказчиком",
    "отменена",
    "отменен",
    "отменено",
    "приостановлена",
    "приостановлен",
    "приостановлено",
    "заморожена",
    "заморожен",
    "заморожено",
    "закрыта заказчиком",
    "закрыт заказчиком",
    "закрыто заказчиком",
    "дубль",
    "дубликат",
    "ошибка заявки",
)


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    return " ".join(re.sub(r"[^0-9a-zа-я]+", " ", text).split())


def _normalize_header(value: Any) -> str:
    return _normalize_text(value).replace(" ", "")


def _normalize_type(value: Any) -> str:
    return str(value or "").strip().upper().replace("А", "A")


def _parse_date(value: Any, *, book: xlrd.Book | None = None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and value > 20_000 and book is not None:
        try:
            parts = xlrd.xldate_as_tuple(value, book.datemode)
            return date(parts[0], parts[1], parts[2])
        except (TypeError, ValueError, xlrd.XLDateError):
            return None
    raw = str(value).strip()
    if not raw:
        return None
    for candidate in (raw, raw[:10]):
        try:
            return datetime.fromisoformat(candidate.replace("Z", "+00:00")).date()
        except ValueError:
            pass
        if len(candidate) >= 10 and candidate[2:3] == "." and candidate[5:6] == ".":
            try:
                return datetime.strptime(candidate[:10], "%d.%m.%Y").date()
            except ValueError:
                pass
    return None


def _format_date(value: date | None) -> str:
    return value.isoformat() if value is not None else ""


def _open_vacancies_sheet(book: xlrd.Book):
    target = SHEET_NAME.strip().lower().replace("ё", "е")
    for name in book.sheet_names():
        if name.strip().lower().replace("ё", "е") == target:
            return book.sheet_by_name(name)
    raise KeyError(f"Лист {SHEET_NAME!r} не найден")


def _find_header_row(sheet: xlrd.sheet.Sheet) -> tuple[int, dict[str, int]]:
    required = {"авс", "месяцзакрытияплан", "месяцзакрытияфакт"}
    for row_idx in range(sheet.nrows):
        row = sheet.row_values(row_idx)
        headers = {
            _normalize_header(value): idx
            for idx, value in enumerate(row)
            if value is not None and str(value).strip()
        }
        if required.issubset(headers):
            return row_idx, headers
    raise RuntimeError(f"Не найдена строка заголовков листа {SHEET_NAME!r}")


def _row_value(row: tuple[Any, ...], headers: dict[str, int], header_key: str) -> Any:
    idx = headers.get(header_key)
    if idx is None or idx >= len(row):
        return None
    return row[idx]


def _exclusion_reason(row: tuple[Any, ...], headers: dict[str, int]) -> str | None:
    text_parts: list[str] = []
    for key in ("кандидат(фио)", "кандидатфио", "комментарии"):
        value = _row_value(row, headers, key)
        if value is not None:
            text_parts.append(str(value))
    for header, idx in headers.items():
        if "статус" in header and idx < len(row) and row[idx] is not None:
            text_parts.append(str(row[idx]))
    haystack = _normalize_text(" ".join(text_parts))
    for phrase in EXCLUSION_PHRASES:
        if _normalize_text(phrase) in haystack:
            return phrase
    return None


def _parse_month_name(value: Any) -> int | None:
    """Номер месяца 1–12 из подписи «Январь», «февраль» и т.п."""
    text = _normalize_text(value)
    if not text:
        return None
    for name, num in MONTH_NAME_TO_NUM.items():
        if text == name or text.startswith(name):
            return num
    return None


def _closed_on_time(plan_month_raw: Any, fact_month_raw: Any) -> bool:
    """
    В срок: в «Месяц закрытия факт» указан месяц и он не позже «Месяц закрытия план».
    Не в срок: факт-пусто или месяц факта > месяца плана.
    """
    plan_m = _parse_month_name(plan_month_raw)
    if plan_m is None:
        return False
    fact_m = _parse_month_name(fact_month_raw)
    if fact_m is None:
        return False
    return fact_m <= plan_m


def _load_vacancies_for_report_month(
    ref_y: int,
    report_month: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = hc_report_path(ref_y, report_month)
    debug: dict[str, Any] = {
        "month": report_month,
        "source_file": str(path),
        "sheet": SHEET_NAME,
        "report_plan_close_month": report_month,
    }

    if not path.exists():
        debug["status"] = "missing_file"
        return [], debug

    try:
        book = xlrd.open_workbook(str(path))
        sheet = _open_vacancies_sheet(book)
        header_row, headers = _find_header_row(sheet)
    except Exception as exc:
        logger.warning("HRD-M1: не удалось прочитать %s: %s", path, exc)
        debug["status"] = "read_error"
        debug["error"] = str(exc)
        return [], debug

    included: list[dict[str, Any]] = []
    excluded_count = 0
    skipped_non_a = 0
    skipped_plan_month = 0

    for row_idx in range(header_row + 1, sheet.nrows):
        row = tuple(sheet.row_values(row_idx))
        vacancy_type = _normalize_type(_row_value(row, headers, "авс"))
        if vacancy_type != "A":
            skipped_non_a += 1
            continue

        plan_month_raw = _row_value(row, headers, "месяцзакрытияплан")
        plan_month = _parse_month_name(plan_month_raw)
        if plan_month != report_month:
            skipped_plan_month += 1
            continue

        reason = _exclusion_reason(row, headers)
        if reason:
            excluded_count += 1
            continue

        fact_month_raw = _row_value(row, headers, "месяцзакрытияфакт")
        fact_date = _parse_date(_row_value(row, headers, "датазакрытияфакт"), book=book)
        plan_date = _parse_date(_row_value(row, headers, "датазакрытияплановая"), book=book)
        included.append({
            "company": str(_row_value(row, headers, "компания") or "").strip(),
            "department": str(_row_value(row, headers, "подразделение") or "").strip(),
            "vacancy": str(_row_value(row, headers, "вакансия") or "").strip(),
            "fact_date": fact_date,
            "plan_date": plan_date,
            "plan_close_month": str(plan_month_raw or "").strip(),
            "fact_close_month": str(fact_month_raw or "").strip(),
            "on_time": _closed_on_time(plan_month_raw, fact_month_raw),
        })

    debug.update({
        "status": "ok",
        "header_row": header_row + 1,
        "included_total": len(included),
        "skipped_non_a": skipped_non_a,
        "skipped_plan_month": skipped_plan_month,
        "excluded_total": excluded_count,
    })
    return included, debug


def _load_monthly_vacancies(
    ref_y: int,
    ref_m: int,
) -> tuple[dict[int, list[dict[str, Any]]], dict[str, Any]]:
    by_month: dict[int, list[dict[str, Any]]] = {}
    sources: list[dict[str, Any]] = []
    excluded_by_month = {m: 0 for m in range(1, 13)}
    excluded_reasons: dict[str, int] = {}

    for month in range(1, ref_m + 1):
        items, source_debug = _load_vacancies_for_report_month(ref_y, month)
        by_month[month] = items
        sources.append(source_debug)
        excluded_by_month[month] = source_debug.get("excluded_total", 0)

    return by_month, {
        "reports_dir": str(HC_REPORTS_DIR),
        "report_sources": sources,
        "excluded_by_month": excluded_by_month,
        "excluded_reasons": excluded_reasons,
    }


def _late_vacancy_rows(
    vacancies: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    late_items = [item for item in vacancies if not item["on_time"]]
    late_items.sort(
        key=lambda item: (
            item.get("fact_date") or date.max,
            item.get("company") or "",
            item.get("department") or "",
            item.get("vacancy") or "",
        )
    )
    for index, item in enumerate(late_items, start=1):
        rows.append({
            "number": index,
            "company": item.get("company") or "",
            "department": item.get("department") or "",
            "vacancy": item.get("vacancy") or "",
            "plan_close_date": _format_date(item.get("plan_date")),
            "fact_close_date": _format_date(item.get("fact_date")),
        })
    return rows


def _late_vacancies_table(
    vacancies: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> dict[str, Any]:
    return {
        "name": f"HRD-M1: вакансии, закрытые не в срок за {MONTH_NAMES[ref_m]} {ref_y}",
        "periodicity": "ежемесячно",
        "description": (
            "Разница между планом и фактом HRD-M1: критические вакансии типа A "
            f"с плановым месяцем закрытия {MONTH_NAMES[ref_m]}, закрытые не в срок."
        ),
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": [
            "Компания",
            "Подразделение",
            "Вакансия",
            "Дата закрытия плановая",
            "Дата закрытия факт",
        ],
        "rows": _late_vacancy_rows(vacancies, ref_y, ref_m),
    }


def _build_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    vacancies_by_month, debug = _load_monthly_vacancies(ref_y, ref_m)

    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        month_items = vacancies_by_month.get(m, [])
        plan = len(month_items)
        fact = sum(1 for item in month_items if item["on_time"])
        monthly_rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": round(fact / plan * 100, 2) if plan > 0 else None,
            "has_data": plan > 0,
            "values_unit": "шт.",
        })

    ref_row = monthly_rows[-1] if monthly_rows else None
    ref_vacancies = vacancies_by_month.get(ref_m, [])
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
            "values_unit": "шт.",
        },
        "reference_analytics": {
            "excluded_total": sum(debug["excluded_by_month"].get(m, 0) for m in range(1, ref_m + 1)),
            "excluded_by_month": {
                m: debug["excluded_by_month"].get(m, 0)
                for m in range(1, ref_m + 1)
            },
            "excluded_reasons": debug["excluded_reasons"],
            "note": "Исключённые вакансии не входят в план/факт HRD-M1.",
        },
        "tables": {
            "HRD-T-M1-LATE-VACANCIES": _late_vacancies_table(ref_vacancies, ref_y, ref_m),
        },
        "debug": {
            "kpi_id": "HRD-M1",
            "status": "ok",
            "rule": (
                "HC_сводный_{year}_{Month}.xls / sheet Вакансии; "
                "plan close month = report file month; type A; "
                "plan = count; fact = on-time if fact close month is set and not after plan close month"
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
        logger.exception("HRD-M1: не удалось сохранить кэш")


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


def get_hrd_m1_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)
    lock_key = f"sup_hrd_m1_{ref_y}_{ref_m:02d}"

    def _load_fresh() -> dict[str, Any] | None:
        source_mtime_ns = reports_mtime_ns(ref_y, ref_m)
        return _load_cache(cache_path, source_mtime_ns=source_mtime_ns, perpetual=perpetual)

    def _compute_and_save() -> dict[str, Any] | None:
        try:
            payload = _build_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("HRD-M1: ошибка расчёта вакансий")
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
