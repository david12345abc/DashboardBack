"""HRD-M9 — укомплектованность штата (НПО).

Формула месяца:
  fact% = сотрудники НПО (не уволенные на конец месяца)
        / (сотрудники НПО + актуальные вакансии) × 100%

Актуальные вакансии — лист «Вакансии» из HC_сводный_{год}_{месяц}:
  • компания = «НПО Турбулентность-Дон»
  • «Дата закрытия факт» пустая
  Для каждого месяца дашборда — свой файл отчёта.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from qualdir.sql_tile_cache import get_ytd_via_cache
from sup.hc_reports import HC_REPORTS_DIR, hc_report_path, open_hc_workbook, reports_mtime_ns
from sup.hrd_m1 import (
    SHEET_NAME,
    _find_header_row,
    _normalize_text,
    _open_vacancies_sheet,
    _parse_date,
    _row_value,
)
from sup.npo_ssc import NPO_ORG_GUID, calc_org_headcount_month_end

logger = logging.getLogger(__name__)

KPI_ID = "HRD-M9"
CACHE_PREFIX = "sup_hrd_m9_staffing"
CACHE_SOURCE_TAG = "sup_hrd_m9_staffing_payload_v1"
CACHE_VERSION = 1
VALUES_UNIT = "%"

# Точное имя компании в HC_сводный (лист Вакансии).
NPO_COMPANY_NAME = "НПО Турбулентность-Дон"


def _is_npo_company(value: Any) -> bool:
    text = _normalize_text(value)
    target = _normalize_text(NPO_COMPANY_NAME)
    return text == target or text.startswith(target)


def _is_blank_fact_date(value: Any, *, book: Any) -> bool:
    return _parse_date(value, book=book) is None


def _load_open_vacancies_npo(year: int, month: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = hc_report_path(year, month)
    debug: dict[str, Any] = {
        "month": month,
        "source_file": str(path),
        "sheet": SHEET_NAME,
        "company_filter": NPO_COMPANY_NAME,
    }
    if not path.exists():
        debug["status"] = "missing_file"
        return [], debug

    book = None
    try:
        book = open_hc_workbook(path)
        sheet = _open_vacancies_sheet(book)
        header_row, headers = _find_header_row(sheet)
    except Exception as exc:
        logger.warning("HRD-M9: не удалось прочитать %s: %s", path, exc)
        debug["status"] = "read_error"
        debug["error"] = str(exc)
        if book is not None and hasattr(book, "close"):
            book.close()
        return [], debug

    open_rows: list[dict[str, Any]] = []
    skipped_other_company = 0
    skipped_closed = 0
    total_rows = 0

    for row_idx in range(header_row + 1, sheet.nrows):
        row = tuple(sheet.row_values(row_idx))
        vacancy = str(_row_value(row, headers, "вакансия") or "").strip()
        company = str(_row_value(row, headers, "компания") or "").strip()
        if not vacancy and not company:
            continue
        total_rows += 1
        if not _is_npo_company(company):
            skipped_other_company += 1
            continue
        fact_date_raw = _row_value(row, headers, "датазакрытияфакт")
        if not _is_blank_fact_date(fact_date_raw, book=book):
            skipped_closed += 1
            continue
        open_rows.append(
            {
                "company": company,
                "department": str(_row_value(row, headers, "подразделение") or "").strip(),
                "vacancy": vacancy,
                "cfo": str(_row_value(row, headers, "цфо") or "").strip(),
                "type": str(_row_value(row, headers, "авс") or "").strip(),
            }
        )

    debug.update(
        {
            "status": "ok",
            "header_row": header_row + 1,
            "rows_scanned": total_rows,
            "open_vacancies": len(open_rows),
            "skipped_other_company": skipped_other_company,
            "skipped_closed_fact_date": skipped_closed,
        }
    )
    if hasattr(book, "close"):
        book.close()
    return open_rows, debug


def _staffing_pct(employees: int, vacancies: int) -> float | None:
    denom = employees + vacancies
    if denom <= 0:
        return None
    return round(employees / denom * 100, 1)


def build_hrd_m9_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    headcount = calc_org_headcount_month_end(ref_y, ref_m, org_guids=(NPO_ORG_GUID,))
    emp_by_m: dict[int, int] = headcount.get("months") or {}

    monthly_rows: list[dict[str, Any]] = []
    vacancy_debug: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        employees = int(emp_by_m.get(m) or 0)
        open_vacs, vdebug = _load_open_vacancies_npo(ref_y, m)
        vacancy_debug.append(vdebug)
        vacancies = len(open_vacs)
        pct = _staffing_pct(employees, vacancies)
        has_data = vdebug.get("status") == "ok" or employees > 0
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": None,
                "fact": pct,
                "kpi_pct": pct,
                "employees": employees,
                "vacancies": vacancies,
                "has_data": has_data and pct is not None,
                "values_unit": VALUES_UNIT,
                "source_file": vdebug.get("source_file"),
            }
        )

    ref_row = next((row for row in monthly_rows if row["month"] == ref_m), None)
    if ref_row is None:
        ref_row = {
            "month": ref_m,
            "year": ref_y,
            "month_name": MONTH_NAMES[ref_m],
            "plan": None,
            "fact": None,
            "kpi_pct": None,
            "employees": 0,
            "vacancies": 0,
            "has_data": False,
            "values_unit": VALUES_UNIT,
        }

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": None,
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": VALUES_UNIT,
            "employees": ref_row.get("employees"),
            "vacancies": ref_row.get("vacancies"),
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok",
            "reports_dir": str(HC_REPORTS_DIR),
            "source_mtime_ns": reports_mtime_ns(ref_y, ref_m),
            "rule": (
                "fact% = NPO employees (not dismissed, month-end) / "
                "(employees + open vacancies) × 100; "
                "open vacancy = company НПО Турбулентность-Дон and empty Дата закрытия факт; "
                "each month uses its own HC_сводный file"
            ),
            "headcount": {
                "departments_count": headcount.get("departments_count"),
                "employees_tracked": headcount.get("employees_tracked"),
                "organization_key": NPO_ORG_GUID,
            },
            "vacancy_sources": vacancy_debug,
            "rows_by_month": [
                {
                    "month": row["month"],
                    "employees": row["employees"],
                    "vacancies": row["vacancies"],
                    "fact": row["fact"],
                }
                for row in monthly_rows
            ],
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    from devdir import ytd_json_cache

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, ref_y, ref_m)


def get_hrd_m9_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="sup_hrd_m9_staffing",
        compute_fn=lambda y, m: build_hrd_m9_payload(y, m),
        kpi_id=KPI_ID,
    )


def main() -> int:
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    args = [a for a in sys.argv[1:] if a.strip()]
    year = int(args[0]) if args else None
    month = int(args[1]) if len(args) > 1 else None
    payload = build_hrd_m9_payload(year=year, month=month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
