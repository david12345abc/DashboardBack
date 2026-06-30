"""METD-M3.F: ФОТ главного метролога из AccountingRegister_Хозрасчетный."""
from __future__ import annotations

import json
import logging
from calendar import monthrange
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from . import cache_manager
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_SOURCE_TAG = "metrolog_fot_account26_v1"
CACHE_VERSION = 1

ACCOUNT_26_ROOT = "fb2bde43-6250-11e7-812d-001e67112509"
SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

ARTICLE_SALARY = "524cd9aa-215a-11e0-b91c-00248c26ee57"
ARTICLE_INSURANCE = "b2913ba1-768e-11e7-812e-001e67112509"
ARTICLES = {
    ARTICLE_SALARY: "Оплата труда (26 сч) НПО АУП!",
    ARTICLE_INSURANCE: "Страховые взносы (26 сч) НПО АУП!",
}
ARTICLE_ORDER = (ARTICLE_SALARY, ARTICLE_INSURANCE)

DEPARTMENTS = {
    "433e13bb-f9a6-11ef-9623-6cb31113810c": "Главный метролог",
    "0577c412-d5fd-11e9-829b-ac1f6b05524d": "Отдел метрологии и сертификации",
    "4668a58a-6eb1-11e2-afce-001e67112509": "Метрологическая служба",
}
DEPARTMENT_ORDER = tuple(DEPARTMENTS.keys())

FOT_PLAN_BY_MONTH = {
    1: 1_010_527,
    2: 1_156_983,
    3: 1_176_478,
    4: 1_339_395,
    5: 1_201_925,
    6: 1_317_708,
    7: 1_583_928,
    8: 1_812_330,
    9: 1_665_424,
    10: 1_725_001,
    11: 1_816_719,
    12: 1_992_689,
}

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def cache_file_path_for_period(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"metrolog_fot_{int(year)}_{int(month):02d}.json"


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _fetch_all(session: requests.Session, url: str, page: int = 5000, timeout: int = 120) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        response = request_with_retry(session, page_url, timeout=timeout, retries=4, label="metrolog_fot")
        if response is None:
            return rows
        if not response.ok:
            logger.error("METD-M3.F HTTP %s: %s", response.status_code, response.text[:300])
            return rows
        batch = response.json().get("value", [])
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def _get_subaccounts(session: requests.Session, parent_guid: str) -> set[str]:
    collected = {parent_guid}
    frontier = [parent_guid]
    while frontier:
        parent = frontier.pop()
        flt = f"Parent_Key eq guid'{parent}'"
        url = (
            f"{BASE}/{quote('ChartOfAccounts_Хозрасчетный')}"
            f"?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select=Ref_Key,Code,Description,Parent_Key"
        )
        for row in _fetch_all(session, url, page=200, timeout=60):
            key = row.get("Ref_Key")
            if key and key not in collected:
                collected.add(key)
                frontier.append(key)
    return collected


def _load_cache(year: int, month: int, *, allow_stale: bool = False) -> dict | None:
    path = cache_file_path_for_period(year, month)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_source") != CACHE_SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if not allow_stale and data.get("cache_date") != date.today().isoformat():
        return None
    return data


def _save_cache(year: int, month: int, payload: dict) -> None:
    data = {
        **payload,
        "cache_source": CACHE_SOURCE_TAG,
        "cache_version": CACHE_VERSION,
        "cache_date": date.today().isoformat(),
    }
    try:
        cache_file_path_for_period(year, month).write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        logger.exception("Не удалось сохранить кэш METD-M3.F")


def _empty_department_row() -> dict:
    return {
        "salary": 0.0,
        "insurance": 0.0,
        "total": 0.0,
        "rows": 0,
        "by_article": {article_key: 0.0 for article_key in ARTICLE_ORDER},
    }


def compute_metrolog_fot_month(year: int, month: int) -> dict:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached
    if not cache_manager.is_force_compute_context():
        stale = _load_cache(year, month, allow_stale=True)
        if stale is not None:
            stale = dict(stale)
            stale["cache_refresh_status"] = "running"
            return stale

    session = requests.Session()
    session.auth = AUTH
    try:
        period_start, period_end = _month_bounds(year, month)
        account_keys = _get_subaccounts(session, ACCOUNT_26_ROOT)
        account_filter = " or ".join(f"AccountDr_Key eq guid'{key}'" for key in sorted(account_keys))
        flt = (
            f"Period ge datetime'{period_start}'"
            f" and Period lt datetime'{period_end}'"
            f" and Active eq true"
            f" and ({account_filter})"
        )
        select = ",".join([
            "Period", "AccountDr_Key", "ПодразделениеDr_Key",
            "Сумма", "Сторно", "ExtDimensionDr1", "ExtDimensionTypeDr1_Key",
        ])
        url = (
            f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
            f"?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        records = _fetch_all(session, url)

        by_dept = {dept_key: _empty_department_row() for dept_key in DEPARTMENT_ORDER}
        skipped = defaultdict(int)
        records_taken = 0

        for rec in records:
            dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
            if dept_key not in by_dept:
                skipped["not_target_department"] += 1
                continue
            if rec.get("ExtDimensionTypeDr1_Key") != SUBCONTO_TYPE_COST:
                skipped["not_cost_article_subconto"] += 1
                continue
            article_key = rec.get("ExtDimensionDr1") or EMPTY
            if article_key not in ARTICLES:
                skipped["not_target_article"] += 1
                continue

            amount = float(rec.get("Сумма", 0) or 0)
            if rec.get("Сторно"):
                amount = -amount

            row = by_dept[dept_key]
            row["by_article"][article_key] += amount
            row["total"] += amount
            row["rows"] += 1
            records_taken += 1
            if article_key == ARTICLE_SALARY:
                row["salary"] += amount
            elif article_key == ARTICLE_INSURANCE:
                row["insurance"] += amount

        matrix = []
        missing_combinations = []
        for dept_key in DEPARTMENT_ORDER:
            row = by_dept[dept_key]
            article_values = {
                article_key: round(float(row["by_article"][article_key]), 2)
                for article_key in ARTICLE_ORDER
            }
            for article_key, value in article_values.items():
                if abs(value) == 0:
                    missing_combinations.append({
                        "department_key": dept_key,
                        "department": DEPARTMENTS[dept_key],
                        "article_key": article_key,
                        "article": ARTICLES[article_key],
                    })
            matrix.append({
                "department_key": dept_key,
                "department": DEPARTMENTS[dept_key],
                "salary": round(float(row["salary"]), 2),
                "insurance": round(float(row["insurance"]), 2),
                "total": round(float(row["total"]), 2),
                "rows": row["rows"],
                "by_article": article_values,
            })

        total_fact = round(sum(row["total"] for row in matrix), 2)
        plan = float(FOT_PLAN_BY_MONTH.get(month, 0))
        payload = {
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
            "period": {"start": period_start[:10], "end": period_end[:10]},
            "plan": plan,
            "fact": total_fact,
            "kpi_pct": round(total_fact / plan * 100, 1) if plan else None,
            "has_data": records_taken > 0,
            "values_unit": "руб.",
            "matrix": matrix,
            "missing_combinations": missing_combinations,
            "totals": {
                "salary": round(sum(row["salary"] for row in matrix), 2),
                "insurance": round(sum(row["insurance"] for row in matrix), 2),
                "total": total_fact,
            },
            "debug": {
                "source": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
                "account": "26",
                "account_root": ACCOUNT_26_ROOT,
                "departments": DEPARTMENTS,
                "articles": ARTICLES,
                "records_total": len(records),
                "records_taken": records_taken,
                "skipped": dict(skipped),
                "accumulation_registers_used": False,
                "children_included": False,
            },
        }
        _save_cache(year, month, payload)
        return payload
    finally:
        session.close()


def get_metrolog_fot_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_y = int(year or today.year)
    ref_m = max(1, min(12, int(month or today.month)))
    if ref_y == today.year and ref_m > today.month:
        ref_m = today.month

    rows = []
    cache_refresh_running = False
    for m in range(1, ref_m + 1):
        month_payload = compute_metrolog_fot_month(ref_y, m)
        cache_refresh_running = cache_refresh_running or month_payload.get("cache_refresh_status") == "running"
        rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": month_payload.get("plan"),
            "fact": month_payload.get("fact"),
            "kpi_pct": month_payload.get("kpi_pct"),
            "has_data": month_payload.get("has_data"),
            "values_unit": "руб.",
        })

    ref_row = rows[-1] if rows else None
    result = {
        "data_granularity": "monthly",
        "monthly_data": rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in rows if row.get("has_data")),
            "months_total": len(rows),
            "values_unit": "руб.",
        },
        "kpi_period": {
            "type": "current_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "debug": {
            "source": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
            "plan_source": "ФОТ ПЛАН Главного метролога",
            "accumulation_registers_used": False,
        },
    }
    if cache_refresh_running:
        result["cache_refresh_status"] = "running"
    return result


def get_metrolog_fot_table(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_y = int(year or today.year)
    ref_m = max(1, min(12, int(month or today.month)))
    month_payload = compute_metrolog_fot_month(ref_y, ref_m)
    table = {
        "name": "ФОТ главного метролога",
        "periodicity": "ежемесячно",
        "description": (
            "Дебетовый оборот счёта 26 из AccountingRegister_Хозрасчетный "
            "по точным подразделениям и статьям ФОТ."
        ),
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
        "columns": [
            "Подразделение",
            "Оплата труда (26 сч) НПО АУП!",
            "Страховые взносы (26 сч) НПО АУП!",
            "Итого ФОТ",
        ],
        "rows": [
            {
                "Подразделение": row["department"],
                "Оплата труда (26 сч) НПО АУП!": row["salary"],
                "Страховые взносы (26 сч) НПО АУП!": row["insurance"],
                "Итого ФОТ": row["total"],
            }
            for row in month_payload["matrix"]
        ],
        "totals": month_payload["totals"],
        "missing_combinations": month_payload["missing_combinations"],
        "debug": month_payload["debug"],
    }
    if month_payload.get("cache_refresh_status"):
        table["cache_refresh_status"] = month_payload.get("cache_refresh_status")
    return table
