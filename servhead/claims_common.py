"""Общая загрузка претензий из OData для плиток servhead (SH-M*)."""
from __future__ import annotations

import calendar
import logging
import os
from collections import defaultdict
from datetime import date
from typing import Any, Callable
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from getkpi.odata_http import request_with_retry

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
CLAIMS_ENTITY = "Catalog_Претензии"

STATUS_SATISFIED = "Удовлетворена"
STATUS_REGISTERED = "Зарегистрирована"
STATUS_IN_PROCESS = frozenset({
    "Обрабатывается",
    "В обработке",
})

FIELD_DATE_PLAN = "ТД_ДатаОкончанияПлан"
FIELD_DATE_FACT = "ДатаОкончания"
PARTNER_FIELD = "Партнер_Key"
PARTNERS_ENTITY = "Catalog_Партнеры"
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


def _normalize_odata_base(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    if base.endswith("/odata/standard.odata"):
        return base
    return f"{base}/odata/standard.odata"


BASE = _normalize_odata_base(os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL))
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)


def kpi_pct(plan: int, fact: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _period_bounds(year: int, month_from: int, month_to: int) -> tuple[str, str]:
    date_from = f"{year}-{month_from:02d}-01T00:00:00"
    last_day = calendar.monthrange(year, month_to)[1]
    date_to = f"{year}-{month_to:02d}-{last_day}T23:59:59"
    return date_from, date_to


def _parse_registration_month(value: str | None) -> tuple[int, int] | None:
    if not value or len(value) < 7:
        return None
    try:
        year = int(value[:4])
        month = int(value[5:7])
    except ValueError:
        return None
    if not 1 <= month <= 12:
        return None
    return year, month


def _status_matches(raw_status: object, fact_statuses: frozenset[str]) -> bool:
    return (str(raw_status or "").strip()) in fact_statuses


def parse_odata_date(value: str | None) -> date | None:
    if not value or len(value) < 10:
        return None
    try:
        year = int(value[:4])
        month = int(value[5:7])
        day = int(value[8:10])
    except ValueError:
        return None
    if year < 1900:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def is_completed_on_time(row: dict[str, Any]) -> bool:
    """Факт исполнения ≤ плановой даты окончания."""
    fact_d = parse_odata_date(row.get(FIELD_DATE_FACT))
    plan_d = parse_odata_date(row.get(FIELD_DATE_PLAN))
    if fact_d is None or plan_d is None:
        return False
    return fact_d <= plan_d


def is_completed_late(row: dict[str, Any]) -> bool:
    """Факт исполнения > плановой даты окончания."""
    fact_d = parse_odata_date(row.get(FIELD_DATE_FACT))
    plan_d = parse_odata_date(row.get(FIELD_DATE_PLAN))
    if fact_d is None or plan_d is None:
        return False
    return fact_d > plan_d


def fetch_claims_sla_counts_by_month(
    session: requests.Session,
    *,
    year: int,
    month_from: int,
    month_to: int,
    log_label: str,
    fact_match: Callable[[dict[str, Any]], bool],
) -> dict[int, dict[str, int]]:
    """План — все обращения за месяц; факт — по предикату SLA (в срок / не в срок)."""
    date_from, date_to = _period_bounds(year, month_from, month_to)
    odata_filter = (
        f"ДатаРегистрации ge datetime'{date_from}'"
        f" and ДатаРегистрации le datetime'{date_to}'"
    )
    select = quote(
        f"Ref_Key,ДатаРегистрации,{FIELD_DATE_FACT},{FIELD_DATE_PLAN},DeletionMark",
        safe=",_",
    )

    counts: dict[int, dict[str, int]] = defaultdict(lambda: {"plan": 0, "fact": 0})
    skip = 0
    page = 5000

    while True:
        url = (
            f"{BASE}/{quote(CLAIMS_ENTITY)}?$format=json"
            f"&$select={select}&$top={page}&$skip={skip}"
            f"&$filter={quote(odata_filter, safe='')}"
        )
        response = request_with_retry(session, url, timeout=120, retries=4, label=log_label)
        if response is None or not response.ok:
            logger.error(
                "%s: OData error HTTP %s",
                log_label,
                response.status_code if response is not None else "drop",
            )
            break
        rows = response.json().get("value") or []
        if not rows:
            break
        for row in rows:
            if row.get("DeletionMark"):
                continue
            parsed = _parse_registration_month(row.get("ДатаРегистрации"))
            if parsed is None or parsed[0] != year:
                continue
            month = parsed[1]
            if month < month_from or month > month_to:
                continue
            counts[month]["plan"] += 1
            if fact_match(row):
                counts[month]["fact"] += 1
        if len(rows) < page:
            break
        skip += len(rows)

    return counts


def fetch_claims_for_registration_month(
    session: requests.Session,
    *,
    year: int,
    month: int,
    log_label: str,
) -> list[dict[str, Any]]:
    """Все претензии за месяц регистрации (для таблиц по клиентам)."""
    date_from, date_to = _period_bounds(year, month, month)
    odata_filter = (
        f"ДатаРегистрации ge datetime'{date_from}'"
        f" and ДатаРегистрации le datetime'{date_to}'"
    )
    select = quote(
        f"Ref_Key,ДатаРегистрации,{PARTNER_FIELD},{FIELD_DATE_FACT},"
        f"{FIELD_DATE_PLAN},DeletionMark",
        safe=",_",
    )

    result: list[dict[str, Any]] = []
    skip = 0
    page = 5000

    while True:
        url = (
            f"{BASE}/{quote(CLAIMS_ENTITY)}?$format=json"
            f"&$select={select}&$top={page}&$skip={skip}"
            f"&$filter={quote(odata_filter, safe='')}"
        )
        response = request_with_retry(session, url, timeout=120, retries=4, label=log_label)
        if response is None or not response.ok:
            logger.error(
                "%s: OData error HTTP %s",
                log_label,
                response.status_code if response is not None else "drop",
            )
            break
        rows = response.json().get("value") or []
        if not rows:
            break
        for row in rows:
            if row.get("DeletionMark"):
                continue
            parsed = _parse_registration_month(row.get("ДатаРегистрации"))
            if parsed != (year, month):
                continue
            result.append(row)
        if len(rows) < page:
            break
        skip += len(rows)

    return result


def load_partner_names(session: requests.Session) -> dict[str, str]:
    """Справочник партнёров Ref_Key → наименование."""
    names: dict[str, str] = {}
    skip = 0
    page = 5000
    select = quote("Ref_Key,Description", safe=",_")
    while True:
        url = (
            f"{BASE}/{quote(PARTNERS_ENTITY)}?$format=json"
            f"&$select={select}&$top={page}&$skip={skip}&$orderby=Ref_Key"
        )
        response = request_with_retry(session, url, timeout=120, retries=4, label="SH-Partners")
        if response is None or not response.ok:
            break
        rows = response.json().get("value") or []
        if not rows:
            break
        for row in rows:
            key = row.get("Ref_Key")
            if key:
                names[key] = (row.get("Description") or "").strip() or key
        if len(rows) < page:
            break
        skip += len(rows)
    return names


def partner_display_name(partner_key: str | None, partners: dict[str, str]) -> str:
    key = (partner_key or "").strip() or EMPTY_GUID
    if key == EMPTY_GUID:
        return "—"
    return partners.get(key) or key


def aggregate_client_sla_rows(
    claims: list[dict[str, Any]],
    partners: dict[str, str],
) -> list[dict[str, Any]]:
    """Агрегация: клиент → всего / в срок / не в срок."""
    buckets: dict[str, dict[str, int]] = defaultdict(lambda: {
        "total": 0,
        "on_time": 0,
        "late": 0,
    })
    labels: dict[str, str] = {}

    for row in claims:
        key = (row.get(PARTNER_FIELD) or "").strip() or EMPTY_GUID
        labels[key] = partner_display_name(key, partners)
        buckets[key]["total"] += 1
        if is_completed_on_time(row):
            buckets[key]["on_time"] += 1
        elif is_completed_late(row):
            buckets[key]["late"] += 1

    rows: list[dict[str, Any]] = []
    for key, counts in buckets.items():
        rows.append({
            "Клиент": labels[key],
            "Всего обращений": counts["total"],
            "В срок": counts["on_time"],
            "Не в срок": counts["late"],
            "client_key": None if key == EMPTY_GUID else key,
        })
    rows.sort(key=lambda item: (-int(item["Всего обращений"]), str(item["Клиент"])))
    return rows


def fetch_claims_counts_by_month(
    session: requests.Session,
    *,
    year: int,
    month_from: int,
    month_to: int,
    fact_statuses: frozenset[str],
    log_label: str,
) -> dict[int, dict[str, int]]:
    """Возвращает {месяц: {"plan": n, "fact": m}} за диапазон месяцев одного года."""
    date_from, date_to = _period_bounds(year, month_from, month_to)
    odata_filter = (
        f"ДатаРегистрации ge datetime'{date_from}'"
        f" and ДатаРегистрации le datetime'{date_to}'"
    )
    select = quote("Ref_Key,ДатаРегистрации,Статус,DeletionMark", safe=",_")

    counts: dict[int, dict[str, int]] = defaultdict(lambda: {"plan": 0, "fact": 0})
    skip = 0
    page = 5000

    while True:
        url = (
            f"{BASE}/{quote(CLAIMS_ENTITY)}?$format=json"
            f"&$select={select}&$top={page}&$skip={skip}"
            f"&$filter={quote(odata_filter, safe='')}"
        )
        response = request_with_retry(session, url, timeout=120, retries=4, label=log_label)
        if response is None or not response.ok:
            logger.error(
                "%s: OData error HTTP %s",
                log_label,
                response.status_code if response is not None else "drop",
            )
            break
        rows = response.json().get("value") or []
        if not rows:
            break
        for row in rows:
            if row.get("DeletionMark"):
                continue
            parsed = _parse_registration_month(row.get("ДатаРегистрации"))
            if parsed is None or parsed[0] != year:
                continue
            month = parsed[1]
            if month < month_from or month > month_to:
                continue
            counts[month]["plan"] += 1
            if _status_matches(row.get("Статус"), fact_statuses):
                counts[month]["fact"] += 1
        if len(rows) < page:
            break
        skip += len(rows)

    return counts


def build_kpi_period(ref_y: int, ref_m: int, month_names: dict[int, str]) -> dict[str, Any]:
    """Текущий календарный месяц — ``current_month``, завершённые — ``last_full_month``."""
    today = date.today()
    period_type = (
        "current_month"
        if (ref_y, ref_m) >= (today.year, today.month)
        else "last_full_month"
    )
    return {
        "type": period_type,
        "year": ref_y,
        "month": ref_m,
        "month_name": month_names[ref_m],
    }


def empty_error_payload(
    *,
    kpi_id: str,
    ref_y: int,
    ref_m: int,
    month_names: dict[int, str],
    error: str,
) -> dict[str, Any]:
    return {
        "data_granularity": "monthly",
        "monthly_data": [],
        "last_full_month_row": None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": month_names[ref_m],
        },
        "ytd": {
            "total_plan": None,
            "total_fact": None,
            "kpi_pct": None,
            "months_with_data": 0,
            "months_total": 0,
            "values_unit": "шт.",
        },
        "debug": {"kpi_id": kpi_id, "status": "error", "error": error},
    }
