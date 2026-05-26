"""OData-логика KPI QD-M7: ``Document_ТД_ПредъявлениеПродукцииНаВыходнойКонтроль``."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any
from urllib.parse import quote

import requests

from qualdir.brak_report import AUTH, BASE, fetch_all

logger = logging.getLogger(__name__)

DOC_ENTITY = "Document_ТД_ПредъявлениеПродукцииНаВыходнойКонтроль"
SELECT_FIELDS = "Ref_Key,Number,Date,ДатаПринятоВРаботу,ДатаПроверкиОТК"
PAGE_SIZE = 5000


def month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def day_period_bounds(day: date) -> tuple[str, str]:
    start = f"{day.isoformat()}T00:00:00"
    end = f"{(day + timedelta(days=1)).isoformat()}T00:00:00"
    return start, end


def _fetch_rows(session: requests.Session, odata_filter: str, *, log_tag: str) -> list[dict[str, Any]]:
    base_url = (
        f"{BASE}/{quote(DOC_ENTITY)}?$format=json"
        f"&$filter={quote(odata_filter, safe='')}"
        f"&$select={quote(SELECT_FIELDS, safe=',_')}"
    )
    try:
        return fetch_all(session, base_url, page=PAGE_SIZE)
    except Exception as exc:
        logger.warning("%s: OData %s", log_tag, exc)
        return []


def _count_docs_by_date(session: requests.Session, year: int, month: int) -> tuple[int, list[dict[str, Any]]]:
    period_start, period_end = month_period_bounds(year, month)
    odata_filter = (
        "DeletionMark eq false"
        f" and Date ge datetime'{period_start}'"
        f" and Date lt datetime'{period_end}'"
    )
    rows = _fetch_rows(session, odata_filter, log_tag="QD-M7 docs")
    samples = [
        {
            "number": str(row.get("Number") or ""),
            "date": str(row.get("Date") or "")[:10],
        }
        for row in rows[:10]
    ]
    return len(rows), samples


def _count_by_field_today(
    session: requests.Session,
    field_name: str,
    *,
    as_of: date,
    log_tag: str,
) -> tuple[int, list[dict[str, Any]]]:
    period_start, period_end = day_period_bounds(as_of)
    odata_filter = (
        "DeletionMark eq false"
        f" and {field_name} ge datetime'{period_start}'"
        f" and {field_name} lt datetime'{period_end}'"
    )
    rows = _fetch_rows(session, odata_filter, log_tag=log_tag)
    samples = [
        {
            "number": str(row.get("Number") or ""),
            field_name: str(row.get(field_name) or "")[:19],
        }
        for row in rows[:10]
    ]
    return len(rows), samples


def compute_accepted_to_work_today(
    *,
    session: requests.Session | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Документы с ``ДатаПринятоВРаботу`` = ``as_of`` (по умолчанию сегодня)."""
    ref_day = as_of or date.today()
    own_session = session is None
    sess = session or requests.Session()
    if own_session:
        sess.auth = AUTH

    count, samples = _count_by_field_today(
        sess,
        "ДатаПринятоВРаботу",
        as_of=ref_day,
        log_tag="QD-M7 accepted",
    )
    period_start, period_end = day_period_bounds(ref_day)
    return {
        "accepted_to_work_today": count,
        "as_of": ref_day.isoformat(),
        "debug": {
            "status": "ok",
            "period_start": period_start,
            "period_end": period_end,
            "included_samples": samples,
        },
    }


def compute_checked_otk_today(
    *,
    session: requests.Session | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Документы с ``ДатаПроверкиОТК`` = ``as_of`` (по умолчанию сегодня)."""
    ref_day = as_of or date.today()
    own_session = session is None
    sess = session or requests.Session()
    if own_session:
        sess.auth = AUTH

    count, samples = _count_by_field_today(
        sess,
        "ДатаПроверкиОТК",
        as_of=ref_day,
        log_tag="QD-M7 checked",
    )
    period_start, period_end = day_period_bounds(ref_day)
    return {
        "checked_otk_today": count,
        "as_of": ref_day.isoformat(),
        "debug": {
            "status": "ok",
            "period_start": period_start,
            "period_end": period_end,
            "included_samples": samples,
        },
    }


def compute_vyhod_kontrol_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Метрики QD-M7 за календарный месяц (только счётчик по ``Date``)."""
    own_session = session is None
    sess = session or requests.Session()
    if own_session:
        sess.auth = AUTH

    period_start, period_end = month_period_bounds(year, month)
    try:
        docs_count, doc_samples = _count_docs_by_date(sess, year, month)
        return {
            "year": year,
            "month": month,
            "docs_count": docs_count,
            "has_data": True,
            "debug": {
                "status": "ok",
                "kpi_id": "QD-M7",
                "doc_entity": DOC_ENTITY,
                "period_start": period_start[:19],
                "period_end": period_end[:19],
                "docs_rule": "Date в месяце",
                "doc_samples": doc_samples,
            },
        }
    except Exception as exc:
        logger.exception("QD-M7: ошибка за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "docs_count": None,
            "has_data": False,
            "debug": {
                "status": "error",
                "kpi_id": "QD-M7",
                "error": str(exc),
            },
        }
