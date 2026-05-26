"""OData-логика KPI QD-M6: ``Document_ТД_ПредъявлениеТМЦнаОТК``."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any
from urllib.parse import quote

import requests

from qualdir.brak_report import AUTH, BASE, fetch_all

logger = logging.getLogger(__name__)

DOC_ENTITY = "Document_ТД_ПредъявлениеТМЦнаОТК"
SELECT_FIELDS = "Ref_Key,Number,Date,СрокИсполнения,ДатаПроверкиОТК"
PAGE_SIZE = 5000


def month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def day_period_bounds(day: date) -> tuple[str, str]:
    start = f"{day.isoformat()}T00:00:00"
    end = f"{(day + timedelta(days=1)).isoformat()}T00:00:00"
    return start, end


def _is_filled_datetime(value: Any) -> bool:
    if value in (None, ""):
        return False
    return str(value)[:10] not in ("0001-01-01", "")


def _date_part(value: Any) -> date | None:
    if not _is_filled_datetime(value):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


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
    rows = _fetch_rows(session, odata_filter, log_tag="QD-M6 docs")
    samples = [
        {
            "number": str(row.get("Number") or ""),
            "date": str(row.get("Date") or "")[:10],
        }
        for row in rows[:10]
    ]
    return len(rows), samples


def _count_delays_by_deadline(session: requests.Session, year: int, month: int) -> tuple[int, list[dict[str, Any]]]:
    period_start, period_end = month_period_bounds(year, month)
    odata_filter = (
        "DeletionMark eq false"
        f" and СрокИсполнения ge datetime'{period_start}'"
        f" and СрокИсполнения lt datetime'{period_end}'"
    )
    rows = _fetch_rows(session, odata_filter, log_tag="QD-M6 delays")
    delay_rows: list[dict[str, Any]] = []
    for row in rows:
        deadline = _date_part(row.get("СрокИсполнения"))
        checked = _date_part(row.get("ДатаПроверкиОТК"))
        if deadline is None or checked is None:
            continue
        if deadline < checked:
            delay_rows.append(row)

    samples = [
        {
            "number": str(row.get("Number") or ""),
            "deadline": str(row.get("СрокИсполнения") or "")[:10],
            "checked_at": str(row.get("ДатаПроверкиОТК") or "")[:10],
        }
        for row in delay_rows[:10]
    ]
    return len(delay_rows), samples


def compute_in_work_today(
    *,
    session: requests.Session | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Документы со сроком исполнения = ``as_of`` (по умолчанию сегодня)."""
    ref_day = as_of or date.today()
    period_start, period_end = day_period_bounds(ref_day)
    own_session = session is None
    sess = session or requests.Session()
    if own_session:
        sess.auth = AUTH

    odata_filter = (
        "DeletionMark eq false"
        f" and СрокИсполнения ge datetime'{period_start}'"
        f" and СрокИсполнения lt datetime'{period_end}'"
    )
    rows = _fetch_rows(sess, odata_filter, log_tag="QD-M6 in_work")
    count = len(rows)
    samples = [
        {
            "number": str(row.get("Number") or ""),
            "deadline": str(row.get("СрокИсполнения") or "")[:19],
        }
        for row in rows[:10]
    ]
    return {
        "in_work_today": count,
        "as_of": ref_day.isoformat(),
        "debug": {
            "status": "ok",
            "period_start": period_start,
            "period_end": period_end,
            "included_samples": samples,
        },
    }


def compute_otk_predyavlenie_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Метрики QD-M6 за календарный месяц."""
    own_session = session is None
    sess = session or requests.Session()
    if own_session:
        sess.auth = AUTH

    period_start, period_end = month_period_bounds(year, month)
    try:
        docs_count, doc_samples = _count_docs_by_date(sess, year, month)
        delay_count, delay_samples = _count_delays_by_deadline(sess, year, month)
        return {
            "year": year,
            "month": month,
            "docs_count": docs_count,
            "delay_count": delay_count,
            "has_data": True,
            "debug": {
                "status": "ok",
                "kpi_id": "QD-M6",
                "doc_entity": DOC_ENTITY,
                "period_start": period_start[:19],
                "period_end": period_end[:19],
                "docs_rule": "Date в месяце",
                "delay_rule": (
                    "СрокИсполнения в месяце и СрокИсполнения < ДатаПроверкиОТК"
                ),
                "doc_samples": doc_samples,
                "delay_samples": delay_samples,
            },
        }
    except Exception as exc:
        logger.exception("QD-M6: ошибка за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "docs_count": None,
            "delay_count": None,
            "has_data": False,
            "debug": {
                "status": "error",
                "kpi_id": "QD-M6",
                "error": str(exc),
            },
        }
