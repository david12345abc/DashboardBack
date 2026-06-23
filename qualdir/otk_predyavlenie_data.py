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
SELECT_FIELDS = "Ref_Key,Number,Date,СрокИсполнения,ДатаПроверкиОТК,Posted,Организация_Key"
SELECT_REJECTION_FIELDS = "Ref_Key,Number,ТоварыДляОТК"
PAGE_SIZE = 5000
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"

ORG_NPO_KEY = "fbca2148-6cfd-11e7-812d-001e67112509"
ORG_ALMAZ_KEY = "fbca2146-6cfd-11e7-812d-001e67112509"
ORG_NPO_NAME = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
ORG_ALMAZ_NAME = "АЛМАЗ ООО"


def _org_filter_clause(organization_key: str | None) -> str:
    if not organization_key:
        return ""
    return f" and Организация_Key eq guid'{organization_key}'"


def _month_docs_filter(
    period_start: str,
    period_end: str,
    *,
    organization_key: str | None = None,
) -> str:
    """Только не удалённые и проведённые документы за период по ``Date``."""
    return (
        "DeletionMark eq false"
        " and Posted eq true"
        f" and Date ge datetime'{period_start}'"
        f" and Date lt datetime'{period_end}'"
        f"{_org_filter_clause(organization_key)}"
    )


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


def _fetch_rows(
    session: requests.Session,
    odata_filter: str,
    *,
    log_tag: str,
    select_fields: str = SELECT_FIELDS,
) -> list[dict[str, Any]]:
    base_url = (
        f"{BASE}/{quote(DOC_ENTITY)}?$format=json"
        f"&$filter={quote(odata_filter, safe='')}"
        f"&$select={quote(select_fields, safe=',_')}"
    )
    try:
        return fetch_all(session, base_url, page=PAGE_SIZE)
    except Exception as exc:
        logger.warning("%s: OData %s", log_tag, exc)
        return []


def _count_docs_by_date(
    session: requests.Session,
    year: int,
    month: int,
    *,
    organization_key: str | None = None,
    log_tag: str = "QD-M6 docs",
) -> tuple[int, list[dict[str, Any]]]:
    period_start, period_end = month_period_bounds(year, month)
    odata_filter = _month_docs_filter(period_start, period_end, organization_key=organization_key)
    rows = _fetch_rows(session, odata_filter, log_tag=log_tag)
    samples = [
        {
            "number": str(row.get("Number") or ""),
            "date": str(row.get("Date") or "")[:10],
        }
        for row in rows[:10]
    ]
    return len(rows), samples


def _count_executed_by_date(
    session: requests.Session,
    year: int,
    month: int,
    *,
    organization_key: str | None = None,
    log_tag: str = "QD-M6 executed",
) -> tuple[int, list[dict[str, Any]]]:
    period_start, period_end = month_period_bounds(year, month)
    odata_filter = _month_docs_filter(period_start, period_end, organization_key=organization_key)
    rows = _fetch_rows(session, odata_filter, log_tag=log_tag)
    executed_rows = [row for row in rows if _is_filled_datetime(row.get("ДатаПроверкиОТК"))]
    samples = [
        {
            "number": str(row.get("Number") or ""),
            "date": str(row.get("Date") or "")[:10],
            "checked_at": str(row.get("ДатаПроверкиОТК") or "")[:10],
        }
        for row in executed_rows[:10]
    ]
    return len(executed_rows), samples


def _line_is_rejected_nomenclature(item: dict[str, Any]) -> bool:
    """Строка ТЧ ``ТоварыДляОТК`` с забракованным наименованием."""
    try:
        rejected_qty = float(item.get("НеПринятоОТК") or 0)
    except (TypeError, ValueError):
        rejected_qty = 0.0
    if rejected_qty > 0:
        return True
    act_key = str(item.get("АктОТКоНесоответствии_Key") or "").strip()
    return bool(act_key and act_key != EMPTY_GUID)


def _count_rejected_nomenclature(
    session: requests.Session,
    year: int,
    month: int,
    *,
    organization_key: str | None = None,
    log_tag: str = "QD-M6 rejected",
) -> tuple[int, list[dict[str, Any]]]:
    period_start, period_end = month_period_bounds(year, month)
    odata_filter = _month_docs_filter(period_start, period_end, organization_key=organization_key)
    rows = _fetch_rows(
        session,
        odata_filter,
        log_tag=log_tag,
        select_fields=SELECT_REJECTION_FIELDS,
    )
    rejected_count = 0
    samples: list[dict[str, Any]] = []
    for doc in rows:
        for item in doc.get("ТоварыДляОТК") or []:
            if not _line_is_rejected_nomenclature(item):
                continue
            rejected_count += 1
            if len(samples) < 10:
                samples.append(
                    {
                        "number": str(doc.get("Number") or ""),
                        "nomenclature_key": str(item.get("Номенклатура_Key") or ""),
                        "rejected_qty": float(item.get("НеПринятоОТК") or 0),
                    }
                )
    return rejected_count, samples


def compute_in_work_today(
    *,
    session: requests.Session | None = None,
    as_of: date | None = None,
    organization_key: str | None = None,
    log_tag: str = "QD-M6 in_work",
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
        " and Posted eq true"
        f" and СрокИсполнения ge datetime'{period_start}'"
        f" and СрокИсполнения lt datetime'{period_end}'"
        f"{_org_filter_clause(organization_key)}"
    )
    rows = _fetch_rows(sess, odata_filter, log_tag=log_tag)
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
    organization_key: str | None = None,
    organization_name: str | None = None,
    kpi_id: str = "QD-M6",
    log_prefix: str = "QD-M6",
) -> dict[str, Any]:
    """Метрики предъявлений на ОТК за календарный месяц."""
    own_session = session is None
    sess = session or requests.Session()
    if own_session:
        sess.auth = AUTH

    period_start, period_end = month_period_bounds(year, month)
    org_rule = (
        f"Организация = {organization_name}"
        if organization_key and organization_name
        else "все организации"
    )
    try:
        docs_count, doc_samples = _count_docs_by_date(
            sess, year, month,
            organization_key=organization_key,
            log_tag=f"{log_prefix} docs",
        )
        executed_count, executed_samples = _count_executed_by_date(
            sess, year, month,
            organization_key=organization_key,
            log_tag=f"{log_prefix} executed",
        )
        rejected_count, rejected_samples = _count_rejected_nomenclature(
            sess, year, month,
            organization_key=organization_key,
            log_tag=f"{log_prefix} rejected",
        )
        return {
            "year": year,
            "month": month,
            "docs_count": docs_count,
            "executed_count": executed_count,
            "rejected_items_count": rejected_count,
            "has_data": True,
            "organization_key": organization_key,
            "organization_name": organization_name,
            "debug": {
                "status": "ok",
                "kpi_id": kpi_id,
                "doc_entity": DOC_ENTITY,
                "organization_key": organization_key,
                "organization_name": organization_name,
                "period_start": period_start[:19],
                "period_end": period_end[:19],
                "plan_rule": f"Date в месяце, Posted = true, {org_rule}",
                "fact_rule": "Date в месяце, Posted = true и заполнена ДатаПроверкиОТК",
                "rejected_rule": (
                    "Строки ТЧ ТоварыДляОТК с НеПринятоОТК > 0"
                    " или заполненным АктОТКоНесоответствии_Key"
                ),
                "doc_samples": doc_samples,
                "executed_samples": executed_samples,
                "rejected_samples": rejected_samples,
            },
        }
    except Exception as exc:
        logger.exception("%s: ошибка за %d-%02d", log_prefix, year, month)
        return {
            "year": year,
            "month": month,
            "docs_count": None,
            "executed_count": None,
            "rejected_items_count": None,
            "has_data": False,
            "organization_key": organization_key,
            "organization_name": organization_name,
            "debug": {
                "status": "error",
                "kpi_id": kpi_id,
                "organization_key": organization_key,
                "organization_name": organization_name,
                "error": str(exc),
            },
        }
