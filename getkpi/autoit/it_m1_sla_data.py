"""Загрузка заявок ``Document_ТД_ЗаявкаВСлужбуСопровождения`` для SLA ИТ-M1."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

import requests

from getkpi.fot_techdir_fact import AUTH, BASE, EMPTY, load_structure, normalize_name, resolve_department_row

logger = logging.getLogger(__name__)

DOC_ENTITY = "Document_ТД_ЗаявкаВСлужбуСопровождения"
EXECUTOR_DEPARTMENT_LABEL = "Отдел информационных технологий"
EXECUTOR_DEPARTMENT_ALIASES: tuple[str, ...] = (
    "отдел информационных технологий",
    "отдел ит",
    "оит",
)

SELECT_FIELDS = "Ref_Key,Number,Date,ПодтверждениеАвтораОбИсполнении,ПодразделениеИсполнитель_Key"
PAGE_SIZE = 5000


def month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _is_filled_datetime(value: Any) -> bool:
    if value in (None, ""):
        return False
    return str(value)[:10] not in ("0001-01-01", "")


def _datetime_in_period(value: Any, period_start: str, period_end: str) -> bool:
    if not _is_filled_datetime(value):
        return False
    return period_start <= str(value) < period_end


def resolve_executor_department_key(session: requests.Session) -> str | None:
    """Ref_Key подразделения «Отдел информационных технологий»."""
    structure_rows, _by_key, _by_parent, exact_index = load_structure(session)
    found = resolve_department_row(
        structure_rows,
        exact_index,
        EXECUTOR_DEPARTMENT_LABEL,
        EXECUTOR_DEPARTMENT_ALIASES,
    )
    if not found:
        return None
    key = str(found.get("Ref_Key") or "").strip()
    if not key or key.lower() == EMPTY.lower():
        return None
    return key


def _fetch_rows(session: requests.Session, odata_filter: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    base_url = (
        f"{BASE}/{quote(DOC_ENTITY)}?$format=json"
        f"&$filter={quote(odata_filter, safe='')}"
        f"&$select={quote(SELECT_FIELDS, safe=',_')}"
    )
    while True:
        url = f"{base_url}&$top={PAGE_SIZE}&$skip={skip}"
        try:
            response = session.get(url, timeout=120)
        except requests.RequestException as exc:
            logger.warning("IT-M1 SLA: OData %s", exc)
            break
        if not response.ok:
            logger.warning(
                "IT-M1 SLA: HTTP %s: %s",
                response.status_code,
                response.text[:300],
            )
            break
        batch = response.json().get("value") or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += len(batch)
    return rows


def compute_it_m1_sla_monthly(year: int, month: int) -> dict[str, Any]:
    """
    План: заявки с ``Date`` в месяце и ``ПодразделениеИсполнитель`` = ОИТ.
    Факт: из них с заполненным ``ПодтверждениеАвтораОбИсполнении``,
    дата подтверждения — в том же месяце.
    """
    counts = {
        "docs_plan": 0,
        "docs_fact": 0,
        "docs_skipped_empty_confirm": 0,
        "docs_skipped_confirm_outside_period": 0,
    }
    period_start, period_end = month_period_bounds(year, month)
    try:
        session = requests.Session()
        session.auth = AUTH
        executor_key = resolve_executor_department_key(session)
        if not executor_key:
            return {
                "year": year,
                "month": month,
                "plan": None,
                "fact": None,
                "counts": counts,
                "debug": {
                    "status": "error",
                    "kpi_id": "IT-M1-SLA",
                    "error": f"Не найдено подразделение «{EXECUTOR_DEPARTMENT_LABEL}» в 1С",
                },
            }

        odata_filter = (
            "DeletionMark eq false"
            f" and Date ge datetime'{period_start}'"
            f" and Date lt datetime'{period_end}'"
            f" and ПодразделениеИсполнитель_Key eq guid'{executor_key}'"
        )
        rows = _fetch_rows(session, odata_filter)
        counts["docs_plan"] = len(rows)

        fact = 0
        samples: list[dict[str, Any]] = []
        for row in rows:
            confirm = row.get("ПодтверждениеАвтораОбИсполнении")
            if not _is_filled_datetime(confirm):
                counts["docs_skipped_empty_confirm"] += 1
                continue
            if not _datetime_in_period(confirm, period_start, period_end):
                counts["docs_skipped_confirm_outside_period"] += 1
                continue
            fact += 1
            if len(samples) < 15:
                samples.append(
                    {
                        "number": str(row.get("Number") or ""),
                        "date": str(row.get("Date") or "")[:10],
                        "confirmed_at": str(confirm)[:19],
                    }
                )

        counts["docs_fact"] = fact
        plan = float(len(rows))
        fact_f = float(fact)
        return {
            "year": year,
            "month": month,
            "plan": plan,
            "fact": fact_f,
            "counts": counts,
            "debug": {
                "status": "ok",
                "kpi_id": "IT-M1-SLA",
                "component": "getkpi.autoit.it_m1_sla_data",
                "doc_entity": DOC_ENTITY,
                "executor_department": EXECUTOR_DEPARTMENT_LABEL,
                "executor_department_key": executor_key,
                "period_start": period_start[:19],
                "period_end": period_end[:19],
                "plan_rule": "Date в месяце + ПодразделениеИсполнитель = ОИТ",
                "fact_rule": (
                    "ПодтверждениеАвтораОбИсполнении заполнено, "
                    "дата подтверждения в месяце"
                ),
                "included_samples": samples,
            },
        }
    except Exception as exc:
        logger.exception("IT-M1 SLA: ошибка за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "plan": None,
            "fact": None,
            "counts": counts,
            "debug": {
                "status": "error",
                "kpi_id": "IT-M1-SLA",
                "error": str(exc),
            },
        }
