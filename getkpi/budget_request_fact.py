"""
Факт бюджета по заявкам на расход ДС: оплаты из регистра ДДС за месяц + фильтры шапки заявки.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import requests

from getkpi.calc_budget_limit import PLAN_RECORDER_TYPE, load_records, period_bounds
from getkpi.calc_budget_techdir_m3 import _nav_description
from getkpi.ext_budj_fact import (
    EXCLUDED_STATUSES,
    ROUND_TOLERANCE,
    load_all_movements_for_zayavki,
    payment_status_label,
)
from getkpi.fot_techdir_fact import AUTH, EMPTY, load_structure, normalize_name
from qualdir.qd_m3_fact import _fetch_znrds_headers_merged, _signed_payment_gross_and_over

logger = logging.getLogger(__name__)

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def norms_from_labels(*labels: str) -> frozenset[str]:
    return frozenset(normalize_name(s) for s in labels if s)


def _structure_row_by_key(structure_by_key: dict[str, Any], k: str) -> dict[str, Any] | None:
    if not k:
        return None
    ks = str(k).strip()
    if ks in structure_by_key:
        return structure_by_key[ks]
    kl = ks.lower()
    for rk, row in structure_by_key.items():
        if str(rk).strip().lower() == kl:
            return row
    return None


def _normalize_presentation(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, dict):
        for key in ("Description", "Name", "Presentation", "Number"):
            v = raw.get(key)
            if v not in (None, ""):
                return normalize_name(str(v))
        return ""
    return normalize_name(str(raw))


def norm_matches_allowed(norm: str, allowed: frozenset[str]) -> bool:
    if not norm:
        return False
    for needle in allowed:
        if len(needle) < 4:
            continue
        if norm == needle or needle in norm or norm in needle:
            return True
    return False


def doc_td_cfo_norm(
    session: requests.Session,
    doc: dict[str, Any],
    structure_by_key: dict[str, Any],
) -> str:
    raw = doc.get("ТД_ЦФО")
    if raw is not None and str(raw).strip():
        return _normalize_presentation(raw)
    nav = _nav_description(session, doc.get("ТД_ЦФО@navigationLinkUrl"))
    if nav:
        return normalize_name(nav)
    k = doc.get("ТД_ЦФО_Key")
    if k and str(k).lower() != EMPTY.lower():
        row = _structure_row_by_key(structure_by_key, str(k))
        if row:
            return normalize_name(str(row.get("Description", "") or ""))
    return ""


def doc_department_norm(doc: dict[str, Any], structure_by_key: dict[str, Any]) -> str:
    raw = doc.get("Подразделение")
    if raw is not None and str(raw).strip():
        if isinstance(raw, dict):
            return _normalize_presentation(raw)
        return normalize_name(str(raw))
    pk = doc.get("Подразделение_Key")
    if not pk or str(pk).lower() == EMPTY.lower():
        return ""
    row = _structure_row_by_key(structure_by_key, str(pk))
    if not row:
        return ""
    return normalize_name(str(row.get("Description", "") or ""))


def _row_request_ref_from_register(r: dict[str, Any]) -> str:
    zk = (r.get("ЗаявкаНаРасходованиеДенежныхСредств_Key") or "").strip()
    if zk and str(zk).lower() != EMPTY.lower():
        return str(zk).lower()
    rt = r.get("Recorder_Type") or ""
    rec = (r.get("Recorder") or "").strip()
    if not rec or str(rec).lower() == EMPTY.lower():
        return ""
    if rt == PLAN_RECORDER_TYPE:
        return str(rec).lower()
    if "ЗаявкаНаРасходование" in rt and "Денеж" in rt:
        return str(rec).lower()
    return ""


def _register_payment_amount_rub(r: dict[str, Any]) -> float:
    gross, over = _signed_payment_gross_and_over(r)
    return gross - over


def compute_budget_request_fact_monthly(
    year: int,
    month: int,
    *,
    kpi_id: str,
    component: str,
    td_cfo_label: str,
    td_cfo_norms: frozenset[str],
    department_label: str | None = None,
    department_norms: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Сумма оплат по заявкам за календарный месяц (руб.) с фильтрами ЦФО и подразделения."""
    require_department = department_norms is not None and len(department_norms) > 0
    counts = {
        "register_rows": 0,
        "register_rows_with_request": 0,
        "requests_with_payment_in_period": 0,
        "requests_loaded": 0,
        "requests_matched_filters": 0,
        "requests_skipped_status": 0,
        "requests_skipped_filters": 0,
        "requests_skipped_not_paid_enough": 0,
    }
    try:
        session = requests.Session()
        session.auth = AUTH
        p_start, p_end = period_bounds(year, month)

        rows = load_records(session, p_start, p_end)
        counts["register_rows"] = len(rows)

        period_by_ref: dict[str, float] = defaultdict(float)
        for r in rows:
            ref = _row_request_ref_from_register(r)
            if not ref:
                continue
            counts["register_rows_with_request"] += 1
            amt = _register_payment_amount_rub(r)
            if amt == 0:
                continue
            period_by_ref[ref] += amt

        paid_refs = [ref for ref, amt in period_by_ref.items() if amt > ROUND_TOLERANCE]
        counts["requests_with_payment_in_period"] = len(paid_refs)
        if not paid_refs:
            return _empty_ok_result(
                year, month, p_start, p_end, counts, kpi_id=kpi_id,
            )

        headers, doc_entity = _fetch_znrds_headers_merged(session, paid_refs)
        counts["requests_loaded"] = len(headers)
        _structure_rows, structure_by_key, _by_parent, _exact_index = load_structure(session)
        total_paid_all = load_all_movements_for_zayavki(session, paid_refs)

        total = 0.0
        included_samples: list[dict[str, Any]] = []
        rejected_filter_samples: list[str] = []
        seen_reject: set[str] = set()

        for ref in paid_refs:
            doc = headers.get(ref)
            if not doc:
                continue
            status = (doc.get("Статус") or "").strip()
            if status in EXCLUDED_STATUSES:
                counts["requests_skipped_status"] += 1
                continue

            doc_sum = float(doc.get("СуммаДокумента") or 0)
            paid_period = period_by_ref.get(ref, 0.0)
            paid_total = total_paid_all.get(ref, paid_period)
            pay_status = payment_status_label(paid_total, doc_sum)
            if pay_status not in {"полная", "частичная"}:
                counts["requests_skipped_not_paid_enough"] += 1
                continue

            td_cfo = doc_td_cfo_norm(session, doc, structure_by_key)
            dept = doc_department_norm(doc, structure_by_key)
            cfo_ok = norm_matches_allowed(td_cfo, td_cfo_norms)
            dept_ok = (
                not require_department
                or norm_matches_allowed(dept, department_norms or frozenset())
            )
            if not (cfo_ok and dept_ok):
                counts["requests_skipped_filters"] += 1
                combo = f"td_cfo={td_cfo or '-'} | podr={dept or '-'}"
                if combo not in seen_reject and len(rejected_filter_samples) < 12:
                    seen_reject.add(combo)
                    rejected_filter_samples.append(combo[:240])
                continue

            counts["requests_matched_filters"] += 1
            total += paid_period
            if len(included_samples) < 20:
                included_samples.append(
                    {
                        "number": str(doc.get("Number") or ""),
                        "pay_status": pay_status,
                        "td_cfo": td_cfo,
                        "department": dept,
                        "paid_in_period_rub": round(paid_period, 2),
                        "doc_sum_rub": round(doc_sum, 2),
                    }
                )

        debug: dict[str, Any] = {
            "status": "ok",
            "kpi_id": kpi_id,
            "component": component,
            "source": (
                "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент + "
                "Document_ЗаявкаНаРасходованиеДенежныхСредств"
            ),
            "period_start": p_start[:19],
            "period_end": p_end[:19],
            "date_basis": "payment_period_in_register",
            "required_td_cfo": td_cfo_label,
            "status_rule": "полная_or_частичная_by_payment_status_label",
            "included_requests_sample": included_samples,
            "rejected_filter_samples": rejected_filter_samples,
            "_counts_hints": (
                "requests_with_payment_in_period: заявки с оплатой в месяце; "
                "requests_matched_filters: ЦФО + подразделение; "
                "requests_skipped_not_paid_enough: нет полной/частичной оплаты по сумме."
            ),
        }
        if department_label:
            debug["required_department"] = department_label

        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": round(total, 2),
            "counts": {**counts, "doc_entity": doc_entity},
            "debug": debug,
        }
    except Exception as exc:
        logger.exception("%s fact: ошибка за %d-%02d", kpi_id, year, month)
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": None,
            "counts": counts,
            "debug": {
                "status": "error",
                "kpi_id": kpi_id,
                "component": component,
                "error": str(exc),
            },
        }


def _empty_ok_result(
    year: int,
    month: int,
    p_start: str,
    p_end: str,
    counts: dict[str, Any],
    *,
    kpi_id: str,
) -> dict[str, Any]:
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU.get(month, str(month)),
        "total_fact": 0.0,
        "counts": counts,
        "debug": {
            "status": "ok",
            "kpi_id": kpi_id,
            "period_start": p_start[:19],
            "period_end": p_end[:19],
            "note": "Нет оплат по заявкам в выбранном месяце",
        },
    }
