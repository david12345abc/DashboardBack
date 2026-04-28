"""
План и факт затрат контура «Технический директор» (TD-M3).

План остаётся из `AccumulationRegister_ОборотыБюджетов_RecordType`
(поле `СуммаСценария`), а факт теперь считается по документам «Заявка на
расходование ДС» с учётом полной / частичной оплаты.

- План: сценарий BUDGET_SCENARIO_NAME («Плановые данные - ЦФО»).
- Факт: оплаченные / частично оплаченные заявки с разбивкой по строкам.
- Подразделения: поддерево(ья) в Catalog_СтруктураПредприятия.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
import re
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from . import calc_budget_techdir_plan_fact as bdg
from . import fot_techdir_fact as fts
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

AUTH = HTTPBasicAuth("odata.user", "npo852456")

BUDGET_SCENARIO_NAME = bdg.BUDGET_SCENARIO_NAME

# Режим контура «Технический директор» для бюджета затрат:
#   "single_root" — один узел по алиасам TD_BUDGET_ROOT_GROUPS;
#   "six_cards" — шесть узлов BUDGET_TD_SIX_CARD_ALIASES;
#   "guid_overrides" — список Ref_Key из BUDGET_GROUP_ROOTS_OVERRIDES.
BUDGET_TD_CONTOUR_MODE = "single_root"

BUDGET_GROUP_ROOTS_OVERRIDES: dict[str, list[str]] = {
    "Технический директор": [
        "f348c51e-d57f-11ea-83c5-ac1f6b05524c",
        "b338c792-f396-11e8-8283-ac1f6b05524d",
        "a37213b5-e6f1-11e7-826b-ac1f6b05524d",
        "e21ab3fd-f82d-11ef-9621-6cb31113810e",
        "8cf17e48-df91-11e9-829b-ac1f6b05524d",
        "22882cd7-d5fc-11e9-829b-ac1f6b05524d",
    ],
}

BUDGET_TD_SIX_CARD_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Служба технического директора", ("служба технического директора",)),
    ("Контур качества (зам по кач.)", ("зам. технического директора по качеству",)),
    ("Сервисная служба", ("сервисная служба",)),
    ("Служба автоматизации", ("служба автоматизации",)),
    ("Метрологическая служба", ("метрологическая служба",)),
    ("Отдел метрологии и сертификации", ("отдел метрологии и сертификации",)),
)

TD_BUDGET_ROOT_GROUPS: dict[str, tuple[str, ...]] = {
    "Технический директор": ("технический директор",),
}

REQUEST_DOC_KEYWORDS = ("заяв", "расход", "денеж", "средств", "оплат")
REQUEST_DOC_ENTITY_DEFAULT = "Document_ЗаявкаНаРасходованиеДенежныхСредств"
REQUEST_DOC_ENTITY_CANDIDATES = (
    REQUEST_DOC_ENTITY_DEFAULT,
    "Document_ЗаявкаНаРасходованиеДС",
    "Document_ЗаявкаНаРасходованиеДенежныхСредствТД",
    "Document_ТД_ЗаявкаНаРасходованиеДенежныхСредств",
)
REQUEST_TAB_ENTITY_CANDIDATES = (
    f"{REQUEST_DOC_ENTITY_DEFAULT}_ТЧ",
    f"{REQUEST_DOC_ENTITY_DEFAULT}_Строки",
    f"{REQUEST_DOC_ENTITY_DEFAULT}_Расшифровка",
    f"{REQUEST_DOC_ENTITY_DEFAULT}_ТабличнаяЧасть",
)

REQUEST_ALLOWED_ARTICLES = frozenset(
    fts.normalize_name(s)
    for s in (
        "Оплата труда (26 сч) НПО АУП!",
        "Страховые взносы (26 сч) НПО АУП!",
    )
)
REQUEST_HEADER_DATE_FIELDS = ("Date", "Дата")
REQUEST_HEADER_STATUS_FIELDS = ("Статус", "Состояние", "Status", "State")
REQUEST_HEADER_PAID_FIELDS = (
    "СуммаОплатыРегл",
    "СуммаОплаты",
    "ОплаченоРегл",
    "Оплачено",
    "СуммаФакт",
    "СуммаПлатежа",
)
REQUEST_LINE_AMOUNT_FIELDS = (
    "Сумма",
    "СуммаСтроки",
    "СуммаОплатыРегл",
    "СуммаОплаты",
    "ОплаченоРегл",
    "Оплачено",
    "СуммаФакт",
)
REQUEST_LINE_DEPT_FIELDS = ("Подразделение_Key", "Подразделение")
REQUEST_LINE_ARTICLE_FIELDS = ("СтатьяРасходов_Key", "СтатьяРасходов", "СтатьяБюджетов")
REQUEST_PERIOD_FIELD = "Date"
REQUEST_PAID_STATUS_HINTS = (
    "оплачен",
    "частич",
    "полная оплата",
    "оплачено",
)
REQUEST_UNPAID_STATUS_HINTS = (
    "не оплачен",
    "неоплачен",
    "аннулир",
    "отмен",
    "не провед",
    "без оплаты",
)
REQUEST_DOC_STATUS_PRIORITY = {
    "full": 3,
    "partial": 2,
    "paid": 1,
    "unknown": 0,
}

MONTH_NAMES = {
    1: "январь",
    2: "февраль",
    3: "март",
    4: "апрель",
    5: "май",
    6: "июнь",
    7: "июль",
    8: "август",
    9: "сентябрь",
    10: "октябрь",
    11: "ноябрь",
    12: "декабрь",
}

BUDGET_PLAN_ARTICLE_KEYS: frozenset[str] = frozenset()
USE_PAYROLL_LIKE_BUDGET_PLAN_ARTICLES = False


def is_budget_plan_article(article_key: str, article_name: str = "") -> bool:
    if not article_key or article_key == fts.EMPTY:
        return False
    if USE_PAYROLL_LIKE_BUDGET_PLAN_ARTICLES:
        n = (article_name or "").lower()
        return "затраты на оплату труда" in n or "налог на заработную" in n
    if BUDGET_PLAN_ARTICLE_KEYS:
        return article_key in BUDGET_PLAN_ARTICLE_KEYS
    return True


def _collect_subtree(root_key: str, by_parent) -> set[str]:
    keys: set[str] = set()
    stack = [root_key]
    while stack:
        current = stack.pop()
        if current in keys:
            continue
        keys.add(current)
        for child in by_parent.get(current, []):
            ck = child.get("Ref_Key")
            if ck:
                stack.append(ck)
    return keys


def _merge_subtrees_for_roots(root_keys: list[str], by_parent) -> set[str]:
    merged: set[str] = set()
    for rk in root_keys:
        if not rk or rk == fts.EMPTY:
            continue
        merged |= _collect_subtree(rk, by_parent)
    return merged


def _override_roots_exist_in_structure(
    root_keys: list[str], by_key: dict[str, Any]
) -> bool:
    return bool(root_keys) and all(k and k in by_key for k in root_keys if k and k != fts.EMPTY)


def _resolve_group_roots(
    structure_rows: list,
    exact_index: defaultdict,
    groups: dict[str, tuple[str, ...]],
) -> tuple[dict[str, dict], list[str]]:
    resolved: dict[str, dict] = {}
    unresolved: list[str] = []
    for display_name, aliases in groups.items():
        found = None
        for alias in aliases:
            found = fts.pick_best_candidate(
                exact_index.get(fts.normalize_name(alias), [])
            )
            if found:
                break
        if not found:
            for alias in aliases:
                an = fts.normalize_name(alias)
                contains = [
                    row
                    for row in structure_rows
                    if an in fts.normalize_name(row.get("Description", ""))
                ]
                found = fts.pick_best_candidate(contains)
                if found:
                    break
        if not found:
            unresolved.append(display_name)
        else:
            resolved[display_name] = found
    return resolved, unresolved


def _resolve_td_six_card_root_keys(
    structure_rows: list, exact_index: defaultdict, by_key: dict[str, Any]
) -> list[str] | None:
    keys: list[str] = []
    for _label, aliases in BUDGET_TD_SIX_CARD_ALIASES:
        found = None
        for alias in aliases:
            found = fts.pick_best_candidate(
                exact_index.get(fts.normalize_name(alias), [])
            )
            if found:
                break
        if not found:
            for alias in aliases:
                an = fts.normalize_name(alias)
                contains = [
                    row
                    for row in structure_rows
                    if an in fts.normalize_name(row.get("Description", ""))
                ]
                found = fts.pick_best_candidate(contains)
                if found:
                    break
        if not found:
            return None
        k = found.get("Ref_Key")
        if not k or k not in by_key:
            return None
        keys.append(k)
    return keys


def load_budget_group_subtrees(session: requests.Session) -> dict[str, set[str]]:
    structure_rows, by_key, by_parent, exact_index = fts.load_structure(session)
    roots_ov = BUDGET_GROUP_ROOTS_OVERRIDES.get("Технический директор", [])

    if BUDGET_TD_CONTOUR_MODE == "guid_overrides" and _override_roots_exist_in_structure(
        roots_ov, by_key
    ):
        return {
            "Технический директор": _merge_subtrees_for_roots(roots_ov, by_parent),
        }

    if BUDGET_TD_CONTOUR_MODE == "single_root":
        resolved, unresolved = _resolve_group_roots(
            structure_rows, exact_index, TD_BUDGET_ROOT_GROUPS
        )
        if "Технический директор" in unresolved or "Технический директор" not in resolved:
            logger.warning(
                "TD-M3: не найден узел «Технический директор» в Catalog_СтруктураПредприятия"
            )
            return {"Технический директор": set()}
        root_key = resolved["Технический директор"]["Ref_Key"]
        return {"Технический директор": _collect_subtree(root_key, by_parent)}

    if BUDGET_TD_CONTOUR_MODE == "six_cards":
        six = _resolve_td_six_card_root_keys(structure_rows, exact_index, by_key)
        if six:
            return {
                "Технический директор": _merge_subtrees_for_roots(six, by_parent),
            }
        logger.warning(
            "TD-M3: не все 6 карточек найдены в структуре; fallback на один корень ТД"
        )

    resolved, unresolved = _resolve_group_roots(
        structure_rows, exact_index, TD_BUDGET_ROOT_GROUPS
    )
    if "Технический директор" in unresolved or "Технический директор" not in resolved:
        logger.warning(
            "TD-M3: не найден «Технический директор» в структуре (проверьте алиасы / режим)"
        )
        return {"Технический директор": set()}

    root_key = resolved["Технический директор"]["Ref_Key"]
    return {"Технический директор": _collect_subtree(root_key, by_parent)}


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _sum_turnover_for_scenario(
    rows: list[dict[str, Any]],
    subtree_keys: set[str],
    scenario_names: dict[str, str],
    article_names: dict[str, str],
    scenario_label: str,
) -> float | None:
    scenario_keys = {k for k, v in scenario_names.items() if v == scenario_label}
    if not scenario_keys:
        return None
    total = 0.0
    for row in rows:
        if row.get("Сценарий_Key") not in scenario_keys:
            continue
        dk = row.get("Подразделение_Key") or ""
        if not dk or dk == fts.EMPTY or dk not in subtree_keys:
            continue
        ak = row.get("СтатьяБюджетов") or ""
        aname = article_names.get(ak, "")
        if not is_budget_plan_article(ak, aname):
            continue
        total += float(row.get("СуммаСценария") or 0)
    return total


_REQUEST_DOC_CACHE: tuple[str, str] | None = None


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("Description", "Name", "Number", "Ref_Key", "Value"):
            raw = value.get(key)
            if raw not in (None, ""):
                return str(raw).strip()
        return ""
    return str(value).strip()


def _pick_first(item: dict[str, Any], fields: tuple[str, ...]) -> Any:
    for field in fields:
        if field in item and item.get(field) not in (None, ""):
            return item.get(field)
    return None


def _classify_request_status(status: str) -> str:
    low = _normalize(status).lower()
    if not low:
        return "unknown"
    if any(h in low for h in REQUEST_UNPAID_STATUS_HINTS):
        return "unpaid"
    if "частич" in low:
        return "partial"
    if any(h in low for h in REQUEST_PAID_STATUS_HINTS):
        return "full"
    return "unknown"


def _looks_like_paid_status(status: str) -> bool:
    return _classify_request_status(status) in {"full", "partial"}


def _score_request_entity(name: str) -> int:
    low = name.lower()
    score = sum(1 for kw in REQUEST_DOC_KEYWORDS if kw in low)
    if "_строк" in low or "_тч" in low or "_таб" in low or "_расшифр" in low:
        score -= 2
    if "заяв" in low and "расход" in low:
        score += 3
    return score


def _discover_request_entities(session: requests.Session) -> tuple[str, str]:
    global _REQUEST_DOC_CACHE
    if _REQUEST_DOC_CACHE is not None:
        return _REQUEST_DOC_CACHE

    doc_entity = REQUEST_DOC_ENTITY_DEFAULT
    tab_entity = f"{doc_entity}_Строки"

    # Сначала быстро пробуем наиболее вероятные имена, чтобы не ждать полный $metadata.
    for candidate in REQUEST_DOC_ENTITY_CANDIDATES:
        url = f"{fts.BASE}/{quote(candidate)}?$format=json&$top=1"
        resp = request_with_retry(session, url, timeout=10, retries=1, label="td_m3/doc_probe")
        if resp is not None and resp.ok:
            doc_entity = candidate
            break

    for candidate in REQUEST_TAB_ENTITY_CANDIDATES:
        url = f"{fts.BASE}/{quote(candidate)}?$format=json&$top=1"
        resp = request_with_retry(session, url, timeout=10, retries=1, label="td_m3/tab_probe")
        if resp is not None and resp.ok:
            tab_entity = candidate
            break

    # Если быстрые кандидаты не сработали, пытаемся угадать по $metadata.
    if doc_entity == REQUEST_DOC_ENTITY_DEFAULT and tab_entity == f"{doc_entity}_Строки":
        try:
            r = request_with_retry(session, f"{fts.BASE}/$metadata", timeout=20, retries=1, label="td_m3/metadata")
            if r is not None and r.ok and r.text:
                names = sorted(set(re.findall(r'EntitySet\s+Name="(Document_[^"]+)"', r.text)))
                doc_candidates = [
                    n for n in names
                    if _score_request_entity(n) > 0
                ]
                if doc_candidates:
                    doc_entity = sorted(
                        doc_candidates,
                        key=lambda n: (_score_request_entity(n), -len(n), n),
                        reverse=True,
                    )[0]
                    prefix = f"{doc_entity}_"
                    tab_candidates = [n for n in names if n.startswith(prefix) and n != doc_entity]
                    if not tab_candidates:
                        tab_candidates = [
                            n for n in names
                            if _score_request_entity(n) > 0 and n != doc_entity
                        ]
                    if tab_candidates:
                        tab_entity = sorted(
                            tab_candidates,
                            key=lambda n: (1 if any(s in n.lower() for s in ("строк", "тч", "таб", "расшифр")) else 0, -len(n), n),
                            reverse=True,
                        )[0]
        except Exception as exc:
            logger.warning("TD-M3: metadata discovery failed: %s", exc)

    _REQUEST_DOC_CACHE = (doc_entity, tab_entity)
    return _REQUEST_DOC_CACHE


def _fetch_all_rows(session: requests.Session, url: str, *, label: str, timeout: int = 120) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top=5000&$skip={skip}"
        resp = request_with_retry(session, page_url, timeout=timeout, retries=4, label=label)
        if resp is None:
            logger.warning("TD-M3: %s request dropped after retries", label)
            return rows
        if not resp.ok:
            logger.warning("TD-M3: %s HTTP %d: %s", label, resp.status_code, resp.text[:200])
            return rows
        batch = resp.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < 5000:
            return rows
        skip += 5000


def _fetch_request_docs(session: requests.Session, doc_entity: str, p_start: str, p_end: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    filters = [
        f"Date ge datetime'{p_start}' and Date lt datetime'{p_end}' and Posted eq true and DeletionMark eq false",
        f"Date ge datetime'{p_start}' and Date lt datetime'{p_end}' and DeletionMark eq false",
        f"Date ge datetime'{p_start}' and Date lt datetime'{p_end}'",
    ]
    for flt in filters:
        url = f"{fts.BASE}/{quote(doc_entity)}?$format=json&$filter={quote(flt, safe='')}"
        docs = _fetch_all_rows(session, url, label="td_m3/docs")
        if docs:
            return docs, {"doc_filter": flt}
    return [], {"doc_filter": filters[-1]}


def _fetch_request_lines(session: requests.Session, tab_entity: str, doc_ref: str) -> list[dict[str, Any]]:
    flt = f"Ref_Key eq guid'{doc_ref}'"
    url = f"{fts.BASE}/{quote(tab_entity)}?$format=json&$filter={quote(flt, safe='')}"
    return _fetch_all_rows(session, url, label="td_m3/lines")


def _resolve_request_group(
    dept_value: Any,
    struct_map: dict[str, str],
    structure_rows: list[dict[str, Any]],
    exact_index: dict[str, list[dict[str, Any]]],
) -> str | None:
    raw = _normalize(dept_value)
    if not raw:
        return None
    if raw in struct_map:
        return struct_map[raw]
    found = fts.resolve_department_row(structure_rows, exact_index, raw, ())
    if found:
        key = found.get("Ref_Key")
        if key and key in struct_map:
            return struct_map[key]
    norm = fts.normalize_name(raw)
    for display_name, aliases in fts.FOT_SPEC:
        if norm == fts.normalize_name(display_name):
            return display_name
        for alias in aliases:
            alias_norm = fts.normalize_name(alias)
            if alias_norm and (alias_norm == norm or alias_norm in norm or norm in alias_norm):
                return display_name
    return None


def _resolve_request_article(article_value: Any, article_names: dict[str, str]) -> str:
    raw = _normalize(article_value)
    if not raw:
        return ""
    name = article_names.get(raw, raw)
    return _normalize(name)


def _line_amount(line: dict[str, Any]) -> float:
    value = _pick_first(line, REQUEST_LINE_AMOUNT_FIELDS)
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _line_paid_amount(line: dict[str, Any]) -> float | None:
    value = _pick_first(line, ("СуммаОплатыРегл", "СуммаОплаты", "ОплаченоРегл", "Оплачено", "СуммаФакт"))
    try:
        amount = float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None
    return amount


def _extract_doc_paid_amount(doc: dict[str, Any]) -> tuple[float | None, str]:
    value = _pick_first(doc, REQUEST_HEADER_PAID_FIELDS)
    try:
        amount = float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        amount = None
    if amount is not None and amount > 0:
        return amount, "header_amount"
    status = _normalize(_pick_first(doc, REQUEST_HEADER_STATUS_FIELDS))
    return None, f"status:{status or 'unknown'}"


def _compute_request_fact_monthly(
    session: requests.Session,
    year: int,
    month: int,
    name_to_key: dict[str, str],
) -> dict[str, Any]:
    p_start, p_end = _month_period_bounds(year, month)
    doc_entity, tab_entity = _discover_request_entities(session)
    structure_rows, by_key, _, exact_index = fts.load_structure(session)
    struct_map = fts.build_struct_key_to_fot_group(name_to_key, by_key)
    article_names = fts.load_cost_articles(session)

    docs, doc_debug = _fetch_request_docs(session, doc_entity, p_start, p_end)
    total_fact = 0.0
    totals_by_group: dict[str, float] = defaultdict(float)
    totals_by_group_lines: dict[str, int] = defaultdict(int)
    counts = {
        "docs_total": len(docs),
        "docs_included": 0,
        "docs_skipped_status": 0,
        "docs_skipped_no_paid_amount": 0,
        "docs_skipped_empty": 0,
        "lines_total": 0,
        "lines_included": 0,
        "lines_skipped_dept": 0,
        "lines_skipped_article": 0,
        "allocation_rule": "doc_paid_amount_proportional_by_line_amount",
    }
    included_docs: list[dict[str, Any]] = []

    for doc in docs:
        if doc.get("DeletionMark") or doc.get("Deleted"):
            counts["docs_skipped_status"] += 1
            continue
        if doc.get("Posted") is False:
            counts["docs_skipped_status"] += 1
            continue

        status = _normalize(_pick_first(doc, REQUEST_HEADER_STATUS_FIELDS))
        doc_ref = _normalize(doc.get("Ref_Key")).lower()
        if not doc_ref or doc_ref == fts.EMPTY:
            counts["docs_skipped_empty"] += 1
            continue

        lines = _fetch_request_lines(session, tab_entity, doc_ref)
        if not lines:
            counts["docs_skipped_empty"] += 1
            continue

        base_total = sum(_line_amount(line) for line in lines)
        if base_total <= 0:
            counts["docs_skipped_empty"] += 1
            continue

        doc_paid_amount, paid_source = _extract_doc_paid_amount(doc)
        status_kind = _classify_request_status(status)
        has_line_paid_values = any((_line_paid_amount(line) or 0) > 0 for line in lines)
        if doc_paid_amount is None and status_kind == "full":
            doc_paid_amount = base_total
            paid_source = "status_full_request_amount"
        elif has_line_paid_values:
            paid_source = "line_paid_amounts"
        elif doc_paid_amount is None and not has_line_paid_values:
            counts["docs_skipped_no_paid_amount"] += 1
            continue

        if doc_paid_amount is not None:
            doc_paid_amount = max(0.0, min(float(doc_paid_amount), float(base_total)))
        counts["docs_included"] += 1
        counts["lines_total"] += len(lines)
        included_docs.append(
            {
                "ref": doc_ref,
                "number": _normalize(doc.get("Number")),
                "date": _normalize(doc.get("Date"))[:10],
                "status": status,
                "paid_amount": round(doc_paid_amount or 0.0, 2),
                "paid_source": paid_source,
                "lines": len(lines),
            }
        )

        for line in lines:
            raw_dept = None
            for field in REQUEST_LINE_DEPT_FIELDS:
                if field in line and line.get(field) not in (None, ""):
                    raw_dept = line.get(field)
                    break
            group_name = _resolve_request_group(raw_dept, struct_map, structure_rows, exact_index)
            if not group_name:
                counts["lines_skipped_dept"] += 1
                continue

            article_value = None
            for field in REQUEST_LINE_ARTICLE_FIELDS:
                if field in line and line.get(field) not in (None, ""):
                    article_value = line.get(field)
                    break
            article_norm = _resolve_request_article(article_value, article_names)
            if article_norm not in REQUEST_ALLOWED_ARTICLES:
                counts["lines_skipped_article"] += 1
                continue

            line_amount = _line_amount(line)
            if line_amount <= 0:
                continue
            line_paid_amount = _line_paid_amount(line)
            if line_paid_amount is not None and line_paid_amount > 0:
                allocated = float(line_paid_amount)
            elif doc_paid_amount is not None:
                allocated = round(doc_paid_amount * (line_amount / base_total), 2)
            else:
                continue
            totals_by_group[group_name] += allocated
            totals_by_group_lines[group_name] += 1
            total_fact += allocated
            counts["lines_included"] += 1

    totals_out: dict[str, dict[str, float]] = {}
    for name in fts.FOT_GROUP_ORDER:
        amt = round(totals_by_group.get(name, 0.0), 2)
        totals_out[name] = {
            "fact_salary": amt,
            "fact_insurance": 0.0,
            "fact_total": amt,
            "lines": totals_by_group_lines.get(name, 0),
        }

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "groups": totals_out,
        "total_fact": round(total_fact, 2),
        "counts": counts,
        "debug": {
            "doc_entity": doc_entity,
            "tab_entity": tab_entity,
            "period_start": p_start,
            "period_end": p_end,
            "date_basis": "document_date",
            "status_rule": "full_or_partial_paid",
            "included_docs": included_docs[:50],
            "doc_debug": doc_debug,
            "department_map_size": len(struct_map),
        },
    }


def compute_td_m3_costs_monthly(year: int, month: int) -> dict[str, Any]:
    session = requests.Session()
    session.auth = AUTH
    scenario_names = bdg.load_budget_scenarios(session)
    name_to_key, _ = fts.load_fot_spec_structure_map(session)
    p0, p1 = _month_period_bounds(year, month)
    rows = bdg.load_budget_turnover_rows(session, p0, p1)

    subtrees = load_budget_group_subtrees(session)
    subtree_keys = subtrees.get("Технический директор") or set()
    total_plan = _sum_turnover_for_scenario(
        rows, subtree_keys, scenario_names, bdg.load_budget_articles(session), BUDGET_SCENARIO_NAME
    )
    request_fact = _compute_request_fact_monthly(session, year, month, name_to_key)
    total_fact = request_fact.get("total_fact")

    has_data = total_plan is not None and total_fact is not None
    return {
        "year": year,
        "month": month,
        "total_plan": round(total_plan, 2) if total_plan is not None else None,
        "total_fact": round(total_fact, 2) if total_fact is not None else None,
        "has_data": has_data,
        "debug": {
            "subtree_size": len(subtree_keys),
            "plan_scenario": BUDGET_SCENARIO_NAME,
            "fact_source": request_fact.get("debug", {}),
            "contour_mode": BUDGET_TD_CONTOUR_MODE,
        },
    }


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def get_td_m3_costs_monthly(
    year: int | None = None, month: int | None = None
) -> dict[str, Any]:
    if year is None or month is None:
        year, month = _last_full_month()
    return compute_td_m3_costs_monthly(year, month)
