"""
calc_budget_fact_techdir.py - fully self-contained TD budget fact.

It calculates, without importing other project calculators:
  1. Budget by account 26 for 19 departments and 5 target articles.
  2. TD-M3 fact budget for the Technical Director subtree.

Final total = account 26 total + TD-M3 fact total.
"""

from __future__ import annotations

import re
import sys
import time
import functools
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"
ACCOUNT_26_ROOT = "fb2bde43-6250-11e7-812d-001e67112509"
SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"
TD_M3_ROOT_NAME = "Технический директор"

FOT_SPEC = [
    ("Эксплуатационная служба", ("эксплуатационная служба",)),
    ("Электрик/энергетик", ("электрик/энергетик", "электрик энергетик")),
    (
        "Зам. технического директора по качеству",
        (
            "зам. технического директора по качеству",
            "зам технического директора по качеству",
            "заместитель тех. директора по качеству",
            "заместитель технического директора по качеству",
        ),
    ),
    ("Специалист по процессному управлению", ("специалист по процессному управлению",)),
    ("ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ", ("заместитель директора по качеству",)),
    ("Лаборатория неразрушающего контроля", ("лаборатория неразрушающего контроля",)),
    ("Отдел управления несоответствиями", ("отдел управления несоответствиями",)),
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    ("АХО", ("ахо",)),
    (
        "Зам. технического директора по сервису",
        (
            "зам. технического директора по сервису",
            "зам технического директора по сервису",
            "заместитель технического директора по сервису",
        ),
    ),
    ("Отдел по работе с рекламациями", ("отдел по работе с рекламациями",)),
    ("Отдел технической поддержки", ("отдел технической поддержки",)),
    ("Сервисная служба", ("сервисная служба",)),
    ("Отдел сервисного обслуживания", ("отдел сервисного обслуживания",)),
    ("Служба технического директора", ("служба технического директора",)),
    ("Сектор качества разработки", ("сектор качества разработки",)),
    (
        "Сектор разработки тех. Решений",
        ("сектор разработки тех. решений", "сектор разработки тех решений"),
    ),
    ("Сектор промышленной безопасности", ("сектор промышленной безопасности",)),
]

FOT_GROUP_ORDER = [name for name, _aliases in FOT_SPEC]
TARGET_ARTICLES = (
    "Оплата труда ГАРАНТИИ (26 сч) пр-во НПО!",
    "Гарантийное обслуживание, поверка, ремонт (26 сч) НПО!",
    "Гарантийное обслуживание, ТМЦ (26) НПО!",
    "Гарантийное обслуживание, транспорт и ГСМ (26 сч) НПО!",
    "Гарантийное обслуживание, командировка (26 сч) НПО!",
)
TD_M3_ARTICLES = TARGET_ARTICLES
MONTH_NAMES = (
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
)

TARGET_ARTICLE_NORMS = {None}
TD_M3_ARTICLE_NORMS = {None}


def normalize_name(value: str) -> str:
    if value is None:
        return ""
    s = re.sub(r"\s+", " ", str(value).strip())
    s = s.lower().replace("ё", "е")
    s = re.sub(r"[^0-9a-zа-я]+", " ", s)
    return " ".join(s.split())


TARGET_ARTICLE_NORMS = frozenset(normalize_name(name) for name in TARGET_ARTICLES)
TD_M3_ARTICLE_NORMS = frozenset(normalize_name(name) for name in TD_M3_ARTICLES)


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_period() -> tuple[int, int]:
    for arg in sys.argv[1:]:
        if len(arg) == 7 and arg[4] == "-":
            return int(arg[:4]), int(arg[5:7])
    return _last_full_month()


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def fetch_all(session: requests.Session, url: str, page: int = 5000, timeout: int = 120):
    rows = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        r = session.get(page_url, timeout=timeout)
        r.raise_for_status()
        batch = r.json().get("value", []) or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def pick_best_candidate(candidates):
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda row: (
            1 if row.get("DeletionMark") else 0,
            len(normalize_name(row.get("Description", ""))),
            row.get("Description", ""),
        ),
    )[0]


def resolve_department_row(rows, exact_index, display_name: str, aliases: tuple[str, ...]):
    search = (display_name,) + aliases
    for alias in search:
        found = pick_best_candidate(exact_index.get(normalize_name(alias), []))
        if found:
            return found
    for alias in search:
        alias_norm = normalize_name(alias)
        contains = [row for row in rows if alias_norm in normalize_name(row.get("Description", ""))]
        found = pick_best_candidate(contains)
        if found:
            return found
    raw_parts = re.split(r"[/\\|]+", display_name)
    if len(raw_parts) >= 2:
        p0, p1 = normalize_name(raw_parts[0]), normalize_name(raw_parts[1])
        if len(p0) >= 2 and len(p1) >= 2:
            dual = [
                row
                for row in rows
                if p0 in normalize_name(row.get("Description", ""))
                and p1 in normalize_name(row.get("Description", ""))
            ]
            found = pick_best_candidate(dual)
            if found:
                return found
    return None


def load_structure(session: requests.Session):
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,DeletionMark"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url, timeout=60)
    by_key = {}
    by_parent = defaultdict(list)
    exact_index = defaultdict(list)
    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        by_parent[row.get("Parent_Key")].append(row)
        exact_index[normalize_name(row.get("Description", ""))].append(row)
    return rows, by_key, by_parent, exact_index


def build_struct_key_to_fot_group(name_to_key: dict[str, str], by_key) -> dict[str, str]:
    spec_keys = frozenset(name_to_key.values())
    ref_to_display = {v: k for k, v in name_to_key.items()}
    out: dict[str, str] = {}
    for k in by_key:
        cur = k
        seen: set[str] = set()
        while cur and cur not in seen and cur != EMPTY:
            seen.add(cur)
            if cur in spec_keys:
                out[k] = ref_to_display[cur]
                break
            row = by_key.get(cur)
            if not row:
                break
            par = row.get("Parent_Key")
            if not par or par == EMPTY:
                break
            cur = par
    return out


def load_fot_spec_structure_map(session: requests.Session):
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,DeletionMark"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url, timeout=60)
    exact_index = defaultdict(list)
    for row in rows:
        exact_index[normalize_name(row.get("Description", ""))].append(row)

    name_to_key: dict[str, str] = {}
    name_to_structure_label: dict[str, str] = {}
    key_first_name: dict[str, str] = {}
    for display_name, aliases in FOT_SPEC:
        found = resolve_department_row(rows, exact_index, display_name, aliases)
        if not found:
            continue
        key = found["Ref_Key"]
        if key in key_first_name and key_first_name[key] != display_name:
            continue
        key_first_name[key] = display_name
        name_to_key[display_name] = key
        name_to_structure_label[display_name] = (found.get("Description") or "").strip()
    return name_to_key, name_to_structure_label


def load_cost_articles(session: requests.Session):
    url = (
        f"{BASE}/{quote('ChartOfCharacteristicTypes_СтатьиРасходов')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url)
    return {row["Ref_Key"]: (row.get("Description") or "").strip() for row in rows if row.get("Ref_Key")}


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
        rows = fetch_all(session, url, page=200, timeout=60)
        for row in rows:
            key = row.get("Ref_Key")
            if key and key not in collected:
                collected.add(key)
                frontier.append(key)
    return collected


def _load_register_rows_26(session: requests.Session, year: int, month: int) -> list[dict]:
    p_start, p_end = _month_period_bounds(year, month)
    target_accounts = _get_subaccounts(session, ACCOUNT_26_ROOT)
    acc_filter = " or ".join(f"AccountDr_Key eq guid'{key}'" for key in sorted(target_accounts))
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and ({acc_filter})"
    )
    sel = (
        "Period,AccountDr_Key,ПодразделениеDr_Key,Сумма,Сторно,"
        "ExtDimensionDr1,ExtDimensionTypeDr1_Key"
    )
    url = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    return fetch_all(session, url)


def _collect_subtree(root_key: str, by_parent) -> set[str]:
    keys: set[str] = set()
    stack = [root_key]
    while stack:
        current = stack.pop()
        if current in keys:
            continue
        keys.add(current)
        for child in by_parent.get(current, []):
            child_key = child.get("Ref_Key")
            if child_key:
                stack.append(child_key)
    return keys


def _load_doc_entities(session: requests.Session) -> list[str]:
    return [
        "Document_ЗаявкаНаРасходованиеДенежныхСредств",
        "Document_ЗаявкаНаРасходованиеДС",
        "Document_ЗаявкаНаРасходованиеДенежныхСредствТД",
        "Document_ТД_ЗаявкаНаРасходованиеДенежныхСредств",
    ]


def _load_tab_entities(doc_entity: str) -> list[str]:
    return [
        f"{doc_entity}_ТЧ",
        f"{doc_entity}_Строки",
        f"{doc_entity}_Расшифровка",
        f"{doc_entity}_ТабличнаяЧасть",
    ]


def _discover_request_entities(session: requests.Session) -> tuple[str, str | None]:
    def _probe(url: str) -> bool:
        try:
            resp = session.get(url, timeout=30)
            if not resp.ok:
                return False
            data = resp.json()
            return bool(data.get("value"))
        except Exception:
            return False

    for doc_entity in _load_doc_entities(session):
        url = f"{BASE}/{quote(doc_entity)}?$format=json&$top=1"
        if not _probe(url):
            continue
        for tab_entity in _load_tab_entities(doc_entity):
            tab_url = f"{BASE}/{quote(tab_entity)}?$format=json&$top=1"
            if _probe(tab_url):
                return doc_entity, tab_entity
        return doc_entity, None
    return _load_doc_entities(session)[0], None


def _fetch_request_docs(session: requests.Session, doc_entity: str, year: int, month: int):
    p_start, p_end = _month_period_bounds(year, month)
    filters = [
        f"Date ge datetime'{p_start}' and Date lt datetime'{p_end}' and Posted eq true and DeletionMark eq false",
        f"Date ge datetime'{p_start}' and Date lt datetime'{p_end}' and DeletionMark eq false",
        f"Date ge datetime'{p_start}' and Date lt datetime'{p_end}'",
    ]
    for flt in filters:
        url = f"{BASE}/{quote(doc_entity)}?$format=json&$filter={quote(flt, safe='')}"
        docs = fetch_all(session, url, timeout=120)
        if docs:
            return docs, {"doc_filter": flt}
    return [], {"doc_filter": filters[-1]}


def _fetch_request_lines(session: requests.Session, tab_entity: str | None, doc_ref: str) -> list[dict]:
    if not tab_entity:
        return []
    for field in ("Owner_Key", "Document_Key", "Ref_Key"):
        flt = f"{field} eq guid'{doc_ref}'"
        url = f"{BASE}/{quote(tab_entity)}?$format=json&$filter={quote(flt, safe='')}"
        try:
            rows = fetch_all(session, url, timeout=120)
        except Exception:
            rows = []
        if rows:
            return rows
    return []


def _extract_request_lines_from_doc(doc: dict) -> list[dict]:
    for value in doc.values():
        if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            return value
    return []


def _pick_first(item: dict, fields: tuple[str, ...]):
    for field in fields:
        if field in item and item.get(field) not in (None, ""):
            return item.get(field)
    return None


def _classify_request_status(status: str) -> str:
    low = normalize_name(status)
    if not low:
        return "unknown"
    if any(h in low for h in ("не оплачен", "неоплачен", "аннулир", "отмен", "не провед", "без оплаты")):
        return "unpaid"
    if "частич" in low:
        return "partial"
    if any(h in low for h in ("оплачен", "оплачено", "полная оплата", "к оплате")):
        return "full"
    return "unknown"


def _line_amount(line: dict) -> float:
    value = _pick_first(line, ("Сумма", "СуммаСтроки", "СуммаОплатыРегл", "СуммаОплаты", "ОплаченоРегл", "Оплачено", "СуммаФакт"))
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _line_paid_amount(line: dict) -> float | None:
    value = _pick_first(line, ("СуммаОплатыРегл", "СуммаОплаты", "ОплаченоРегл", "Оплачено", "СуммаФакт"))
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _extract_doc_paid_amount(doc: dict) -> float | None:
    value = _pick_first(doc, ("СуммаОплатыРегл", "СуммаОплаты", "ОплаченоРегл", "Оплачено", "СуммаФакт", "СуммаПлатежа", "СуммаДокумента"))
    try:
        amount = float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None
    return amount if amount is not None and amount > 0 else None


def _classify_article_name(value) -> str:
    norm = normalize_name(str(value or ""))
    if norm in TD_M3_ARTICLE_NORMS:
        for target in TD_M3_ARTICLES:
            if normalize_name(target) == norm:
                return target
    return ""


def _nav_description(session: requests.Session, nav_url: str | None, cache: dict[str, str]) -> str:
    if not nav_url:
        return ""
    if nav_url in cache:
        return cache[nav_url]
    try:
        resp = session.get(f"{BASE}/{nav_url}", timeout=20)
        if not resp.ok or not resp.text:
            cache[nav_url] = ""
            return ""
        root = ET.fromstring(resp.text)
        ns = {"d": "http://schemas.microsoft.com/ado/2007/08/dataservices"}
        node = root.find(".//d:Description", ns)
        text = (node.text or "").strip() if node is not None else ""
    except Exception:
        text = ""
    cache[nav_url] = text
    return text


def _request_department_candidates(session: requests.Session, doc: dict, line: dict, nav_cache: dict[str, str]) -> list[str]:
    candidates: list[str] = []
    for field in ("Подразделение_Key", "Подразделение", "ТД_ЦФО_Key", "ТД_ЦФО"):
        val = line.get(field)
        if val not in (None, ""):
            candidates.append(str(val).strip())
    for field in ("Подразделение_Key", "Подразделение", "ТД_ЦФО_Key", "ТД_ЦФО"):
        val = doc.get(field)
        if val not in (None, ""):
            candidates.append(str(val).strip())
        nav_desc = _nav_description(session, doc.get(f"{field.replace('_Key', '')}@navigationLinkUrl"), nav_cache)
        if nav_desc:
            candidates.append(nav_desc)
    return candidates


def _resolve_request_group(raw_value: str, struct_map: dict[str, str], structure_rows: list[dict], exact_index: dict) -> str | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    if value in struct_map:
        return struct_map[value]
    found = resolve_department_row(structure_rows, exact_index, value, ())
    if found:
        key = found.get("Ref_Key")
        if key and key in struct_map:
            return struct_map[key]
    norm = normalize_name(value)
    for display_name, aliases in FOT_SPEC:
        if norm == normalize_name(display_name):
            return display_name
        for alias in aliases:
            alias_norm = normalize_name(alias)
            if alias_norm and (alias_norm == norm or alias_norm in norm or norm in alias_norm):
                return display_name
    return None


def calc_budget_26_month(session: requests.Session, year: int, month: int) -> dict:
    p_start, p_end = _month_period_bounds(year, month)
    structure_rows, by_key, _, _exact_index = load_structure(session)
    name_to_key, name_to_structure_label = load_fot_spec_structure_map(session)
    struct_map = build_struct_key_to_fot_group(name_to_key, by_key)
    article_names = load_cost_articles(session)
    records = _load_register_rows_26(session, year, month)

    matrix = {dept: {"by_article": defaultdict(float), "total": 0.0, "rows": 0} for dept in FOT_GROUP_ORDER}
    article_totals = defaultdict(float)
    counts = {
        "records_total": len(records),
        "records_taken": 0,
        "skipped_no_dept": 0,
        "skipped_not_target_dept": 0,
        "skipped_not_target_article": 0,
    }
    total = 0.0

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
        if not dept_key or dept_key == EMPTY:
            counts["skipped_no_dept"] += 1
            continue
        group_name = struct_map.get(dept_key)
        if not group_name:
            counts["skipped_not_target_dept"] += 1
            continue
        if rec.get("ExtDimensionTypeDr1_Key") != SUBCONTO_TYPE_COST:
            counts["skipped_not_target_article"] += 1
            continue

        article_key = rec.get("ExtDimensionDr1") or ""
        article_name = article_names.get(article_key, "")
        canonical_article = _classify_article_name(article_name)
        if not canonical_article:
            counts["skipped_not_target_article"] += 1
            continue

        try:
            amount = float(rec.get("Сумма") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if rec.get("Сторно"):
            amount = -amount
        if amount == 0:
            continue

        row = matrix[group_name]
        row["by_article"][canonical_article] += amount
        row["total"] += amount
        row["rows"] += 1
        article_totals[canonical_article] += amount
        total += amount
        counts["records_taken"] += 1

    departments = []
    for dept in FOT_GROUP_ORDER:
        row = matrix[dept]
        departments.append(
            {
                "department": dept,
                "structure_label": name_to_structure_label.get(dept, ""),
                "by_article": {article: round(float(row["by_article"][article]), 2) for article in TARGET_ARTICLES},
                "total": round(float(row["total"]), 2),
                "rows": int(row["rows"]),
            }
        )

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month - 1],
        "period": {"start": p_start[:10], "end": p_end[:10]},
        "departments": departments,
        "article_totals": {article: round(float(amount), 2) for article, amount in article_totals.items()},
        "total": round(total, 2),
        "counts": counts,
        "departments_count": len(FOT_GROUP_ORDER),
        "records_total": len(records),
        "records_taken": counts["records_taken"],
    }


def calc_td_m3_fact_month(session: requests.Session, year: int, month: int, verbose: bool = False) -> dict:
    p_start, p_end = _month_period_bounds(year, month)
    structure_rows, by_key, by_parent, exact_index = load_structure(session)
    root = resolve_department_row(structure_rows, exact_index, TD_M3_ROOT_NAME, ())
    subtree_keys = _collect_subtree(root["Ref_Key"], by_parent) if root else set()
    nav_cache: dict[str, str] = {}

    log = print if verbose else (lambda *args, **kwargs: None)

    log("  [1/5] Определение сущностей документа и строк...")
    doc_entity, tab_entity = _discover_request_entities(session)
    log(f"        документ: {doc_entity}")
    log(f"        строки:   {tab_entity or '<из документа>'}")
    log("  [2/5] Загрузка заявок за период...")
    docs, _meta = _fetch_request_docs(session, doc_entity, year, month)
    log(f"        документов найдено: {len(docs)}")
    if not docs:
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month - 1],
            "period": {"start": p_start[:10], "end": p_end[:10]},
            "total_fact": 0.0,
            "records_total": 0,
            "records_taken": 0,
            "skipped_not_scenario": 0,
            "skipped_not_dept": 0,
            "subtree_size": len(subtree_keys),
            "groups": [],
        }

    totals_by_group: dict[str, float] = defaultdict(float)
    totals_by_group_docs: dict[str, int] = defaultdict(int)
    totals_by_group_lines: dict[str, int] = defaultdict(int)
    included_docs: list[dict] = []

    counts = {
        "docs_total": len(docs),
        "docs_included": 0,
        "docs_skipped_status": 0,
        "docs_skipped_no_paid_amount": 0,
        "docs_skipped_empty": 0,
        "lines_total": 0,
        "lines_included": 0,
        "lines_skipped_dept": 0,
        "lines_skipped_empty": 0,
        "allocation_rule": "doc_paid_amount_proportional_by_line_amount",
    }

    total_fact = 0.0

    name_to_key, _name_to_structure_label = load_fot_spec_structure_map(session)
    struct_map = build_struct_key_to_fot_group(name_to_key, by_key)

    log("  [3/5] Обработка документов и распределение сумм...")
    for idx, doc in enumerate(docs, start=1):
        if idx % 25 == 0:
            log(f"        обработано документов: {idx}/{len(docs)}")
        if doc.get("DeletionMark") or doc.get("Deleted") or doc.get("Posted") is False:
            counts["skipped_not_scenario"] += 1
            continue

        doc_ref = str(doc.get("Ref_Key") or "").strip()
        if not doc_ref:
            counts["docs_skipped_empty"] += 1
            continue

        lines = _extract_request_lines_from_doc(doc)
        if not lines:
            lines = _fetch_request_lines(session, tab_entity, doc_ref)
        if not lines:
            counts["docs_skipped_empty"] += 1
            continue

        status = str(_pick_first(doc, ("Статус", "Состояние", "Status", "State")) or "")
        status_kind = _classify_request_status(status)
        has_line_paid_values = any((_line_paid_amount(line) or 0) > 0 for line in lines)
        if status_kind == "unpaid" and not has_line_paid_values:
            counts["docs_skipped_status"] += 1
            continue

        base_total = sum(_line_amount(line) for line in lines)
        if base_total <= 0:
            counts["docs_skipped_empty"] += 1
            continue

        doc_paid_amount = _extract_doc_paid_amount(doc)
        paid_source = "header_paid_amount"
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
                "number": str(doc.get("Number") or "").strip(),
                "date": str(doc.get("Date") or "")[:10],
                "status": status,
                "status_kind": status_kind,
                "paid_amount": round(doc_paid_amount or 0.0, 2),
                "paid_source": paid_source,
                "lines": len(lines),
            }
        )

        doc_allocated = 0.0
        for line in lines:
            raw_dept_values = _request_department_candidates(session, doc, line, nav_cache)
            group_name = None
            for raw_value in raw_dept_values:
                group_name = _resolve_request_group(raw_value, struct_map, structure_rows, exact_index)
                if group_name:
                    break
            if not group_name:
                counts["lines_skipped_dept"] += 1
                continue

            line_amount = _line_amount(line)
            if line_amount <= 0:
                counts["lines_skipped_empty"] += 1
                continue

            line_paid_amount = _line_paid_amount(line)
            if line_paid_amount is not None and line_paid_amount > 0:
                allocated = float(line_paid_amount)
            elif doc_paid_amount is not None:
                allocated = round(doc_paid_amount * (line_amount / base_total), 2)
            else:
                continue

            totals_by_group[group_name] += allocated
            totals_by_group_docs[group_name] += 1
            totals_by_group_lines[group_name] += 1
            total_fact += allocated
            doc_allocated += allocated
            counts["lines_included"] += 1

        if doc_paid_amount is not None and doc_allocated > 0:
            diff = round(float(doc_paid_amount) - float(doc_allocated), 2)
            if abs(diff) >= 0.01:
                counts.setdefault("doc_allocation_diff_total", 0.0)
                counts["doc_allocation_diff_total"] = round(
                    float(counts.get("doc_allocation_diff_total", 0.0)) + diff,
                    2,
                )

    groups = []
    for name in FOT_GROUP_ORDER:
        groups.append(
            {
                "department": name,
                "fact_total": round(float(totals_by_group.get(name, 0.0)), 2),
                "rows": int(totals_by_group_lines.get(name, 0)),
                "docs": int(totals_by_group_docs.get(name, 0)),
            }
        )

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month - 1],
        "period": {"start": p_start[:10], "end": p_end[:10]},
        "total_fact": round(total_fact, 2),
        "records_total": len(docs),
        "records_taken": counts["docs_included"],
        "skipped_not_scenario": counts["docs_skipped_status"],
        "skipped_not_dept": counts["lines_skipped_dept"],
        "counts": counts,
        "subtree_size": len(subtree_keys),
        "groups": groups,
        "included_docs": included_docs[:50],
    }


def get_td_m3_fact_month(year: int, month: int) -> dict:
    session = requests.Session()
    session.auth = AUTH
    return calc_td_m3_fact_month(session, year, month, verbose=False)


def calc_month(year: int, month: int) -> dict:
    session = requests.Session()
    session.auth = AUTH

    budget_26 = calc_budget_26_month(session, year, month)
    budget_m3 = calc_td_m3_fact_month(session, year, month, verbose=True)

    total_26 = float(budget_26.get("total") or 0)
    total_m3 = float(budget_m3.get("total_fact") or 0)

    return {
        "year": year,
        "month": month,
        "month_name": budget_26.get("month_name") or budget_m3.get("month_name"),
        "budget_26": budget_26,
        "budget_m3": budget_m3,
        "total_26": round(total_26, 2),
        "total_m3_fact": round(total_m3, 2),
        "total": round(total_26 + total_m3, 2),
    }


def _fmt(value: float) -> str:
    return f"{value:,.2f}"


def print_budget_26(result: dict) -> None:
    print("=" * 140)
    print(f"  БЮДЖЕТ ТЕХДИРЕКЦИИ ПО СЧЁТУ 26 · {result['month_name']} {result['year']}")
    print(f"  Период: {result['period']['start']} — {result['period']['end']} (lt)")
    print("=" * 140)
    print(
        f"  Документов/записей: {result['records_total']} | "
        f"включено: {result['records_taken']} | "
        f"без подразделения: {result['counts']['skipped_no_dept']} | "
        f"нецелевые подразделения: {result['counts']['skipped_not_target_dept']} | "
        f"нецелевые статьи: {result['counts']['skipped_not_target_article']}"
    )

    col_width = 20
    dept_width = 44
    total_width = 16
    print()
    header = f"  {'Подразделение':<{dept_width}}"
    for article in TARGET_ARTICLES:
        header += f" {article[:col_width]:>{col_width}}"
    header += f" {'Итого':>{total_width}}"
    print(header)
    print(f"  {'-' * dept_width} {'-' * col_width * len(TARGET_ARTICLES)} {'-' * total_width}")

    column_totals = {article: 0.0 for article in TARGET_ARTICLES}
    for row in result["departments"]:
        line = f"  {row['department']:<{dept_width}}"
        for article in TARGET_ARTICLES:
            val = float(row["by_article"].get(article, 0.0))
            column_totals[article] += val
            line += f" {_fmt(val):>{col_width}}"
        line += f" {_fmt(row['total']):>{total_width}}"
        print(line)

    print(f"  {'-' * dept_width} {'-' * col_width * len(TARGET_ARTICLES)} {'-' * total_width}")
    total_line = f"  {'ИТОГО по статьям':<{dept_width}}"
    for article in TARGET_ARTICLES:
        total_line += f" {_fmt(column_totals[article]):>{col_width}}"
    total_line += f" {_fmt(result['total']):>{total_width}}"
    print(total_line)
    print()
    print(f"  Общий итог по счёту 26: {_fmt(result['total'])}")


def print_budget_m3(result: dict) -> None:
    print("\n" + "=" * 140)
    print(f"  БЮДЖЕТ TD-M3 (ФАКТ) · {result['month_name']} {result['year']}")
    period = result.get("period") or {}
    print(f"  Период: {period.get('start', '')} — {period.get('end', '')} (lt)")
    print("=" * 140)
    print(
        f"  Записей: {result['records_total']} | включено: {result['records_taken']} | "
        f"не те записи: {result['skipped_not_scenario']} | "
        f"нецелевое подразделение: {result['skipped_not_dept']}"
    )
    print(f"  Факт TD-M3: {_fmt(result['total_fact'])}")


def print_month(result: dict) -> None:
    print(f"\n  СУММА ДВУХ ЛОГИК ТЕХДИРЕКЦИИ · {result['month_name']} {result['year']}")
    print(f"  Бюджет сч. 26:       {_fmt(result['total_26'])}")
    print(f"  Бюджет TD-M3 (факт): {_fmt(result['total_m3_fact'])}")
    print(f"  {'-' * 30}")
    print(f"  ИТОГО:               {_fmt(result['total'])}")
    print()
    print_budget_26(result["budget_26"])
    print_budget_m3(result["budget_m3"])


def main() -> None:
    year, month = parse_period()
    t0 = time.time()
    result = calc_month(year, month)
    print_month(result)
    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
