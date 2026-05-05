from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import date
from typing import Any
from urllib.parse import quote

import requests

from . import calc_budget_limit, calc_fot_management
from .calc_budget_limit import AUTH, EMPTY, period_bounds
from .calc_prod_deputy_pc_common import (
    PC_BUDGET_PLAN,
    ShopKey,
    _normalize_period,
    build_payload,
    cache_path,
    load_json,
    month_row,
    save_json,
)

SOURCE_TAG_BUDGET = "prod_deputy_pc_budget_v4_paid_requests_by_cfo_nav"

PC_BUDGET_CFO_NAME: dict[ShopKey, str] = {
    "pc1": "Производство №1",
    "pc2": "Производство №2",
}

REQUEST_DOC_ENTITY = "Document_ЗаявкаНаРасходованиеДенежныхСредств"
_NAV_DESC_CACHE: dict[str, str] = {}


def _normalize_name(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    raw = raw.replace("№", " ")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in raw).split())


def _fetch_all(session: requests.Session, url: str, page: int = 5000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        r = session.get(f"{url}{sep}$top={page}&$skip={skip}", timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _load_request_docs(session: requests.Session, refs: set[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    ref_list = sorted(ref for ref in refs if ref and ref != EMPTY)
    for i in range(0, len(ref_list), 20):
        batch = ref_list[i:i + 20]
        flt = " or ".join(f"Ref_Key eq guid'{ref}'" for ref in batch)
        url = (
            f"{calc_budget_limit.BASE}/{quote(REQUEST_DOC_ENTITY)}"
            f"?$format=json&$filter={quote(flt, safe='')}"
        )
        for row in _fetch_all(session, url, page=100):
            ref = row.get("Ref_Key")
            if ref:
                out[ref] = row
    return out


def _nav_description(session: requests.Session, nav_url: str | None) -> str:
    if not nav_url:
        return ""
    cached = _NAV_DESC_CACHE.get(nav_url)
    if cached is not None:
        return cached
    try:
        r = session.get(f"{calc_budget_limit.BASE}/{nav_url}", timeout=30)
        if not r.ok or not r.text:
            _NAV_DESC_CACHE[nav_url] = ""
            return ""
        root = ET.fromstring(r.text)
        ns = {"d": "http://schemas.microsoft.com/ado/2007/08/dataservices"}
        desc = root.find(".//d:Description", ns)
        text = str(desc.text or "").strip() if desc is not None else ""
    except Exception:
        text = ""
    _NAV_DESC_CACHE[nav_url] = text
    return text


def _request_cfo_values(
    session: requests.Session,
    doc: dict[str, Any],
    structure_by_key: dict[str, dict],
) -> list[str]:
    values: list[str] = []
    for field in ("ТД_ЦФО", "ТД_ЦФО_Key"):
        value = doc.get(field)
        if value not in (None, ""):
            values.append(str(value).strip())
            row = structure_by_key.get(str(value).strip())
            if row and row.get("Description"):
                values.append(str(row["Description"]).strip())
        nav_name = field.replace("_Key", "")
        desc = _nav_description(session, doc.get(f"{nav_name}@navigationLinkUrl"))
        if desc:
            values.append(desc)
    return values


def _is_request_for_shop(
    session: requests.Session,
    doc: dict[str, Any],
    shop: ShopKey,
    structure_by_key: dict[str, dict],
) -> bool:
    target = _normalize_name(PC_BUDGET_CFO_NAME[shop])
    for value in _request_cfo_values(session, doc, structure_by_key):
        normalized = _normalize_name(value)
        if normalized == target or target in normalized:
            return True
    return False


def _budget_fact_paid_requests(session: requests.Session, shop: ShopKey, year: int, month: int) -> float:
    p_start, p_end = period_bounds(year, month)
    rows = calc_budget_limit.load_records(session, p_start, p_end)
    paid_by_request: dict[str, float] = {}

    for row in rows:
        sign = -1 if row.get("Сторно") else 1
        paid = float(row.get("СуммаОплаты") or 0) * sign
        if paid <= 0:
            continue
        request_key = row.get("ЗаявкаНаРасходованиеДенежныхСредств_Key")
        if not request_key or request_key == EMPTY:
            continue
        paid_by_request[request_key] = paid_by_request.get(request_key, 0.0) + paid

    if not paid_by_request:
        return 0.0

    docs = _load_request_docs(session, set(paid_by_request))
    structure_by_key, _by_parent = calc_fot_management._load_structure(session)
    total = 0.0
    for ref, paid in paid_by_request.items():
        doc = docs.get(ref)
        if doc and _is_request_for_shop(session, doc, shop, structure_by_key):
            total += paid
    return round(total, 2)


def get_pc_budget_monthly(shop: ShopKey, year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path("budget", shop, ref_year, ref_month)

    cached = load_json(path)
    if (
        cached is not None
        and cached.get("source") == SOURCE_TAG_BUDGET
        and cached.get("cache_date") == today.isoformat()
    ):
        return cached

    months_out: list[dict] = []
    session = requests.Session()
    session.auth = AUTH
    for mm in range(1, ref_month + 1):
        plan = float(PC_BUDGET_PLAN[shop][mm - 1])
        fact = _budget_fact_paid_requests(session, shop, ref_year, mm)
        months_out.append(month_row(ref_year, mm, plan, fact))

    payload = build_payload(SOURCE_TAG_BUDGET, shop, ref_year, ref_month, months_out)
    save_json(path, payload)
    return payload


__all__ = ["get_pc_budget_monthly"]
