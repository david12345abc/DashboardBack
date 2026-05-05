"""
Бюджет и ФОТ по производственным цехам для заместителя операционного директора.

Плитки:
  PD-M3.B1 — Бюджет (ПЦ1)
  PD-M3.B2 — Бюджет (ПЦ2)
  PD-M3.F1 — ФОТ (ПЦ1)
  PD-M3.F2 — ФОТ (ПЦ2)
"""
from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import requests

from . import calc_budget_techdir_plan_fact as bdg
from . import calc_fot_management, fot_techdir_plan
from . import calc_budget_limit
from .calc_budget_limit import AUTH, EMPTY, period_bounds
from .calc_fot_management import MONTH_RU, _normalize_period, _prorate_if_current

ShopKey = Literal["pc1", "pc2"]

PC_SHOP_ROOT_NAME: dict[ShopKey, str] = {
    "pc1": "Производственный цех №1",
    "pc2": "Производственный цех №2",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG_BUDGET = "prod_deputy_pc_budget_v4_paid_requests_by_cfo_nav"
SOURCE_TAG_FOT = "prod_deputy_pc_fot_v1"

PC_BUDGET_PLAN: dict[ShopKey, list[float]] = {
    "pc1": [
        64_758_916, 61_406_486, 99_353_943, 112_441_820,
        112_591_529, 130_145_238, 115_597_218, 116_376_536,
        123_139_472, 97_269_434, 91_633_804, 139_775_508,
    ],
    "pc2": [
        9_497_714, 12_208_323, 20_287_862, 23_073_103,
        24_128_390, 29_098_537, 34_492_161, 36_666_379,
        34_215_075, 31_442_195, 19_803_164, 16_736_543,
    ],
}

PC_BUDGET_CFO_NAME: dict[ShopKey, str] = {
    "pc1": "Производство №1",
    "pc2": "Производство №2",
}

REQUEST_DOC_ENTITY = "Document_ЗаявкаНаРасходованиеДенежныхСредств"
_NAV_DESC_CACHE: dict[str, str] = {}


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _cache_path(metric: str, shop: ShopKey, year: int, ref_month: int) -> Path:
    return CACHE_DIR / f"prod_deputy_{metric}_{shop}_{year}_{ref_month:02d}.json"


def _subtree_keys_for_shop(session: requests.Session, shop: ShopKey) -> frozenset[str]:
    root_name = PC_SHOP_ROOT_NAME[shop]
    by_key, by_parent = calc_fot_management._load_structure(session)
    root = calc_fot_management._resolve_department_root(by_key, root_name)
    rows = calc_fot_management._collect_subtree_ordered(root["Ref_Key"], by_key, by_parent)
    return frozenset(row["Ref_Key"] for row in rows if row.get("Ref_Key"))


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
        if row.get("Сторно"):
            sign = -1
        else:
            sign = 1
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


def _fot_plan_for_subtree(
    session: requests.Session,
    year: int,
    month: int,
    department_keys: frozenset[str],
) -> float:
    p_start, p_end = period_bounds(year, month)
    scenario_names = bdg.load_budget_scenarios(session)
    article_names = bdg.load_budget_articles(session)
    rows = fot_techdir_plan.load_budget_rows(session, p_start, p_end)
    total = 0.0

    for row in rows:
        if scenario_names.get(row.get("Сценарий_Key"), "") != fot_techdir_plan.BUDGET_SCENARIO:
            continue
        dk = row.get("Подразделение_Key") or ""
        if not dk or dk == EMPTY or dk not in department_keys:
            continue
        article_key = row.get("СтатьяБюджетов") or ""
        article_name = article_names.get(article_key, "")
        if not fot_techdir_plan.classify_plan_article(article_name, "payroll"):
            continue
        total += float(row.get("СуммаСценария") or 0)

    return round(total, 2)


def _build_payload(
    *,
    source_tag: str,
    shop: ShopKey,
    ref_year: int,
    ref_month: int,
    months_out: list[dict],
) -> dict:
    today = date.today()
    with_data = [row for row in months_out if row.get("has_data")]
    last_data_row = with_data[-1] if with_data else (months_out[-1] if months_out else None)
    total_plan = sum(float(row.get("plan") or 0) for row in months_out if row.get("plan") is not None)
    total_fact = sum(float(row.get("fact") or 0) for row in months_out)

    return {
        "cache_date": today.isoformat(),
        "source": source_tag,
        "shop": shop,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(last_data_row) if last_data_row else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if months_out else None,
            "total_fact": round(total_fact, 2) if months_out else None,
            "kpi_pct": round(total_fact / total_plan * 100, 1) if total_plan > 0 else None,
            "months_with_data": len(with_data),
            "months_total": len(months_out),
            "values_unit": "руб." if months_out else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": (last_data_row or {}).get("year", ref_year),
            "month": (last_data_row or {}).get("month", ref_month),
            "month_name": (last_data_row or {}).get("month_name", MONTH_RU[ref_month].lower()),
        },
    }


def get_pc_budget_monthly(shop: ShopKey, year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path("budget", shop, ref_year, ref_month)

    cached = _load_json(cache_path)
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
        months_out.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan, 2),
            "fact": round(fact, 2),
            "kpi_pct": round(fact / plan * 100, 1) if plan > 0 else None,
            "has_data": abs(plan) > 0 or abs(fact) > 0,
            "values_unit": "руб.",
        })

    payload = _build_payload(
        source_tag=SOURCE_TAG_BUDGET,
        shop=shop,
        ref_year=ref_year,
        ref_month=ref_month,
        months_out=months_out,
    )
    _save_json(cache_path, payload)
    return payload


def get_pc_fot_monthly(shop: ShopKey, year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path("fot", shop, ref_year, ref_month)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG_FOT:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    session = requests.Session()
    session.auth = AUTH
    root_name = PC_SHOP_ROOT_NAME[shop]
    department_keys = _subtree_keys_for_shop(session, shop)

    months_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        fact_payload = calc_fot_management.calc_fact_for_department_root(session, ref_year, mm, root_name)
        fact = float(fact_payload.get("total") or 0)
        plan_raw = _fot_plan_for_subtree(session, ref_year, mm, department_keys)
        plan = _prorate_if_current(plan_raw, ref_year, mm)
        months_out.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan, 2) if plan is not None else None,
            "fact": round(fact, 2),
            "kpi_pct": round(fact / plan * 100, 1) if plan and plan > 0 else None,
            "has_data": (plan is not None and plan > 0) or abs(fact) > 0,
            "values_unit": "руб.",
        })

    payload = _build_payload(
        source_tag=SOURCE_TAG_FOT,
        shop=shop,
        ref_year=ref_year,
        ref_month=ref_month,
        months_out=months_out,
    )
    _save_json(cache_path, payload)
    return payload
