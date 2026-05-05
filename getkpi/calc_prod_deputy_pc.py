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
from datetime import date
from pathlib import Path
from typing import Literal

import requests

from . import calc_budget_limit, calc_budget_techdir_plan_fact as bdg
from . import calc_fot_management, fot_techdir_plan
from .calc_budget_limit import AUTH, EMPTY, DdsCache, period_bounds
from .calc_fot_management import MONTH_RU, _normalize_period, _prorate_if_current

ShopKey = Literal["pc1", "pc2"]

PC_SHOP_ROOT_NAME: dict[ShopKey, str] = {
    "pc1": "Производственный цех №1",
    "pc2": "Производственный цех №2",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG_BUDGET = "prod_deputy_pc_budget_v1"
SOURCE_TAG_FOT = "prod_deputy_pc_fot_v1"


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
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG_BUDGET:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    session = requests.Session()
    session.auth = AUTH
    department_keys = _subtree_keys_for_shop(session, shop)
    dds = DdsCache(session)

    months_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        row = calc_budget_limit.calc_month(
            session,
            ref_year,
            mm,
            dds,
            department_keys=department_keys,
        )
        plan = float(row.get("plan_total") or 0)
        fact = float(row.get("fact_total") or 0)
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
