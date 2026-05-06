from __future__ import annotations

import json
from datetime import date
from typing import Any, Literal
from urllib.parse import quote

import requests

from .calc_tekuchest_opdir import (
    AUTH,
    BASE,
    CAT_STRUKTURA,
    EMPTY,
    MONTH_RU,
    aggregate_month,
    load_all_tekuchest_docs,
    _normalize_period,
)
from .cache_manager import CACHE_DIR

ShopKey = Literal["pc1", "pc2"]

SOURCE_TAG = "prod_deputy_turnover_pc_v1"

PC1_DEPARTMENTS = (
    "Служба ремонта и обслуживания оборудования",
    "Планово-диспетчерская служба",
    "Участок ремонта пром.оборудования производственного цеха №1",
    "Производственный цех №1",
    "Монтажный участок №2",
    "Механический цех",
    "Механический участок №1",
    "Сборочный участок №1",
    "Сборочный участок №2",
    "Участок ультразвуковых датчиков",
    "Производство несерийных изделий",
    "Экспериментальный производственный цех",
    "Цех БМИ",
)

PC2_DEPARTMENTS = (
    "Производственный цех №2",
    "Участок переповерки приборов",
    "Ремонтный участок стендов",
    "Участок ремонта гарантийных приборов",
    "Участок Гранд SPI",
    "Участок сборки счетчиков",
    "Участок СПУ-5 (АЛМАЗ)",
    "Участок упаковки",
    "Участок ремонта гарантийных плат",
)


def cache_path(shop: ShopKey, year: int, ref_month: int):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"prod_deputy_turnover_{shop}_{year}_{ref_month:02d}.json"


def _load_json(path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _normalize_name(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    raw = raw.replace("№", " ")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in raw).split())


def _department_names(shop: ShopKey) -> tuple[str, ...]:
    return PC1_DEPARTMENTS if shop == "pc1" else PC2_DEPARTMENTS


def _load_structure(session: requests.Session) -> list[dict[str, Any]]:
    flt = quote("DeletionMark eq false", safe="")
    sel = quote("Ref_Key,Description,Parent_Key", safe=",_")
    url = f"{BASE}/{quote(CAT_STRUKTURA)}?$format=json&$filter={flt}&$select={sel}"
    rows: list[dict[str, Any]] = []
    skip = 0
    page = 500
    while True:
        r = session.get(f"{url}&$top={page}&$skip={skip}", timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _resolve_department_keys(
    rows: list[dict[str, Any]],
    names: tuple[str, ...],
) -> tuple[dict[str, str], list[str]]:
    by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_norm.setdefault(_normalize_name(row.get("Description")), []).append(row)

    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    for name in names:
        target = _normalize_name(name)
        candidates = by_norm.get(target) or [
            row for row in rows
            if target and target in _normalize_name(row.get("Description"))
        ]
        candidates.sort(key=lambda row: str(row.get("Description") or ""))
        if candidates and candidates[0].get("Ref_Key"):
            resolved[candidates[0]["Ref_Key"]] = candidates[0].get("Description") or name
        else:
            unresolved.append(name)
    return resolved, unresolved


def _kpi_pct(plan: float, fact: float) -> float | None:
    return round(fact / plan * 100, 1) if plan > 0 else None


def get_prod_deputy_turnover_monthly(
    shop: ShopKey,
    year: int | None = None,
    month: int | None = None,
) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(shop, ref_year, ref_month)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    session = requests.Session()
    session.auth = AUTH

    structure_rows = _load_structure(session)
    dept_names, unresolved = _resolve_department_keys(structure_rows, _department_names(shop))
    all_docs = load_all_tekuchest_docs(session)
    docs_dept = [doc for doc in all_docs if doc.get("Подразделение_Key", EMPTY) in dept_names]

    months_out: list[dict] = []
    total_plan = 0.0
    total_fact = 0.0
    for mm in range(1, ref_month + 1):
        summary, children = aggregate_month(docs_dept, dept_names, ref_year, mm)
        plan_total = round(float(summary.get("plan") or 0), 2)
        fact_total = round(float(summary.get("fact") or 0), 2)
        row = {
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": plan_total,
            "fact": fact_total,
            "kpi_pct": _kpi_pct(plan_total, fact_total),
            "has_data": abs(plan_total) > 0 or abs(fact_total) > 0,
            "values_unit": "чел.",
            "plan_by_dept": {
                child["name"]: round(float(child.get("plan") or 0), 2)
                for child in children
                if float(child.get("plan") or 0) != 0
            },
            "fact_by_dept": {
                child["name"]: round(float(child.get("fact") or 0), 2)
                for child in children
                if float(child.get("fact") or 0) != 0
            },
        }
        months_out.append(row)
        total_plan += plan_total
        total_fact += fact_total

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "shop": shop,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(months_out[-1]) if months_out else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if months_out else None,
            "total_fact": round(total_fact, 2) if months_out else None,
            "kpi_pct": _kpi_pct(total_plan, total_fact),
            "months_with_data": sum(1 for row in months_out if row.get("has_data")),
            "months_total": len(months_out),
            "values_unit": "чел." if months_out else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
        "debug": {
            "departments_requested": list(_department_names(shop)),
            "departments_resolved": dept_names,
            "unresolved_departments": unresolved,
        },
    }
    _save_json(path, payload)
    return payload


__all__ = ["ShopKey", "cache_path", "get_prod_deputy_turnover_monthly"]
