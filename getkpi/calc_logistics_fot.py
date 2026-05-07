from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests

from .cache_manager import CACHE_DIR
from .calc_fot_management import (
    ACCOUNT_26_ROOT,
    ARTICLE_ORDER,
    ARTICLE_SET,
    AUTH,
    BASE,
    COST_ARTICLES,
    EMPTY,
    SUBCONTO_TYPE_COST,
    _fetch_all,
    _get_subaccounts,
    _load_structure,
    _resolve_department_root,
    normalize_name,
    period_bounds,
)

SOURCE_TAG = "logistics_fot_v3_account26_mto_fact"
TARGET_DEPARTMENT_NAME = "Отдел МТО"

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

LOGISTICS_FOT_PLAN = (
    1_050_784,
    1_176_121,
    1_348_550,
    1_297_619,
    1_298_008,
    1_496_772,
    1_588_396,
    1_470_042,
    1_660_871,
    1_531_188,
    1_249_810,
    1_285_241,
)


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_fot_{year}_{ref_month:02d}.json"


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


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month) if month is not None else (today.month if ref_year == today.year else 12)
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _month_row(year: int, month: int, plan: float, fact_payload: dict | None) -> dict:
    fact_payload = fact_payload or {}
    fact_total = float(fact_payload.get("total") or 0)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "plan": round(float(plan), 2),
        "fact": round(fact_total, 2),
        "period_plan": round(float(plan), 2),
        "period_fact": round(fact_total, 2),
        "kpi_pct": round(fact_total / float(plan) * 100, 1) if plan else None,
        "has_data": True,
        "values_unit": "руб.",
        "fact_by_article": fact_payload.get("by_article") or {},
        "fact_matrix": fact_payload.get("matrix") or [],
        "fact_totals": fact_payload.get("totals") or {},
        "records_taken": fact_payload.get("records_taken"),
    }


def _blank_article_totals() -> dict[str, float]:
    return {article_key: 0.0 for article_key in ARTICLE_ORDER}


def _load_cost_article_names(session: requests.Session) -> dict[str, str]:
    article_names: dict[str, str] = dict(COST_ARTICLES)
    keys = [key for key in ARTICLE_ORDER if key and key != EMPTY]
    if not keys:
        return article_names
    flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in keys)
    url = (
        f"{BASE}/{quote('Catalog_СтатьиЗатрат')}"
        f"?$format=json&$filter={quote(flt, safe='')}"
        "&$select=Ref_Key,Description"
    )
    try:
        for row in _fetch_all(session, url, page=100):
            key = row.get("Ref_Key")
            if key and row.get("Description"):
                article_names[str(key)] = str(row["Description"]).strip()
    except requests.RequestException:
        return article_names
    return article_names


def _calc_mto_fact(session: requests.Session, year: int, month: int) -> dict:
    p_start, p_end = period_bounds(year, month)
    structure_by_key, _by_parent = _load_structure(session)
    department = _resolve_department_root(structure_by_key, TARGET_DEPARTMENT_NAME)
    department_key = department["Ref_Key"]
    department_name = str(department.get("Description") or TARGET_DEPARTMENT_NAME).strip()
    article_names = _load_cost_article_names(session)
    article_name_norms = {
        key: normalize_name(article_names.get(key) or COST_ARTICLES.get(key))
        for key in ARTICLE_ORDER
    }
    target_accounts = _get_subaccounts(session, ACCOUNT_26_ROOT)
    account_filter = " or ".join(f"AccountDr_Key eq guid'{key}'" for key in sorted(target_accounts))
    flt = (
        f"Period ge datetime'{p_start}'"
        f" and Period lt datetime'{p_end}'"
        f" and Active eq true"
        f" and ({account_filter})"
    )
    sel = ",".join([
        "Period",
        "AccountDr_Key",
        "ПодразделениеDr_Key",
        "Сумма",
        "Сторно",
        "ExtDimensionDr1",
        "ExtDimensionTypeDr1_Key",
    ])
    url = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = _fetch_all(session, url)

    by_article = _blank_article_totals()
    counts = {
        "records_total": len(records),
        "records_taken": 0,
        "skipped_no_dept": 0,
        "skipped_not_target_dept": 0,
        "skipped_not_target_article": 0,
    }
    target_article_norms = {normalize_name(name) for name in COST_ARTICLES.values()}
    target_article_by_norm = {
        normalize_name(name): key
        for key, name in COST_ARTICLES.items()
    }

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
        if dept_key == EMPTY:
            counts["skipped_no_dept"] += 1
            continue
        if dept_key != department_key:
            counts["skipped_not_target_dept"] += 1
            continue

        article_key = None
        if rec.get("ExtDimensionTypeDr1_Key") == SUBCONTO_TYPE_COST:
            candidate = rec.get("ExtDimensionDr1")
            candidate_norm = article_name_norms.get(candidate)
            if candidate in ARTICLE_SET:
                article_key = candidate
            elif candidate_norm in target_article_norms:
                article_key = target_article_by_norm.get(candidate_norm)
        if article_key not in ARTICLE_SET:
            counts["skipped_not_target_article"] += 1
            continue

        amount = float(rec.get("Сумма") or 0)
        if rec.get("Сторно"):
            amount = -amount
        by_article[article_key] += amount
        counts["records_taken"] += 1

    by_article = {key: round(value, 2) for key, value in by_article.items()}
    total = round(sum(float(value or 0) for value in by_article.values()), 2)
    matrix_row = {
        "department_key": department_key,
        "department": department_name,
        "by_article": by_article,
        "salary": by_article.get(ARTICLE_ORDER[0], 0.0),
        "insurance": by_article.get(ARTICLE_ORDER[1], 0.0),
        "total": total,
        "rows": counts["records_taken"],
    }
    return {
        "total": total,
        "by_article": by_article,
        "by_dept": {department_key: total},
        "matrix": [matrix_row],
        "totals": {
            "by_article": by_article,
            "by_department": {department_key: total},
            "total": total,
        },
        "department": {
            "key": department_key,
            "name": department_name,
        },
        "cost_articles": {
            key: article_names.get(key) or COST_ARTICLES[key]
            for key in ARTICLE_ORDER
        },
        **counts,
    }


def get_logistics_fot_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = _cache_path(ref_year, ref_month)
    cached = _load_json(path)
    if cached and cached.get("source") == SOURCE_TAG and cached.get("cache_date") == today.isoformat():
        return cached

    session = requests.Session()
    session.auth = AUTH
    facts_by_month = {
        mm: _calc_mto_fact(session, ref_year, mm)
        for mm in range(1, ref_month + 1)
    }
    months = [
        _month_row(ref_year, idx + 1, plan, facts_by_month.get(idx + 1))
        for idx, plan in enumerate(LOGISTICS_FOT_PLAN)
    ]
    total_plan = sum(LOGISTICS_FOT_PLAN)
    total_fact = sum(float(row.get("fact") or 0) for row in months[:ref_month])
    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months,
        "last_full_month_row": months[ref_month - 1],
        "ytd": {
            "total_plan": round(float(total_plan), 2),
            "total_fact": round(total_fact, 2),
            "kpi_pct": round(total_fact / float(total_plan) * 100, 1) if total_plan else None,
            "months_with_data": ref_month,
            "months_total": 12,
            "values_unit": "руб.",
        },
        "kpi_period": {
            "type": "year",
            "year": ref_year,
        },
    }
    _save_json(path, payload)
    return payload


__all__ = ["get_logistics_fot_monthly"]
