from __future__ import annotations

from datetime import date
from typing import Any
from urllib.parse import quote

import requests

from . import calc_fot_management
from .calc_budget_limit import AUTH, EMPTY, period_bounds
from .calc_prod_deputy_pc_common import (
    PC_FOT_PLAN,
    ShopKey,
    _normalize_period,
    build_payload,
    cache_path,
    load_json,
    month_row,
    save_json,
)

SOURCE_TAG_FOT = "prod_deputy_pc_fot_v4_pc1_pc2_account26_fact"

PC1_FOT_DEPARTMENTS = (
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
    "Служба подготовки производства",
)

PC2_FOT_DEPARTMENTS = (
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


def _normalize_name(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    raw = raw.replace("№", " ")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in raw).split())


def _resolve_department_keys(
    structure_by_key: dict[str, dict],
    department_names: tuple[str, ...],
) -> dict[str, str]:
    rows = [row for row in structure_by_key.values() if not row.get("DeletionMark")]
    by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_norm.setdefault(_normalize_name(row.get("Description")), []).append(row)

    resolved: dict[str, str] = {}
    for name in department_names:
        target = _normalize_name(name)
        candidates = by_norm.get(target) or [
            row
            for row in rows
            if target and target in _normalize_name(row.get("Description"))
        ]
        candidates.sort(key=lambda row: str(row.get("Description") or ""))
        if candidates and candidates[0].get("Ref_Key"):
            resolved[name] = candidates[0]["Ref_Key"]
    return resolved


def _blank_fot_articles_row() -> dict[str, Any]:
    return {
        "salary": 0.0,
        "insurance": 0.0,
        "total": 0.0,
        "rows": 0,
        "by_article": {article: 0.0 for article in calc_fot_management.ARTICLE_ORDER},
    }


def _fot_fact_account26(
    session: requests.Session,
    year: int,
    month: int,
    department_names: tuple[str, ...],
) -> dict[str, Any]:
    p_start, p_end = period_bounds(year, month)
    structure_by_key, _by_parent = calc_fot_management._load_structure(session)
    dept_name_to_key = _resolve_department_keys(structure_by_key, department_names)
    dept_key_to_name = {key: name for name, key in dept_name_to_key.items()}

    target_accounts = calc_fot_management._get_subaccounts(session, calc_fot_management.ACCOUNT_26_ROOT)
    acc_or = " or ".join(f"AccountDr_Key eq guid'{account}'" for account in sorted(target_accounts))
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and ({acc_or})"
    )
    sel = ",".join([
        "Period", "AccountDr_Key", "ПодразделениеDr_Key",
        "Сумма", "Сторно", "ExtDimensionDr1", "ExtDimensionTypeDr1_Key",
    ])
    url = (
        f"{calc_fot_management.BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = calc_fot_management._fetch_all(session, url)

    by_dept = {name: _blank_fot_articles_row() for name in department_names}
    by_article = {article: 0.0 for article in calc_fot_management.ARTICLE_ORDER}
    skipped_no_dept = 0
    skipped_not_target_dept = 0
    skipped_not_target_article = 0
    taken = 0

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
        if dept_key == EMPTY:
            skipped_no_dept += 1
            continue
        dept_name = dept_key_to_name.get(dept_key)
        if not dept_name:
            skipped_not_target_dept += 1
            continue

        article_key = None
        if rec.get("ExtDimensionTypeDr1_Key") == calc_fot_management.SUBCONTO_TYPE_COST:
            article_key = rec.get("ExtDimensionDr1")
        if article_key not in calc_fot_management.ARTICLE_SET:
            skipped_not_target_article += 1
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        row = by_dept[dept_name]
        row["by_article"][article_key] += amount
        row["total"] += amount
        row["rows"] += 1
        if article_key == calc_fot_management.ARTICLE_ORDER[0]:
            row["salary"] += amount
        elif article_key == calc_fot_management.ARTICLE_ORDER[1]:
            row["insurance"] += amount
        by_article[article_key] += amount
        taken += 1

    matrix = []
    for dept_name in department_names:
        row = by_dept[dept_name]
        matrix.append({
            "department": dept_name,
            "department_key": dept_name_to_key.get(dept_name, ""),
            "salary": round(row["salary"], 2),
            "insurance": round(row["insurance"], 2),
            "total": round(row["total"], 2),
            "rows": row["rows"],
            "by_article": {
                calc_fot_management.COST_ARTICLES.get(key, key): round(value, 2)
                for key, value in row["by_article"].items()
            },
        })

    total = round(sum(float(row["total"] or 0) for row in by_dept.values()), 2)
    by_article_named = {
        calc_fot_management.COST_ARTICLES.get(key, key): round(value, 2)
        for key, value in by_article.items()
    }
    return {
        "total": total,
        "matrix": matrix,
        "by_article": by_article_named,
        "totals": {
            "salary": round(by_article.get(calc_fot_management.ARTICLE_ORDER[0], 0.0), 2),
            "insurance": round(by_article.get(calc_fot_management.ARTICLE_ORDER[1], 0.0), 2),
            "total": total,
            "by_article": by_article_named,
        },
        "departments_count": len(dept_name_to_key),
        "unresolved_departments": [
            name for name in department_names if name not in dept_name_to_key
        ],
        "records_total": len(records),
        "records_taken": taken,
        "skipped_no_dept": skipped_no_dept,
        "skipped_not_target_dept": skipped_not_target_dept,
        "skipped_not_target_article": skipped_not_target_article,
    }


def _departments_for_shop(shop: ShopKey) -> tuple[str, ...]:
    return PC1_FOT_DEPARTMENTS if shop == "pc1" else PC2_FOT_DEPARTMENTS


def get_pc_fot_monthly(shop: ShopKey, year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path("fot", shop, ref_year, ref_month)

    cached = load_json(path)
    if (
        cached is not None
        and cached.get("source") == SOURCE_TAG_FOT
        and cached.get("cache_date") == today.isoformat()
    ):
        return cached

    months_out: list[dict] = []
    session = None
    for mm in range(1, ref_month + 1):
        plan = float(PC_FOT_PLAN[shop][mm - 1])
        extra = {}
        if session is None:
            session = requests.Session()
            session.auth = AUTH
        fact_payload = _fot_fact_account26(session, ref_year, mm, _departments_for_shop(shop))
        fact = float(fact_payload.get("total") or 0)
        extra = {
            "fact_matrix": fact_payload.get("matrix") or [],
            "fact_totals": fact_payload.get("totals") or {},
            "unresolved_departments": fact_payload.get("unresolved_departments") or [],
        }
        months_out.append(month_row(ref_year, mm, plan, fact, **extra))

    payload = build_payload(SOURCE_TAG_FOT, shop, ref_year, ref_month, months_out)
    save_json(path, payload)
    return payload


__all__ = ["get_pc_fot_monthly"]
