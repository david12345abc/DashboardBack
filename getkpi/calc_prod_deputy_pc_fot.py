from __future__ import annotations

from collections import defaultdict
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

SOURCE_TAG_FOT = "prod_deputy_pc_fot_v7_selected_month_pc1_pc2"

ALMAZ_ORG = "fbca2146-6cfd-11e7-812d-001e67112509"
ACCOUNT_25_ROOT = "fb2bde42-6250-11e7-812d-001e67112509"
ACCOUNT_26_ROOT = "fb2bde43-6250-11e7-812d-001e67112509"

PC1_TURB_ARTICLES = {
    "524cd9aa-215a-11e0-b91c-00248c26ee57": "Оплата труда (26 сч) НПО АУП!",
    "b2913ba1-768e-11e7-812e-001e67112509": "Страховые взносы (26 сч) НПО АУП!",
    "d7ff61fc-16ff-11e8-826f-ac1f6b05524d": "Оплата труда (26 сч) пр-во ПРОЕКТЫ!",
    "cdef00a4-2760-11ec-870c-ac1f6b05524d": "Оплата труда (26 сч) УРМЦ!",
    "ad496ef1-65ce-11ea-832e-ac1f6b05524c": "Страховые взносы (26 сч) НПО пр-во ПРОЕКТЫ!",
    "dfccf566-2760-11ec-870c-ac1f6b05524d": "Страховые взносы (26 сч) УРМЦ!",
    "f616cb07-bbc0-11ec-887a-ac1f6b05524d": "Оплата труда (26 сч) БМИ!",
    "ff81ee87-bbc0-11ec-887a-ac1f6b05524d": "Страховые взносы (26 сч) БМИ!",
}
PC1_TURB_ARTICLE_ORDER = tuple(PC1_TURB_ARTICLES)
PC1_TURB_ARTICLE_SET = frozenset(PC1_TURB_ARTICLES)
PC1_TURB_SALARY_ARTICLES = {
    "524cd9aa-215a-11e0-b91c-00248c26ee57",
    "d7ff61fc-16ff-11e8-826f-ac1f6b05524d",
    "cdef00a4-2760-11ec-870c-ac1f6b05524d",
    "f616cb07-bbc0-11ec-887a-ac1f6b05524d",
}

PC2_ALMAZ_ARTICLES = {
    "03500036-7745-11e7-812e-001e67112509": "Оплата труда (сч 26) АЛМАЗ!",
    "d79cd550-7745-11e7-812e-001e67112509": "Страховые взносы (сч 26) АЛМАЗ!",
    "f48f70a1-3728-11e8-8271-ac1f6b05524d": "Оплата труда (сч 25) АЛМАЗ!",
    "227ca5b4-76c3-11e7-812e-001e67112509": "Страховые взносы (сч 25) АЛМАЗ!",
}
PC2_ALMAZ_ARTICLE_SET = frozenset(PC2_ALMAZ_ARTICLES)
PC2_ALMAZ_ACCOUNT26_ARTICLES = (
    "03500036-7745-11e7-812e-001e67112509",
    "d79cd550-7745-11e7-812e-001e67112509",
)
PC2_ALMAZ_ACCOUNT25_ARTICLES = (
    "f48f70a1-3728-11e8-8271-ac1f6b05524d",
    "227ca5b4-76c3-11e7-812e-001e67112509",
)
PC2_ALMAZ_SALARY_ARTICLES = {
    PC2_ALMAZ_ACCOUNT26_ARTICLES[0],
    PC2_ALMAZ_ACCOUNT25_ARTICLES[0],
}

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


def _blank_fot_articles_row(article_order: tuple[str, ...]) -> dict[str, Any]:
    return {
        "salary": 0.0,
        "insurance": 0.0,
        "total": 0.0,
        "rows": 0,
        "by_article": {article: 0.0 for article in article_order},
    }


def _blank_pc2_almaz_row() -> dict[str, Any]:
    return {
        "salary": 0.0,
        "insurance": 0.0,
        "total": 0.0,
        "rows": 0,
        "by_article": {article: 0.0 for article in PC2_ALMAZ_ACCOUNT26_ARTICLES},
    }


def _article_bucket(article_key: str) -> str:
    return "salary" if article_key in PC2_ALMAZ_SALARY_ARTICLES else "insurance"


def _named_article_totals(values: dict[str, float]) -> dict[str, float]:
    return {
        PC2_ALMAZ_ARTICLES.get(key, key): round(value, 2)
        for key, value in values.items()
    }


def _fot_fact_account26(
    session: requests.Session,
    year: int,
    month: int,
    department_names: tuple[str, ...],
    articles: dict[str, str] | None = None,
    article_order: tuple[str, ...] | None = None,
    salary_articles: set[str] | None = None,
) -> dict[str, Any]:
    p_start, p_end = period_bounds(year, month)
    article_names = articles or calc_fot_management.COST_ARTICLES
    article_order = article_order or tuple(calc_fot_management.ARTICLE_ORDER)
    article_set = frozenset(article_names)
    salary_articles = salary_articles or {article_order[0]}

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

    by_dept = {name: _blank_fot_articles_row(article_order) for name in department_names}
    by_article = {article: 0.0 for article in article_order}
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
        if article_key not in article_set:
            skipped_not_target_article += 1
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        row = by_dept[dept_name]
        row["by_article"][article_key] += amount
        row["total"] += amount
        row["rows"] += 1
        if article_key in salary_articles:
            row["salary"] += amount
        else:
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
                article_names.get(key, key): round(value, 2)
                for key, value in row["by_article"].items()
            },
        })

    total = round(sum(float(row["total"] or 0) for row in by_dept.values()), 2)
    by_article_named = {
        article_names.get(key, key): round(value, 2)
        for key, value in by_article.items()
    }
    missing_combinations = []
    for dept_name in department_names:
        row = by_dept[dept_name]
        for article in article_order:
            if abs(row["by_article"].get(article, 0.0)) < 0.005:
                missing_combinations.append({
                    "account": "26",
                    "department": dept_name,
                    "article": article_names.get(article, article),
                })
    salary_total = sum(
        value for key, value in by_article.items()
        if key in salary_articles
    )
    insurance_total = sum(
        value for key, value in by_article.items()
        if key not in salary_articles
    )
    return {
        "total": total,
        "matrix": matrix,
        "by_article": by_article_named,
        "totals": {
            "salary": round(salary_total, 2),
            "insurance": round(insurance_total, 2),
            "total": total,
            "by_article": by_article_named,
        },
        "departments_count": len(dept_name_to_key),
        "unresolved_departments": [
            name for name in department_names if name not in dept_name_to_key
        ],
        "missing_combinations": missing_combinations,
        "records_total": len(records),
        "records_taken": taken,
        "skipped_no_dept": skipped_no_dept,
        "skipped_not_target_dept": skipped_not_target_dept,
        "skipped_not_target_article": skipped_not_target_article,
    }


def _fot_fact_pc2_almaz_accounts25_26(
    session: requests.Session,
    year: int,
    month: int,
) -> dict[str, Any]:
    p_start, p_end = period_bounds(year, month)
    structure_by_key, _by_parent = calc_fot_management._load_structure(session)
    dept_name_to_key = _resolve_department_keys(structure_by_key, PC2_FOT_DEPARTMENTS)
    dept_key_to_name = {key: name for name, key in dept_name_to_key.items()}

    account25_keys = calc_fot_management._get_subaccounts(session, ACCOUNT_25_ROOT)
    account26_keys = calc_fot_management._get_subaccounts(session, ACCOUNT_26_ROOT)
    target_accounts = account25_keys | account26_keys
    acc_or = " or ".join(f"AccountDr_Key eq guid'{account}'" for account in sorted(target_accounts))
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and Организация_Key eq guid'{ALMAZ_ORG}'"
        f" and ({acc_or})"
    )
    sel = ",".join([
        "Period", "AccountDr_Key", "Организация_Key", "ПодразделениеDr_Key",
        "Сумма", "Сторно", "ExtDimensionDr1", "ExtDimensionTypeDr1_Key",
    ])
    url = (
        f"{calc_fot_management.BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = calc_fot_management._fetch_all(session, url)

    by_dept = {name: _blank_pc2_almaz_row() for name in PC2_FOT_DEPARTMENTS}
    by_article: dict[str, float] = {article: 0.0 for article in PC2_ALMAZ_ARTICLE_SET}
    by_account: dict[str, float] = defaultdict(float)
    account25_by_article = {article: 0.0 for article in PC2_ALMAZ_ACCOUNT25_ARTICLES}
    account25_rows = 0
    taken = 0
    skipped_not_target_dept = 0
    skipped_not_target_article = 0
    skipped_wrong_account_article = 0

    for rec in records:
        if rec.get("ExtDimensionTypeDr1_Key") != calc_fot_management.SUBCONTO_TYPE_COST:
            skipped_not_target_article += 1
            continue

        article_key = rec.get("ExtDimensionDr1")
        if article_key not in PC2_ALMAZ_ARTICLE_SET:
            skipped_not_target_article += 1
            continue

        account_key = rec.get("AccountDr_Key")
        is_account25 = account_key in account25_keys
        is_account26 = account_key in account26_keys
        if is_account25 and article_key not in PC2_ALMAZ_ACCOUNT25_ARTICLES:
            skipped_wrong_account_article += 1
            continue
        if is_account26 and article_key not in PC2_ALMAZ_ACCOUNT26_ARTICLES:
            skipped_wrong_account_article += 1
            continue
        if not is_account25 and not is_account26:
            skipped_wrong_account_article += 1
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        if is_account25:
            account25_by_article[article_key] += amount
            by_article[article_key] += amount
            by_account["25"] += amount
            account25_rows += 1
            taken += 1
            continue

        dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
        dept_name = dept_key_to_name.get(dept_key)
        if not dept_name:
            skipped_not_target_dept += 1
            continue

        row = by_dept[dept_name]
        row["by_article"][article_key] += amount
        row["total"] += amount
        row["rows"] += 1
        if _article_bucket(article_key) == "salary":
            row["salary"] += amount
        else:
            row["insurance"] += amount
        by_article[article_key] += amount
        by_account["26"] += amount
        taken += 1

    matrix = []
    missing_combinations = []
    for dept_name in PC2_FOT_DEPARTMENTS:
        row = by_dept[dept_name]
        for article in PC2_ALMAZ_ACCOUNT26_ARTICLES:
            if abs(row["by_article"].get(article, 0.0)) < 0.005:
                missing_combinations.append({
                    "account": "26",
                    "department": dept_name,
                    "article": PC2_ALMAZ_ARTICLES[article],
                })
        matrix.append({
            "department": dept_name,
            "department_key": dept_name_to_key.get(dept_name, ""),
            "salary": round(row["salary"], 2),
            "insurance": round(row["insurance"], 2),
            "total": round(row["total"], 2),
            "rows": row["rows"],
            "by_article": _named_article_totals(row["by_article"]),
        })

    for article in PC2_ALMAZ_ACCOUNT25_ARTICLES:
        if abs(account25_by_article.get(article, 0.0)) < 0.005:
            missing_combinations.append({
                "account": "25",
                "department": None,
                "article": PC2_ALMAZ_ARTICLES[article],
            })

    account26_total = sum(float(row["total"] or 0) for row in by_dept.values())
    account25_total = sum(account25_by_article.values())
    total = round(account26_total + account25_total, 2)
    salary_total = sum(
        value for key, value in by_article.items()
        if _article_bucket(key) == "salary"
    )
    insurance_total = sum(
        value for key, value in by_article.items()
        if _article_bucket(key) == "insurance"
    )

    return {
        "total": total,
        "matrix": matrix,
        "by_article": _named_article_totals(by_article),
        "by_account": {key: round(value, 2) for key, value in by_account.items()},
        "account25_total": round(account25_total, 2),
        "account25_by_article": _named_article_totals(account25_by_article),
        "account25_rows": account25_rows,
        "totals": {
            "salary": round(salary_total, 2),
            "insurance": round(insurance_total, 2),
            "total": total,
            "by_article": _named_article_totals(by_article),
            "by_account": {key: round(value, 2) for key, value in by_account.items()},
            "account25_total": round(account25_total, 2),
            "account25_by_article": _named_article_totals(account25_by_article),
        },
        "departments_count": len(dept_name_to_key),
        "unresolved_departments": [
            name for name in PC2_FOT_DEPARTMENTS if name not in dept_name_to_key
        ],
        "missing_combinations": missing_combinations,
        "records_total": len(records),
        "records_taken": taken,
        "skipped_not_target_dept": skipped_not_target_dept,
        "skipped_not_target_article": skipped_not_target_article,
        "skipped_wrong_account_article": skipped_wrong_account_article,
        "source_rules": {
            "source": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
            "organization": "АЛМАЗ ООО",
            "accounts": ["25", "26"],
            "account25_department_filter": False,
            "account26_department_filter": True,
        },
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
        if shop == "pc2":
            fact_payload = _fot_fact_pc2_almaz_accounts25_26(session, ref_year, mm)
        else:
            fact_payload = _fot_fact_account26(
                session,
                ref_year,
                mm,
                _departments_for_shop(shop),
                articles=PC1_TURB_ARTICLES,
                article_order=PC1_TURB_ARTICLE_ORDER,
                salary_articles=PC1_TURB_SALARY_ARTICLES,
            )
            fact_payload["source_rules"] = {
                "source": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
                "accounts": ["26"],
                "department_filter": True,
                "articles_count": len(PC1_TURB_ARTICLES),
            }
        fact = float(fact_payload.get("total") or 0)
        extra = {
            "fact_matrix": fact_payload.get("matrix") or [],
            "fact_totals": fact_payload.get("totals") or {},
            "unresolved_departments": fact_payload.get("unresolved_departments") or [],
            "missing_combinations": fact_payload.get("missing_combinations") or [],
            "source_rules": fact_payload.get("source_rules") or {},
        }
        months_out.append(month_row(ref_year, mm, plan, fact, **extra))

    payload = build_payload(SOURCE_TAG_FOT, shop, ref_year, ref_month, months_out)
    save_json(path, payload)
    return payload


__all__ = ["get_pc_fot_monthly"]
