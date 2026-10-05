"""Факт бюджета ПЦ1/ПЦ2 по отчёту «Списание безналичных денежных средств».

Отбор как в универсальном отчёте 1С:
  документ «Списание безналичных денежных средств», таблица «Расшифровка платежа»;
  статья ДДС в группе «ПР-ВО1» (Турбулентность-Дон) или «ПР-ВО2» (Алмаз);
  проведён, не помечен на удаление;
  факт = Σ Сумма за календарный месяц (дата документа).
"""
from __future__ import annotations

from datetime import date
from typing import Any
from urllib.parse import quote

import requests

from . import calc_budget_limit
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
from .odata_http import request_with_retry

SOURCE_TAG_BUDGET = "prod_deputy_pc_budget_v7_writeoff_summa"

WRITEOFF_DOC = "Document_СписаниеБезналичныхДенежныхСредств"
ARTICLE_CATALOG = "Catalog_СтатьиДвиженияДенежныхСредств"

# Группы справочника «Статьи движения денежных средств».
FOLDER_BY_SHOP: dict[ShopKey, str] = {
    "pc1": "dfd3bfe8-533f-11eb-84f3-ac1f6b05524d",  # ПР-ВО1
    "pc2": "ea1d8ad3-533f-11eb-84f3-ac1f6b05524d",  # ПР-ВО2
}

_ARTICLES: dict[str, dict[str, str]] = {}
_MONTH_LINES: dict[tuple[int, int], list[tuple[str, float]]] = {}


def _odata_rows(session: requests.Session, url: str, page: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        response = request_with_retry(
            session,
            f"{url}{sep}$top={page}&$skip={skip}",
            timeout=180,
            label="pc-budget-writeoff",
        )
        if response is None or not response.ok:
            status = getattr(response, "status_code", None)
            raise RuntimeError(f"OData списаний ДС недоступен: HTTP {status}")
        batch = response.json().get("value") or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _catalog_children(session: requests.Session, parent_key: str) -> list[dict[str, Any]]:
    flt = f"Parent_Key eq guid'{parent_key}'"
    url = (
        f"{calc_budget_limit.BASE}/{quote(ARTICLE_CATALOG)}"
        f"?$format=json"
        f"&$select={quote('Ref_Key,Description,IsFolder,DeletionMark', safe=',')}"
        f"&$filter={quote(flt, safe='')}"
    )
    return _odata_rows(session, url, page=200)


def _articles_in_folder(session: requests.Session, folder_key: str) -> dict[str, str]:
    """Статьи группы, включая вложенные папки. Ключ — Ref_Key в нижнем регистре."""
    cached = _ARTICLES.get(folder_key)
    if cached is not None:
        return cached

    articles: dict[str, str] = {}
    stack = [folder_key]
    seen: set[str] = set()
    while stack:
        parent = stack.pop()
        if parent in seen:
            continue
        seen.add(parent)
        for row in _catalog_children(session, parent):
            ref = str(row.get("Ref_Key") or "").strip()
            if not ref or ref == EMPTY:
                continue
            if row.get("IsFolder"):
                stack.append(ref)
                continue
            articles[ref.lower()] = str(row.get("Description") or "").strip()

    if not articles:
        raise RuntimeError(f"В группе статей {folder_key} нет элементов")
    _ARTICLES[folder_key] = articles
    return articles


def _writeoff_lines(session: requests.Session, year: int, month: int) -> list[tuple[str, float]]:
    key = (year, month)
    cached = _MONTH_LINES.get(key)
    if cached is not None:
        return cached

    start, end = period_bounds(year, month)
    flt = (
        f"Date ge datetime'{start}' and Date lt datetime'{end}'"
        f" and Posted eq true and DeletionMark eq false"
    )
    url = (
        f"{calc_budget_limit.BASE}/{quote(WRITEOFF_DOC)}"
        f"?$format=json"
        f"&$select={quote('Ref_Key,РасшифровкаПлатежа', safe=',')}"
        f"&$filter={quote(flt, safe='')}"
    )
    lines: list[tuple[str, float]] = []
    for doc in _odata_rows(session, url, page=40):
        for row in doc.get("РасшифровкаПлатежа") or []:
            article = str(row.get("СтатьяДвиженияДенежныхСредств_Key") or "").strip().lower()
            if not article or article == EMPTY:
                continue
            lines.append((article, float(row.get("Сумма") or 0)))
    _MONTH_LINES[key] = lines
    return lines


def _budget_fact_writeoff(session: requests.Session, shop: ShopKey, year: int, month: int) -> float:
    articles = _articles_in_folder(session, FOLDER_BY_SHOP[shop])
    total = 0.0
    for article, amount in _writeoff_lines(session, year, month):
        if article in articles:
            total += amount
    return round(total, 2)


def get_pc_budget_monthly(shop: ShopKey, year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path("budget", shop, ref_year, ref_month)

    cached = load_json(path)
    if cached is not None and cached.get("source") == SOURCE_TAG_BUDGET:
        from . import cache_manager

        if cached.get("cache_date") == today.isoformat() or not cache_manager.is_force_compute_context():
            return cached

    months_out: list[dict] = []
    session = requests.Session()
    session.auth = AUTH
    for mm in range(1, ref_month + 1):
        plan = float(PC_BUDGET_PLAN[shop][mm - 1])
        fact = _budget_fact_writeoff(session, shop, ref_year, mm)
        months_out.append(month_row(ref_year, mm, plan, fact))

    payload = build_payload(SOURCE_TAG_BUDGET, shop, ref_year, ref_month, months_out)
    save_json(path, payload)
    return payload


__all__ = ["get_pc_budget_monthly"]
