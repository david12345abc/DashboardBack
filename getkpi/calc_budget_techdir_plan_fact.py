"""
Вспомогательные запросы OData к AccumulationRegister_ОборотыБюджетов и справочникам бюджета.

Используется: fot_techdir_plan.py (TD-M4 план), calc_budget_techdir_m3.py (TD-M3 план/факт).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from . import fot_techdir_fact as fts

BASE = fts.BASE
EMPTY = fts.EMPTY

# Имя сценария в 1С (как в описании регистра оборотов бюджетов).
BUDGET_SCENARIO_NAME = "Плановые данные - ЦФО"

# OData: в разных конфигурациях доступен один из справочников сценариев.
SCENARIO_CATALOG_CANDIDATES: tuple[str, ...] = (
    "Catalog_Сценарии",
    "Catalog_СценарииПланирования",
)


def fetch_all(
    session, url: str, page: int = 5000, timeout: int = 120
) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        r = session.get(page_url, timeout=timeout)
        if not r.ok:
            raise RuntimeError(
                f"OData {page_url[:120]}… HTTP {r.status_code}: {r.text[:400]}"
            )
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def load_budget_scenarios(session) -> dict[str, str]:
    """Ref_Key → наименование сценария; объединяем все доступные каталоги OData."""
    out: dict[str, str] = {}
    last_err: RuntimeError | None = None
    for cat in SCENARIO_CATALOG_CANDIDATES:
        url = (
            f"{BASE}/{quote(cat)}"
            "?$format=json&$select=Ref_Key,Description&$orderby=Ref_Key"
        )
        try:
            rows = fetch_all(session, url, timeout=90)
        except RuntimeError as e:
            last_err = e
            continue
        for row in rows:
            k = row.get("Ref_Key")
            if k:
                out[k] = (row.get("Description") or "").strip()
    if not out:
        raise last_err if last_err else RuntimeError(
            "не удалось загрузить ни один справочник сценариев бюджета"
        )
    return out


def load_budget_articles(session) -> dict[str, str]:
    url = (
        f"{BASE}/{quote('Catalog_СтатьиБюджетов')}"
        "?$format=json&$select=Ref_Key,Description&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url, timeout=120)
    return {
        row["Ref_Key"]: (row.get("Description") or "").strip()
        for row in rows
        if row.get("Ref_Key")
    }


def load_budget_turnover_rows(
    session, p_start: str, p_end: str
) -> list[dict[str, Any]]:
    """Строки AccumulationRegister_ОборотыБюджетов за интервал [p_start, p_end)."""
    flt = (
        f"ПериодПланирования ge datetime'{p_start}' and "
        f"ПериодПланирования lt datetime'{p_end}' and Active eq true"
    )
    sel = (
        "ПериодПланирования,Сценарий_Key,СтатьяБюджетов,Подразделение_Key,СуммаСценария"
    )
    url = (
        f"{BASE}/{quote('AccumulationRegister_ОборотыБюджетов_RecordType')}"
        f"?$format=json&$filter={quote(flt, safe='')}&$select={quote(sel, safe=',_')}"
    )
    return fetch_all(session, url, timeout=120)
