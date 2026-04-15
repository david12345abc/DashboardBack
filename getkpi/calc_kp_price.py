"""
calc_kp_price.py — Плитка «Цена фактическая / Цена расчетная» (KD-M9)

Источник: Document_КоммерческоеПредложениеКлиенту

Цена фактическая = реквизит СуммаДокумента (если 0, то СуммаДокументаТКП)
Цена расчетная    = Σ(СуммаСНДС) − Σ(СуммаРучнойСкидки) по всем строкам ТЧ «Товары»

Фильтр: Статус ∈ {"Действует", "Исполнено"}, дата в заданном периоде, DeletionMark=false.

API:
  from getkpi.calc_kp_price import get_kp_price_monthly
  data = get_kp_price_monthly(2026, 3)                # агрегат всех отделов
  data = get_kp_price_monthly(2026, 3, dept_guid='…') # только один отдел
"""
from __future__ import annotations

import json
import logging
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
ENTITY = "Document_КоммерческоеПредложениеКлиенту"
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборуд. и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": "Сектор рекламы и PR",
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": "Отдел тендерных продаж",
}
DEPT_SET = frozenset(DEPARTMENTS.keys())

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"kp_price_{year}_{ref_month:02d}.json"


def _load_cache(year: int, ref_month: int) -> dict | None:
    p = _cache_path(year, ref_month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_date") == date.today().isoformat():
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, ref_month: int, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, ref_month), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _fetch_docs_for_month(session: requests.Session,
                          year: int, month: int) -> list[dict]:
    """Загружает КП со статусом 'Действует'/'Исполнено' за один месяц."""
    if month == 12:
        p_start = f"{year}-12-01T00:00:00"
        p_end = f"{year + 1}-01-01T00:00:00"
    else:
        p_start = f"{year}-{month:02d}-01T00:00:00"
        p_end = f"{year}-{month + 1:02d}-01T00:00:00"

    flt = (
        f"Date ge datetime'{p_start}'"
        f" and Date lt datetime'{p_end}'"
        f" and (Статус eq 'Действует' or Статус eq 'Исполнено')"
        f" and DeletionMark eq false"
    )
    sel = (
        "Ref_Key,Date,СуммаДокумента,СуммаДокументаТКП,"
        "Менеджер_Key,Товары"
    )

    docs: list[dict] = []
    skip = 0
    PAGE = 500

    while True:
        url = (
            f"{BASE}/{quote(ENTITY)}"
            f"?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
            f"&$orderby=Ref_Key"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("KP_PRICE HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("KP_PRICE HTTP %d: %s", r.status_code, r.text[:300])
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        docs.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break

    return docs


_manager_dept_cache: dict[str, str] = {}


def _resolve_manager_depts(session: requests.Session,
                           manager_keys: set[str]) -> dict[str, str]:
    """Возвращает {Менеджер_Key: Подразделение_Key} через Catalog_Пользователи."""
    result: dict[str, str] = {}
    to_fetch = manager_keys - set(_manager_dept_cache)

    for mk in to_fetch:
        url = (
            f"{BASE}/{quote('Catalog_Пользователи')}"
            f"(guid'{mk}')?$format=json&$select=Подразделение_Key"
        )
        try:
            r = session.get(url, timeout=15)
        except Exception:
            continue
        if r.ok:
            dept = r.json().get("Подразделение_Key", EMPTY)
            _manager_dept_cache[mk] = dept

    for mk in manager_keys:
        result[mk] = _manager_dept_cache.get(mk, EMPTY)
    return result


def _aggregate_docs(docs: list[dict],
                    mgr_to_dept: dict[str, str]) -> dict:
    """Агрегирует цену фактическую/расчетную по подразделениям."""
    fact_by_dept: dict[str, float] = defaultdict(float)
    calc_by_dept: dict[str, float] = defaultdict(float)
    total_fact = 0.0
    total_calc = 0.0

    for doc in docs:
        mgr_key = doc.get("Менеджер_Key", EMPTY)
        dept_key = mgr_to_dept.get(mgr_key, EMPTY)

        sum_doc = doc.get("СуммаДокумента", 0) or 0
        sum_tkp = doc.get("СуммаДокументаТКП", 0) or 0
        fact_price = sum_doc if sum_doc != 0 else sum_tkp

        tovary = doc.get("Товары", [])
        sum_s_nds = 0.0
        sum_ruch = 0.0
        for t in tovary:
            sum_s_nds += (t.get("СуммаСНДС", 0) or 0)
            sum_ruch += (t.get("СуммаРучнойСкидки", 0) or 0)
        calc_price = sum_s_nds - sum_ruch

        total_fact += fact_price
        total_calc += calc_price

        if dept_key != EMPTY:
            fact_by_dept[dept_key] += fact_price
            calc_by_dept[dept_key] += calc_price

    return {
        "total_fact": round(total_fact, 2),
        "total_calc": round(total_calc, 2),
        "fact_by_dept": {k: round(v, 2) for k, v in fact_by_dept.items()},
        "calc_by_dept": {k: round(v, 2) for k, v in calc_by_dept.items()},
        "doc_count": len(docs),
    }


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Полный агрегат или срез по одному подразделению."""
    sliced = []
    for row in payload.get("months", []):
        if dept_guid is None:
            fact = row.get("total_fact", 0)
            calc = row.get("total_calc", 0)
        else:
            fact = row.get("fact_by_dept", {}).get(dept_guid, 0)
            calc = row.get("calc_by_dept", {}).get(dept_guid, 0)
        sliced.append({
            "year": row["year"],
            "month": row["month"],
            "fact": fact,
            "calc": calc,
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced,
    }


def get_kp_price_monthly(year: int | None = None,
                         month: int | None = None,
                         dept_guid: str | None = None) -> dict:
    """
    Помесячные данные «Цена фактическая / Цена расчетная» (январь..ref_month).

    dept_guid=None  — сумма по всем документам (коммерческий директор).
    dept_guid='…'   — только документы указанного подразделения.

    Возвращает:
      {"year": …, "ref_month": …, "months": [
          {"year": …, "month": …, "fact": …, "calc": …}, …
      ]}
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    cached = _load_cache(ref_y, ref_m)
    if cached is not None:
        return _slice_payload(cached, dept_guid)

    session = requests.Session()
    session.auth = AUTH

    logger.info("calc_kp_price: loading KP docs for %d months 1-%d", ref_y, ref_m)
    t0 = time.time()

    all_docs_by_month: list[tuple[int, list[dict]]] = []
    all_mgr_keys: set[str] = set()
    for m in range(1, ref_m + 1):
        docs = _fetch_docs_for_month(session, ref_y, m)
        all_docs_by_month.append((m, docs))
        for d in docs:
            mk = d.get("Менеджер_Key", EMPTY)
            if mk != EMPTY:
                all_mgr_keys.add(mk)

    logger.info("calc_kp_price: resolving %d unique managers", len(all_mgr_keys))
    mgr_to_dept = _resolve_manager_depts(session, all_mgr_keys)

    out_months = []
    for m, docs in all_docs_by_month:
        agg = _aggregate_docs(docs, mgr_to_dept)
        out_months.append({
            "year": ref_y,
            "month": m,
            "total_fact": agg["total_fact"],
            "total_calc": agg["total_calc"],
            "fact_by_dept": agg["fact_by_dept"],
            "calc_by_dept": agg["calc_by_dept"],
            "doc_count": agg["doc_count"],
        })
        logger.info("  month %d: %d docs, fact=%.2f, calc=%.2f",
                     m, agg["doc_count"], agg["total_fact"], agg["total_calc"])

    logger.info("calc_kp_price: done in %.1fs", time.time() - t0)

    payload = {
        "cache_date": today.isoformat(),
        "year": ref_y,
        "ref_month": ref_m,
        "months": out_months,
    }
    _save_cache(ref_y, ref_m, payload)
    return _slice_payload(payload, dept_guid)


if __name__ == "__main__":
    import functools
    sys.stdout.reconfigure(encoding="utf-8")
    _print = functools.partial(print, flush=True)

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    _print(f"\n{'═' * 60}")
    _print(f"  ЦЕНА ФАКТИЧЕСКАЯ / ЦЕНА РАСЧЁТНАЯ")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_kp_price_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'Факт':>16s} {'Расчёт':>16s} {'%':>8s}")
    _print(f"  {'─' * 54}")
    for row in data.get("months", []):
        f = row["fact"]
        c = row["calc"]
        pct = f"{f / c * 100:.1f}" if c else "—"
        _print(f"  {MONTH_RU[row['month']]:<12s} {f:>16,.2f} {c:>16,.2f} {pct:>8s}")

    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
