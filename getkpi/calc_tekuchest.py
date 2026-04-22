"""
calc_tekuchest.py — Текучесть персонала: План / Факт по подразделениям.

Источник: Document_ТД_ТекучестьПерсонала.
  - ВидДокумента = '0' → план (колонка «План» в ТЧ)
  - ВидДокумента = '1' → факт (колонка «Факт» в ТЧ)
  - Табличная часть «Текучесть» (inline): Месяц, План, Факт

API:
  from getkpi.calc_tekuchest import get_tekuchest_monthly
  data = get_tekuchest_monthly(2026, 3)                # агрегат всех отделов
  data = get_tekuchest_monthly(2026, 3, dept_guid='…') # только один отдел
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

from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборуд. и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
}
DEPT_SET = frozenset(DEPARTMENTS.keys())

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

ENTITY = "Document_ТД_ТекучестьПерсонала"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"tekuchest_{year}_{ref_month:02d}.json"


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


def _fetch_documents(session: requests.Session) -> list[dict]:
    """Загружает все не-удалённые документы Document_ТД_ТекучестьПерсонала."""
    flt = quote("DeletionMark eq false", safe="")
    docs: list[dict] = []
    skip = 0
    PAGE = 500
    while True:
        url = (
            f"{BASE}/{quote(ENTITY)}"
            f"?$format=json"
            f"&$filter={flt}"
            f"&$orderby=Ref_Key"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=60, retries=4, label="Tekuchest")
        if r is None:
            logger.error("Tekuchest: request dropped after retries")
            break
        if not r.ok:
            logger.error("Tekuchest HTTP %d: %s", r.status_code, r.text[:300])
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        docs.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break
    return docs


def _calc_monthly(docs: list[dict], year: int, ref_month: int) -> dict:
    """
    Агрегировать план/факт по месяцам и подразделениям.
    Возвращает {month: {"plan": total, "fact": total,
                        "by_dept": {guid: {"plan": ..., "fact": ...}}}}
    """
    docs_dept = [d for d in docs
                 if d.get("Подразделение_Key", EMPTY) in DEPT_SET]

    result: dict[int, dict] = {}
    for m in range(1, ref_month + 1):
        result[m] = {
            "plan": 0.0, "fact": 0.0,
            "by_dept": {d: {"plan": 0.0, "fact": 0.0} for d in DEPT_SET},
        }

    for doc in docs_dept:
        dept_key = doc.get("Подразделение_Key", EMPTY)
        vid = str(doc.get("ВидДокумента", ""))
        rows = doc.get("Текучесть", [])

        for row in rows:
            mes = row.get("Месяц", "")
            if not mes or mes[:4] != str(year):
                continue
            try:
                m = int(mes[5:7])
            except (ValueError, IndexError):
                continue
            if m < 1 or m > ref_month:
                continue

            if vid == "0":
                plan_val = float(row.get("План", 0) or 0)
                result[m]["plan"] += plan_val
                result[m]["by_dept"][dept_key]["plan"] += plan_val
            elif vid == "1":
                fact_val = float(row.get("Факт", 0) or 0)
                result[m]["fact"] += fact_val
                result[m]["by_dept"][dept_key]["fact"] += fact_val

    for m in result:
        result[m]["plan"] = round(result[m]["plan"], 2)
        result[m]["fact"] = round(result[m]["fact"], 2)
        for d in result[m]["by_dept"]:
            result[m]["by_dept"][d]["plan"] = round(result[m]["by_dept"][d]["plan"], 2)
            result[m]["by_dept"][d]["fact"] = round(result[m]["by_dept"][d]["fact"], 2)

    return result


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Полный агрегат или срез по одному подразделению."""
    if dept_guid is None:
        return payload
    sliced = []
    for row in payload.get("months", []):
        bd = row.get("by_dept", {}).get(dept_guid, {})
        sliced.append({
            "year": row["year"],
            "month": row["month"],
            "plan": bd.get("plan", 0),
            "fact": bd.get("fact", 0),
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced,
    }


def get_tekuchest_monthly(year: int | None = None,
                          month: int | None = None,
                          dept_guid: str | None = None) -> dict:
    """
    Помесячная текучесть (январь..ref_month).

    dept_guid=None  — сумма по всем подразделениям (коммерческий директор).
    dept_guid='…'   — только указанное подразделение.

    Возвращает:
      {"year": …, "ref_month": …, "months": [
          {"year": …, "month": …, "plan": …, "fact": …, "by_dept": {…}},
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

    logger.info("calc_tekuchest: loading documents for %d months 1-%d", ref_y, ref_m)
    docs = _fetch_documents(session)
    computed = _calc_monthly(docs, ref_y, ref_m)

    out_months = []
    for m in range(1, ref_m + 1):
        cm = computed[m]
        out_months.append({
            "year": ref_y,
            "month": m,
            "plan": cm["plan"],
            "fact": cm["fact"],
            "by_dept": cm["by_dept"],
        })

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
    _print(f"  ТЕКУЧЕСТЬ ПЕРСОНАЛА")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_tekuchest_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'План':>10s} {'Факт':>10s}")
    _print(f"  {'─' * 34}")
    for row in data.get("months", []):
        _print(f"  {MONTH_RU[row['month']]:<12s} "
               f"{row['plan']:>10.2f} "
               f"{row['fact']:>10.2f}")

    _print(f"\n  По подразделениям ({MONTH_RU[m]} {y}):")
    _print(f"  {'─' * 55}")
    for row in data.get("months", []):
        if row["month"] != m:
            continue
        for dk, dv in (row.get("by_dept") or {}).items():
            name = DEPARTMENTS.get(dk, dk)
            _print(f"    {name:<40s} п={dv['plan']:>7.2f}  ф={dv['fact']:>7.2f}")

    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
