"""
calc_plans.py — Плановые значения: Деньги, Отгрузки, Договоры.

Источник: AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС
  (РегистрНакопления.ТД_ПланированиеДоговоровОтгрузокДС)

ВидПланирования (enum ТД_ВидыПланированияПлановПродаж):
  - Деньги
  - Отгрузки
  - Договоры

За каждый месяц суммируются записи по всем целевым подразделениям.

Запуск standalone:
  python calc_plans.py                # текущий год до последнего полного месяца
  python calc_plans.py 2026 3         # январь–март 2026

API:
  from getkpi.calc_plans import get_plans_monthly
  data = get_plans_monthly()
  # {"year": 2026, "ref_month": 3, "months": [
  #   {"month": 1, "plan_money": ..., "plan_shipments": ..., "plan_contracts": ...},
  #   ...
  # ]}
"""
from __future__ import annotations

import calendar
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
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPT_KEYS = frozenset({
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
})

REGISTER_CANDIDATES = [
    "AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС",
    "AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType",
]

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

KIND_MONEY = "Деньги"
KIND_SHIPMENTS = "Отгрузки"
KIND_CONTRACTS = "Договоры"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _monthly_cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"plans_monthly_{year}_{month:02d}.json"


def _load_all_register(session: requests.Session,
                       year: int, max_month: int) -> list[dict]:
    """
    Загрузить записи регистра за период январь–max_month указанного года.
    """
    d_from = f"{year}-01-01T00:00:00"
    last_day = calendar.monthrange(year, max_month)[1]
    d_to = f"{year}-{max_month:02d}-{last_day}T23:59:59"

    sel = "Period,Подразделение_Key,ВидПланирования,Сумма"

    entity = None
    for candidate in REGISTER_CANDIDATES:
        url = f"{BASE}/{candidate}?$format=json&$top=1"
        try:
            r = session.get(url, timeout=15)
            if r.ok:
                entity = candidate
                break
        except Exception:
            continue

    if entity is None:
        logger.error("Cannot find plans register in OData")
        return []

    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}'",
        safe="",
    )

    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{entity}?$format=json"
            f"&$select={sel}"
            f"&$filter={flt}"
            f"&$top=5000&$skip={skip}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("Plans HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("Plans HTTP %d: %s", r.status_code, r.text[:300])
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000

    return rows


def _aggregate(rows: list[dict], year: int, max_month: int) -> dict[int, dict[str, float]]:
    """
    Агрегация: month → {"Деньги": sum, "Отгрузки": sum, "Договоры": sum}
    """
    result: dict[int, dict[str, float]] = {
        m: {KIND_MONEY: 0.0, KIND_SHIPMENTS: 0.0, KIND_CONTRACTS: 0.0}
        for m in range(1, max_month + 1)
    }

    for row in rows:
        dept = row.get("Подразделение_Key", "")
        if dept not in DEPT_KEYS or dept == EMPTY:
            continue

        period_str = (row.get("Period") or "")[:10]
        if len(period_str) < 7:
            continue
        try:
            m = int(period_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > max_month:
            continue

        kind = row.get("ВидПланирования", "")
        amount = float(row.get("Сумма") or 0)

        if kind in result[m]:
            result[m][kind] += amount

    return result


def get_plans_monthly(year: int | None = None,
                      month: int | None = None) -> dict:
    """
    Помесячные планы (Деньги, Отгрузки, Договоры) для коммерческого директора.
    Кэшируется на день.
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    mc = _monthly_cache_path(ref_y, ref_m)
    if mc.exists():
        try:
            with open(mc, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("cache_date") == today.isoformat():
                return data
        except (OSError, json.JSONDecodeError):
            pass

    session = requests.Session()
    session.auth = AUTH

    raw = _load_all_register(session, ref_y, ref_m)
    agg = _aggregate(raw, ref_y, ref_m)

    out_months = []
    for m in range(1, ref_m + 1):
        d = agg.get(m, {})
        out_months.append({
            "year": ref_y,
            "month": m,
            "month_name": MONTH_NAMES[m],
            "plan_money": round(d.get(KIND_MONEY, 0), 2),
            "plan_shipments": round(d.get(KIND_SHIPMENTS, 0), 2),
            "plan_contracts": round(d.get(KIND_CONTRACTS, 0), 2),
        })

    payload = {
        "cache_date": today.isoformat(),
        "year": ref_y,
        "ref_month": ref_m,
        "months": out_months,
    }

    try:
        with open(mc, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass

    return payload


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    import functools
    print = functools.partial(print, flush=True)

    args = sys.argv[1:]
    today = date.today()
    if len(args) >= 2:
        y, m = int(args[0]), int(args[1])
    else:
        y, m = _last_full_month(today)

    print(f"\n{'═' * 60}")
    print(f"  ПЛАНЫ: Деньги / Отгрузки / Договоры")
    print(f"  Период: январь – {MONTH_NAMES[m]} {y}")
    print(f"{'═' * 60}")

    t0 = time.time()
    data = get_plans_monthly(y, m)
    months = data.get("months", [])

    print(f"\n{'Месяц':<12s} {'Деньги':>18s} {'Отгрузки':>18s} {'Договоры':>18s}")
    print("─" * 70)
    totals = {"m": 0.0, "s": 0.0, "c": 0.0}
    for row in months:
        pm = row["plan_money"]
        ps = row["plan_shipments"]
        pc = row["plan_contracts"]
        totals["m"] += pm
        totals["s"] += ps
        totals["c"] += pc
        print(f"  {row['month_name']:<10s} {pm:>18,.2f} {ps:>18,.2f} {pc:>18,.2f}")
    print("─" * 70)
    print(f"  {'ИТОГО':<10s} {totals['m']:>18,.2f} {totals['s']:>18,.2f} {totals['c']:>18,.2f}")
    print(f"\n  Время: {time.time() - t0:.1f}с")
    print(f"{'═' * 60}")
