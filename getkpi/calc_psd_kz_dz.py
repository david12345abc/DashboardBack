"""
calc_psd_kz_dz.py — Кредиторская (КЗ) и дебиторская (ДЗ) задолженность
по расчётам с поставщиками для ПСД.

Воспроизводит итоги отчёта 1С:
  «Ведомость расчётов с поставщиками»
  Период:        01.01.{year} — 31.{end}.{year}
  Данные отчёта: В валюте регл. учёта
  Отбор:         Организация = ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО

Источник: AccumulationRegister_РасчетыСПоставщикамиПоСрокам/Balance
  Period = следующий день 00:00:00 (например, 01.04.2026 для периода по 31.03)
  Фильтр: Catalog_ОбъектыРасчетов.Организация_Key = {НПО}

  КЗ (наш долг поставщикам)            = Σ ДолгРеглBalance
  ДЗ (авансы, выданные поставщикам)    = Σ ПредоплатаРеглBalance

Запуск:
  python calc_psd_kz_dz.py                     # на 31 марта текущего года
  python calc_psd_kz_dz.py 2026-03-31          # на конкретную дату (конец периода)
  python calc_psd_kz_dz.py 2026-03-31 --json   # + сохранить JSON
  python calc_psd_kz_dz.py 2026-03-31 --all    # включить обе организации Турб.Дон
"""

import functools
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

# ═══════════════════════════════════════════════════════
# КОНФИГ
# ═══════════════════════════════════════════════════════

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"
TOLERANCE = 0.01

# Организации «Турбулентность Дон»
TURB_ORG_NPO = {
    "fbca2148-6cfd-11e7-812d-001e67112509": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
}
TURB_ORG_ALL = {
    "fbca2143-6cfd-11e7-812d-001e67112509": "Турбулентность-Дон ООО",
    "fbca2148-6cfd-11e7-812d-001e67112509": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
}

REG_SUPP = "AccumulationRegister_РасчетыСПоставщикамиПоСрокам"


# ═══════════════════════════════════════════════════════
# HTTP-УТИЛИТЫ
# ═══════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.auth = AUTH
    return s


def fetch_all(session: requests.Session, base_url: str, page: int = 5000) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    sep = "&" if "?" in base_url else "?"
    while True:
        url = f"{base_url}{sep}$top={page}&$skip={skip}"
        r = session.get(url, timeout=120)
        r.raise_for_status()
        chunk = r.json().get("value", [])
        rows.extend(chunk)
        if len(chunk) < page:
            break
        skip += page
    return rows


# ═══════════════════════════════════════════════════════
# ШАГ 1: Balance регистра на Period = (end_date + 1 день) 00:00:00
# ═══════════════════════════════════════════════════════

def load_supplier_balance(session: requests.Session, end_date: date) -> list[dict]:
    """
    Balance регистра РасчетыСПоставщикамиПоСрокам на начало дня (end_date + 1).
    Это воспроизводит поведение отчёта 1С «Ведомость расчётов с поставщиками»,
    где конец периода — 00:00:00 следующего дня.
    """
    period_iso = f"{(end_date + timedelta(days=1)).isoformat()}T00:00:00"
    url = (
        f"{BASE}/{REG_SUPP}/Balance"
        f"?$format=json"
        f"&Period=datetime'{period_iso}'"
        f"&$select=ОбъектРасчетов_Key,ДолгРеглBalance,ПредоплатаРеглBalance"
    )
    return fetch_all(session, url, page=5000)


# ═══════════════════════════════════════════════════════
# ШАГ 2: каталог ОбъектовРасчетов, фильтр по организации
# ═══════════════════════════════════════════════════════

def load_obj_keys_by_org(session: requests.Session, org_guid: str) -> set[str]:
    """Все Ref_Key из Catalog_ОбъектыРасчетов для одной организации."""
    flt = quote(f"Организация_Key eq guid'{org_guid}'", safe="")
    url = (
        f"{BASE}/Catalog_ОбъектыРасчетов"
        f"?$format=json"
        f"&$select=Ref_Key"
        f"&$filter={flt}"
    )
    rows = fetch_all(session, url, page=5000)
    return {str(r.get("Ref_Key", EMPTY)).lower() for r in rows}


# ═══════════════════════════════════════════════════════
# ШАГ 3: агрегация
# ═══════════════════════════════════════════════════════

def aggregate(
    rows: list[dict],
    org_obj_keys: dict[str, set[str]],   # org_guid_lower -> set(obj_key_lower)
) -> dict[str, dict[str, float]]:
    """
    Σ ДолгРеглBalance (КЗ) и Σ ПредоплатаРеглBalance (ДЗ) по каждой организации.
    """
    obj_to_org: dict[str, str] = {}
    for org_g, obj_set in org_obj_keys.items():
        for ok in obj_set:
            obj_to_org[ok] = org_g

    result: dict[str, dict[str, float]] = {
        g: {"kz": 0.0, "dz": 0.0} for g in org_obj_keys
    }
    for row in rows:
        obj_key = str(row.get("ОбъектРасчетов_Key", EMPTY)).lower()
        org_key = obj_to_org.get(obj_key)
        if not org_key:
            continue
        dolg = float(row.get("ДолгРеглBalance") or 0)
        avans = float(row.get("ПредоплатаРеглBalance") or 0)
        if dolg > TOLERANCE:
            result[org_key]["kz"] += dolg
        if avans > TOLERANCE:
            result[org_key]["dz"] += avans
    return result


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}

    if args:
        end_date = date.fromisoformat(args[0])
    else:
        today = date.today()
        end_date = date(today.year, 3, 31)

    save_json = "--json" in flags
    include_all_orgs = "--all" in flags
    orgs = TURB_ORG_ALL if include_all_orgs else TURB_ORG_NPO

    print("=" * 78)
    print(f"  КЗ и ДЗ (ведомость расчётов с поставщиками) · ПСД")
    print(f"  Период:        01.01.{end_date.year} — {end_date}")
    print(f"  Данные отчёта: В валюте регл. учёта")
    print(f"  Организации:   {', '.join(orgs.values())}")
    print("=" * 78)

    session = make_session()
    t0 = time.time()

    # ─── 1. Balance регистра на (end_date + 1) 00:00:00 ───
    period_end = end_date + timedelta(days=1)
    print(f"\n▸ Шаг 1 · {REG_SUPP}/Balance на {period_end}T00:00:00")
    rows = load_supplier_balance(session, end_date)
    print(f"  Записей: {len(rows)}")

    # ─── 2. Каталог ОбъектовРасчетов по организациям ───
    print(f"\n▸ Шаг 2 · Catalog_ОбъектыРасчетов — фильтр по Организация_Key")
    org_obj_keys: dict[str, set[str]] = {}
    for org_guid, org_name in orgs.items():
        keys = load_obj_keys_by_org(session, org_guid)
        org_obj_keys[org_guid.lower()] = keys
        print(f"  {org_name}: {len(keys)} объектов расчётов")

    # ─── 3. Агрегация ───
    print(f"\n▸ Шаг 3 · Σ ДолгРеглBalance (КЗ) / Σ ПредоплатаРеглBalance (ДЗ)")
    agg = aggregate(rows, org_obj_keys)

    # ─── 4. Вывод ───
    print("\n" + "=" * 78)
    print(f"  РЕЗУЛЬТАТ на {end_date}")
    print("=" * 78)

    total_kz = 0.0
    total_dz = 0.0
    print(f"\n  {'Организация':<42s} {'КЗ (наш долг)':>16s} {'ДЗ (авансы)':>16s}")
    print(f"  {'─' * 74}")
    for guid, name in orgs.items():
        g = guid.lower()
        kz = agg.get(g, {}).get("kz", 0.0)
        dz = agg.get(g, {}).get("dz", 0.0)
        total_kz += kz
        total_dz += dz
        print(f"  {name:<42s} {kz:>16,.2f} {dz:>16,.2f}")
    print(f"  {'─' * 74}")
    print(f"  {'ИТОГО':<42s} {total_kz:>16,.2f} {total_dz:>16,.2f}")

    print(f"\n  КЗ (наш долг поставщикам):         {total_kz:>18,.2f} руб.")
    print(f"  ДЗ (авансы, выданные поставщикам): {total_dz:>18,.2f} руб.")

    elapsed = time.time() - t0
    print(f"\n  Время: {elapsed:.1f}с")

    # ─── 5. JSON ───
    if save_json:
        result = {
            "end_date": end_date.isoformat(),
            "period_from": f"{end_date.year}-01-01",
            "period_to": end_date.isoformat(),
            "generated": datetime.now().isoformat(),
            "filter": list(orgs.values()),
            "currency": "регл.",
            "register": REG_SUPP,
            "kz": round(total_kz, 2),
            "dz": round(total_dz, 2),
            "by_org": {
                name: {
                    "kz": round(agg.get(guid.lower(), {}).get("kz", 0.0), 2),
                    "dz": round(agg.get(guid.lower(), {}).get("dz", 0.0), 2),
                }
                for guid, name in orgs.items()
            },
        }
        out_path = os.path.join(
            os.path.dirname(__file__),
            f"psd_kz_dz_{end_date.isoformat()}.json",
        )
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n  → {out_path}")


if __name__ == "__main__":
    main()
