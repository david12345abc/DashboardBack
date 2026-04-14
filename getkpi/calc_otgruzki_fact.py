"""
calc_otgruzki_fact.py — Факт отгрузок за месяц (помесячно).

Источники (воспроизведение 1С-запроса «ОТГРУЗКИ ФАКТ»):

  1. AccumulationRegister_РаспоряженияНаОтгрузку_RecordType
     ВидДвиженияРегистра = 'Расход'
     → -Сумма * курс валюты заказа

  2. AccumulationRegister_СебестоимостьТоваров_RecordType
     Recorder_Type = ВозвратТоваровОтКлиента, Договор.ТипДоговора = СКомиссионером
     → -СтоимостьРегл (expense-часть)

Фильтрация:
  – Подразделение заказа ∈ 6 целевых отделов продаж, ≠ пусто
  – Соглашение заказа ≠ пусто
  – НЕ ЗаказКлиента.ТД_НеУчитыватьВПланФакте
  – Партнёры-перепродажа (CASE ОПБО / без МГС / с сопровождением)
  – Пересчёт валюты: USD/EUR/BYN/KZT → RUB

API:
  from getkpi.calc_otgruzki_fact import get_otgruzki_monthly
  data = get_otgruzki_monthly()
"""
from __future__ import annotations

import calendar
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел эталонного оборудования",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел ключевых клиентов",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел ПАО Газпром",
}
DEPT_SET = frozenset(DEPARTMENTS.keys())
OPBO_DEPT = "7587c178-92f6-11f0-96f9-6cb31113810e"

EXCLUDE_PARTNER_NAMES = {
    "АЛМАЗ ООО (рабочий)",
    "Турбулентность-Дон ООО",
    "Турбулентность-ДОН ООО НПО",
    "СКТБ Турбо-Дон ООО",
    "Метрогазсервис ООО",
}
EXCLUDE_PARTNER_NAMES_NO_MGS = EXCLUDE_PARTNER_NAMES - {"Метрогазсервис ООО"}

CURRENCY_KEYS = {
    "0a7c6f22-e1b6-11df-963e-001cc4d04388": "USD",
    "d328a18d-7405-11e0-81cd-001583b3d75c": "EUR",
    "095e2c36-45dc-11ec-8756-ac1f6b05524d": "BYN",
    "e2bc7bc0-de2e-11ef-95fc-6cb31113810e": "KZT",
    "3fdf75b4-6252-11e7-812d-001e67112509": "RUB",
}

EXCHANGE_RATES = {
    "USD": 90.0,
    "EUR": 98.0,
    "BYN": 28.0,
    "KZT": 0.19,
    "RUB": 1.0,
}

BATCH = 15
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

REG_OTGRUZKA = "AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
REG_SEBESTOIMOST = "AccumulationRegister_СебестоимостьТоваров_RecordType"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"otgruzki_{year}_{month:02d}.json"


def _monthly_cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"otgruzki_monthly_{year}_{month:02d}.json"


def _load_cache(year: int, month: int) -> float | None:
    p = _cache_path(year, month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_date") == date.today().isoformat():
            return data.get("total")
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, month: int, total: float) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), "w", encoding="utf-8") as f:
            json.dump({"cache_date": date.today().isoformat(), "total": total},
                      f, ensure_ascii=False)
    except OSError:
        pass


def _load_rashod_records(session: requests.Session,
                         year: int, max_month: int) -> list[dict]:
    """Загрузить записи расхода из РаспоряженияНаОтгрузку за январь–max_month."""
    last_day = calendar.monthrange(year, max_month)[1]
    d_from = f"{year}-01-01T00:00:00"
    d_to = f"{year}-{max_month:02d}-{last_day}T23:59:59"

    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}' "
        f"and ВидДвиженияРегистра eq 'Расход'",
        safe="",
    )
    sel = "Period,Распоряжение,Распоряжение_Type,Сумма"

    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{REG_OTGRUZKA}?$format=json"
            f"&$filter={flt}&$select={sel}"
            f"&$top=5000&$skip={skip}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("Otgruzki HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("Otgruzki HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _load_vozvrat_komisioner(session: requests.Session,
                             year: int, max_month: int) -> list[dict]:
    """Загрузить возвраты от комиссионеров из СебестоимостьТоваров."""
    last_day = calendar.monthrange(year, max_month)[1]
    d_from = f"{year}-01-01T00:00:00"
    d_to = f"{year}-{max_month:02d}-{last_day}T23:59:59"

    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}' "
        f"and Recorder_Type eq 'StandardODATA.Document_ВозвратТоваровОтКлиента' "
        f"and RecordType eq 'Expense'",
        safe="",
    )
    sel = "Period,Recorder,СтоимостьРегл,Подразделение_Key"

    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{REG_SEBESTOIMOST}?$format=json"
            f"&$filter={flt}&$select={sel}"
            f"&$top=5000&$skip={skip}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("Vozvrat HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("Vozvrat HTTP %d: %s", r.status_code, r.text[:200])
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _batch_load_orders(session: requests.Session,
                       order_keys: list[str]) -> dict[str, dict]:
    """Загрузить ЗаказКлиента: подразделение, валюта, партнёр, соглашение и т.д."""
    result: dict[str, dict] = {}
    for i in range(0, len(order_keys), BATCH):
        batch = order_keys[i:i + BATCH]
        flt = quote(
            " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
            safe="",
        )
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json"
            f"&$filter={flt}"
            f"&$select=Ref_Key,Подразделение_Key,Партнер_Key,Валюта_Key,"
            f"Соглашение_Key,ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи"
            f"&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    result[it["Ref_Key"]] = {
                        "dept": it.get("Подразделение_Key", ""),
                        "partner": it.get("Партнер_Key", ""),
                        "currency": it.get("Валюта_Key", ""),
                        "agreement": it.get("Соглашение_Key", ""),
                        "ne_uchit": it.get("ТД_НеУчитыватьВПланФакте", False),
                        "soprovozhd": it.get("ТД_СопровождениеПродажи", False),
                    }
        except Exception:
            pass
    return result


def _batch_load_partners(session: requests.Session,
                         partner_keys: list[str]) -> dict[str, str]:
    """Загрузить имена партнёров."""
    result: dict[str, str] = {}
    for i in range(0, len(partner_keys), BATCH):
        batch = partner_keys[i:i + BATCH]
        flt = quote(
            " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
            safe="",
        )
        url = (
            f"{BASE}/Catalog_Партнеры?$format=json"
            f"&$filter={flt}&$select=Ref_Key,Description"
            f"&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    result[it["Ref_Key"]] = it.get("Description", "").strip()
        except Exception:
            pass
    return result


def _batch_load_vozvrat_docs(session: requests.Session,
                             doc_keys: list[str]) -> dict[str, dict]:
    """Загрузить ВозвратТоваровОтКлиента: подразделение, партнёр, договор."""
    result: dict[str, dict] = {}
    for i in range(0, len(doc_keys), BATCH):
        batch = doc_keys[i:i + BATCH]
        flt = quote(
            " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
            safe="",
        )
        url = (
            f"{BASE}/Document_ВозвратТоваровОтКлиента?$format=json"
            f"&$filter={flt}"
            f"&$select=Ref_Key,Подразделение_Key,Партнер_Key,"
            f"Соглашение_Key,Договор_Key"
            f"&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    result[it["Ref_Key"]] = {
                        "dept": it.get("Подразделение_Key", ""),
                        "partner": it.get("Партнер_Key", ""),
                        "agreement": it.get("Соглашение_Key", ""),
                        "contract": it.get("Договор_Key", ""),
                    }
        except Exception:
            pass
    return result


def _batch_load_contracts(session: requests.Session,
                          contract_keys: list[str]) -> dict[str, str]:
    """Загрузить тип договора (ТипДоговора)."""
    result: dict[str, str] = {}
    for i in range(0, len(contract_keys), BATCH):
        batch = contract_keys[i:i + BATCH]
        flt = quote(
            " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
            safe="",
        )
        url = (
            f"{BASE}/Catalog_ДоговорыКонтрагентов?$format=json"
            f"&$filter={flt}"
            f"&$select=Ref_Key,ТипДоговора"
            f"&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    result[it["Ref_Key"]] = it.get("ТипДоговора", "")
        except Exception:
            pass
    return result


def _calc_main_otgruzki(session: requests.Session,
                        rashod_rows: list[dict],
                        year: int, max_month: int) -> dict[int, float]:
    """Рассчитать основную часть отгрузок из РаспоряженияНаОтгрузку."""
    order_keys_set: set[str] = set()
    for row in rashod_rows:
        ok = row.get("Распоряжение", "")
        if ok and ok != EMPTY:
            order_keys_set.add(ok)

    orders = _batch_load_orders(session, sorted(order_keys_set))

    partner_keys_set: set[str] = set()
    for od in orders.values():
        pk = od.get("partner", "")
        if pk and pk != EMPTY:
            partner_keys_set.add(pk)

    partners_map = _batch_load_partners(session, sorted(partner_keys_set))
    excl_full = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES}
    excl_no_mgs = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    monthly: dict[int, float] = {m: 0.0 for m in range(1, max_month + 1)}

    for row in rashod_rows:
        period_str = (row.get("Period") or "")[:10]
        if len(period_str) < 7:
            continue
        try:
            m = int(period_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > max_month:
            continue

        ok = row.get("Распоряжение", "")
        if not ok or ok == EMPTY:
            continue
        od = orders.get(ok)
        if not od:
            continue

        dept = od["dept"]
        if dept not in DEPT_SET or dept == EMPTY:
            continue
        if od["agreement"] in ("", EMPTY):
            continue
        if od["ne_uchit"]:
            continue

        pk = od["partner"]
        soprovozhd = od["soprovozhd"]
        if dept == OPBO_DEPT:
            if pk in excl_no_mgs:
                continue
        else:
            if pk in excl_full and not soprovozhd:
                continue

        amount = float(row.get("Сумма") or 0)
        cur_code = CURRENCY_KEYS.get(od["currency"], "RUB")
        rate = EXCHANGE_RATES.get(cur_code, 1.0)

        monthly[m] += -amount * rate

    return monthly


def _calc_vozvrat_komisioner(session: requests.Session,
                             vozvrat_rows: list[dict],
                             year: int, max_month: int) -> dict[int, float]:
    """Рассчитать возвраты от комиссионеров (вычитаются из отгрузок)."""
    doc_keys_set: set[str] = set()
    for row in vozvrat_rows:
        dk = row.get("Recorder", "")
        if dk and dk != EMPTY:
            doc_keys_set.add(dk)

    if not doc_keys_set:
        return {m: 0.0 for m in range(1, max_month + 1)}

    docs = _batch_load_vozvrat_docs(session, sorted(doc_keys_set))

    contract_keys_set: set[str] = set()
    for dd in docs.values():
        ck = dd.get("contract", "")
        if ck and ck != EMPTY:
            contract_keys_set.add(ck)
    contracts = _batch_load_contracts(session, sorted(contract_keys_set))

    partner_keys_set: set[str] = set()
    for dd in docs.values():
        pk = dd.get("partner", "")
        if pk and pk != EMPTY:
            partner_keys_set.add(pk)
    partners_map = _batch_load_partners(session, sorted(partner_keys_set))
    excl_full = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES}
    excl_no_mgs = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    komisioner_docs: set[str] = set()
    for dk, dd in docs.items():
        ck = dd.get("contract", "")
        if contracts.get(ck, "") == "СКомиссионером":
            komisioner_docs.add(dk)

    monthly: dict[int, float] = {m: 0.0 for m in range(1, max_month + 1)}

    for row in vozvrat_rows:
        dk = row.get("Recorder", "")
        if dk not in komisioner_docs:
            continue
        dd = docs.get(dk)
        if not dd:
            continue

        dept = dd["dept"]
        if dept not in DEPT_SET or dept == EMPTY:
            continue
        if dd["agreement"] in ("", EMPTY):
            continue

        pk = dd["partner"]
        if dept == OPBO_DEPT:
            if pk in excl_no_mgs:
                continue
        else:
            if pk in excl_full:
                continue

        cost = float(row.get("СтоимостьРегл") or 0)
        if cost <= 0:
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

        monthly[m] += -cost

    return monthly


def get_otgruzki_monthly(year: int | None = None,
                         month: int | None = None) -> dict:
    """
    Помесячные факты отгрузок (январь..ref_month).
    Кэш на день.
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

    rashod = _load_rashod_records(session, ref_y, ref_m)
    main_monthly = _calc_main_otgruzki(session, rashod, ref_y, ref_m)

    try:
        vozvrat = _load_vozvrat_komisioner(session, ref_y, ref_m)
        voz_monthly = _calc_vozvrat_komisioner(session, vozvrat, ref_y, ref_m)
    except Exception:
        voz_monthly = {m: 0.0 for m in range(1, ref_m + 1)}

    out_months = []
    for m in range(1, ref_m + 1):
        cached = _load_cache(ref_y, m)
        if cached is not None:
            fact = cached
        else:
            fact = round(main_monthly.get(m, 0) + voz_monthly.get(m, 0), 2)
            _save_cache(ref_y, m, fact)
        out_months.append({"year": ref_y, "month": m, "fact": fact})

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

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    print(f"\n{'═' * 60}")
    print(f"  ОТГРУЗКИ ФАКТ (РаспоряженияНаОтгрузку)")
    print(f"  Период: январь – {MONTH_RU[m]} {y}")
    print(f"{'═' * 60}")

    t0 = time.time()
    data = get_otgruzki_monthly(y, m)

    print(f"\n{'Месяц':<12s} {'Факт':>20s}")
    print("─" * 35)
    total = 0.0
    for row in data.get("months", []):
        f = row["fact"]
        total += f
        print(f"  {MONTH_RU[row['month']]:<10s} {f:>20,.2f}")
    print("─" * 35)
    print(f"  {'ИТОГО':<10s} {total:>20,.2f}")
    print(f"\n  Время: {time.time() - t0:.1f}с")
    print(f"{'═' * 60}")
