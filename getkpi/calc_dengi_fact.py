"""
calc_dengi_fact.py — Факт поступления ДС (Деньги) за месяц.

Три ветки (UNION ALL в запросе 1С):
  1. Клиентские оплаты: ДвиженияДенежныеСредстваКонтрагент,
     ОбъектРасчетов → Catalog → Объект ССЫЛКА ЗаказКлиента
  2. Комиссия: тот же регистр,
     РасчетныйДокумент ССЫЛКА ОтчетКомиссионера, Регистратор ССЫЛКА ПоступлениеБезнал
  3. Взаимозачёты: ДвиженияКонтрагентКонтрагент,
     ОбъектРасчетов → Catalog → Объект ССЫЛКА ЗаказКлиента

API:
  from getkpi.calc_dengi_fact import get_dengi_monthly
  data = get_dengi_monthly()           # по умолчанию last_full_month
  data = get_dengi_monthly(2026, 3)    # январь-март 2026
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
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборуд. и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
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

ORDER_TYPE_MARKER = "Document_ЗаказКлиента"
UUID_POSTUPLENIE_BEZNAL = "e7564dde-28b6-4a9e-a627-9e70425af124"
UUID_RD_COMMISSION = frozenset({
    "a1e67513-8fde-4b86-8a14-990ca9d1a362",
    "233c1a97-85b9-4b2c-94ef-25531415f8c7",
})

BATCH = 15
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

REG_DS = "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType"
REG_KK = "AccumulationRegister_ДвиженияКонтрагентКонтрагент_RecordType"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"dengi_{year}_{month:02d}.json"


def _monthly_cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"dengi_monthly_{year}_{month:02d}.json"


def _load_cache(year: int, month: int) -> float | None:
    p = _cache_path(year, month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("total")
    except (OSError, json.JSONDecodeError):
        return None


def _save_cache(year: int, month: int, total: float) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), "w", encoding="utf-8") as f:
            json.dump({"total": total}, f, ensure_ascii=False)
    except OSError:
        pass


def _is_empty_ref(val) -> bool:
    return not val or val == "" or val == EMPTY


def _extract_uuid(type_str) -> str:
    if not type_str:
        return ""
    if "UnavailableEntity_" in type_str:
        return type_str.split("UnavailableEntity_", 1)[1].lower()
    return type_str.lower()


def _period_month(row: dict) -> int | None:
    period_str = (row.get("Period") or "")[:10]
    if len(period_str) < 7:
        return None
    try:
        return int(period_str[5:7])
    except (ValueError, IndexError):
        return None


# ═══════════════════════════════════════════════════════
# Загрузка данных
# ═══════════════════════════════════════════════════════

def _load_ds_register(session: requests.Session,
                      year: int, max_month: int) -> list[dict]:
    last_day = calendar.monthrange(year, max_month)[1]
    d_from = f"{year}-01-01T00:00:00"
    d_to = f"{year}-{max_month:02d}-{last_day}T23:59:59"

    sel = (
        "Period,Active,ХозяйственнаяОперация,Партнер_Key,Подразделение_Key,"
        "СуммаОплаты,СуммаОплатыРегл,СуммаПостоплаты,СуммаПостоплатыРегл,"
        "ОбъектРасчетов,ОбъектРасчетов_Type,"
        "РасчетныйДокумент,РасчетныйДокумент_Type,"
        "Recorder,Recorder_Type,Сторно"
    )
    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}' and Active eq true",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (f"{BASE}/{REG_DS}?$format=json&$top=5000&$skip={skip}"
               f"&$filter={flt}&$select={sel}")
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("DS register HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("DS register HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000

    return [x for x in rows if not x.get("Сторно")]


def _load_kk_register(session: requests.Session,
                      year: int, max_month: int) -> list[dict]:
    probe = session.get(f"{BASE}/{REG_KK}?$format=json&$top=1", timeout=20)
    if probe.status_code != 200:
        return []

    last_day = calendar.monthrange(year, max_month)[1]
    d_from = f"{year}-01-01T00:00:00"
    d_to = f"{year}-{max_month:02d}-{last_day}T23:59:59"

    sel = quote(
        "Period,Active,ОбъектРасчетов,ОбъектРасчетов_Type,"
        "Партнер_Key,Подразделение_Key,Сумма,СуммаРегл,"
        "ХозяйственнаяОперация,Сторно",
        safe=",_",
    )
    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}' and Active eq true",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (f"{BASE}/{REG_KK}?$format=json&$top=5000&$skip={skip}"
               f"&$filter={flt}&$select={sel}")
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("KK register HTTP error: %s", e)
            break
        if not r.ok:
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000

    return [x for x in rows if not x.get("Сторно")]


def _batch_load_catalog(session: requests.Session,
                        obj_keys: set[str]) -> dict[str, dict]:
    cat_select = quote("Ref_Key,Подразделение_Key,Партнер_Key,Соглашение,Объект,Объект_Type", safe=",_")
    catalog: dict[str, dict] = {}
    keys = sorted(obj_keys)
    for i in range(0, len(keys), BATCH):
        batch = keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (f"{BASE}/Catalog_ОбъектыРасчетов?$format=json"
               f"&$filter={flt}&$select={cat_select}&$top={BATCH}")
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    catalog[it["Ref_Key"]] = {
                        "dept": it.get("Подразделение_Key", ""),
                        "partner": it.get("Партнер_Key", ""),
                        "agreement": it.get("Соглашение", ""),
                        "obj": it.get("Объект", ""),
                        "obj_type": it.get("Объект_Type", ""),
                    }
        except Exception:
            pass
    return catalog


def _batch_load_orders(session: requests.Session,
                       order_keys: set[str]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    keys = sorted(order_keys)
    for i in range(0, len(keys), BATCH):
        batch = keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (f"{BASE}/Document_ЗаказКлиента?$format=json"
               f"&$filter={flt}"
               f"&$select=Ref_Key,Подразделение_Key,Партнер_Key,"
               f"ТД_СопровождениеПродажи,ТД_НеУчитыватьВПланФакте"
               f"&$top={BATCH}")
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    result[it["Ref_Key"]] = {
                        "dept": it.get("Подразделение_Key", ""),
                        "partner": it.get("Партнер_Key", ""),
                        "soprovozhd": it.get("ТД_СопровождениеПродажи", False),
                        "ne_uchit": it.get("ТД_НеУчитыватьВПланФакте", False),
                    }
        except Exception:
            pass
    return result


def _batch_load_partners(session: requests.Session,
                         partner_keys: set[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    keys = sorted(partner_keys)
    for i in range(0, len(keys), BATCH):
        batch = keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (f"{BASE}/Catalog_Партнеры?$format=json"
               f"&$filter={flt}&$select=Ref_Key,Description&$top={BATCH}")
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    result[it["Ref_Key"]] = it.get("Description", "").strip()
        except Exception:
            pass
    return result


# ═══════════════════════════════════════════════════════
# Расчёт трёх веток
# ═══════════════════════════════════════════════════════

def _calc_branch1(ds_rows: list[dict], catalog: dict, orders: dict,
                  excl_full: set, excl_no_mgs: set,
                  max_month: int) -> dict[int, float]:
    """Ветка 1: Клиентские оплаты (СуммаОплатыРегл)."""
    monthly: dict[int, float] = {m: 0.0 for m in range(1, max_month + 1)}

    for row in ds_rows:
        m = _period_month(row)
        if m is None or m < 1 or m > max_month:
            continue

        obj_key = row.get("ОбъектРасчетов", "")
        if _is_empty_ref(obj_key):
            continue
        cat = catalog.get(obj_key)
        if not cat:
            continue
        if _is_empty_ref(cat["dept"]) or cat["dept"] not in DEPT_SET:
            continue
        if _is_empty_ref(cat["agreement"]):
            continue
        if ORDER_TYPE_MARKER not in (cat.get("obj_type") or ""):
            continue

        order = orders.get(cat.get("obj", ""))
        if not order:
            continue
        if order["ne_uchit"] or order["soprovozhd"]:
            continue

        order_dept = order.get("dept", "")
        cat_partner = cat.get("partner", "")
        if order_dept == OPBO_DEPT:
            if cat_partner in excl_no_mgs:
                continue
        else:
            if cat_partner in excl_full and not order["soprovozhd"]:
                continue

        amt = float(row.get("СуммаОплатыРегл") or row.get("СуммаОплаты") or 0)
        if not amt:
            continue
        if row.get("ХозяйственнаяОперация") == "ВозвратОплатыКлиенту":
            amt = -amt

        monthly[m] += amt

    return monthly


def _calc_branch2(ds_rows: list[dict], catalog: dict, orders: dict,
                  excl_full: set,
                  max_month: int) -> dict[int, float]:
    """Ветка 2: Комиссия (СуммаПостоплатыРегл)."""
    monthly: dict[int, float] = {m: 0.0 for m in range(1, max_month + 1)}

    for row in ds_rows:
        m = _period_month(row)
        if m is None or m < 1 or m > max_month:
            continue

        rd_uuid = _extract_uuid(row.get("РасчетныйДокумент_Type"))
        if rd_uuid not in UUID_RD_COMMISSION:
            continue
        rec_uuid = _extract_uuid(row.get("Recorder_Type"))
        if rec_uuid != UUID_POSTUPLENIE_BEZNAL:
            continue

        reg_partner = row.get("Партнер_Key", "")
        if reg_partner in excl_full:
            continue

        reg_dept = row.get("Подразделение_Key", "")
        if _is_empty_ref(reg_dept) or reg_dept not in DEPT_SET:
            obj_key = row.get("ОбъектРасчетов", "")
            cat = catalog.get(obj_key) if obj_key else None
            if cat:
                order = orders.get(cat.get("obj", ""))
                if order and order["dept"] in DEPT_SET:
                    reg_dept = order["dept"]
                elif cat["dept"] in DEPT_SET:
                    reg_dept = cat["dept"]
                else:
                    continue
            else:
                continue

        amt = float(row.get("СуммаПостоплатыРегл") or row.get("СуммаПостоплаты") or 0)
        if not amt:
            continue

        monthly[m] += amt

    return monthly


def _calc_branch3(kk_rows: list[dict], catalog: dict, orders: dict,
                  excl_full: set,
                  max_month: int) -> dict[int, float]:
    """Ветка 3: Взаимозачёты (СуммаРегл)."""
    monthly: dict[int, float] = {m: 0.0 for m in range(1, max_month + 1)}

    for row in kk_rows:
        m = _period_month(row)
        if m is None or m < 1 or m > max_month:
            continue

        obj_key = row.get("ОбъектРасчетов", "")
        if _is_empty_ref(obj_key):
            continue
        cat = catalog.get(obj_key)
        if not cat:
            continue
        if _is_empty_ref(cat["dept"]) or cat["dept"] not in DEPT_SET:
            continue
        if _is_empty_ref(cat["agreement"]):
            continue
        if cat.get("partner", "") in excl_full:
            continue
        if ORDER_TYPE_MARKER not in (cat.get("obj_type") or ""):
            continue

        order = orders.get(cat.get("obj", ""))
        if not order:
            continue
        if order["ne_uchit"] or order["soprovozhd"]:
            continue

        amt = float(row.get("СуммаРегл") or row.get("Сумма") or 0)
        if not amt:
            continue

        monthly[m] += amt

    return monthly


# ═══════════════════════════════════════════════════════
# Публичный API
# ═══════════════════════════════════════════════════════

def get_dengi_monthly(year: int | None = None,
                      month: int | None = None) -> dict:
    """
    Помесячные факты ДС (январь..ref_month).
    Результат кэшируется.
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
            return data
        except (OSError, json.JSONDecodeError):
            pass

    all_cached = True
    for m in range(1, ref_m + 1):
        if _load_cache(ref_y, m) is None:
            all_cached = False
            break

    if all_cached:
        out_months = []
        for m in range(1, ref_m + 1):
            out_months.append({"year": ref_y, "month": m, "fact": _load_cache(ref_y, m)})
        payload = {"year": ref_y, "ref_month": ref_m, "months": out_months}
        try:
            with open(mc, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except OSError:
            pass
        return payload

    session = requests.Session()
    session.auth = AUTH

    logger.info("calc_dengi_fact: loading registers for %d months 1-%d", ref_y, ref_m)
    ds_rows = _load_ds_register(session, ref_y, ref_m)
    kk_rows = _load_kk_register(session, ref_y, ref_m)

    obj_keys: set[str] = set()
    for x in ds_rows:
        ok = x.get("ОбъектРасчетов", "")
        if ok and ok != EMPTY:
            obj_keys.add(ok)
    for x in kk_rows:
        ok = x.get("ОбъектРасчетов", "")
        if ok and ok != EMPTY:
            obj_keys.add(ok)

    catalog = _batch_load_catalog(session, obj_keys)

    order_guids: set[str] = set()
    for cat_entry in catalog.values():
        if ORDER_TYPE_MARKER in (cat_entry.get("obj_type") or ""):
            og = cat_entry.get("obj", "")
            if og and og != EMPTY:
                order_guids.add(og)

    orders = _batch_load_orders(session, order_guids)

    all_partner_keys: set[str] = set()
    for c in catalog.values():
        pk = c.get("partner", "")
        if pk and pk != EMPTY:
            all_partner_keys.add(pk)
    for o in orders.values():
        pk = o.get("partner", "")
        if pk and pk != EMPTY:
            all_partner_keys.add(pk)

    partners_map = _batch_load_partners(session, all_partner_keys)
    excl_full = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES}
    excl_no_mgs = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    b1 = _calc_branch1(ds_rows, catalog, orders, excl_full, excl_no_mgs, ref_m)
    b2 = _calc_branch2(ds_rows, catalog, orders, excl_full, ref_m)
    b3 = _calc_branch3(kk_rows, catalog, orders, excl_full, ref_m)

    out_months = []
    for m in range(1, ref_m + 1):
        cached = _load_cache(ref_y, m)
        if cached is not None:
            fact = cached
        else:
            fact = round(b1.get(m, 0) + b2.get(m, 0) + b3.get(m, 0), 2)
            _save_cache(ref_y, m, fact)
        out_months.append({"year": ref_y, "month": m, "fact": fact})

    payload = {"year": ref_y, "ref_month": ref_m, "months": out_months}
    try:
        with open(mc, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass
    return payload


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    import functools
    _print = functools.partial(print, flush=True)

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    _print(f"\n{'═' * 60}")
    _print(f"  ФАКТ ДС (Деньги)")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_dengi_monthly(y, m)

    _print(f"\n{'Месяц':<12s} {'Факт':>20s}")
    _print("─" * 35)
    total = 0.0
    for row in data.get("months", []):
        f = row["fact"]
        total += f
        _print(f"  {MONTH_RU[row['month']]:<10s} {f:>20,.2f}")
    _print("─" * 35)
    _print(f"  {'ИТОГО':<10s} {total:>20,.2f}")
    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
