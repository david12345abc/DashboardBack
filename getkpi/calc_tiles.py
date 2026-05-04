"""
calc_tiles.py — Три плитки факта по месяцам: Деньги / Отгрузки / Договоры.

Для каждого месяца (январь — текущий) считает:
  1. Факт отгрузки  (AccumulationRegister_РаспоряженияНаОтгрузку_RecordType)
  2. Факт ДС        (AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType)
  3. Факт договоров  (Catalog_СоглашенияСКлиентами + $expand=Договор)

Результат: tiles_{год}.json + консольная таблица.

Использование:
  python calc_tiles.py [год]           # по умолчанию 2026
  python calc_tiles.py 2026 --force    # без кэша
"""
import requests, sys, time, json, os, calendar, functools
from datetime import datetime
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

# ═══════════════════════════════════════════════════════
# КОНФИГ
# ═══════════════════════════════════════════════════════

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",  # ВЭД
    "34497ef7-810f-11e4-80d6-001e67112509",  # Эталонное
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",  # БМИ
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",  # Ключевые клиенты
    "7587c178-92f6-11f0-96f9-6cb31113810e",  # Дилерские (ОПБО)
    "bd7b5184-9f9c-11e4-80da-001e67112509",  # ПАО Газпром
}
OPBO_DEPT = "7587c178-92f6-11f0-96f9-6cb31113810e"

EXCLUDE_PARTNER_NAMES = {
    "АЛМАЗ ООО (рабочий)", "Турбулентность-Дон ООО",
    "Турбулентность-ДОН ООО НПО", "СКТБ Турбо-Дон ООО", "Метрогазсервис ООО",
}
EXCLUDE_PARTNER_NAMES_NO_MGS = EXCLUDE_PARTNER_NAMES - {"Метрогазсервис ООО"}

EXCLUDE_PARTNER_KEYS = frozenset({
    "6ff45495-a8c5-11e7-8266-ac1f6b05524d",  # АЛМАЗ ООО (рабочий)
    "6ac41964-88a0-11e7-812e-001e67112509",  # Турбулентность-Дон ООО
    "6cdfe9f3-a8c4-11e7-8266-ac1f6b05524d",  # Турбулентность-ДОН ООО НПО
    "4babc7a7-a8c7-11e7-8266-ac1f6b05524d",  # СКТБ Турбо-Дон ООО
    "d7f5ff44-a8c6-11e7-8266-ac1f6b05524d",  # Метрогазсервис ООО
    "237a2c5f-3b94-11e7-812b-001e67112509",  # Газпром межрегионгаз Владикавказ, ООО
})
EXCLUDE_PARTNER_KEYS_NO_MGS = EXCLUDE_PARTNER_KEYS - frozenset({
    "d7f5ff44-a8c6-11e7-8266-ac1f6b05524d"
})

EXCLUDE_PARTNERS_DOGOVORY = frozenset({
    "6ff45495-a8c5-11e7-8266-ac1f6b05524d",  # АЛМАЗ ООО (рабочий)
    "6ac41964-88a0-11e7-812e-001e67112509",  # Турбулентность-Дон ООО
    "6cdfe9f3-a8c4-11e7-8266-ac1f6b05524d",  # Турбулентность-ДОН ООО НПО
    "4babc7a7-a8c7-11e7-8266-ac1f6b05524d",  # СКТБ Турбо-Дон ООО
    "d7f5ff44-a8c6-11e7-8266-ac1f6b05524d",  # Метрогазсервис ООО
})

DEPARTMENTS_NAMED = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Эталонное оборудование",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Ключевые клиенты",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Дилерские продажи",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "ПАО Газпром",
}

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

ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"

UUID_REC_POSTUPLENIE_BEZNAL = "e7564dde-28b6-4a9e-a627-9e70425af124"
UUID_RD_COMMISSION_CANDIDATES = frozenset({
    "a1e67513-8fde-4b86-8a14-990ca9d1a362",
    "233c1a97-85b9-4b2c-94ef-25531415f8c7",
})

CLIENT_OPS = {
    "ПоступлениеОплатыОтКлиента",
    "ПоступлениеОплатыОтКлиентаПоПлатежнойКарте",
    "ВозвратОплатыКлиенту",
}

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _period(year, month):
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _odata_pages(session, url_template, top=5000, timeout=120):
    rows = []
    skip = 0
    while True:
        url = url_template + f"&$top={top}&$skip={skip}"
        r = session.get(url, timeout=timeout)
        if not r.ok:
            break
        chunk = r.json().get("value", [])
        rows.extend(chunk)
        if len(chunk) < top:
            break
        skip += top
    return rows


def _extract_uuid(type_str):
    if not type_str:
        return None
    idx = type_str.rfind("_")
    if idx >= 0:
        return type_str[idx + 1:].lower()
    return None


# ═══════════════════════════════════════════════════════
# ОБЩИЕ РЕСУРСЫ (загружаются один раз)
# ═══════════════════════════════════════════════════════

class SharedContext:
    def __init__(self, session):
        self.session = session
        self.partners = {}
        self.exclude_full = set()
        self.exclude_no_mgs = set()
        self._loaded = False

    def ensure_partners(self, partner_keys):
        missing = [k for k in partner_keys if k not in self.partners and k != EMPTY]
        if not missing:
            return
        BATCH = 15
        for i in range(0, len(missing), BATCH):
            batch = missing[i:i + BATCH]
            flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
            url = (
                f"{BASE}/Catalog_Партнеры?$format=json"
                f"&$filter={flt}&$select=Ref_Key,Description&$top={BATCH}"
            )
            r = self.session.get(url, timeout=30)
            if r.ok:
                for item in r.json().get("value", []):
                    self.partners[item["Ref_Key"]] = item.get("Description", "").strip()
        self.exclude_full = set(EXCLUDE_PARTNER_KEYS) | {
            k for k, v in self.partners.items() if v in EXCLUDE_PARTNER_NAMES
        }
        self.exclude_no_mgs = set(EXCLUDE_PARTNER_KEYS_NO_MGS) | {
            k for k, v in self.partners.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS
        }


def _order_passes_shipment(o, ctx):
    if not o:
        return False
    if o["dept"] not in DEPARTMENTS or o["dept"] == EMPTY:
        return False
    if o["agreement"] == EMPTY or not o["agreement"]:
        return False
    if o["dept"] == OPBO_DEPT:
        if o["partner"] in ctx.exclude_no_mgs:
            return False
    else:
        if o["partner"] in ctx.exclude_full and not o["soprovozhd"]:
            return False
    if o["ne_uchit"]:
        return False
    return True


def _order_passes_ds(o, ctx):
    if not o:
        return False
    if o["dept"] not in DEPARTMENTS or o["dept"] == EMPTY:
        return False
    if o["agreement"] == EMPTY or not o["agreement"]:
        return False
    if o["partner"] in ctx.exclude_full:
        return False
    if o["soprovozhd"]:
        return False
    if o["ne_uchit"]:
        return False
    return True


def _load_orders_by_guids(session, guids):
    orders = {}
    select = quote(
        "Ref_Key,Подразделение_Key,Партнер_Key,Соглашение_Key,"
        "ТД_СопровождениеПродажи,ТД_НеУчитыватьВПланФакте,Валюта_Key",
        safe=",_"
    )
    BATCH = 15
    for i in range(0, len(guids), BATCH):
        batch = guids[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{g}'" for g in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json"
            f"&$select={select}&$top={BATCH}&$filter={flt}"
        )
        r = session.get(url, timeout=30)
        if r.ok:
            for item in r.json().get("value", []):
                orders[item["Ref_Key"]] = {
                    "dept": item.get("Подразделение_Key", EMPTY),
                    "partner": item.get("Партнер_Key", EMPTY),
                    "agreement": item.get("Соглашение_Key", EMPTY),
                    "soprovozhd": item.get("ТД_СопровождениеПродажи", False),
                    "ne_uchit": item.get("ТД_НеУчитыватьВПланФакте", False),
                    "currency": item.get("Валюта_Key", EMPTY),
                }
    return orders


def _currency_rate(currency_key):
    code = CURRENCY_KEYS.get(currency_key, "RUB")
    return EXCHANGE_RATES.get(code, 1.0)


# ═══════════════════════════════════════════════════════
# 1. ФАКТ ОТГРУЗКИ
# ═══════════════════════════════════════════════════════

def calc_shipment_fact(session, year, month, ctx):
    p_start, p_end = _period(year, month)
    period_filter = quote(
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}' and Active eq true",
        safe=""
    )
    select_fields = "Period,Active,Распоряжение,Распоряжение_Type,ВидДвиженияРегистра,Сумма,Сторно"
    entries = _odata_pages(
        session,
        f"{BASE}/AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
        f"?$format=json&$filter={period_filter}&$select={select_fields}",
    )
    order_entries = [
        {"guid": r["Распоряжение"], "Сумма": r.get("Сумма") or 0, "ВидДвижения": r.get("ВидДвиженияРегистра", "")}
        for r in entries if r.get("Распоряжение_Type") == ORDER_TYPE
    ]
    guids = sorted(set(e["guid"] for e in order_entries if e.get("guid")))
    orders = _load_orders_by_guids(session, guids)
    all_pks = {o["partner"] for o in orders.values() if o["partner"] != EMPTY}
    ctx.ensure_partners(all_pks)

    passed = {g for g in guids if _order_passes_shipment(orders.get(g), ctx)}
    total = 0.0
    by_dept = {}
    for e in order_entries:
        if e["guid"] not in passed or e["ВидДвижения"] != "Расход":
            continue
        o = orders.get(e["guid"])
        s = abs(e["Сумма"] or 0) * _currency_rate((o or {}).get("currency", EMPTY))
        total += s
        if o:
            by_dept[o["dept"]] = by_dept.get(o["dept"], 0) + s
    return {"total": total, "by_dept": by_dept}


# ═══════════════════════════════════════════════════════
# 2. ФАКТ ДС
# ═══════════════════════════════════════════════════════

def _scan_all_orders_obj_keys(session, needed_keys):
    """Полный скан Document_ЗаказКлиента для связки ОбъектРасчетов → заказ."""
    select = quote(
        "Ref_Key,Подразделение_Key,Партнер_Key,Соглашение_Key,"
        "ТД_СопровождениеПродажи,ТД_НеУчитыватьВПланФакте,ОбъектРасчетов_Key",
        safe=",_"
    )
    orders_by_obj = {}
    skip = 0
    PAGE = 500
    while True:
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json"
            f"&$select={select}&$orderby=Ref_Key&$top={PAGE}&$skip={skip}"
        )
        r = session.get(url, timeout=120)
        if not r.ok:
            break
        chunk = r.json().get("value", [])
        if not chunk:
            break
        for item in chunk:
            k = item.get("ОбъектРасчетов_Key")
            if not k or k == EMPTY:
                continue
            k_s = str(k).lower()
            if k_s not in needed_keys:
                continue
            orec = {
                "ref": item["Ref_Key"],
                "dept": item.get("Подразделение_Key", EMPTY),
                "partner": item.get("Партнер_Key", EMPTY),
                "agreement": item.get("Соглашение_Key", EMPTY),
                "soprovozhd": item.get("ТД_СопровождениеПродажи", False),
                "ne_uchit": item.get("ТД_НеУчитыватьВПланФакте", False),
            }
            lst = orders_by_obj.setdefault(k_s, [])
            if not any(x["ref"] == orec["ref"] for x in lst):
                lst.append(orec)
        skip += len(chunk)
        found = sum(1 for k in needed_keys if k in orders_by_obj)
        if found >= len(needed_keys) or len(chunk) < PAGE:
            break
    return orders_by_obj


def calc_ds_fact(session, year, month, ctx, orders_by_obj_cache=None):
    p_start, p_end = _period(year, month)
    REG_DS = "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType"
    ds_select = (
        "Period,Active,ХозяйственнаяОперация,Партнер_Key,Подразделение_Key,"
        "СуммаОплаты,СуммаОплатыРегл,СуммаПостоплаты,СуммаПостоплатыРегл,"
        "ОбъектРасчетов,ОбъектРасчетов_Type,РасчетныйДокумент_Type,"
        "Recorder_Type,Сторно"
    )
    period_filter = quote(
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}' and Active eq true",
        safe=""
    )
    ds_entries = _odata_pages(
        session,
        f"{BASE}/{REG_DS}?$format=json&$filter={period_filter}&$select={ds_select}",
    )
    ds_client = [
        r for r in ds_entries
        if r.get("ХозяйственнаяОперация") in CLIENT_OPS and not r.get("Сторно")
    ]

    REG_KK = "AccumulationRegister_ДвиженияКонтрагентКонтрагент_RecordType"
    kk_entries = []
    kk_probe = session.get(f"{BASE}/{REG_KK}?$format=json&$top=1", timeout=20)
    if kk_probe.ok:
        kk_select = quote(
            "Period,Active,ОбъектРасчетов,ОбъектРасчетов_Type,"
            "Партнер_Key,Подразделение_Key,Сумма,СуммаРегл,Сторно",
            safe=",_",
        )
        kk_filter = quote(
            f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}' and Active eq true",
            safe="",
        )
        kk_entries = _odata_pages(
            session,
            f"{BASE}/{REG_KK}?$format=json&$filter={kk_filter}&$select={kk_select}",
        )

    obj_keys = set()
    for r in ds_entries:
        ok = r.get("ОбъектРасчетов")
        if ok and ok != EMPTY:
            obj_keys.add(ok)
    for r in kk_entries:
        if r.get("Сторно"):
            continue
        ok = r.get("ОбъектРасчетов")
        if ok and ok != EMPTY:
            obj_keys.add(ok)

    catalog = {}
    if obj_keys:
        cat_select = quote("Ref_Key,Подразделение_Key,Партнер_Key,Соглашение,Объект,Объект_Type", safe=",_")
        keys = sorted(obj_keys)
        BATCH = 15
        for i in range(0, len(keys), BATCH):
            batch = keys[i:i + BATCH]
            flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
            url = (
                f"{BASE}/Catalog_ОбъектыРасчетов?$format=json"
                f"&$filter={flt}&$select={cat_select}&$top={BATCH}"
            )
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    catalog[it["Ref_Key"]] = {
                        "dept": it.get("Подразделение_Key", EMPTY),
                        "partner": it.get("Партнер_Key", EMPTY),
                        "agreement": it.get("Соглашение", EMPTY),
                        "obj": it.get("Объект", EMPTY),
                        "obj_type": it.get("Объект_Type", ""),
                    }

    order_guids = sorted({
        c["obj"] for c in catalog.values()
        if c.get("obj") and c.get("obj") != EMPTY and "Document_ЗаказКлиента" in (c.get("obj_type") or "")
    })
    orders = _load_orders_by_guids(session, order_guids)

    all_pks = {c["partner"] for c in catalog.values() if c.get("partner") and c.get("partner") != EMPTY}
    all_pks.update(o["partner"] for o in orders.values() if o.get("partner") and o.get("partner") != EMPTY)
    ctx.ensure_partners(all_pks)

    def resolve_order(cat):
        if not cat or "Document_ЗаказКлиента" not in (cat.get("obj_type") or ""):
            return None
        return orders.get(cat.get("obj"))

    total = 0.0
    by_dept = {}

    for r in ds_client:
        ok = r.get("ОбъектРасчетов")
        if not ok or ok == EMPTY:
            continue
        cat = catalog.get(ok)
        o = resolve_order(cat)
        if not cat or not o:
            continue
        if cat["dept"] not in DEPARTMENTS or cat["dept"] == EMPTY:
            continue
        if cat["agreement"] == EMPTY or not cat["agreement"]:
            continue
        if o["ne_uchit"] or o["soprovozhd"]:
            continue
        cat_partner = cat.get("partner", EMPTY)
        if o["dept"] == OPBO_DEPT:
            if cat_partner in ctx.exclude_no_mgs:
                continue
        else:
            if cat_partner in ctx.exclude_full and not o["soprovozhd"]:
                continue
        sm = float(r.get("СуммаОплатыРегл") or r.get("СуммаОплаты") or 0)
        if r.get("ХозяйственнаяОперация") == "ВозвратОплатыКлиенту":
            total -= sm
            by_dept[cat["dept"]] = by_dept.get(cat["dept"], 0) - sm
        else:
            total += sm
            by_dept[cat["dept"]] = by_dept.get(cat["dept"], 0) + sm

    for r in ds_entries:
        if r.get("Сторно") or not r.get("Active", True):
            continue
        rec_uuid = _extract_uuid(r.get("Recorder_Type"))
        rd_uuid = _extract_uuid(r.get("РасчетныйДокумент_Type"))
        if rec_uuid != UUID_REC_POSTUPLENIE_BEZNAL or rd_uuid not in UUID_RD_COMMISSION_CANDIDATES:
            continue
        sm = float(r.get("СуммаПостоплатыРегл") or r.get("СуммаПостоплаты") or 0)
        if not sm:
            continue
        ok = r.get("ОбъектРасчетов")
        cat = catalog.get(ok) if ok and ok != EMPTY else None
        o = resolve_order(cat)
        if cat:
            if cat.get("partner") in ctx.exclude_full:
                continue
            dept_c = cat.get("dept", EMPTY)
        else:
            if r.get("Партнер_Key") in ctx.exclude_full:
                continue
            dept_c = r.get("Подразделение_Key")
        if not dept_c or dept_c == EMPTY or dept_c not in DEPARTMENTS:
            continue
        total += sm
        by_dept[dept_c] = by_dept.get(dept_c, 0) + sm

    for r in kk_entries:
        if r.get("Сторно"):
            continue
        ok = r.get("ОбъектРасчетов")
        if not ok or ok == EMPTY:
            continue
        cat = catalog.get(ok)
        o = resolve_order(cat)
        if not cat or not o:
            continue
        if cat["dept"] not in DEPARTMENTS or cat["dept"] == EMPTY:
            continue
        if cat["agreement"] == EMPTY or not cat["agreement"]:
            continue
        if cat["partner"] in ctx.exclude_full:
            continue
        if o["ne_uchit"] or o["soprovozhd"]:
            continue
        sm = float(r.get("СуммаРегл") or r.get("Сумма") or 0)
        if not sm:
            continue
        total += sm
        by_dept[cat["dept"]] = by_dept.get(cat["dept"], 0) + sm

    return {"total": total, "by_dept": by_dept}


# ═══════════════════════════════════════════════════════
# 3. ФАКТ ДОГОВОРОВ
# ═══════════════════════════════════════════════════════

def calc_dogovory_fact(session, year, month, ctx):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"

    select_fields = (
        "Спецификация_Key,Подразделение_Key,Партнер_Key,"
        "ЗаказКлиента_Key,СуммаДоговора,ДатаПодписания,"
        "ТД_СопровождениеПродажи,ТД_ПодтвержденоБухгалтером"
    )
    rows = _odata_pages(
        session,
        f"{BASE}/InformationRegister_ТД_ДоговорыПодписанные?$format=json"
        f"&$select={select_fields}",
    )

    month_rows = []
    for row in rows:
        dt = (row.get("ДатаПодписания") or "")[:10]
        if not (date_from <= dt <= date_to):
            continue
        dept = row.get("Подразделение_Key", EMPTY)
        if dept not in DEPARTMENTS or dept == EMPTY:
            continue
        month_rows.append(row)

    spec_keys = sorted({
        row.get("Спецификация_Key", "")
        for row in month_rows
        if row.get("Спецификация_Key", "") not in ("", EMPTY)
    })
    spec_status = {}
    BATCH = 15
    for i in range(0, len(spec_keys), BATCH):
        batch = spec_keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Catalog_СоглашенияСКлиентами?$format=json"
            f"&$filter={flt}&$select=Ref_Key,Статус&$top={BATCH}"
        )
        r = session.get(url, timeout=30)
        if r.ok:
            for it in r.json().get("value", []):
                spec_status[it["Ref_Key"]] = it.get("Статус", "")

    partner_keys = {
        row.get("Партнер_Key", "")
        for row in month_rows
        if row.get("Партнер_Key", "") not in ("", EMPTY)
    }
    ctx.ensure_partners(partner_keys)

    order_keys = sorted({
        row.get("ЗаказКлиента_Key", "")
        for row in month_rows
        if row.get("ЗаказКлиента_Key", "") not in ("", EMPTY)
    })
    orders = _load_orders_by_guids(session, order_keys)

    total = 0.0
    by_dept = {}
    for row in month_rows:
        if spec_status.get(row.get("Спецификация_Key", "")) != "Действует":
            continue
        pk = row.get("Партнер_Key", EMPTY)
        dept = row.get("Подразделение_Key", EMPTY)
        soprovozhd = row.get("ТД_СопровождениеПродажи", False)
        if dept == OPBO_DEPT:
            if pk in ctx.exclude_no_mgs:
                continue
        else:
            if pk in ctx.exclude_full and not soprovozhd:
                continue
        order_key = row.get("ЗаказКлиента_Key", "")
        order = orders.get(order_key)
        if order and order.get("ne_uchit"):
            continue
        amount = float(row.get("СуммаДоговора") or 0)
        amount *= _currency_rate((order or {}).get("currency", EMPTY))
        total += amount
        by_dept[dept] = by_dept.get(dept, 0) + amount
    return {"total": total, "by_dept": by_dept}


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--force"]
    year = int(args[0]) if args else 2026

    now = datetime.now()
    max_month = now.month if now.year == year else (12 if now.year > year else 0)
    if max_month == 0:
        print(f"Год {year} ещё не наступил.")
        sys.exit(1)

    session = requests.Session()
    session.auth = AUTH
    ctx = SharedContext(session)
    orders_by_obj_cache = {}
    t0 = time.time()

    cache_file = os.path.join(os.path.dirname(__file__), f"tiles_{year}.json")
    existing = {}
    if not force and os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            existing = json.load(f)

    print(f"\n{'═' * 60}")
    print(f"  ПЛИТКИ ФАКТА · {year}")
    print(f"  Месяцы: январь — {MONTH_RU[max_month].lower()}")
    print(f"{'═' * 60}")

    results = []

    for m in range(1, max_month + 1):
        t_m = time.time()
        label = MONTH_RU[m]

        cached_month = None
        if not force and existing.get("months"):
            for em in existing["months"]:
                if em.get("month") == m:
                    cached_month = em
                    break

        if cached_month and m < max_month:
            results.append(cached_month)
            print(f"\n  {label:<10s} (кэш)  отгр={cached_month['отгрузки_факт']:>15,.2f}"
                  f"  ДС={cached_month['деньги_факт']:>15,.2f}"
                  f"  дог={cached_month['договоры_факт']:>15,.2f}")
            continue

        print(f"\n▸ {label} {year} ...")

        print(f"    отгрузка...", end="")
        ship = calc_shipment_fact(session, year, m, ctx)
        print(f" {ship['total']:,.2f}")

        print(f"    ДС...", end="")
        ds = calc_ds_fact(session, year, m, ctx, orders_by_obj_cache)
        print(f" {ds['total']:,.2f}")

        print(f"    договоры...", end="")
        dog = calc_dogovory_fact(session, year, m, ctx)
        print(f" {dog['total']:,.2f}")

        by_dept_merged = {}
        for dk in DEPARTMENTS:
            by_dept_merged[dk] = {
                "name": DEPARTMENTS_NAMED.get(dk, dk),
                "отгрузки": ship["by_dept"].get(dk, 0),
                "деньги": ds["by_dept"].get(dk, 0),
                "договоры": dog["by_dept"].get(dk, 0),
            }

        month_result = {
            "month": m,
            "label": label,
            "отгрузки_факт": round(ship["total"], 2),
            "деньги_факт": round(ds["total"], 2),
            "договоры_факт": round(dog["total"], 2),
            "by_department": by_dept_merged,
        }
        results.append(month_result)
        print(f"    ✓ {time.time() - t_m:.1f}с")

    # Сохраняем JSON
    output = {"year": year, "generated": datetime.now().isoformat(), "months": results}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Сводная таблица
    print(f"\n{'═' * 72}")
    print(f"  {'Месяц':<12s} {'Отгрузки':>18s} {'Деньги (ДС)':>18s} {'Договоры':>18s}")
    print(f"  {'─' * 68}")
    t_ship = t_ds = t_dog = 0.0
    for r in results:
        print(f"  {r['label']:<12s} {r['отгрузки_факт']:>18,.2f} {r['деньги_факт']:>18,.2f} {r['договоры_факт']:>18,.2f}")
        t_ship += r["отгрузки_факт"]
        t_ds += r["деньги_факт"]
        t_dog += r["договоры_факт"]
    print(f"  {'─' * 68}")
    print(f"  {'ИТОГО':<12s} {t_ship:>18,.2f} {t_ds:>18,.2f} {t_dog:>18,.2f}")
    print(f"{'═' * 72}")
    print(f"  → {cache_file}")
    print(f"  Время: {time.time() - t0:.1f}с")
