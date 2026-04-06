"""
calc_vp_fast.py — Оптимизированный расчёт Валовой Прибыли из 1С OData.

Замена цепочки: step1_scan → fetch_costs → resolve_orgs → step2_calc

Ключевые оптимизации:
  1. _RecordType + $filter по Period + $select       → 0.7с вместо ~10 мин
  2. Батчевый маппинг организаций (20 GUID/запрос)   → в 20 раз меньше запросов
  3. Кэширование ключей «Наше предприятие»           → 0 запросов при повторе
  4. Один скрипт вместо четырёх запусков

Запуск:
  python calc_vp_fast.py <месяц> <год>
  python calc_vp_fast.py <месяц> <год> --force   # игнорировать все кэши
"""
import requests, sys, time, json, functools, os
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

# ─────────────────────────── config ───────────────────────────

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
}

NASHE = "5fc811cd-6251-11e7-812d-001e67112509"

PRELIM_ORGS = {
    "171272c0-ef41-11e9-829c-ac1f6b05524d",
    "fbca2148-6cfd-11e7-812d-001e67112509",
    "fbca2145-6cfd-11e7-812d-001e67112509",
    "fbca2143-6cfd-11e7-812d-001e67112509",
}

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

SELECT_FIELDS = (
    "Period,Active,Подразделение_Key,АналитикаУчетаПоПартнерам_Key,"
    "АналитикаУчетаНоменклатуры_Key,ВидЗапасов_Key,ТипЗапасов,Количество,"
    "СуммаВыручки,Стоимость,ДопРасходы,Трудозатраты,"
    "ПостатейныеПостоянныеСНДС,ПостатейныеПеременныеСНДС,РасходыНаПродажуСНДС"
)

ENTRY_KEYS = [k.strip() for k in SELECT_FIELDS.split(",")]

COST_KEYS = [
    "Стоимость", "СтоимостьДопРасходы", "Трудозатраты",
    "ПостатейныеПостоянныеСНДС", "ПостатейныеПеременныеСНДС",
]

BATCH = 5000

# ─────────────────────────── args ───────────────────────────

force = "--force" in sys.argv
args = [a for a in sys.argv[1:] if not a.startswith("-")]

if len(args) != 2:
    print("Использование: python calc_vp_fast.py <месяц> <год> [--force]")
    print("Пример:        python calc_vp_fast.py 3 2026")
    sys.exit(1)

month, year = int(args[0]), int(args[1])

if month == 12:
    P_START = f"{year}-12-01T00:00:00"
    P_END = f"{year + 1}-01-01T00:00:00"
else:
    P_START = f"{year}-{month:02d}-01T00:00:00"
    P_END = f"{year}-{month + 1:02d}-01T00:00:00"

mn = MONTH_RU[month]
F_VP = f"вп_{mn}_{year}_cache.json"
F_COST = f"стоимость_товаров_{mn}_{year}.json"
F_ORG = f"аналитика_орг_{mn}_{year}.json"
F_NASHE = "nashe_keys_cache.json"


def load_cache(path):
    if os.path.exists(path) and not force:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return None


session = requests.Session()
session.auth = AUTH
T0 = time.time()

print(f"\n{'═' * 55}")
print(f"  Валовая прибыль · {mn.title()} {year}")
print(f"  {P_START[:10]} — {P_END[:10]}")
print(f"{'═' * 55}")


# ═══════════════════════════════════════════════════════
# 1. СКАН РЕГИСТРА  — _RecordType + $filter + $select
# ═══════════════════════════════════════════════════════
#
# _RecordType даёт плоские записи (не вложенные в RecordSet),
# поэтому $filter по Period работает на стороне сервера 1С.
# $select ограничивает набор полей → минимальный трафик.
#
# Было:  полный скан 55К записей через RecordSet     → ~10 мин, ~300 MB
# Стало: $filter + $select через _RecordType          → ~1с, ~1.4 MB

print(f"\n▸ Шаг 1 · Регистр ВыручкаИСебестоимостьПродаж")

cached = load_cache(F_VP)
if cached is not None:
    entries = cached["entries"]
    print(f"  кэш: {len(entries)} записей")
else:
    t = time.time()
    period_filter = quote(
        f"Period ge datetime'{P_START}' and Period lt datetime'{P_END}' and Active eq true",
        safe=""
    )

    entries = []
    skip = 0
    pg = 0

    while True:
        pg += 1
        url = (
            f"{BASE}/AccumulationRegister_ВыручкаИСебестоимостьПродаж_RecordType"
            f"?$format=json&$top={BATCH}&$skip={skip}"
            f"&$filter={period_filter}&$select={SELECT_FIELDS}"
        )
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        rows = resp.json().get("value", [])

        for row in rows:
            entries.append({k: row.get(k) for k in ENTRY_KEYS})

        elapsed = time.time() - t
        print(f"  стр.{pg}: {len(rows)} записей · итого {len(entries)} · {elapsed:.1f}с")

        if len(rows) < BATCH:
            break
        skip += BATCH

    with open(F_VP, "w", encoding="utf-8") as fh:
        json.dump({
            "period": f"{mn.title()} {year}",
            "count": len(entries),
            "entries": entries,
        }, fh, ensure_ascii=False)
    print(f"  ✓ {len(entries)} записей за {time.time() - t:.1f}с → {F_VP}")


# ═══════════════════════════════════════════════════════
# 2. СЕБЕСТОИМОСТЬ (СтоимостьТоваров)
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 2 · СтоимостьТоваров")

cached = load_cache(F_COST)
if cached is not None and isinstance(cached, dict):
    cost_lookup = cached
    print(f"  кэш: {len(cost_lookup)} ключей")
else:
    t = time.time()
    flt = quote(
        f"Date ge datetime'{P_START}' and Date lt datetime'{P_END}' and Posted eq true",
        safe=""
    )
    url = (
        f"{BASE}/Document_РасчетСебестоимостиТоваров"
        f"?$format=json&$top=50&$filter={flt}"
        f"&$select=Ref_Key,Date,ПредварительныйРасчет"
    )
    resp = session.get(url, timeout=60)
    docs = resp.json().get("value", []) if resp.ok else []

    if not docs:
        flt2 = quote("Posted eq true", safe="")
        url2 = (
            f"{BASE}/Document_РасчетСебестоимостиТоваров"
            f"?$format=json&$top=50&$filter={flt2}&$orderby=Date desc"
        )
        resp2 = session.get(url2, timeout=60)
        if resp2.ok:
            docs = [
                d for d in resp2.json().get("value", [])
                if d.get("Date", "")[:7] == f"{year}-{month:02d}"
            ]

    print(f"  документов: {len(docs)}")
    for d in docs:
        print(f"    {d['Ref_Key'][:8]}.. Предварит={d.get('ПредварительныйРасчет')}")

    all_recs = []
    for i, doc in enumerate(docs, 1):
        recorder_type = quote(
            "StandardODATA.Document_РасчетСебестоимостиТоваров", safe=""
        )
        url = (
            f"{BASE}/InformationRegister_СтоимостьТоваров"
            f"(Recorder='{doc['Ref_Key']}',Recorder_Type='{recorder_type}')"
            f"?$format=json"
        )
        try:
            resp = session.get(url, timeout=120)
            if resp.ok:
                rs = resp.json().get("RecordSet", [])
                all_recs.extend(rs)
                print(f"  [{i}/{len(docs)}] {len(rs)} записей")
        except Exception as ex:
            print(f"  [{i}/{len(docs)}] ошибка: {ex}")

    cost_lookup = {}
    for rec in all_recs:
        key = (
            f"{rec.get('АналитикаУчетаНоменклатуры_Key')}"
            f"|{rec.get('ВидЗапасов_Key')}"
        )
        entry = {}
        has_value = False
        for field in COST_KEYS:
            v = rec.get(field, 0) or 0
            entry[field] = v
            if v != 0:
                has_value = True
        if has_value:
            existing = cost_lookup.get(key)
            if not existing or entry.get("Стоимость", 0) > existing.get("Стоимость", 0):
                cost_lookup[key] = entry

    with open(F_COST, "w", encoding="utf-8") as fh:
        json.dump(cost_lookup, fh, ensure_ascii=False)
    print(f"  ✓ {len(cost_lookup)} ключей за {time.time() - t:.0f}с")


# ═══════════════════════════════════════════════════════
# 3. МАППИНГ ОРГАНИЗАЦИЙ  — батчевый (20 GUID/запрос)
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 3 · Маппинг организаций")

dept_entries = [e for e in entries if e.get("Подразделение_Key") in DEPARTMENTS]
needed_keys = {
    e.get("АналитикаУчетаПоПартнерам_Key")
    for e in dept_entries
    if e.get("АналитикаУчетаПоПартнерам_Key")
}

org_map = {}
cached = load_cache(F_ORG)
if cached is not None and isinstance(cached, dict):
    org_map = cached
    missing = needed_keys - set(org_map)
else:
    missing = needed_keys

if missing:
    t = time.time()
    print(f"  всего: {len(needed_keys)}, к загрузке: {len(missing)}")
    todo = list(missing)
    ORG_BATCH = 20

    for i in range(0, len(todo), ORG_BATCH):
        batch = todo[i : i + ORG_BATCH]
        flt = quote(
            " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
            safe=""
        )
        url = (
            f"{BASE}/Catalog_КлючиАналитикиУчетаПоПартнерам"
            f"?$format=json&$select=Ref_Key,Организация_Key"
            f"&$top={ORG_BATCH}&$filter={flt}"
        )
        resp = session.get(url, timeout=30)
        if resp.ok:
            for item in resp.json().get("value", []):
                org_map[item["Ref_Key"]] = item.get("Организация_Key", "")
        for k in batch:
            org_map.setdefault(k, "")

        done = min(i + ORG_BATCH, len(todo))
        if done % 100 < ORG_BATCH or done >= len(todo):
            print(f"  {done}/{len(todo)}")

    with open(F_ORG, "w", encoding="utf-8") as fh:
        json.dump(org_map, fh, ensure_ascii=False)
    print(f"  ✓ {len(org_map)} ключей за {time.time() - t:.0f}с")
else:
    print(f"  кэш: {len(org_map)} ключей")


# ═══════════════════════════════════════════════════════
# 4. КЛЮЧИ «НАШЕ ПРЕДПРИЯТИЕ»  — кэш, не зависит от месяца
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 4 · Наше предприятие")

nashe_keys = set()
cached = load_cache(F_NASHE)
if cached is not None and isinstance(cached, list):
    nashe_keys = set(cached)
    print(f"  кэш: {len(nashe_keys)} ключей")
else:
    flt = quote(f"Партнер_Key eq guid'{NASHE}'", safe="")
    sk = 0
    while True:
        url = (
            f"{BASE}/Catalog_КлючиАналитикиУчетаПоПартнерам"
            f"?$format=json&$filter={flt}&$select=Ref_Key&$top=500&$skip={sk}"
        )
        resp = session.get(url, timeout=30)
        if not resp.ok:
            break
        items = resp.json().get("value", [])
        if not items:
            break
        nashe_keys |= {it["Ref_Key"] for it in items}
        if len(items) < 500:
            break
        sk += 500

    with open(F_NASHE, "w", encoding="utf-8") as fh:
        json.dump(list(nashe_keys), fh, ensure_ascii=False)
    print(f"  загружено: {len(nashe_keys)} ключей")


# ═══════════════════════════════════════════════════════
# 5. РАСЧЁТ ВАЛОВОЙ ПРИБЫЛИ
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 5 · Расчёт")

filtered = [e for e in entries if e.get("Подразделение_Key") in DEPARTMENTS]
print(f"  отделы:  {len(entries)} → {len(filtered)}")

before = len(filtered)
filtered = [
    e for e in filtered
    if e.get("АналитикаУчетаПоПартнерам_Key") not in nashe_keys
]
print(f"  −наше:   −{before - len(filtered)}, осталось {len(filtered)}")

tot = dict(vp=0, vyr=0, seb=0, st=0, dop=0, trud=0, pp=0, pv=0, rash=0)
cnt = 0
skip_k = 0
prelim_cnt = 0

for e in filtered:
    if e.get("ТипЗапасов") == "КомиссионныйТовар":
        skip_k += 1
        continue

    vyr = e.get("СуммаВыручки", 0) or 0
    st = e.get("Стоимость", 0) or 0
    dop = e.get("ДопРасходы", 0) or 0
    trd = e.get("Трудозатраты", 0) or 0
    pp = e.get("ПостатейныеПостоянныеСНДС", 0) or 0
    pv = e.get("ПостатейныеПеременныеСНДС", 0) or 0
    rp = e.get("РасходыНаПродажуСНДС", 0) or 0
    kol = e.get("Количество", 0) or 0

    ak = e.get("АналитикаУчетаПоПартнерам_Key", "")
    if st == 0 and org_map.get(ak, "") in PRELIM_ORGS and cost_lookup:
        ck = (
            f"{e.get('АналитикаУчетаНоменклатуры_Key')}"
            f"|{e.get('ВидЗапасов_Key')}"
        )
        uc = cost_lookup.get(ck)
        if uc:
            st = round(kol * (uc.get("Стоимость", 0) or 0), 2)
            dop = round(kol * (uc.get("СтоимостьДопРасходы", 0) or 0), 2)
            trd = round(kol * (uc.get("Трудозатраты", 0) or 0), 2)
            pp = round(kol * (uc.get("ПостатейныеПостоянныеСНДС", 0) or 0), 2)
            pv = round(kol * (uc.get("ПостатейныеПеременныеСНДС", 0) or 0), 2)
            prelim_cnt += 1

    seb = st + dop + trd + pp + pv + rp
    tot["vp"] += vyr - seb
    tot["vyr"] += vyr
    tot["seb"] += seb
    tot["st"] += st
    tot["dop"] += dop
    tot["trud"] += trd
    tot["pp"] += pp
    tot["pv"] += pv
    tot["rash"] += rp
    cnt += 1

elapsed = time.time() - T0
rent = (tot["vp"] / tot["vyr"] * 100) if tot["vyr"] else 0

print(f"\n  Записей: {cnt}  |  Комиссия: {skip_k}  |  Предварит.: {prelim_cnt}")
print(f"\n{'═' * 55}")
print(f"  Выручка:                {tot['vyr']:>18,.2f}")
print(f"  ─── Себестоимость ───")
print(f"  Стоимость:              {tot['st']:>18,.2f}")
print(f"  ДопРасходы:             {tot['dop']:>18,.2f}")
print(f"  Трудозатраты:           {tot['trud']:>18,.2f}")
print(f"  ПостатейныеПостоянные:  {tot['pp']:>18,.2f}")
print(f"  ПостатейныеПеременные:  {tot['pv']:>18,.2f}")
print(f"  РасходыНаПродажу:       {tot['rash']:>18,.2f}")
print(f"  ИТОГО себестоимость:    {tot['seb']:>18,.2f}")
print(f"{'═' * 55}")
print(f"  ВАЛОВАЯ ПРИБЫЛЬ:        {tot['vp']:>18,.2f}")
print(f"  Рентабельность:         {rent:>17.2f}%")
print(f"{'═' * 55}")
print(f"  Время выполнения: {elapsed:.1f}с")
