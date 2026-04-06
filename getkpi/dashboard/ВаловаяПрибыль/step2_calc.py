"""
Шаг 2: Расчёт Валовой Прибыли из кэша регистра ВыручкаИСебестоимостьПродаж.

Логика (ДанныеОтчета=1):
  ВП = СуммаВыручки - Себестоимость

  Себестоимость для каждой записи:
    Если организация с предварительным расчётом И Стоимость=0:
      Все компоненты = Количество × unit_cost из СтоимостьТоваров
    Иначе: берём из регистра как есть

  КомиссионныйТовар → ВП = 0

Фильтры:
  - Подразделение в списке отделов продаж
  - Партнёр ≠ НашеПредприятие
"""
import requests, sys, functools, json, os
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

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

NASHE_PREDPRIYATIE = "5fc811cd-6251-11e7-812d-001e67112509"

PRELIM_ORGS = {
    "171272c0-ef41-11e9-829c-ac1f6b05524d",
    "fbca2148-6cfd-11e7-812d-001e67112509",
    "fbca2145-6cfd-11e7-812d-001e67112509",
    "fbca2143-6cfd-11e7-812d-001e67112509",
}

COST_COMPONENT_MAP = {
    "Стоимость": "Стоимость",
    "ДопРасходы": "СтоимостьДопРасходы",
    "Трудозатраты": "Трудозатраты",
    "ПостатейныеПостоянныеСНДС": "ПостатейныеПостоянныеСНДС",
    "ПостатейныеПеременныеСНДС": "ПостатейныеПеременныеСНДС",
}

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

if len(sys.argv) == 3:
    month, year = int(sys.argv[1]), int(sys.argv[2])
else:
    print("=== Выбор периода ===")
    while True:
        try:
            month = int(input("Месяц (1-12): "))
            if 1 <= month <= 12:
                break
            print("  Введите число от 1 до 12")
        except ValueError:
            print("  Введите число от 1 до 12")
    while True:
        try:
            year = int(input("Год (например 2026): "))
            if 2000 <= year <= 2100:
                break
            print("  Введите корректный год")
        except ValueError:
            print("  Введите корректный год")

month_name = MONTH_NAMES[month]
prefix = f"вп_{month_name.lower()}_{year}"
CACHE = f"{prefix}_cache.json"
COST_FILE = f"стоимость_товаров_{month_name.lower()}_{year}.json"
ORG_MAP_FILE = f"аналитика_орг_{month_name.lower()}_{year}.json"

print(f"\nПериод: {month_name} {year}")
print(f"Кэш: {CACHE}\n")

with open(CACHE, "r", encoding="utf-8") as f:
    data = json.load(f)
entries = data["entries"]
print(f"Записей в кэше: {len(entries)}")

# --- Загрузка стоимостей (все компоненты) ---
cost_lookup = {}
if os.path.exists(COST_FILE):
    print(f"Загрузка стоимостей из {COST_FILE}...")
    with open(COST_FILE, "r", encoding="utf-8") as f:
        cost_lookup = json.load(f)
    print(f"  Ключей: {len(cost_lookup)}")

# --- Загрузка маппинга организаций ---
org_map = {}
if os.path.exists(ORG_MAP_FILE):
    print(f"Загрузка орг. маппинга из {ORG_MAP_FILE}...")
    with open(ORG_MAP_FILE, "r", encoding="utf-8") as f:
        org_map = json.load(f)
    print(f"  Ключей: {len(org_map)}")

# 1. Фильтр по подразделениям
filtered = [e for e in entries if e.get("Подразделение_Key") in DEPARTMENTS]
print(f"\n1. Фильтр по подразделениям: {len(entries)} → {len(filtered)}")

# 2. Исключение НашеПредприятие
print("\n2. Исключение 'Наше предприятие'...")
nashe_keys = set()
flt = quote(f"Партнер_Key eq guid'{NASHE_PREDPRIYATIE}'", safe="")
skip_val = 0
while True:
    url = (f"{BASE}/Catalog_КлючиАналитикиУчетаПоПартнерам"
           f"?$format=json&$filter={flt}&$select=Ref_Key&$top=500&$skip={skip_val}")
    r = requests.get(url, auth=AUTH, timeout=30)
    if r.status_code != 200:
        break
    items = r.json().get("value", [])
    if not items:
        break
    for it in items:
        nashe_keys.add(it["Ref_Key"])
    if len(items) < 500:
        break
    skip_val += 500
print(f"  КлючейАналитики: {len(nashe_keys)}")

before = len(filtered)
filtered = [e for e in filtered if e.get("АналитикаУчетаПоПартнерам_Key") not in nashe_keys]
print(f"  Исключено: {before - len(filtered)}, осталось: {len(filtered)}")

# 3. Расчёт
print("\n3. Расчёт Валовой Прибыли...")
total_vp = 0.0
total_vyruchka = 0.0
total_sebest = 0.0
total_stoimost = 0.0
total_dop = 0.0
total_trud = 0.0
total_post_post = 0.0
total_post_per = 0.0
total_rash = 0.0
count = 0
skip_komission = 0
prelim_count = 0

for e in filtered:
    if e.get("ТипЗапасов") == "КомиссионныйТовар":
        skip_komission += 1
        continue

    vyruchka = e.get("СуммаВыручки", 0) or 0
    stoimost = e.get("Стоимость", 0) or 0
    dop = e.get("ДопРасходы", 0) or 0
    trud = e.get("Трудозатраты", 0) or 0
    post_post = e.get("ПостатейныеПостоянныеСНДС", 0) or 0
    post_per = e.get("ПостатейныеПеременныеСНДС", 0) or 0
    rash_prod = e.get("РасходыНаПродажуСНДС", 0) or 0
    kol = e.get("Количество", 0) or 0

    analitika_key = e.get("АналитикаУчетаПоПартнерам_Key", "")
    org_key = org_map.get(analitika_key, "")
    is_prelim = org_key in PRELIM_ORGS

    if stoimost == 0 and is_prelim and cost_lookup:
        cost_key = f"{e.get('АналитикаУчетаНоменклатуры_Key')}|{e.get('ВидЗапасов_Key')}"
        unit_costs = cost_lookup.get(cost_key)
        if unit_costs:
            stoimost = round(kol * (unit_costs.get("Стоимость", 0) or 0), 2)
            dop = round(kol * (unit_costs.get("СтоимостьДопРасходы", 0) or 0), 2)
            trud = round(kol * (unit_costs.get("Трудозатраты", 0) or 0), 2)
            post_post = round(kol * (unit_costs.get("ПостатейныеПостоянныеСНДС", 0) or 0), 2)
            post_per = round(kol * (unit_costs.get("ПостатейныеПеременныеСНДС", 0) or 0), 2)
            prelim_count += 1

    sebest = stoimost + dop + trud + post_post + post_per + rash_prod
    vp = vyruchka - sebest

    total_vp += vp
    total_vyruchka += vyruchka
    total_sebest += sebest
    total_stoimost += stoimost
    total_dop += dop
    total_trud += trud
    total_post_post += post_post
    total_post_per += post_per
    total_rash += rash_prod
    count += 1

print(f"  Записей: {count}")
print(f"  КомиссионныйТовар: {skip_komission}")
print(f"  Предв. стоимость подставлена: {prelim_count}")
print(f"\n{'='*50}")
print(f"  Выручка:                {total_vyruchka:>18,.2f}")
print(f"  ─── Себестоимость ───")
print(f"  Стоимость:              {total_stoimost:>18,.2f}")
print(f"  ДопРасходы:             {total_dop:>18,.2f}")
print(f"  Трудозатраты:           {total_trud:>18,.2f}")
print(f"  ПостатейныеПостоянные:  {total_post_post:>18,.2f}")
print(f"  ПостатейныеПеременные:  {total_post_per:>18,.2f}")
print(f"  РасходыНаПродажу:       {total_rash:>18,.2f}")
print(f"  ИТОГО себестоимость:    {total_sebest:>18,.2f}")
print(f"{'='*50}")
print(f"  ВАЛОВАЯ ПРИБЫЛЬ:        {total_vp:>18,.2f}")
rent = (total_vp / total_vyruchka * 100) if total_vyruchka else 0
print(f"  Рентабельность:         {rent:>17.2f}%")
