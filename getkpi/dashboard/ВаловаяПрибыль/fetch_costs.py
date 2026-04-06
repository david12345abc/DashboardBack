"""
Загрузка стоимостей из InformationRegister_СтоимостьТоваров
для документов РасчетСебестоимостиТоваров за выбранный месяц.
Сохраняет ВСЕ компоненты себестоимости.
"""
import requests, sys, functools, json
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

COST_FIELDS = [
    "АналитикаУчетаНоменклатуры_Key", "ВидЗапасов_Key",
    "Стоимость", "СтоимостьДопРасходы", "Трудозатраты",
    "ПостатейныеПостоянныеСНДС", "ПостатейныеПеременныеСНДС",
]

if len(sys.argv) == 3:
    month, year = int(sys.argv[1]), int(sys.argv[2])
else:
    month = int(input("Месяц (1-12): "))
    year = int(input("Год: "))

month_name = MONTH_NAMES[month]
prefix = f"стоимость_товаров_{month_name.lower()}_{year}"
OUT_FILE = f"{prefix}.json"

if month == 12:
    period_start = f"{year}-12-01"
    period_end = f"{year+1}-01-01"
else:
    period_start = f"{year}-{month:02d}-01"
    period_end = f"{year}-{month+1:02d}-01"

print(f"Период: {month_name} {year}")
print(f"Ищем Document_РасчетСебестоимостиТоваров за {period_start}..{period_end}")

# 1. Найти документы расчёта себестоимости за период
session = requests.Session()
session.auth = AUTH

flt = quote(f"Date ge datetime'{period_start}T00:00:00' and Date lt datetime'{period_end}T00:00:00' and Posted eq true", safe="")
url = (f"{BASE}/Document_РасчетСебестоимостиТоваров"
       f"?$format=json&$top=50&$filter={flt}"
       f"&$select=Ref_Key,Date,Number,Posted,ПредварительныйРасчет,ТипЗаписи")
r = session.get(url, timeout=60)
docs = r.json().get("value", []) if r.status_code == 200 else []

# Также проверим документы на конец месяца (дата=последний день)
if not docs:
    flt2 = quote(f"Posted eq true", safe="")
    url2 = f"{BASE}/Document_РасчетСебестоимостиТоваров?$format=json&$top=50&$filter={flt2}&$orderby=Date desc"
    r2 = session.get(url2, timeout=60)
    if r2.status_code == 200:
        all_docs = r2.json().get("value", [])
        docs = [d for d in all_docs if d.get("Date", "")[:7] == f"{year}-{month:02d}"]

print(f"Документов расчёта: {len(docs)}")
for d in docs:
    print(f"  {d['Ref_Key']}: Предварит={d.get('ПредварительныйРасчет')}, Дата={d.get('Date')}")

# 2. Загрузить записи из СтоимостьТоваров по каждому документу
all_records = []
for i, doc in enumerate(docs, 1):
    guid = doc["Ref_Key"]
    recorder_type = quote("StandardODATA.Document_РасчетСебестоимостиТоваров", safe="")
    url = (f"{BASE}/InformationRegister_СтоимостьТоваров"
           f"(Recorder='{guid}',Recorder_Type='{recorder_type}')"
           f"?$format=json")
    try:
        r = session.get(url, timeout=120)
        if r.status_code == 200:
            record_set = r.json().get("RecordSet", [])
            print(f"  [{i}/{len(docs)}] {guid}: {len(record_set)} записей")
            all_records.extend(record_set)
        elif r.status_code == 404:
            print(f"  [{i}/{len(docs)}] {guid}: не найден")
        else:
            print(f"  [{i}/{len(docs)}] {guid}: {r.status_code}")
    except Exception as ex:
        print(f"  [{i}/{len(docs)}] {guid}: ошибка {ex}")

print(f"\nИтого записей: {len(all_records)}")

# 3. Построить lookup: (АналитикаУчетаНоменклатуры_Key, ВидЗапасов_Key) → {Стоимость, ДопРасходы, ...}
lookup = {}
for rec in all_records:
    key = f"{rec.get('АналитикаУчетаНоменклатуры_Key')}|{rec.get('ВидЗапасов_Key')}"
    entry = {}
    has_value = False
    for field in COST_FIELDS[2:]:
        v = rec.get(field, 0) or 0
        entry[field] = v
        if v != 0:
            has_value = True
    if has_value:
        if key not in lookup:
            lookup[key] = entry
        else:
            existing = lookup[key]
            if entry.get("Стоимость", 0) > existing.get("Стоимость", 0):
                lookup[key] = entry

print(f"Уникальных ключей с ненулевыми данными: {len(lookup)}")

# 4. Сохранить
with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(lookup, f, ensure_ascii=False)
print(f"Сохранено в {OUT_FILE}")

# Статистика
total_st = sum(v.get("Стоимость", 0) for v in lookup.values())
total_dop = sum(v.get("СтоимостьДопРасходы", 0) for v in lookup.values())
total_trud = sum(v.get("Трудозатраты", 0) for v in lookup.values())
total_post = sum(v.get("ПостатейныеПостоянныеСНДС", 0) for v in lookup.values())
total_per = sum(v.get("ПостатейныеПеременныеСНДС", 0) for v in lookup.values())
print(f"\n  Итого Стоимость: {total_st:,.2f}")
print(f"  Итого ДопРасходы: {total_dop:,.2f}")
print(f"  Итого Трудозатраты: {total_trud:,.2f}")
print(f"  Итого ПостатейныеПостоянные: {total_post:,.2f}")
print(f"  Итого ПостатейныеПеременные: {total_per:,.2f}")
