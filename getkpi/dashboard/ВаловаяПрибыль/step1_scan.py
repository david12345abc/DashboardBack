"""
Шаг 1: Скан регистра ВыручкаИСебестоимостьПродаж за выбранный месяц.
Сохраняет кэш с нужными полями для расчёта Валовой Прибыли.
"""
import requests, sys, time, json, functools, os
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

BATCH = 1000
WORKERS = 3
ESTIMATED_TOTAL = 56000

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

FIELDS_TO_SAVE = [
    "Period", "Active", "Подразделение_Key", "АналитикаУчетаПоПартнерам_Key",
    "АналитикаУчетаНоменклатуры_Key", "ВидЗапасов_Key",
    "ТипЗапасов", "Количество",
    "СуммаВыручки", "Стоимость", "ДопРасходы", "Трудозатраты",
    "ПостатейныеПостоянныеСНДС", "ПостатейныеПеременныеСНДС", "РасходыНаПродажуСНДС",
]

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

if month == 12:
    PERIOD_START = f"{year}-12-01"
    PERIOD_END = f"{year + 1}-01-01"
else:
    PERIOD_START = f"{year}-{month:02d}-01"
    PERIOD_END = f"{year}-{month + 1:02d}-01"

month_name = MONTH_NAMES[month]
prefix = f"вп_{month_name.lower()}_{year}"
CACHE = f"{prefix}_cache.json"

print(f"\nПериод: {month_name} {year} ({PERIOD_START} .. {PERIOD_END})")
print(f"Кэш: {CACHE}\n")

if os.path.exists(CACHE):
    print("Загрузка из кэша...")
    with open(CACHE, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"Записей в кэше: {data['count']}")
else:
    print(f"Полный скан регистра ВыручкаИСебестоимостьПродаж (batch={BATCH}, workers={WORKERS})...")
    t0 = time.time()

    session = requests.Session()
    session.auth = AUTH

    def fetch_page(skip):
        url = f"{BASE}/AccumulationRegister_ВыручкаИСебестоимостьПродаж?$format=json&$top={BATCH}&$skip={skip}"
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        rows = resp.json().get("value", [])
        page_entries = []
        for row in rows:
            for rec in row.get("RecordSet", []):
                p = rec.get("Period", "")
                if p >= PERIOD_START and p < PERIOD_END and rec.get("Active", True):
                    entry = {k: rec.get(k) for k in FIELDS_TO_SAVE}
                    page_entries.append(entry)
        return skip, len(rows), page_entries

    skips = list(range(0, ESTIMATED_TOTAL, BATCH))
    entries = []
    total = 0
    done = 0
    max_nonempty_skip = -1

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_page, s): s for s in skips}
        for f in as_completed(futures):
            skip_val = futures[f]
            s, row_count, page_entries = f.result()
            total += row_count
            entries.extend(page_entries)
            if row_count > 0:
                max_nonempty_skip = max(max_nonempty_skip, skip_val)
            done += 1
            print(f"  [{done}/{len(skips)}] skip={s:>6} → {row_count} записей | {month_name}: {len(entries)} строк | {time.time()-t0:.0f}с")

    if max_nonempty_skip >= ESTIMATED_TOTAL - BATCH:
        print("  Дозагрузка оставшихся страниц...")
        extra_skip = ESTIMATED_TOTAL
        while True:
            s, row_count, page_entries = fetch_page(extra_skip)
            if row_count == 0:
                break
            total += row_count
            entries.extend(page_entries)
            print(f"  [доп] skip={extra_skip} → {row_count} записей | {month_name}: {len(entries)} строк")
            extra_skip += BATCH

    data = {"period": f"{month_name} {year}", "count": len(entries), "entries": entries}
    with open(CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"\n  Итого записей регистра: {total}")
    print(f"  За {month_name} {year}: {len(entries)}")
    print(f"  Время: {time.time()-t0:.0f}с")
    print(f"  Сохранено в {CACHE}")
