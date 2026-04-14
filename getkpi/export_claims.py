"""
export_claims.py — Выгрузка претензий (Catalog_Претензии) через OData.

Использование:
  python export_claims.py [ГГГГ-ММ]   # по умолчанию 2026-03

Фильтры:
  - Только за указанный месяц (по ДатаРегистрации)
  - Статус ≠ Удовлетворена
  - Подразделение заказа клиента — одно из 6 целевых

Результат: CSV-файл претензии_<период>.csv
"""
import requests, sys, csv, time, os, calendar
from datetime import datetime
from requests.auth import HTTPBasicAuth

sys.stdout.reconfigure(encoding="utf-8")

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ALLOWED_DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",  # Отдел ВЭД
    "34497ef7-810f-11e4-80d6-001e67112509",  # Отдел продаж эталонного оборудования и услуг
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",  # Отдел продаж БМИ
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",  # Отдел по работе с ключевыми клиентами
    "7587c178-92f6-11f0-96f9-6cb31113810e",  # Отдел дилерских продаж
    "bd7b5184-9f9c-11e4-80da-001e67112509",  # Отдел по работе с ПАО Газпром
}

# ── Параметр: месяц ──
period_arg = None
for a in sys.argv[1:]:
    if len(a) == 7 and a[4] == "-":
        period_arg = a
        break

if period_arg:
    year, month = int(period_arg[:4]), int(period_arg[5:7])
else:
    year, month = 2026, 3

last_day = calendar.monthrange(year, month)[1]
date_from = f"{year}-{month:02d}-01T00:00:00"
date_to = f"{year}-{month:02d}-{last_day}T23:59:59"
month_label = f"{year}-{month:02d}"
print(f"Период: {month_label}")

session = requests.Session()
session.auth = AUTH

t0 = time.time()

# ── 1. Загрузка претензий ──
print("▸ Загрузка Catalog_Претензии...")
select_claims = (
    "Ref_Key,Code,Description,Партнер_Key,ДатаРегистрации,ДатаОкончания,"
    "ТД_ДатаОкончанияПлан,ТД_ЗаказКлиента_Key,ТД_Номенклатура_Key,"
    "ТД_Характеристика_Key,ОписаниеПретензии,DeletionMark,Статус"
)
claims = []
skip = 0
while True:
    odata_filter = (
        f"ДатаРегистрации ge datetime'{date_from}'"
        f" and ДатаРегистрации le datetime'{date_to}'"
        f" and Статус ne 'Удовлетворена'"
    )
    url = (
        f"{BASE}/Catalog_Претензии?$format=json"
        f"&$select={select_claims}&$top=5000&$skip={skip}"
        f"&$filter={odata_filter}"
    )
    r = session.get(url, timeout=120)
    if not r.ok:
        print(f"  Ошибка HTTP {r.status_code}: {r.text[:300]}")
        break
    rows = r.json().get("value", [])
    claims.extend(rows)
    if len(rows) < 5000:
        break
    skip += 5000

print(f"  Загружено: {len(claims)} претензий · {time.time()-t0:.1f}с")

# ── 2. Справочник партнёров ──
print("▸ Загрузка партнёров...")
partners = {}
p_skip = 0
while True:
    url = f"{BASE}/Catalog_Партнеры?$format=json&$top=5000&$skip={p_skip}&$select=Ref_Key,Description"
    r = session.get(url, timeout=60)
    if not r.ok:
        break
    rows = r.json().get("value", [])
    for p in rows:
        partners[p["Ref_Key"]] = p.get("Description", "").strip()
    if len(rows) < 5000:
        break
    p_skip += 5000
print(f"  Партнёров: {len(partners)}")

# ── 3. Справочник подразделений ──
print("▸ Загрузка подразделений...")
depts = {}
r = session.get(f"{BASE}/Catalog_СтруктураПредприятия?$format=json&$top=5000&$select=Ref_Key,Description", timeout=30)
if r.ok:
    for d in r.json().get("value", []):
        depts[d["Ref_Key"]] = d.get("Description", "").strip()
print(f"  Подразделений: {len(depts)}")

# ── 4. Заказы клиентов (для подразделения и суммы) ──
print("▸ Загрузка заказов (подразделение + сумма)...")
order_keys_needed = set()
for c in claims:
    ok = c.get("ТД_ЗаказКлиента_Key")
    if ok and ok != EMPTY:
        order_keys_needed.add(ok)
print(f"  Уникальных заказов в претензиях: {len(order_keys_needed)}")

orders_info = {}
o_skip = 0
o_select = "Ref_Key,Number,Date,Подразделение_Key,СуммаДокумента"
while True:
    url = f"{BASE}/Document_ЗаказКлиента?$format=json&$top=500&$skip={o_skip}&$select={o_select}"
    r = session.get(url, timeout=120)
    if not r.ok:
        break
    chunk = r.json().get("value", [])
    if not chunk:
        break
    for item in chunk:
        rk = item["Ref_Key"]
        if rk in order_keys_needed:
            orders_info[rk] = item
    o_skip += len(chunk)
    if len(orders_info) >= len(order_keys_needed):
        break
print(f"  Найдено заказов: {len(orders_info)} из {len(order_keys_needed)} · {time.time()-t0:.1f}с")

# ── 5. Номенклатура / Характеристика — пробуем загрузить ──
nom_names = {}
char_names = {}
for cat_name, target in [
    ("Catalog_Номенклатура", nom_names),
    ("Catalog_ХарактеристикиНоменклатуры", char_names),
]:
    r = session.get(f"{BASE}/{cat_name}?$format=json&$top=1", timeout=10)
    if r.ok:
        print(f"▸ Загрузка {cat_name}...")
        c_skip = 0
        while True:
            url = f"{BASE}/{cat_name}?$format=json&$top=5000&$skip={c_skip}&$select=Ref_Key,Description"
            r2 = session.get(url, timeout=60)
            if not r2.ok:
                break
            rows = r2.json().get("value", [])
            for item in rows:
                target[item["Ref_Key"]] = item.get("Description", "").strip()
            if len(rows) < 5000:
                break
            c_skip += 5000
        print(f"  Записей: {len(target)}")
    else:
        print(f"  {cat_name}: не опубликован (HTTP {r.status_code}), будут GUID")

# ── 6. Формируем CSV (только целевые подразделения) ──
out_name = f"претензии_{month_label}.csv"
out_path = os.path.join(os.path.dirname(__file__), out_name)

print(f"\n▸ Запись в {out_name}...")
written = 0
skipped_dept = 0
skipped_no_order = 0

with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f, delimiter=";")
    w.writerow([
        "Код",
        "Наименование",
        "Партнер (Клиент)",
        "Дата обращения",
        "Дата окончания (план)",
        "Дата окончания",
        "Заказ клиента",
        "Подразделение заказа",
        "Номенклатура",
        "Характеристика",
        "Сумма документа заказа",
        "Описание претензии",
        "Статус",
    ])

    for c in claims:
        order_key = c.get("ТД_ЗаказКлиента_Key", "")
        order = orders_info.get(order_key)

        if order:
            order_dept_key = order.get("Подразделение_Key", "")
        else:
            skipped_no_order += 1
            continue

        if order_dept_key not in ALLOWED_DEPARTMENTS:
            skipped_dept += 1
            continue

        partner = partners.get(c.get("Партнер_Key", ""), c.get("Партнер_Key", ""))
        date_reg = (c.get("ДатаРегистрации") or "")[:10]
        date_plan = (c.get("ТД_ДатаОкончанияПлан") or "")[:10]
        date_end = (c.get("ДатаОкончания") or "")[:10]

        order_num = order.get("Number", "").strip()
        order_dept = depts.get(order_dept_key, order_dept_key)
        order_sum = order.get("СуммаДокумента", 0)

        nom_key = c.get("ТД_Номенклатура_Key", "")
        nom = nom_names.get(nom_key, nom_key if nom_key and nom_key != EMPTY else "")

        char_key = c.get("ТД_Характеристика_Key", "")
        char = char_names.get(char_key, char_key if char_key and char_key != EMPTY else "")

        desc = (c.get("ОписаниеПретензии") or "").replace("\r\n", " ").replace("\n", " ")
        status = c.get("Статус", "")

        w.writerow([
            c.get("Code", ""),
            c.get("Description", ""),
            partner,
            date_reg,
            date_plan,
            date_end,
            order_num,
            order_dept,
            nom,
            char,
            order_sum,
            desc,
            status,
        ])
        written += 1

print(f"✓ Готово: {written} строк · {out_path}")
print(f"  Пропущено: без заказа — {skipped_no_order}, подразделение вне списка — {skipped_dept}")
print(f"  Всего претензий (до фильтра подразделений): {len(claims)}")
print(f"  Время: {time.time()-t0:.1f}с")
