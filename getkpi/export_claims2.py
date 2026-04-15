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
from urllib.parse import quote

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


def load_catalog_full(entity, select_fields, label):
    """Загрузка справочника целиком с пагинацией."""
    result = {}
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$top={PAGE}&$skip={skip}"
            f"&$select={quote(select_fields, safe=',_')}"
            f"&$orderby=Ref_Key"
        )
        r = session.get(url, timeout=120)
        if not r.ok:
            print(f"  {label}: ошибка HTTP {r.status_code}")
            break
        rows = r.json().get("value", [])
        if not rows:
            break
        for item in rows:
            result[item["Ref_Key"]] = item
        if len(rows) < PAGE:
            break
        skip += len(rows)
    return result


def fetch_single(entity, guid, select_fields):
    """Точечная загрузка одной записи по GUID."""
    url = f"{BASE}/{quote(entity)}(guid'{guid}')?$format=json&$select={quote(select_fields, safe=',_')}"
    r = session.get(url, timeout=15)
    if r.ok:
        return r.json()
    return None


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
raw = load_catalog_full("Catalog_Партнеры", "Ref_Key,Description", "Партнёры")
partners = {k: v.get("Description", "").strip() for k, v in raw.items()}
print(f"  Партнёров: {len(partners)}")

# Дозагрузка отсутствующих партнёров поштучно
needed_partner_keys = {c.get("Партнер_Key") for c in claims
                       if c.get("Партнер_Key") and c.get("Партнер_Key") != EMPTY}
missing_p = needed_partner_keys - set(partners.keys())
if missing_p:
    print(f"  Дозагрузка {len(missing_p)} отсутствующих партнёров...")
    for pk in missing_p:
        item = fetch_single("Catalog_Партнеры", pk, "Ref_Key,Description")
        if item:
            partners[pk] = (item.get("Description") or "").strip()

# ── 3. Справочник подразделений ──
print("▸ Загрузка подразделений...")
depts = {}
r = session.get(
    f"{BASE}/{quote('Catalog_СтруктураПредприятия')}?$format=json&$top=5000"
    f"&$select=Ref_Key,Description&$orderby=Ref_Key",
    timeout=30,
)
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
    url = (
        f"{BASE}/{quote('Document_ЗаказКлиента')}?$format=json"
        f"&$top=500&$skip={o_skip}&$select={o_select}"
    )
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

# ── 5. Номенклатура: рабочее наименование (Description) + код (Code) ──
print("▸ Загрузка Catalog_Номенклатура (Description + Code)...")
raw_nom = load_catalog_full("Catalog_Номенклатура", "Ref_Key,Description,Code", "Номенклатура")
nom_names = {}
for k, v in raw_nom.items():
    desc = (v.get("Description") or "").strip()
    code = (v.get("Code") or "").strip()
    nom_names[k] = f"{desc} ({code})" if desc and code else desc or code
print(f"  Номенклатура: {len(nom_names)}")

# Дозагрузка отсутствующей номенклатуры поштучно
needed_nom_keys = {c.get("ТД_Номенклатура_Key") for c in claims
                   if c.get("ТД_Номенклатура_Key") and c.get("ТД_Номенклатура_Key") != EMPTY}
missing_n = needed_nom_keys - set(nom_names.keys())
if missing_n:
    print(f"  Дозагрузка {len(missing_n)} отсутствующих номенклатур...")
    for nk in missing_n:
        item = fetch_single("Catalog_Номенклатура", nk, "Ref_Key,Description,Code")
        if item:
            desc = (item.get("Description") or "").strip()
            code = (item.get("Code") or "").strip()
            nom_names[nk] = f"{desc} ({code})" if desc and code else desc or code

# ── 5b. Характеристики номенклатуры ──
char_names = {}
r = session.get(f"{BASE}/{quote('Catalog_ХарактеристикиНоменклатуры')}?$format=json&$top=1", timeout=10)
if r.ok:
    print("▸ Загрузка Catalog_ХарактеристикиНоменклатуры...")
    raw_char = load_catalog_full(
        "Catalog_ХарактеристикиНоменклатуры", "Ref_Key,Description", "Характеристики"
    )
    char_names = {k: v.get("Description", "").strip() for k, v in raw_char.items()}
    print(f"  Характеристик: {len(char_names)}")

    needed_char_keys = {c.get("ТД_Характеристика_Key") for c in claims
                        if c.get("ТД_Характеристика_Key") and c.get("ТД_Характеристика_Key") != EMPTY}
    missing_c = needed_char_keys - set(char_names.keys())
    if missing_c:
        print(f"  Дозагрузка {len(missing_c)} отсутствующих характеристик...")
        for ck in missing_c:
            item = fetch_single("Catalog_ХарактеристикиНоменклатуры", ck, "Ref_Key,Description")
            if item:
                char_names[ck] = (item.get("Description") or "").strip()
else:
    print(f"  Характеристики: не опубликован (HTTP {r.status_code}), будут GUID")

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
