"""
calc_fact_fast.py — Оптимизированный расчёт факта отгрузки и поступления ДС.

Адаптация подхода calc_vp_fast.py (RecordType + $filter + $select):
  - Факт отгрузки:  AccumulationRegister_РаспоряженияНаОтгрузку_RecordType
  - Факт ДС:        AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType
                     + связка ОбъектРасчетов ↔ Document_ЗаказКлиента.ОбъектРасчетов_Key
                       (как «ОбъектРасчетов.Объект ССЫЛКА ЗаказКлиента» в запросе 1С)
  - План:           AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType
                     (при 401 — выдать права odata.user)

Логика фильтрации (из 1С-запроса):
  1. Подразделение заказа — в списке 6 отделов продаж
  2. Подразделение ≠ пусто
  3. Соглашение ≠ пусто
  4. Партнёр не из списка перепродажи (если не ТД_СопровождениеПродажи)
     - Для ОПБО: список без МГС
     - Для остальных: полный список
  5. Не ТД_НеУчитыватьВПланФакте
  6. Для ДС: ВозвратОплатыКлиенту → знак минус

Запуск:
  python calc_fact_fast.py <месяц> <год>
  python calc_fact_fast.py <месяц> <год> --force
  python calc_fact_fast.py <месяц> <год> --partner-key <GUID>   # как отбор «Партнёр» в отчёте
  python calc_fact_fast.py <месяц> <год> --manager-key <GUID> # как «Ответственный» (Менеджер заказа)
"""
import requests, sys, time, json, functools, os
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

# ─────────────────────────── config ───────────────────────────

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
    "АЛМАЗ ООО (рабочий)",
    "Турбулентность-Дон ООО",
    "Турбулентность-ДОН ООО НПО",
    "СКТБ Турбо-Дон ООО",
    "Метрогазсервис ООО",
}

EXCLUDE_PARTNER_NAMES_NO_MGS = EXCLUDE_PARTNER_NAMES - {"Метрогазсервис ООО"}

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"


def order_passes_plan_fact(o, exclude_full, exclude_no_mgs):
    """Отбор заказа по правилам план-факт (как измерение ОбъектРасчетов в запросе 1С)."""
    if not o:
        return False
    if o["dept"] not in DEPARTMENTS or o["dept"] == EMPTY:
        return False
    if o["agreement"] == EMPTY or not o["agreement"]:
        return False
    if o["dept"] == OPBO_DEPT:
        if o["partner"] in exclude_no_mgs:
            return False
    else:
        if o["partner"] in exclude_full and not o["soprovozhd"]:
            return False
    if o["ne_uchit"]:
        return False
    return True


def ds_row_payment_amount(row):
    """Сумма оплаты в движении (приоритет регл., как в отчётах в рублях)."""
    regl = row.get("СуммаОплатыРегл")
    if regl is not None and regl != 0:
        return float(regl)
    sm = row.get("СуммаОплаты")
    return float(sm or 0)


# ─────────────────────────── args ───────────────────────────

force = "--force" in sys.argv
argv_rest = [a for a in sys.argv[1:] if a != "--force"]

FILTER_PARTNER_KEY = None
FILTER_MANAGER_KEY = None
i = 0
positional = []
while i < len(argv_rest):
    a = argv_rest[i]
    if a == "--partner-key" and i + 1 < len(argv_rest):
        FILTER_PARTNER_KEY = argv_rest[i + 1].strip().lower()
        i += 2
        continue
    if a == "--manager-key" and i + 1 < len(argv_rest):
        FILTER_MANAGER_KEY = argv_rest[i + 1].strip().lower()
        i += 2
        continue
    if not a.startswith("-"):
        positional.append(a)
    i += 1

args = positional

if len(args) != 2:
    print("Использование: python calc_fact_fast.py <месяц> <год> [--force]")
    print("                [--partner-key GUID] [--manager-key GUID]")
    print("Пример:        python calc_fact_fast.py 3 2026")
    sys.exit(1)

month, year = int(args[0]), int(args[1])

if month == 12:
    P_START = f"{year}-12-01T00:00:00"
    P_END = f"{year + 1}-01-01T00:00:00"
else:
    P_START = f"{year}-{month:02d}-01T00:00:00"
    P_END = f"{year}-{month + 1:02d}-01T00:00:00"

mn = MONTH_RU[month]
F_REGISTER = f"{mn}_{year}_register_cache.json"
F_ORDERS = f"{mn}_{year}_order_guids_final.json"
F_PARTNERS = "partners_exclude_cache.json"


def load_cache(path):
    if os.path.exists(path) and not force:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return None


session = requests.Session()
session.auth = AUTH
T0 = time.time()

print(f"\n{'═' * 55}")
print(f"  Факт отгрузки · {mn.title()} {year}")
print(f"  {P_START[:10]} — {P_END[:10]}")
if FILTER_PARTNER_KEY:
    print(f"  Отбор ДС/контекст: Партнёр_Key = {FILTER_PARTNER_KEY}")
if FILTER_MANAGER_KEY:
    print(f"  Отбор ДС:          Менеджер_Key = {FILTER_MANAGER_KEY}")
print(f"{'═' * 55}")


# ═══════════════════════════════════════════════════════
# 1. СКАН РЕГИСТРА РаспоряженияНаОтгрузку
# ═══════════════════════════════════════════════════════
#
# _RecordType + $filter по Period → ~0.5с вместо ~минут полного скана

print(f"\n▸ Шаг 1 · Регистр РаспоряженияНаОтгрузку")

cached = load_cache(F_REGISTER)
if cached is not None:
    entries = cached["entries"]
    print(f"  кэш: {len(entries)} записей")
else:
    t = time.time()

    period_filter = quote(
        f"Period ge datetime'{P_START}' and Period lt datetime'{P_END}' and Active eq true",
        safe=""
    )
    select_fields = "Period,Active,Распоряжение,Распоряжение_Type,ВидДвиженияРегистра,Сумма,Сторно"

    entries = []
    skip = 0
    pg = 0

    while True:
        pg += 1
        url = (
            f"{BASE}/AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
            f"?$format=json&$top=5000&$skip={skip}"
            f"&$filter={period_filter}&$select={select_fields}"
        )
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        rows = resp.json().get("value", [])

        for row in rows:
            if row.get("Распоряжение_Type") == ORDER_TYPE:
                entries.append({
                    "guid": row.get("Распоряжение"),
                    "Сумма": row.get("Сумма", 0) or 0,
                    "ВидДвижения": row.get("ВидДвиженияРегистра", ""),
                    "Сторно": row.get("Сторно", False),
                })

        print(f"  стр.{pg}: {len(rows)} записей · ЗаказКлиента: {len(entries)} · {time.time() - t:.1f}с")
        if len(rows) < 5000:
            break
        skip += 5000

    with open(F_REGISTER, "w", encoding="utf-8") as fh:
        json.dump({"entries": entries}, fh, ensure_ascii=False)
    print(f"  ✓ {len(entries)} записей за {time.time() - t:.1f}с → {F_REGISTER}")

order_guids = sorted(set(e["guid"] for e in entries if e.get("guid")))
print(f"  Уникальных заказов: {len(order_guids)}")


# ═══════════════════════════════════════════════════════
# 2. ЗАГРУЗКА РЕКВИЗИТОВ ЗАКАЗОВ (батчами по 15)
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 2 · Реквизиты заказов")

select_order = quote(
    "Ref_Key,Подразделение_Key,Партнер_Key,Соглашение_Key,"
    "ТД_СопровождениеПродажи,ТД_НеУчитыватьВПланФакте",
    safe=",_"
)

orders = {}
ORDER_BATCH = 15
t = time.time()

for i in range(0, len(order_guids), ORDER_BATCH):
    batch = order_guids[i : i + ORDER_BATCH]
    flt = quote(
        " or ".join(f"Ref_Key eq guid'{g}'" for g in batch),
        safe=""
    )
    url = (
        f"{BASE}/Document_ЗаказКлиента"
        f"?$format=json&$select={select_order}"
        f"&$top={ORDER_BATCH}&$filter={flt}"
    )
    resp = session.get(url, timeout=30)
    if resp.ok:
        for item in resp.json().get("value", []):
            orders[item["Ref_Key"]] = {
                "dept": item.get("Подразделение_Key", EMPTY),
                "partner": item.get("Партнер_Key", EMPTY),
                "agreement": item.get("Соглашение_Key", EMPTY),
                "soprovozhd": item.get("ТД_СопровождениеПродажи", False),
                "ne_uchit": item.get("ТД_НеУчитыватьВПланФакте", False),
            }

    done = min(i + ORDER_BATCH, len(order_guids))
    if done % 100 < ORDER_BATCH or done >= len(order_guids):
        print(f"  {done}/{len(order_guids)} заказов")

print(f"  ✓ {len(orders)} заказов за {time.time() - t:.0f}с")


# ═══════════════════════════════════════════════════════
# 3. ЗАГРУЗКА ИМЁН ПАРТНЁРОВ (для фильтрации перепродажи)
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 3 · Партнёры для исключения")

unique_partner_keys = list({
    o["partner"] for o in orders.values()
    if o["partner"] != EMPTY
})

partners = {}
cached = load_cache(F_PARTNERS)
if cached is not None and isinstance(cached, dict):
    partners = cached
    missing_pk = [k for k in unique_partner_keys if k not in partners]
else:
    missing_pk = unique_partner_keys

if missing_pk:
    t = time.time()
    P_BATCH = 15
    for i in range(0, len(missing_pk), P_BATCH):
        batch = missing_pk[i : i + P_BATCH]
        flt = quote(
            " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
            safe=""
        )
        url = (
            f"{BASE}/Catalog_Партнеры"
            f"?$format=json&$filter={flt}&$select=Ref_Key,Description&$top={P_BATCH}"
        )
        resp = session.get(url, timeout=30)
        if resp.ok:
            for item in resp.json().get("value", []):
                partners[item["Ref_Key"]] = item.get("Description", "").strip()

    with open(F_PARTNERS, "w", encoding="utf-8") as fh:
        json.dump(partners, fh, ensure_ascii=False)
    print(f"  загружено {len(missing_pk)} имён за {time.time() - t:.0f}с")
else:
    print(f"  кэш: {len(partners)} партнёров")

exclude_keys_full = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES}
exclude_keys_no_mgs = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}
print(f"  Партнёры-перепродажа (полный): {len(exclude_keys_full)}")
print(f"  Партнёры-перепродажа (без МГС): {len(exclude_keys_no_mgs)}")


# ═══════════════════════════════════════════════════════
# 4. ПРИМЕНЕНИЕ ФИЛЬТРОВ (логика из 1С-запроса)
# ═══════════════════════════════════════════════════════
#
# Фильтры:
#   - Подразделение IN (6 отделов) и ≠ пусто
#   - Соглашение ≠ пусто
#   - Для ОПБО: партнёр НЕ в списке без МГС
#   - Для остальных: НЕ (партнёр в полном списке И НЕ ТД_СопровождениеПродажи)
#   - НЕ ТД_НеУчитыватьВПланФакте

print(f"\n▸ Шаг 4 · Фильтрация заказов")

stats = {"dept": 0, "agreement": 0, "partner": 0, "ne_uchit": 0, "not_found": 0, "passed": 0}
passed_guids = set()

for guid in order_guids:
    o = orders.get(guid)
    if not o:
        stats["not_found"] += 1
        continue

    if o["dept"] not in DEPARTMENTS or o["dept"] == EMPTY:
        stats["dept"] += 1
        continue

    if o["agreement"] == EMPTY or not o["agreement"]:
        stats["agreement"] += 1
        continue

    if o["dept"] == OPBO_DEPT:
        if o["partner"] in exclude_keys_no_mgs:
            stats["partner"] += 1
            continue
    else:
        if o["partner"] in exclude_keys_full and not o["soprovozhd"]:
            stats["partner"] += 1
            continue

    if o["ne_uchit"]:
        stats["ne_uchit"] += 1
        continue

    passed_guids.add(guid)
    stats["passed"] += 1

print(f"  −подразделение:       {stats['dept']}")
print(f"  −соглашение:          {stats['agreement']}")
print(f"  −партнёр/перепродажа: {stats['partner']}")
print(f"  −НеУчитыватьВПФ:     {stats['ne_uchit']}")
print(f"  −не найден заказ:     {stats['not_found']}")
print(f"  ✓ прошли фильтры:    {stats['passed']}")

with open(F_ORDERS, "w", encoding="utf-8") as fh:
    json.dump(sorted(passed_guids), fh)


# ═══════════════════════════════════════════════════════
# 5. РАСЧЁТ ФАКТА ОТГРУЗКИ
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 5 · Расчёт факта отгрузки")

total_rashod = 0.0
count_rashod = 0
by_dept = {}

for e in entries:
    if e["guid"] not in passed_guids:
        continue
    if e["ВидДвижения"] != "Расход":
        continue

    summa = e["Сумма"] or 0
    total_rashod += summa
    count_rashod += 1

    o = orders.get(e["guid"])
    if o:
        dept = o["dept"]
        by_dept[dept] = by_dept.get(dept, 0) + summa

elapsed = time.time() - T0

print(f"\n{'═' * 55}")
print(f"  ФАКТ ОТГРУЗКИ: {mn.title()} {year}")
print(f"{'═' * 55}")
print(f"  Строк Расход:       {count_rashod}")
print(f"  Сумма Расход:       {total_rashod:>18,.2f}")
print(f"  Абс. значение:      {abs(total_rashod):>18,.2f}")

if by_dept:
    print(f"\n  По подразделениям:")
    # Resolve department names
    dept_names = {}
    dept_keys = list(by_dept.keys())
    for i in range(0, len(dept_keys), 10):
        batch = dept_keys[i:i+10]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = f"{BASE}/Catalog_СтруктураПредприятия?$format=json&$filter={flt}&$select=Ref_Key,Description&$top=10"
        resp = session.get(url, timeout=15)
        if resp.ok:
            for item in resp.json().get("value", []):
                dept_names[item["Ref_Key"]] = item.get("Description", "")

    for dept_key, summa in sorted(by_dept.items(), key=lambda x: -abs(x[1])):
        name = dept_names.get(dept_key, dept_key[:12] + "…")
        print(f"    {name:<45} {summa:>14,.2f}")

print(f"{'═' * 55}")


# ═══════════════════════════════════════════════════════
# 6. ФАКТ ДС (ДвиженияДенежныеСредстваКонтрагент)
# ═══════════════════════════════════════════════════════
#
# Логика из 1С-запроса:
#   SELECT СуммаОплатыОборот (с инверсией для ВозвратОплатыКлиенту)
#   FROM ДвиженияДенежныеСредстваКонтрагент.Обороты
#   WHERE ОбъектРасчетов.Объект ССЫЛКА ЗаказКлиента
#     AND ОбъектРасчетов.Подразделение IN (&Отделы)
#     AND ОбъектРасчетов.Соглашение <> пусто
#     AND <фильтры по партнёрам>
#
# Связка с заказом: в OData у заказа есть ОбъектРасчетов_Key = GUID из поля
# ОбъектРасчетов движения (тип ключа в выгрузке — UnavailableEntity, но GUID тот же).
# Фильтры план/факт применяются к реквизитам заказа (подразделение, соглашение, партнёр,
# ТД_НеУчитыватьВПланФакте), как в виртуальной таблице Обороты по измерению ОбъектРасчетов.

print(f"\n▸ Шаг 6 · Факт ДС (денежные средства)")

total_ds = None
total_ds_vozvrat = None

CLIENT_OPS = {
    "ПоступлениеОплатыОтКлиента",
    "ПоступлениеОплатыОтКлиентаПоПлатежнойКарте",
    "ВозвратОплатыКлиенту",
}

REG_DS = "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType"
ds_url = f"{BASE}/{REG_DS}?$format=json&$top=1"
ds_resp = session.get(ds_url, timeout=15)

if ds_resp.status_code == 404:
    print("  ⚠ Регистр НЕ ОПУБЛИКОВАН в OData!")
    print("    Нужно опубликовать ДвиженияДенежныеСредстваКонтрагент в 1С")
elif ds_resp.status_code == 401:
    print("  ⚠ Нет доступа (401). Нужно дать права odata.user.")
elif ds_resp.ok:
    t = time.time()

    ds_select = (
        "Period,Active,ХозяйственнаяОперация,Партнер_Key,"
        "СуммаОплаты,СуммаОплатыРегл,ОбъектРасчетов,ОбъектРасчетов_Type,Сторно"
    )
    period_filter_ds = quote(
        f"Period ge datetime'{P_START}' and Period lt datetime'{P_END}' and Active eq true",
        safe=""
    )

    ds_entries = []
    skip = 0
    pg = 0

    while True:
        pg += 1
        url = (
            f"{BASE}/{REG_DS}"
            f"?$format=json&$top=5000&$skip={skip}"
            f"&$filter={period_filter_ds}&$select={ds_select}"
        )
        resp = session.get(url, timeout=120)
        if not resp.ok:
            print(f"  Ошибка: {resp.status_code}")
            break
        rows = resp.json().get("value", [])
        ds_entries.extend(rows)
        if pg == 1 or len(rows) < 5000:
            print(f"  стр.{pg}: {len(rows)} записей · итого {len(ds_entries)} · {time.time()-t:.1f}с")
        if len(rows) < 5000:
            break
        skip += 5000

    ds_client = [
        r for r in ds_entries
        if r.get("ХозяйственнаяОперация") in CLIENT_OPS and not r.get("Сторно")
    ]
    print(f"  Клиентских операций (без сторно): {len(ds_client)} из {len(ds_entries)}")

    obj_keys = set()
    for r in ds_client:
        ok = r.get("ОбъектРасчетов")
        if ok and ok != EMPTY:
            obj_keys.add(ok)

    print(f"  Уникальных ОбъектРасчетов: {len(obj_keys)}")

    # В публикации 1С нельзя отбирать Document_ЗаказКлиента по ОбъектРасчетов_Key ($filter → 500).
    # Сканируем заказы страницами и собираем только нужные ключи; выходим, когда все найдены.
    select_ds_order = quote(
        "Ref_Key,Подразделение_Key,Партнер_Key,Соглашение_Key,Менеджер_Key,"
        "ТД_СопровождениеПродажи,ТД_НеУчитыватьВПланФакте,ОбъектРасчетов_Key,"
        "DeletionMark",
        safe=",_",
    )

    # На один ключ расчётов может приходиться несколько заказов — храним списки,
    # при разборе движения выбираем заказ с тем же Партнер_Key, что и в строке ДС.
    orders_by_obj = {}
    needed = {str(k).lower() for k in obj_keys}
    skip_scan = 0
    scan_pg = 0
    PAGE = 500
    t_scan = time.time()
    dup_warned = set()

    def _add_order_for_key(k_s, orec):
        lst = orders_by_obj.setdefault(k_s, [])
        if not any(x["ref"] == orec["ref"] for x in lst):
            lst.append(orec)
            if len(lst) > 1 and k_s not in dup_warned:
                dup_warned.add(k_s)
                print(f"  ⚠ несколько заказов на один ОбъектРасчетов_Key: {k_s[:13]}…")

    while True:
        scan_pg += 1
        url = (
            f"{BASE}/Document_ЗаказКлиента"
            f"?$format=json&$select={select_ds_order}"
            f"&$orderby=Ref_Key&$top={PAGE}&$skip={skip_scan}"
        )
        resp = session.get(url, timeout=120)
        if not resp.ok:
            print(f"  ⚠ скан заказов: HTTP {resp.status_code}")
            break
        chunk = resp.json().get("value", [])
        if not chunk:
            break
        for item in chunk:
            k = item.get("ОбъектРасчетов_Key")
            if not k or k == EMPTY:
                continue
            k_s = str(k).lower()
            if k_s not in needed:
                continue
            if item.get("DeletionMark"):
                continue
            orec = {
                "ref": item["Ref_Key"],
                "dept": item.get("Подразделение_Key", EMPTY),
                "partner": item.get("Партнер_Key", EMPTY),
                "agreement": item.get("Соглашение_Key", EMPTY),
                "manager": item.get("Менеджер_Key", EMPTY),
                "soprovozhd": item.get("ТД_СопровождениеПродажи", False),
                "ne_uchit": item.get("ТД_НеУчитыватьВПланФакте", False),
            }
            _add_order_for_key(k_s, orec)
        skip_scan += len(chunk)
        found_distinct = sum(1 for k in needed if k in orders_by_obj)
        if scan_pg % 30 == 0 or found_distinct >= len(needed):
            print(
                f"  скан заказов стр.{scan_pg}: просмотрено {skip_scan}, "
                f"ключей с заказами {found_distinct}/{len(needed)} · {time.time()-t_scan:.1f}с"
            )
        if found_distinct >= len(needed):
            break
        if len(chunk) < PAGE:
            break

    missing_keys = len(needed) - sum(1 for k in needed if k in orders_by_obj)
    if missing_keys:
        print(f"  ⚠ не найдено заказов для {missing_keys} ключей ОбъектРасчетов")
    print(f"  Ключей ОбъектРасчетов с ≥1 заказом: {len(orders_by_obj)}")

    ds_partner_keys = set()
    for lst in orders_by_obj.values():
        for o in lst:
            if o["partner"] != EMPTY:
                ds_partner_keys.add(o["partner"])
    missing_ds_pk = [k for k in ds_partner_keys if k not in partners]
    if missing_ds_pk:
        P_BATCH = 15
        for j in range(0, len(missing_ds_pk), P_BATCH):
            batch = missing_ds_pk[j : j + P_BATCH]
            flt = quote(
                " or ".join(f"Ref_Key eq guid'{k}'" for k in batch),
                safe="",
            )
            url = (
                f"{BASE}/Catalog_Партнеры"
                f"?$format=json&$filter={flt}&$select=Ref_Key,Description&$top={P_BATCH}"
            )
            resp = session.get(url, timeout=30)
            if resp.ok:
                for item in resp.json().get("value", []):
                    partners[item["Ref_Key"]] = item.get("Description", "").strip()
        with open(F_PARTNERS, "w", encoding="utf-8") as fh:
            json.dump(partners, fh, ensure_ascii=False)

    exclude_ds_full = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES}
    exclude_ds_no_mgs = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    def resolve_order_for_ds(obj_guid, partner_key):
        lst = orders_by_obj.get(str(obj_guid).lower())
        if not lst:
            return None
        if len(lst) == 1:
            return lst[0]
        pk = partner_key or EMPTY
        for cand in lst:
            if cand["partner"] == pk:
                return cand
        return lst[0]

    total_ds = 0.0
    total_ds_vozvrat = 0.0
    count_ds = 0
    skipped_no_order = 0
    skipped_filter = 0
    by_dept_ds = {}

    for r in ds_client:
        ok = r.get("ОбъектРасчетов")
        if not ok or ok == EMPTY:
            skipped_no_order += 1
            continue
        o = resolve_order_for_ds(ok, r.get("Партнер_Key"))
        if not o:
            skipped_no_order += 1
            continue
        if not order_passes_plan_fact(o, exclude_ds_full, exclude_ds_no_mgs):
            skipped_filter += 1
            continue

        if FILTER_MANAGER_KEY:
            om = str(o.get("manager") or EMPTY).lower()
            if om != FILTER_MANAGER_KEY:
                skipped_filter += 1
                continue
        if FILTER_PARTNER_KEY:
            op = str(o.get("partner") or EMPTY).lower()
            if op != FILTER_PARTNER_KEY:
                skipped_filter += 1
                continue

        summa = ds_row_payment_amount(r)
        if r.get("ХозяйственнаяОперация") == "ВозвратОплатыКлиенту":
            total_ds -= summa
            total_ds_vozvrat += summa
        else:
            total_ds += summa
        count_ds += 1
        dept = o["dept"]
        if dept and dept != EMPTY:
            by_dept_ds[dept] = by_dept_ds.get(dept, 0.0) + (
                -summa if r.get("ХозяйственнаяОперация") == "ВозвратОплатыКлиенту" else summa
            )

    print(f"  Пропуск: нет заказа по ключу — {skipped_no_order}")
    print(f"  Пропуск: заказ не прошёл отбор план/факт — {skipped_filter}")

    print(f"\n{'═' * 55}")
    print(f"  ФАКТ ДС: {mn.title()} {year}")
    print(f"{'═' * 55}")
    print(f"  Записей (после отбора): {count_ds}")
    print(f"  Поступления (брутто):   {total_ds + total_ds_vozvrat:>18,.2f}")
    print(f"  Возвраты:                 {-total_ds_vozvrat:>18,.2f}")
    print(f"  ИТОГО ДС:                 {total_ds:>18,.2f}")
    print(f"{'═' * 55}")
    print(f"  {time.time()-t:.1f}с")

    if by_dept_ds:
        dept_names_ds = {}
        dkeys = list(by_dept_ds.keys())
        for i in range(0, len(dkeys), 10):
            batch = dkeys[i : i + 10]
            flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
            url = (
                f"{BASE}/Catalog_СтруктураПредприятия"
                f"?$format=json&$filter={flt}&$select=Ref_Key,Description&$top=10"
            )
            resp = session.get(url, timeout=15)
            if resp.ok:
                for item in resp.json().get("value", []):
                    dept_names_ds[item["Ref_Key"]] = item.get("Description", "")
        print(f"\n  Факт ДС по подразделениям (как в отчёте):")
        for dept_key, summa in sorted(by_dept_ds.items(), key=lambda x: -abs(x[1])):
            name = dept_names_ds.get(dept_key, str(dept_key)[:12] + "…")
            print(f"    {name:<45} {summa:>14,.2f}")


# ═══════════════════════════════════════════════════════
# 7. ПЛАН (ТД_ПланированиеДоговоровОтгрузокДС)
# ═══════════════════════════════════════════════════════

print(f"\n▸ Шаг 7 · План")

REG_PLAN = "AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType"
plan_url = f"{BASE}/{REG_PLAN}?$format=json&$top=1"
plan_resp = session.get(plan_url, timeout=15)

if plan_resp.status_code == 404:
    print("  ⚠ Регистр плана НЕ ОПУБЛИКОВАН в OData!")
elif plan_resp.status_code == 401:
    print("  ⚠ Нет доступа (401). Нужно дать права odata.user на регистр:")
    print("    AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС")
elif plan_resp.ok:
    print("  Регистр плана доступен! Загрузка...")
    t = time.time()

    period_filter = quote(
        f"Period ge datetime'{P_START}' and Period lt datetime'{P_END}'",
        safe=""
    )

    plan_entries = []
    skip = 0
    pg = 0

    while True:
        pg += 1
        url = (
            f"{BASE}/{REG_PLAN}"
            f"?$format=json&$top=5000&$skip={skip}"
            f"&$filter={period_filter}"
        )
        resp = session.get(url, timeout=120)
        if not resp.ok:
            print(f"  Ошибка: {resp.status_code}")
            break
        rows = resp.json().get("value", [])
        plan_entries.extend(rows)
        print(f"  стр.{pg}: {len(rows)} записей · итого {len(plan_entries)}")
        if len(rows) < 5000:
            break
        skip += 5000

    if plan_entries:
        print(f"  Поля: {list(plan_entries[0].keys())[:10]}...")
        # TODO: суммирование плана по подразделениям
        print(f"  ✓ {len(plan_entries)} записей за {time.time()-t:.1f}с")


# ═══════════════════════════════════════════════════════

print(f"\n{'═' * 55}")
print(f"  Время выполнения: {time.time() - T0:.1f}с")
print(f"{'═' * 55}")

# Кэш для API KPI KD-M2 (после обновлённой логики ДС ↔ ЗаказКлиента)
KD_M2_CACHE = f"kd_m2_{mn}_{year}.json"
shipment_abs = abs(total_rashod)
money = round(total_ds, 2) if total_ds is not None else round(shipment_abs, 2)
kd_m2_payload = {
    "month": month,
    "year": year,
    "month_name": mn,
    "shipment_fact": round(shipment_abs, 2),
    "ds_fact": round(total_ds, 2) if total_ds is not None else None,
    "ds_vozvrat": round(total_ds_vozvrat, 2) if total_ds_vozvrat is not None else None,
    "money_fact": money,
}
with open(KD_M2_CACHE, "w", encoding="utf-8") as fh:
    json.dump(kd_m2_payload, fh, ensure_ascii=False)
print(f"\n  → {KD_M2_CACHE} (money_fact={money:,.2f} ₽) — для getkpi KD-M2")
