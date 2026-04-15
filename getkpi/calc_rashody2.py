"""
calc_rashody.py — Расходы по статьям бюджета по подразделениям.

Источники:
  • Счёт 44  — дебетовый оборот (12 статей затрат)
  • Счёт 51  — дебетовый оборот (2 статьи ДДС) + СДС (3 статьи ДДС по кредиту 51)
  • Счёт 71.01 — дебетовое сальдо на конец периода (кредитовое/нулевое исключается)

Алгоритм:
  1. По каждому подразделению суммируется Дт-оборот 44 по 12 статьям.
  2. Группа 44-ВРТ (4 статьи) сравнивается с СДС (3 статьи списания ДС).
     Если СДС > 44-ВРТ → в итог подставляется СДС вместо 44-ВРТ.
  3. Добавляется Дт-оборот 51 (2 статьи).
  4. Добавляется дебетовое сальдо 71.01 на конец периода
     (кредитовое/нулевое сальдо по сотруднику → 0).

Использование:
  python calc_rashody.py [ГГГГ-ММ]   # по умолчанию 2026-03
"""
import requests, sys, time, functools
from collections import defaultdict
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

# ═══════════════════════════════════════════════════════════
#  СПРАВОЧНИКИ  (GUID из 1С)
# ═══════════════════════════════════════════════════════════

# ── Счета (ChartOfAccounts_Хозрасчетный) ──
ACCOUNTS_44 = {
    "fb2bde54-6250-11e7-812d-001e67112509",  # 44
    "fb2bde55-6250-11e7-812d-001e67112509",  # 44.01
    "fb2bde56-6250-11e7-812d-001e67112509",  # 44.02
}
ACC_51 = "fb2bde64-6250-11e7-812d-001e67112509"
ACC_7101 = "fb2bded3-6250-11e7-812d-001e67112509"

# ── 8 подразделений ──
DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборуд. и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": "Сектор рекламы и PR",
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": "Отдел тендерных продаж",
}
DEPT_SET = frozenset(DEPARTMENTS)

# ── 12 статей затрат для счёта 44 (ChartOfCharacteristicTypes_СтатьиРасходов) ──
ART_44 = {
    "f6b5f6a7-3418-11e8-8271-ac1f6b05524d": "Интернет-продвижение (44 сч)!",
    "19919da8-56cb-11e0-b816-008048428575": "Командировка (44 сч)!",
    "992a514f-782a-11eb-854d-ac1f6b05524d": "Оплата труда (44 сч) возмещение НПО!",
    "cc7c4aa6-3767-11ea-82f3-ac1f6b05524d": "Оплата труда (44 сч) ТД НПО!",
    "d211f6cb-ff4a-11e7-826d-ac1f6b05524d": "Представительские расходы (44.01) ТД!",
    "3cba9c50-341a-11e8-8271-ac1f6b05524d": "Расходы на выставки, конференцию (44 сч)!",
    "cdc34aed-ed4a-11e6-8128-001e67112509": "Рекламная продукция (44)!",
    "a04a98f8-782a-11eb-854d-ac1f6b05524d": "Страховые взносы (44.01) возмещение НПО!",
    "0360bf95-3768-11ea-82f3-ac1f6b05524d": "Страховые взносы (44.01) ТД НПО!",
    "9900a8d0-1b83-11e8-826f-ac1f6b05524d": "Таможенные расходы (44.01)!",
    "eee66843-ae99-11e3-adf9-001e67112509": "Тендер ТОЛЬКО НПО- Информационные услуги-электронной площадке/сайт (44 сч)!",
    "f33409f4-5d1d-11e9-828d-ac1f6b05524d": "Транспортные расходы по доставке ТМЦ на выставки (44 сч)!",
}
ART_44_SET = frozenset(ART_44)
ART_44_ORDER = list(ART_44.keys())
ART_44_SHORT = {
    "f6b5f6a7-3418-11e8-8271-ac1f6b05524d": "Интернет-продвиж.",
    "19919da8-56cb-11e0-b816-008048428575": "Командировка",
    "992a514f-782a-11eb-854d-ac1f6b05524d": "Опл.труда возм.НПО",
    "cc7c4aa6-3767-11ea-82f3-ac1f6b05524d": "Опл.труда ТД НПО",
    "d211f6cb-ff4a-11e7-826d-ac1f6b05524d": "Представ.расходы",
    "3cba9c50-341a-11e8-8271-ac1f6b05524d": "Выставки/конф.",
    "cdc34aed-ed4a-11e6-8128-001e67112509": "Рекламн.продукция",
    "a04a98f8-782a-11eb-854d-ac1f6b05524d": "Страх.вз. возм.НПО",
    "0360bf95-3768-11ea-82f3-ac1f6b05524d": "Страх.вз. ТД НПО",
    "9900a8d0-1b83-11e8-826f-ac1f6b05524d": "Таможенные",
    "eee66843-ae99-11e3-adf9-001e67112509": "Тендер НПО инф.усл.",
    "f33409f4-5d1d-11e9-828d-ac1f6b05524d": "Трансп.расх.выст.",
}

# ── Группа 44-ВРТ (4 статьи) ──
VRT_KEYS = frozenset([
    "3cba9c50-341a-11e8-8271-ac1f6b05524d",  # Расходы на выставки
    "cdc34aed-ed4a-11e6-8128-001e67112509",  # Рекламная продукция
    "eee66843-ae99-11e3-adf9-001e67112509",  # Тендер НПО
    "f33409f4-5d1d-11e9-828d-ac1f6b05524d",  # Транспортные
])

# ── 2 статьи ДДС для счёта 51 (Catalog_СтатьиДвиженияДенежныхСредств) ──
ART_51 = {
    "f6f0d74d-d476-11e5-810b-001e67112509": "Прочие выплаты, связанные с производством (пр.)_2_КС_РУК_3.6.",
    "33a983cc-0812-11ed-8930-ac1f6b05524d": "Прочие выплаты, связанные с производством (пр.БМИ)_2_КС_РУК_3.6.",
}
ART_51_SET = frozenset(ART_51)

# ── 3 статьи СДС (Catalog_СтатьиДвиженияДенежныхСредств) ──
ART_SDS = {
    "5ec4fd64-c8a9-11e5-810b-001e67112509": "Выставки, конференции, затраты на рекламу_2_КС_ОС_3.3.",
    "badd95fb-ec45-11e6-8127-001e67112509": "Печатная продукция (визитки, буклеты, листовки, плакаты)_2_КС_ОМ_3.3.3.",
    "55eeaa60-533f-11eb-84f3-ac1f6b05524d": "Тендеры затраты по подпискам и участию_2_КС_ОТП_3.4.",
}
ART_SDS_SET = frozenset(ART_SDS)

# Тип субконто «Статьи затрат»
SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

# ═══════════════════════════════════════════════════════════
#  ПАРАМЕТРЫ ПЕРИОДА
# ═══════════════════════════════════════════════════════════
period_arg = None
for a in sys.argv[1:]:
    if len(a) == 7 and a[4] == "-":
        period_arg = a
        break

if period_arg:
    year, month = int(period_arg[:4]), int(period_arg[5:7])
else:
    year, month = 2026, 3

if month == 12:
    P_START = f"{year}-12-01T00:00:00"
    P_END   = f"{year + 1}-01-01T00:00:00"
else:
    P_START = f"{year}-{month:02d}-01T00:00:00"
    P_END   = f"{year}-{month + 1:02d}-01T00:00:00"

session = requests.Session()
session.auth = AUTH
T0 = time.time()

print(f"\n{'═' * 78}")
print(f"  РАСХОДЫ ПО СТАТЬЯМ БЮДЖЕТА · {MONTH_RU[month]} {year}")
print(f"  Период: {P_START[:10]} — {P_END[:10]} (lt)")
print(f"{'═' * 78}")


def load_records(flt, sel, label, limit=50000):
    """Загрузка записей RecordsWithExtDimensions с пагинацией."""
    records = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}"
            f"/RecordsWithExtDimensions"
            f"?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = session.get(url, timeout=120)
        if not r.ok:
            print(f"  [{label}] HTTP {r.status_code}: {r.text[:200]}")
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        records.extend(batch)
        skip += len(batch)
        if len(records) >= limit:
            print(f"  [{label}] ПРЕДЕЛ {limit} записей!")
            break
        if len(batch) < PAGE:
            break
    return records


# ═══════════════════════════════════════════════════════════
#  БЛОК 1: СЧЁТ 44 — Дт-оборот по 12 статьям затрат
# ═══════════════════════════════════════════════════════════
print(f"\n[1] Загрузка Дт-оборота счёта 44 ...")

acc_or = " or ".join(f"AccountDr_Key eq guid'{a}'" for a in ACCOUNTS_44)
flt44 = (
    f"Period ge datetime'{P_START}' and Period lt datetime'{P_END}'"
    f" and Active eq true and ({acc_or})"
)
sel44 = "Period,AccountDr_Key,ПодразделениеDr_Key,Сумма,Сторно,ExtDimensionDr1,ExtDimensionTypeDr1_Key"

recs44 = load_records(flt44, sel44, "Счёт 44")
print(f"    Записей: {len(recs44)}  ({time.time()-T0:.1f}с)")

# matrix44[dept_key][article_key] = sum
matrix44 = defaultdict(lambda: defaultdict(float))
total_all_44 = 0.0

for rec in recs44:
    amount = rec.get("Сумма", 0) or 0
    if rec.get("Сторно"):
        amount = -amount
    total_all_44 += amount
    dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
    ext_dim1 = rec.get("ExtDimensionDr1")
    ext_type1 = rec.get("ExtDimensionTypeDr1_Key")
    article_key = ext_dim1 if ext_type1 == SUBCONTO_TYPE_COST and ext_dim1 else None
    if dept_key in DEPT_SET and article_key in ART_44_SET:
        matrix44[dept_key][article_key] += amount


# ═══════════════════════════════════════════════════════════
#  БЛОК 2: СДС — кредитовый оборот счёта 51 по 3 статьям ДДС
# ═══════════════════════════════════════════════════════════
print(f"\n[2] Загрузка СДС (кредит 51, 3 статьи ДДС) ...")

flt_sds = (
    f"AccountCr_Key eq guid'{ACC_51}'"
    f" and Active eq true"
    f" and Period ge datetime'{P_START}'"
    f" and Period lt datetime'{P_END}'"
)
sel_sds = "Period,Сумма,Сторно,ПодразделениеDr_Key,AccountDr_Key,ExtDimensionCr2"

recs_sds_all = load_records(flt_sds, sel_sds, "СДС")
print(f"    Записей (всего Кт 51): {len(recs_sds_all)}  ({time.time()-T0:.1f}с)")

# sds[dept_key] = total СДС
sds_by_dept = defaultdict(float)
sds_detail = defaultdict(lambda: defaultdict(float))
sds_no_dept = defaultdict(float)

for rec in recs_sds_all:
    ext2cr = rec.get("ExtDimensionCr2", "")
    if ext2cr not in ART_SDS_SET:
        continue
    amount = rec.get("Сумма", 0) or 0
    if rec.get("Сторно"):
        amount = -amount
    dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
    if dept_key in DEPT_SET:
        sds_by_dept[dept_key] += amount
        sds_detail[dept_key][ext2cr] += amount
    else:
        sds_no_dept[ext2cr] += amount

sds_no_dept_total = sum(sds_no_dept.values())
print(f"    СДС (в нашем периметре): {sum(sds_by_dept.values()):,.2f}")
if sds_no_dept_total:
    print(f"    СДС без подразделения:   {sds_no_dept_total:,.2f} (не включены)")


# ═══════════════════════════════════════════════════════════
#  БЛОК 3: СЧЁТ 51 — Дт-оборот по 2 статьям ДДС
# ═══════════════════════════════════════════════════════════
print(f"\n[3] Загрузка Дт-оборота счёта 51 (2 статьи) ...")

flt51 = (
    f"AccountDr_Key eq guid'{ACC_51}'"
    f" and Active eq true"
    f" and Period ge datetime'{P_START}'"
    f" and Period lt datetime'{P_END}'"
)
sel51 = "Period,Сумма,Сторно,ПодразделениеDr_Key,ExtDimensionDr2"

recs51_all = load_records(flt51, sel51, "Счёт 51")
print(f"    Записей (всего Дт 51): {len(recs51_all)}  ({time.time()-T0:.1f}с)")

# acc51[dept_key] = total
acc51_by_dept = defaultdict(float)
acc51_no_dept = 0.0

for rec in recs51_all:
    ext2 = rec.get("ExtDimensionDr2", "")
    if ext2 not in ART_51_SET:
        continue
    amount = rec.get("Сумма", 0) or 0
    if rec.get("Сторно"):
        amount = -amount
    dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
    if dept_key in DEPT_SET:
        acc51_by_dept[dept_key] += amount
    else:
        acc51_no_dept += amount

total_51 = sum(acc51_by_dept.values()) + acc51_no_dept
if total_51:
    print(f"    Итого по ст.51 (в периметре): {sum(acc51_by_dept.values()):,.2f}")
    if acc51_no_dept:
        print(f"    Без подразделения:            {acc51_no_dept:,.2f}")
else:
    print(f"    Записей с целевыми статьями не найдено.")


# ═══════════════════════════════════════════════════════════
#  БЛОК 4: СЧЁТ 71.01 — дебетовое сальдо на конец периода
#  Кредитовое или нулевое сальдо по сотруднику исключается.
# ═══════════════════════════════════════════════════════════
print(f"\n[4] Загрузка дебетового сальдо по 71.01 на {P_END[:10]} ...")

flt_bal = f"Account_Key eq guid'{ACC_7101}'"
url_bal = (
    f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}"
    f"/Balance(Period=datetime'{P_END}')"
    f"?$format=json&$top=5000"
    f"&$filter={quote(flt_bal, safe='')}"
)
r_bal = session.get(url_bal, timeout=120)
bal_7101 = defaultdict(float)
bal_7101_total_all = 0.0
bal_7101_excluded_cr = 0.0
bal_7101_excluded_cr_count = 0

if r_bal.ok:
    bal_items = r_bal.json().get("value", [])
    print(f"    Строк (сотрудников): {len(bal_items)}  ({time.time()-T0:.1f}с)")
    for item in bal_items:
        bal = item.get("СуммаBalance", 0) or 0
        dept_key = item.get("Подразделение_Key", EMPTY)
        bal_7101_total_all += bal
        if bal <= 0:
            bal_7101_excluded_cr += bal
            bal_7101_excluded_cr_count += 1
            continue
        if dept_key in DEPT_SET:
            bal_7101[dept_key] += bal
    print(f"    Дебетовое сальдо (в периметре): {sum(bal_7101.values()):,.2f}")
    print(f"    Сальдо всего (до фильтра):      {bal_7101_total_all:,.2f}")
    if bal_7101_excluded_cr_count:
        print(f"    Исключено кредитовое/нулевое:   {bal_7101_excluded_cr:,.2f} ({bal_7101_excluded_cr_count} строк)")
else:
    print(f"    HTTP {r_bal.status_code}: {r_bal.text[:200]}")
    print(f"    ВНИМАНИЕ: Сальдо 71.01 не загружено!")


# ═══════════════════════════════════════════════════════════
#  РАСЧЁТ ИТОГОВ
# ═══════════════════════════════════════════════════════════
print(f"\n{'═' * 78}")
print(f"  РАСЧЁТ ИТОГОВ")
print(f"{'═' * 78}")

DEPT_ORDER = list(DEPARTMENTS.keys())
DEPT_SHORT = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Эталон.обор.",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Ключ.клиенты",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Дилерские",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Газпром",
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": "Реклама/PR",
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": "Тендерные",
}

# ── Таблица по счёту 44 (12 статей × 8 подразделений) ──
print(f"\n[A] Дебетовый оборот счёта 44 по статьям:")
hdr = f"{'Статья':<22}" + "".join(f"{DEPT_SHORT[d]:>14}" for d in DEPT_ORDER) + f"{'ИТОГО':>14}"
print(f"  {hdr}")
print(f"  {'─' * len(hdr)}")

for ak in ART_44_ORDER:
    row = f"  {ART_44_SHORT[ak]:<22}"
    row_total = 0.0
    for dk in DEPT_ORDER:
        v = matrix44[dk][ak]
        row += f"{v:>14,.2f}"
        row_total += v
    row += f"{row_total:>14,.2f}"
    print(row)

# Итого по подразделениям
row_totals = f"  {'ИТОГО 44':<22}"
grand_44_our = 0.0
for dk in DEPT_ORDER:
    s = sum(matrix44[dk][ak] for ak in ART_44_ORDER)
    row_totals += f"{s:>14,.2f}"
    grand_44_our += s
row_totals += f"{grand_44_our:>14,.2f}"
print(f"  {'─' * len(hdr)}")
print(row_totals)

# ── 44-ВРТ ──
print(f"\n[B] Группа 44-ВРТ (4 статьи):")
vrt_row = f"  {'44-ВРТ':<22}"
vrt_by_dept = {}
for dk in DEPT_ORDER:
    v = sum(matrix44[dk][ak] for ak in VRT_KEYS)
    vrt_by_dept[dk] = v
    vrt_row += f"{v:>14,.2f}"
vrt_row += f"{sum(vrt_by_dept.values()):>14,.2f}"
print(vrt_row)

# ── СДС ──
sds_row = f"  {'СДС':<22}"
for dk in DEPT_ORDER:
    v = sds_by_dept.get(dk, 0)
    sds_row += f"{v:>14,.2f}"
sds_row += f"{sum(sds_by_dept.values()):>14,.2f}"
print(sds_row)

# ── Флаг замены ──
flag_row = f"  {'Замена на СДС?':<22}"
replace_flags = {}
for dk in DEPT_ORDER:
    sds_val = sds_by_dept.get(dk, 0)
    vrt_val = vrt_by_dept[dk]
    replace = sds_val > vrt_val
    replace_flags[dk] = replace
    flag_row += f"{'ДА':>14}" if replace else f"{'нет':>14}"
print(flag_row)

# ── Фактический блок ВРТ/СДС ──
effective_row = f"  {'В итог (ВРТ/СДС)':<22}"
effective_by_dept = {}
for dk in DEPT_ORDER:
    effective_by_dept[dk] = sds_by_dept.get(dk, 0) if replace_flags[dk] else vrt_by_dept[dk]
    effective_row += f"{effective_by_dept[dk]:>14,.2f}"
effective_row += f"{sum(effective_by_dept.values()):>14,.2f}"
print(effective_row)

# ── Счёт 51 ──
print(f"\n[C] Дебетовый оборот счёта 51 (2 статьи):")
row51 = f"  {'Счёт 51':<22}"
for dk in DEPT_ORDER:
    v = acc51_by_dept.get(dk, 0)
    row51 += f"{v:>14,.2f}"
row51 += f"{sum(acc51_by_dept.values()):>14,.2f}"
print(row51)
if acc51_no_dept:
    print(f"  ⚠ Без подразделения: {acc51_no_dept:,.2f}")

# ── Счёт 71.01 (только Дт-сальдо) ──
print(f"\n[D] Дебетовое сальдо 71.01 на {P_END[:10]} (Кт/ноль исключены):")
row71 = f"  {'71.01 Дт сальдо':<22}"
for dk in DEPT_ORDER:
    v = bal_7101.get(dk, 0)
    row71 += f"{v:>14,.2f}"
row71 += f"{sum(bal_7101.values()):>14,.2f}"
print(row71)

# ═══════════════════════════════════════════════════════════
#  ИТОГ ПО ПОДРАЗДЕЛЕНИЯМ
# ═══════════════════════════════════════════════════════════
print(f"\n{'═' * 78}")
print(f"  ИТОГО РАСХОДЫ ПО ПОДРАЗДЕЛЕНИЯМ")
print(f"{'═' * 78}")

print(f"\n  {'Подразделение':<44} {'44(без ВРТ)':>14} {'ВРТ/СДС':>14} {'Сч.51':>14} {'71.01':>14} {'ИТОГО':>14}")
print(f"  {'─' * 114}")

grand_total = 0.0
for dk in DEPT_ORDER:
    dname = DEPARTMENTS[dk]
    # 44 без ВРТ = сумма по 8 статьям (не входящим в VRT_KEYS)
    s44_non_vrt = sum(matrix44[dk][ak] for ak in ART_44_ORDER if ak not in VRT_KEYS)
    s_vrt_eff = effective_by_dept[dk]
    s_51 = acc51_by_dept.get(dk, 0)
    s_71 = bal_7101.get(dk, 0)
    total_dept = s44_non_vrt + s_vrt_eff + s_51 + s_71
    grand_total += total_dept

    flag_str = " *СДС" if replace_flags[dk] else ""
    print(f"  {dname:<44} {s44_non_vrt:>14,.2f} {s_vrt_eff:>14,.2f} {s_51:>14,.2f} {s_71:>14,.2f} {total_dept:>14,.2f}{flag_str}")

print(f"  {'─' * 114}")
print(f"  {'ИТОГО':>44} "
      f"{sum(sum(matrix44[dk][ak] for ak in ART_44_ORDER if ak not in VRT_KEYS) for dk in DEPT_ORDER):>14,.2f} "
      f"{sum(effective_by_dept.values()):>14,.2f} "
      f"{sum(acc51_by_dept.values()):>14,.2f} "
      f"{sum(bal_7101.values()):>14,.2f} "
      f"{grand_total:>14,.2f}")

# ── Детализация СДС ──
print(f"\n{'─' * 78}")
print(f"  Детализация СДС (3 статьи списания ДС):")
for dk in DEPT_ORDER:
    if dk in sds_detail:
        print(f"\n  {DEPARTMENTS[dk]}:")
        for sk, sv in sds_detail[dk].items():
            print(f"    {ART_SDS[sk][:60]}: {sv:,.2f}")

if sds_no_dept:
    print(f"\n  БЕЗ ПОДРАЗДЕЛЕНИЯ (не включено в итог):")
    for sk, sv in sds_no_dept.items():
        print(f"    {ART_SDS[sk][:60]}: {sv:,.2f}")

# ── Отсутствующие данные ──
print(f"\n{'─' * 78}")
print(f"  Контроль:")
print(f"  Дт-оборот 44 (все подразделения): {total_all_44:,.2f}")
print(f"  В т.ч. наши 8 подразделений:      {grand_44_our:,.2f}")
print(f"  Время: {time.time()-T0:.1f}с")

missing = []
for dk in DEPT_ORDER:
    for ak in ART_44_ORDER:
        if matrix44[dk][ak] == 0:
            missing.append((DEPT_SHORT[dk], ART_44_SHORT[ak]))
if missing:
    print(f"\n  Нулевые комбинации (подразделение × статья 44): {len(missing)} из {len(DEPT_ORDER)*len(ART_44_ORDER)}")
