"""
inspect_plan.py — плановый ФОТ техдирекции по плановым начислениям.

Источник:
  - InformationRegister_ПлановыеНачисления_RecordType
  - InformationRegister_КадроваяИсторияСотрудников_RecordType
  - Catalog_ПодразделенияОрганизаций
  - ChartOfCalculationTypes_Начисления

Логика:
  1. Берём активные плановые начисления на конец месяца.
  2. Для каждого сотрудника определяем актуальное оргподразделение по кадровой истории.
  3. Агрегируем суммы по контуру технического директора и его веткам.

Важно:
  - Это именно план по начислениям, а не бюджетная статья "Налог на заработную плату".
  - Для plan используется оргструктура из Catalog_ПодразделенияОрганизаций,
    а не Catalog_СтруктураПредприятия, как в fact-скрипте.

Использование:
  python inspect_plan.py [ГГГГ-ММ]   # по умолчанию 2026-03
"""

import functools
import sys
import time
from calendar import monthrange
from collections import defaultdict
from datetime import date
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

import requests

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

# Корни в Catalog_ПодразделенияОрганизаций, подобранные по фактической оргструктуре.
PLAN_GROUP_ROOTS = {
    "Технический директор": [
        "f348c51e-d57f-11ea-83c5-ac1f6b05524c",  # Служба технического директора
        "b338c792-f396-11e8-8283-ac1f6b05524d",  # Служба качества
        "a37213b5-e6f1-11e7-826b-ac1f6b05524d",  # Сервисная служба
        "e21ab3fd-f82d-11ef-9621-6cb31113810e",  # Служба автоматизации
        "8cf17e48-df91-11e9-829b-ac1f6b05524d",  # Метрологическая служба
        "22882cd7-d5fc-11e9-829b-ac1f6b05524d",  # Отдел метрологии и сертификации
    ],
    "Заместитель тех. директора по качеству": [
        "b338c792-f396-11e8-8283-ac1f6b05524d",  # Служба качества
    ],
    "Зам. технического директора по сервису": [
        "a37213b5-e6f1-11e7-826b-ac1f6b05524d",  # Сервисная служба
    ],
    "Сектор качества разработки": [
        "75d572f4-e4bd-11f0-976f-6cb31113810e",  # старый ключ
        "5d1da688-f077-11f0-977e-6cb31113810e",  # новый ключ
    ],
    "Зам. технического директора по проектированию, автоматизации и метрологии": [
        "6077b515-f396-11e8-8283-ac1f6b05524d",  # Проектный офис
        "af809fa2-e4bd-11f0-976f-6cb31113810e",  # Сектор разработки технических решений
        "e21ab3fd-f82d-11ef-9621-6cb31113810e",  # Служба автоматизации
        "8cf17e48-df91-11e9-829b-ac1f6b05524d",  # Метрологическая служба
        "22882cd7-d5fc-11e9-829b-ac1f6b05524d",  # Отдел метрологии и сертификации
    ],
    "Заместитель тех.директора по пром.безопасности": [
        "d6669ecf-e4bd-11f0-976f-6cb31113810e",  # Сектор промышленной безопасности
    ],
    "Эксплуатационная служба": [
        "07cf0cbf-d580-11ea-83c5-ac1f6b05524c",  # Эксплуатационная служба
    ],
}

GROUP_ORDER = list(PLAN_GROUP_ROOTS.keys())

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def parse_period():
    period_arg = None
    for arg in sys.argv[1:]:
        if len(arg) == 7 and arg[4] == "-":
            period_arg = arg
            break

    if period_arg:
        year, month = int(period_arg[:4]), int(period_arg[5:7])
    else:
        year, month = 2026, 3

    last_day = monthrange(year, month)[1]
    target_ts = f"{year}-{month:02d}-{last_day:02d}T23:59:59"
    month_start = f"{year}-{month:02d}-01"
    return year, month, target_ts, month_start


def fetch_all(session: requests.Session, url: str, page: int = 5000):
    rows = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        r = session.get(page_url, timeout=120)
        if not r.ok:
            print(f"  HTTP {r.status_code}: {r.text[:300]}")
            sys.exit(1)
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def load_org_depts(session: requests.Session):
    url = (
        f"{BASE}/{quote('Catalog_ПодразделенияОрганизаций')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,Owner_Key"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url)
    by_key = {}
    by_parent = defaultdict(list)
    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        by_parent[row.get("Parent_Key")].append(row)
    return rows, by_key, by_parent


def collect_subtree(root_keys, by_parent):
    keys = set()
    stack = list(root_keys)
    while stack:
        current = stack.pop()
        if current in keys:
            continue
        keys.add(current)
        for child in by_parent.get(current, []):
            child_key = child.get("Ref_Key")
            if child_key:
                stack.append(child_key)
    return keys


def load_group_subtrees(by_parent):
    result = {}
    for display_name, root_keys in PLAN_GROUP_ROOTS.items():
        result[display_name] = collect_subtree(root_keys, by_parent)
    return result


def load_hr_assignments(session: requests.Session, target_ts: str):
    flt = f"Period le datetime'{target_ts}' and Active eq true"
    sel = "Period,Сотрудник_Key,Подразделение_Key,ДействуетДо"
    url = (
        f"{BASE}/{quote('InformationRegister_КадроваяИсторияСотрудников_RecordType')}"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    rows = fetch_all(session, url)

    employee_dept = {}
    employee_period = {}

    for row in rows:
        employee_key = row.get("Сотрудник_Key")
        dept_key = row.get("Подразделение_Key")
        period = row.get("Period") or ""
        valid_to = row.get("ДействуетДо") or "0001-01-01T00:00:00"

        if not employee_key or not dept_key:
            continue
        if valid_to[:4] != "0001" and valid_to < target_ts[:10]:
            continue
        if employee_key not in employee_period or period > employee_period[employee_key]:
            employee_period[employee_key] = period
            employee_dept[employee_key] = dept_key

    return employee_dept


def load_accrual_names(session: requests.Session):
    url = (
        f"{BASE}/{quote('ChartOfCalculationTypes_Начисления')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url)
    return {row["Ref_Key"]: (row.get("Description") or "").strip() for row in rows if row.get("Ref_Key")}


def load_plan_accruals(session: requests.Session, target_ts: str):
    flt = f"Period le datetime'{target_ts}' and Используется eq true"
    sel = "Period,Сотрудник_Key,Начисление_Key,Размер,ДействуетДо"
    url = (
        f"{BASE}/{quote('InformationRegister_ПлановыеНачисления_RecordType')}"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    rows = fetch_all(session, url)

    latest = {}
    for row in rows:
        employee_key = row.get("Сотрудник_Key")
        accrual_key = row.get("Начисление_Key")
        period = row.get("Period") or ""
        valid_to = row.get("ДействуетДо") or "0001-01-01T00:00:00"

        if not employee_key or not accrual_key:
            continue
        if valid_to[:4] != "0001" and valid_to < target_ts[:10]:
            continue

        pair_key = (employee_key, accrual_key)
        if pair_key not in latest or period > (latest[pair_key].get("Period") or ""):
            latest[pair_key] = row

    return latest


def is_relevant_plan_accrual(description: str) -> bool:
    if not description:
        return False
    text = description.lower()
    if "не использовать" in text:
        return False
    return True


def accrual_bucket(description: str) -> str:
    text = description.lower()
    if text in {"оплата по окладу", "оплата по часовому тарифу"}:
        return "base"
    return "other"


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def compute_td_fot_plan(year: int, month: int) -> dict:
    last_day = monthrange(year, month)[1]
    target_ts = f"{year}-{month:02d}-{last_day:02d}T23:59:59"

    session = requests.Session()
    session.auth = AUTH

    _, _, by_parent = load_org_depts(session)
    group_subtrees = load_group_subtrees(by_parent)
    employee_dept = load_hr_assignments(session, target_ts)
    accrual_names = load_accrual_names(session)
    latest_plan_accruals = load_plan_accruals(session, target_ts)

    totals = defaultdict(lambda: {"base": 0.0, "other": 0.0, "total": 0.0})
    employees = defaultdict(set)
    accrual_totals = defaultdict(float)
    skipped_no_hr = 0
    skipped_not_target = 0

    all_target_depts = set()
    for keys in group_subtrees.values():
        all_target_depts.update(keys)

    for (employee_key, accrual_key), row in latest_plan_accruals.items():
        dept_key = employee_dept.get(employee_key)
        if not dept_key:
            skipped_no_hr += 1
            continue
        if dept_key not in all_target_depts:
            skipped_not_target += 1
            continue

        amount = float(row.get("Размер") or 0)
        if amount == 0:
            continue

        accrual_name = accrual_names.get(accrual_key, "")
        if not is_relevant_plan_accrual(accrual_name):
            continue

        bucket = accrual_bucket(accrual_name)
        accrual_totals[accrual_name] += amount

        for group_name in GROUP_ORDER:
            if dept_key not in group_subtrees[group_name]:
                continue
            totals[group_name][bucket] += amount
            totals[group_name]["total"] += amount
            employees[group_name].add(employee_key)

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month],
        "groups": {name: dict(totals[name]) for name in GROUP_ORDER},
        "total_plan": round(totals["Технический директор"]["total"], 2),
        "employee_counts": {name: len(employees[name]) for name in GROUP_ORDER},
        "accrual_totals": dict(accrual_totals),
        "skipped_no_hr": skipped_no_hr,
        "skipped_not_target": skipped_not_target,
    }


def get_td_fot_plan_monthly(year: int | None = None, month: int | None = None) -> dict:
    if year is None or month is None:
        year, month = _last_full_month()
    return compute_td_fot_plan(year, month)


def main():
    year, month, target_ts, month_start = parse_period()
    session = requests.Session()
    session.auth = AUTH
    t0 = time.time()

    print(f"\n{'=' * 92}")
    print(f"  ПЛАНОВЫЙ ФОТ ТЕХНИЧЕСКОГО ДИРЕКТОРА · {MONTH_RU[month]} {year}")
    print(f"  Срез на дату: {target_ts[:10]}")
    print(f"{'=' * 92}")

    print("\n[1] Загрузка оргподразделений ...")
    _, dept_by_key, by_parent = load_org_depts(session)
    group_subtrees = load_group_subtrees(by_parent)
    for group_name in GROUP_ORDER:
        print(f"  {group_name}: {len(group_subtrees[group_name])} оргподразделений")

    print("\n[2] Загрузка кадровой истории ...")
    employee_dept = load_hr_assignments(session, target_ts)
    print(f"  Актуальных назначений сотрудников: {len(employee_dept)}")

    print("\n[3] Загрузка начислений ...")
    accrual_names = load_accrual_names(session)
    latest_plan_accruals = load_plan_accruals(session, target_ts)
    print(f"  Активных плановых начислений (последние версии): {len(latest_plan_accruals)}")

    print("\n[4] Агрегация планового ФОТ ...")
    totals = defaultdict(lambda: {"base": 0.0, "other": 0.0, "total": 0.0})
    employees = defaultdict(set)
    accrual_totals = defaultdict(float)
    skipped_no_hr = 0
    skipped_not_target = 0

    all_target_depts = set()
    for keys in group_subtrees.values():
        all_target_depts.update(keys)

    for (employee_key, accrual_key), row in latest_plan_accruals.items():
        dept_key = employee_dept.get(employee_key)
        if not dept_key:
            skipped_no_hr += 1
            continue
        if dept_key not in all_target_depts:
            skipped_not_target += 1
            continue

        amount = float(row.get("Размер") or 0)
        if amount == 0:
            continue

        accrual_name = accrual_names.get(accrual_key, "")
        if not is_relevant_plan_accrual(accrual_name):
            continue

        bucket = accrual_bucket(accrual_name)
        accrual_totals[accrual_name] += amount

        for group_name in GROUP_ORDER:
            if dept_key not in group_subtrees[group_name]:
                continue
            totals[group_name][bucket] += amount
            totals[group_name]["total"] += amount
            employees[group_name].add(employee_key)

    print(f"  Без кадрового подразделения: {skipped_no_hr}")
    print(f"  Вне контура техдирекции: {skipped_not_target}")

    print(f"\n{'=' * 112}")
    print(f"  ПЛАНОВЫЙ ФОТ ТЕХДИРЕКЦИИ — {MONTH_RU[month]} {year}")
    print(f"{'=' * 112}")
    print(
        f"  {'Подразделение':<70} {'Оклад/тариф':>14} "
        f"{'Прочие начисл.':>16} {'ИТОГО ПЛАН':>14}"
    )
    print(f"  {'-' * 70} {'-' * 14} {'-' * 16} {'-' * 14}")

    for group_name in GROUP_ORDER:
        base = totals[group_name]["base"]
        other = totals[group_name]["other"]
        total = totals[group_name]["total"]
        print(f"  {group_name:<70} {base:>14,.2f} {other:>16,.2f} {total:>14,.2f}")

    print(
        f"\n  ИТОГО по контуру 'Технический директор': "
        f"{totals['Технический директор']['total']:,.2f} руб."
    )

    if accrual_totals:
        print("\n  Плановые начисления, вошедшие в расчёт:")
        for name, amount in sorted(accrual_totals.items(), key=lambda item: -abs(item[1])):
            print(f"    {amount:>14,.2f}  {name}")

    print("\n  Количество сотрудников в группах:")
    for group_name in GROUP_ORDER:
        print(f"    {group_name}: {len(employees[group_name])}")

    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
