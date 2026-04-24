"""
calc_fot_techdir.py — ФОТ технического директора и его подразделений.

Считает ФОТ по регистру бухгалтерии (Хозрасчётный) на основании
дебетового оборота по зарплатным статьям затрат:
  - "Оплата труда ..."
  - "Страховые взносы ..."

Для техдирекции расчёт идёт по счетам:
  - 25   (Общепроизводственные расходы)
  - 26   (Общехозяйственные расходы)
  - 44.02
и их субсчетам.

Подразделения ищутся в Catalog_СтруктураПредприятия по актуальным
названиям/алиасам, после чего в расчёт попадает весь их дочерний контур.

Использование:
  python calc_fot_techdir.py [ГГГГ-ММ]   # по умолчанию 2026-03
"""

import functools
import re
import sys
import time
from collections import defaultdict
from datetime import date
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

import requests

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

# Корневые счета для ФОТ техдирекции.
ACCOUNT_ROOTS = {
    "25": "fb2bde42-6250-11e7-812d-001e67112509",
    "26": "fb2bde43-6250-11e7-812d-001e67112509",
    "44.02": "fb2bde56-6250-11e7-812d-001e67112509",
}

SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

GROUPS = {
    "Технический директор": [
        "технический директор",
    ],
    "Заместитель тех. директора по качеству": [
        "заместитель тех директора по качеству",
        "зам тех директора по качеству",
        "зам технического директора по качеству",
    ],
    "Зам. технического директора по сервису": [
        "зам технического директора по сервису",
        "заместитель технического директора по сервису",
    ],
    "Сектор качества разработки": [
        "сектор качества разработки",
    ],
    "Зам. технического директора по проектированию, автоматизации и метрологии": [
        "зам технического директора по проектированию автоматизации и метрологии",
        "заместитель технического директора по проектированию автоматизации и метрологии",
        "сектор разработки тех решений",
    ],
    "Заместитель тех.директора по пром.безопасности": [
        "заместитель тех директора по пром безопасности",
        "зам тех директора по пром безопасности",
        "сектор промышленной безопасности",
    ],
    "Эксплуатационная служба": [
        "эксплуатационная служба",
    ],
}

GROUP_ORDER = list(GROUPS.keys())

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def normalize_name(value: str) -> str:
    value = (value or "").lower().replace("ё", "е")
    value = re.sub(r"[^0-9a-zа-я]+", " ", value)
    return " ".join(value.split())


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

    if month == 12:
        p_start = f"{year}-12-01T00:00:00"
        p_end = f"{year + 1}-01-01T00:00:00"
    else:
        p_start = f"{year}-{month:02d}-01T00:00:00"
        p_end = f"{year}-{month + 1:02d}-01T00:00:00"

    return year, month, p_start, p_end


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


def load_structure(session: requests.Session):
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,DeletionMark"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url)
    by_key = {}
    by_parent = defaultdict(list)
    exact_index = defaultdict(list)

    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        by_parent[row.get("Parent_Key")].append(row)
        exact_index[normalize_name(row.get("Description", ""))].append(row)

    return rows, by_key, by_parent, exact_index


def pick_best_candidate(candidates):
    if not candidates:
        return None
    candidates = sorted(
        candidates,
        key=lambda row: (
            1 if row.get("DeletionMark") else 0,
            len(normalize_name(row.get("Description", ""))),
            row.get("Description", ""),
        ),
    )
    return candidates[0]


def resolve_group_roots(rows, exact_index):
    resolved = {}
    unresolved = []

    for display_name, aliases in GROUPS.items():
        found = None

        for alias in aliases:
            found = pick_best_candidate(exact_index.get(normalize_name(alias), []))
            if found:
                break

        if not found:
            for alias in aliases:
                alias_norm = normalize_name(alias)
                contains = [
                    row for row in rows
                    if alias_norm in normalize_name(row.get("Description", ""))
                ]
                found = pick_best_candidate(contains)
                if found:
                    break

        if not found:
            unresolved.append(display_name)
            continue

        resolved[display_name] = found

    return resolved, unresolved


def collect_subtree(root_key: str, by_parent):
    keys = set()
    stack = [root_key]
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


def get_subaccounts(session: requests.Session, parent_guid: str):
    collected = {parent_guid}
    frontier = [parent_guid]

    while frontier:
        parent = frontier.pop()
        flt = f"Parent_Key eq guid'{parent}'"
        url = (
            f"{BASE}/{quote('ChartOfAccounts_Хозрасчетный')}"
            f"?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select=Ref_Key,Code,Description,Parent_Key"
        )
        rows = fetch_all(session, url, page=200)
        for row in rows:
            key = row.get("Ref_Key")
            if key and key not in collected:
                collected.add(key)
                frontier.append(key)

    return collected


def load_cost_articles(session: requests.Session):
    url = (
        f"{BASE}/{quote('ChartOfCharacteristicTypes_СтатьиРасходов')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url)
    return {row["Ref_Key"]: (row.get("Description") or "").strip() for row in rows if row.get("Ref_Key")}


def is_fot_article(description: str) -> bool:
    text = normalize_name(description)
    if not text:
        return False
    if "оплата труда" not in text and "страховые взносы" not in text:
        return False
    if "архив" in text or "не использовать" in text or "не использов" in text:
        return False
    return True


def article_bucket(description: str) -> str:
    text = normalize_name(description)
    if "оплата труда" in text:
        return "salary"
    if "страховые взносы" in text:
        return "insurance"
    return "other"


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def compute_td_fot_fact(year: int, month: int) -> dict:
    if month == 12:
        p_start = f"{year}-12-01T00:00:00"
        p_end = f"{year + 1}-01-01T00:00:00"
    else:
        p_start = f"{year}-{month:02d}-01T00:00:00"
        p_end = f"{year}-{month + 1:02d}-01T00:00:00"

    session = requests.Session()
    session.auth = AUTH

    structure_rows, by_key, by_parent, exact_index = load_structure(session)
    group_roots, unresolved = resolve_group_roots(structure_rows, exact_index)
    if unresolved:
        raise RuntimeError(f"Не удалось найти подразделения: {', '.join(unresolved)}")

    group_subtrees = {}
    for display_name in GROUP_ORDER:
        root = group_roots[display_name]
        root_key = root["Ref_Key"]
        group_subtrees[display_name] = collect_subtree(root_key, by_parent)

    target_accounts = set()
    for guid in ACCOUNT_ROOTS.values():
        target_accounts.update(get_subaccounts(session, guid))

    article_names = load_cost_articles(session)

    account_filter = " or ".join(
        f"AccountDr_Key eq guid'{key}'" for key in sorted(target_accounts)
    )
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true"
        f" and ({account_filter})"
    )
    sel = (
        "Period,AccountDr_Key,ПодразделениеDr_Key,Сумма,Сторно,"
        "ExtDimensionDr1,ExtDimensionTypeDr1_Key"
    )
    url = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}"
        f"/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = fetch_all(session, url)

    totals = defaultdict(lambda: {"salary": 0.0, "insurance": 0.0, "total": 0.0, "rows": 0})
    article_totals = defaultdict(float)
    skipped_no_dept = 0
    skipped_not_fot = 0

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        if not dept_key or dept_key == EMPTY:
            skipped_no_dept += 1
            continue

        if rec.get("ExtDimensionTypeDr1_Key") != SUBCONTO_TYPE_COST:
            skipped_not_fot += 1
            continue

        article_key = rec.get("ExtDimensionDr1")
        article_name = article_names.get(article_key, "")
        if not is_fot_article(article_name):
            skipped_not_fot += 1
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        bucket = article_bucket(article_name)
        if bucket not in ("salary", "insurance"):
            continue

        article_totals[article_name] += amount

        for display_name in GROUP_ORDER:
            if dept_key not in group_subtrees[display_name]:
                continue
            totals[display_name][bucket] += amount
            totals[display_name]["total"] += amount
            totals[display_name]["rows"] += 1

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month],
        "groups": {name: dict(totals[name]) for name in GROUP_ORDER},
        "total_fact": round(totals["Технический директор"]["total"], 2),
        "article_totals": dict(article_totals),
        "skipped_no_dept": skipped_no_dept,
        "skipped_not_fot": skipped_not_fot,
    }


def get_td_fot_fact_monthly(year: int | None = None, month: int | None = None) -> dict:
    if year is None or month is None:
        year, month = _last_full_month()
    return compute_td_fot_fact(year, month)


def main():
    year, month, p_start, p_end = parse_period()
    session = requests.Session()
    session.auth = AUTH
    t0 = time.time()

    print(f"\n{'=' * 88}")
    print(f"  ФОТ ТЕХНИЧЕСКОГО ДИРЕКТОРА · {MONTH_RU[month]} {year}")
    print(f"  Период: {p_start[:10]} — {p_end[:10]} (lt)")
    print(f"{'=' * 88}")

    print("\n[1] Загрузка структуры предприятия ...")
    structure_rows, by_key, by_parent, exact_index = load_structure(session)
    print(f"  Подразделений загружено: {len(structure_rows)}")

    group_roots, unresolved = resolve_group_roots(structure_rows, exact_index)
    if unresolved:
        print("  Не удалось найти подразделения:")
        for name in unresolved:
            print(f"    - {name}")
        sys.exit(1)

    group_subtrees = {}
    for display_name in GROUP_ORDER:
        root = group_roots[display_name]
        root_key = root["Ref_Key"]
        group_subtrees[display_name] = collect_subtree(root_key, by_parent)

    print("  Целевые корни:")
    for display_name in GROUP_ORDER:
        root = group_roots[display_name]
        subtree_size = len(group_subtrees[display_name])
        print(
            f"    - {display_name} -> {root.get('Description')} "
            f"({subtree_size} узл.)"
        )

    print("\n[2] Подготовка счетов ФОТ ...")
    target_accounts = set()
    for code, guid in ACCOUNT_ROOTS.items():
        subs = get_subaccounts(session, guid)
        target_accounts.update(subs)
        print(f"  Счёт {code}: {len(subs)} узл. в иерархии")

    print("\n[3] Загрузка статей затрат ...")
    article_names = load_cost_articles(session)
    print(f"  Статей затрат загружено: {len(article_names)}")

    print("\n[4] Загрузка проводок RecordsWithExtDimensions ...")
    account_filter = " or ".join(
        f"AccountDr_Key eq guid'{key}'" for key in sorted(target_accounts)
    )
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true"
        f" and ({account_filter})"
    )
    sel = (
        "Period,AccountDr_Key,ПодразделениеDr_Key,Сумма,Сторно,"
        "ExtDimensionDr1,ExtDimensionTypeDr1_Key"
    )
    url = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}"
        f"/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = fetch_all(session, url)
    print(f"  Проводок загружено: {len(records)}")

    print("\n[5] Агрегация ФОТ по техдирекции ...")
    totals = defaultdict(lambda: {"salary": 0.0, "insurance": 0.0, "total": 0.0, "rows": 0})
    article_totals = defaultdict(float)
    skipped_no_dept = 0
    skipped_not_fot = 0

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        if not dept_key or dept_key == EMPTY:
            skipped_no_dept += 1
            continue

        if rec.get("ExtDimensionTypeDr1_Key") != SUBCONTO_TYPE_COST:
            skipped_not_fot += 1
            continue

        article_key = rec.get("ExtDimensionDr1")
        article_name = article_names.get(article_key, "")
        if not is_fot_article(article_name):
            skipped_not_fot += 1
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        bucket = article_bucket(article_name)
        if bucket not in ("salary", "insurance"):
            continue

        article_totals[article_name] += amount

        for display_name in GROUP_ORDER:
            if dept_key not in group_subtrees[display_name]:
                continue
            totals[display_name][bucket] += amount
            totals[display_name]["total"] += amount
            totals[display_name]["rows"] += 1

    print(f"  Пропущено без подразделения: {skipped_no_dept}")
    print(f"  Пропущено не-ФОТ статей/субконто: {skipped_not_fot}")

    print(f"\n{'=' * 100}")
    print(f"  ФОТ ТЕХНИЧЕСКОГО ДИРЕКТОРА — {MONTH_RU[month]} {year}")
    print(f"{'=' * 100}")
    print(f"  {'Подразделение':<70} {'Оплата труда':>14} {'Страх. взносы':>16} {'ИТОГО ФОТ':>14}")
    print(f"  {'-' * 70} {'-' * 14} {'-' * 16} {'-' * 14}")

    for display_name in GROUP_ORDER:
        salary = totals[display_name]["salary"]
        insurance = totals[display_name]["insurance"]
        total = totals[display_name]["total"]
        print(f"  {display_name:<70} {salary:>14,.2f} {insurance:>16,.2f} {total:>14,.2f}")

    print(f"\n  ИТОГО по контуру 'Технический директор': {totals['Технический директор']['total']:,.2f} руб.")

    if article_totals:
        print("\n  Статьи, вошедшие в расчёт:")
        for name, amount in sorted(article_totals.items(), key=lambda item: -abs(item[1])):
            print(f"    {amount:>14,.2f}  {name}")

    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
