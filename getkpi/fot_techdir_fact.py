"""
ФОТ техдира — факт (19 п/п, сч. 26, п. 4.1–4.2) + общие константы/загрузка структуры.

Считает: AccountingRegister_Хозрасчетный / RecordsWithExtDimensions (дебет, сч. 26,
статьи п. 4.2, подразделения — Catalog_СтруктураПредприятия).

Использование:
  python fot_techdir_fact.py [ГГГГ-ММ]   # по умолчанию 2026-01

План: см. fot_techdir_plan.py. Другие скрипты (заявки, бюджет) импортируют BASE, FOT_GROUP_ORDER
и load_fot_spec_structure_map отсюда.
"""

from __future__ import annotations

import re
import sys
import time
from collections import defaultdict
from datetime import date
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from .odata_http import request_with_retry

# ----- OData / общие ---------------------------------------------------------

# Совпадает с остальными techdir-скриптами (OData 1С).
BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
EMPTY = "00000000-0000-0000-0000-000000000000"

AUTH = HTTPBasicAuth("odata.user", "npo852456")

ACCOUNT_ROOTS_FACT = {
    "26": "fb2bde43-6250-11e7-812d-001e67112509",
}

SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

FOT_SPEC_ARTICLES = (
    "Оплата труда (26 сч) НПО АУП!",
    "Страховые взносы (26 сч) НПО АУП!",
)

# План (Catalog_СтатьиБюджетов): в fot_techdir_plan, не в факте; держим здесь для одного модуля спецификации.
FOT_BUDGET_ARTICLE_ADDONS: tuple[str, ...] = (
    "ЦФО БМИ",
    "ЦФО ПЦ2",
)

FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    ("Эксплуатационная служба", ("эксплуатационная служба",)),
    ("Электрик/энергетик", ("электрик/энергетик", "электрик энергетик")),
    (
        "Зам. технического директора по качеству",
        (
            "зам. технического директора по качеству",
            "зам технического директора по качеству",
            "заместитель тех. директора по качеству",
            "заместитель технического директора по качеству",
        ),
    ),
    ("Специалист по процессному управлению", ("специалист по процессному управлению",)),
    ("ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ", ("заместитель директора по качеству",)),
    (
        "Лаборатория неразрушающего контроля",
        ("лаборатория неразрушающего контроля",),
    ),
    (
        "Отдел управления несоответствиями",
        ("отдел управления несоответствиями",),
    ),
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    ("АХО", ("ахо",)),
    (
        "Зам. технического директора по сервису",
        (
            "зам. технического директора по сервису",
            "зам технического директора по сервису",
            "заместитель технического директора по сервису",
        ),
    ),
    (
        "Отдел по работе с рекламациями",
        ("отдел по работе с рекламациями",),
    ),
    ("Отдел технической поддержки", ("отдел технической поддержки",)),
    ("Сервисная служба", ("сервисная служба",)),
    (
        "Отдел сервисного обслуживания",
        ("отдел сервисного обслуживания",),
    ),
    ("Служба технического директора", ("служба технического директора",)),
    ("Сектор качества разработки", ("сектор качества разработки",)),
    (
        "Сектор разработки тех. Решений",
        (
            "сектор разработки тех. решений",
            "сектор разработки тех решений",
        ),
    ),
    (
        "Сектор промышленной безопасности",
        ("сектор промышленной безопасности",),
    ),
]

FOT_GROUP_ORDER = [t[0] for t in FOT_SPEC]

MONTH_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

FOT_PLAN_ORG_ROOT_OVERRIDES: dict[str, str] = {}


def normalize_name(value: str) -> str:
    if value is None:
        return ""
    s = re.sub(r"\s+", " ", (value or "").strip())
    s = s.lower().replace("ё", "е")
    s = re.sub(r"[^0-9a-zа-я]+", " ", s)
    return " ".join(s.split())


_FOT_SPEC_ARTICLE_NORMS = frozenset(normalize_name(s) for s in FOT_SPEC_ARTICLES)
_FOT_SALARY_NORM = normalize_name(FOT_SPEC_ARTICLES[0])
FOT_BUDGET_ARTICLE_ADDON_NORMS = frozenset(
    normalize_name(s) for s in FOT_BUDGET_ARTICLE_ADDONS
)


def fetch_all(session, url: str, page: int = 5000, timeout: int = 120):
    rows = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        r = request_with_retry(session, page_url, timeout=timeout, retries=4, label="fot_techdir_fact")
        if r is None:
            print("  HTTP no-response after retries")
            return rows
        if not r.ok:
            print(f"  HTTP {r.status_code}: {r.text[:300]}")
            return rows
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def pick_best_candidate(candidates):
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda row: (
            1 if row.get("DeletionMark") else 0,
            len(normalize_name(row.get("Description", ""))),
            row.get("Description", ""),
        ),
    )[0]


def resolve_department_row(rows, exact_index, display_name: str, aliases: tuple[str, ...]):
    search = (display_name,) + aliases
    for alias in search:
        found = pick_best_candidate(exact_index.get(normalize_name(alias), []))
        if found:
            return found
    for alias in search:
        alias_norm = normalize_name(alias)
        contains = [
            row
            for row in rows
            if alias_norm in normalize_name(row.get("Description", ""))
        ]
        found = pick_best_candidate(contains)
        if found:
            return found
    raw_parts = re.split(r"[/\\|]+", display_name)
    if len(raw_parts) >= 2:
        p0, p1 = normalize_name(raw_parts[0]), normalize_name(raw_parts[1])
        if len(p0) >= 2 and len(p1) >= 2:
            dual = [
                row
                for row in rows
                if p0 in normalize_name(row.get("Description", ""))
                and p1 in normalize_name(row.get("Description", ""))
            ]
            found = pick_best_candidate(dual)
            if found:
                return found
    return None


def load_fot_spec_structure_map(session):
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,DeletionMark"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url, timeout=60)
    exact_index = defaultdict(list)
    for row in rows:
        exact_index[normalize_name(row.get("Description", ""))].append(row)

    name_to_key: dict[str, str] = {}
    name_to_structure_label: dict[str, str] = {}
    key_first_name: dict[str, str] = {}
    for display_name, aliases in FOT_SPEC:
        found = resolve_department_row(rows, exact_index, display_name, aliases)
        if not found:
            print(f"  Не удалось найти подразделение (структура): {display_name}")
            continue
        k = found["Ref_Key"]
        if k in key_first_name and key_first_name[k] != display_name:
            print(
                f"  Один Ref_Key у двух строк перечня: {key_first_name[k]!r} и {display_name!r}"
            )
            continue
        key_first_name[k] = display_name
        name_to_key[display_name] = k
        name_to_structure_label[display_name] = (found.get("Description") or "").strip()

    return name_to_key, name_to_structure_label


def load_structure(session):
    """Вся структура предприятия: строки, по Ref_Key, по Parent_Key, индекс по имени."""
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,DeletionMark"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url, timeout=60)
    by_key: dict[str, dict] = {}
    by_parent: defaultdict = defaultdict(list)
    exact_index: defaultdict = defaultdict(list)
    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        by_parent[row.get("Parent_Key")].append(row)
        exact_index[normalize_name(row.get("Description", ""))].append(row)
    return rows, by_key, by_parent, exact_index


def build_struct_key_to_fot_group(
    name_to_key: dict[str, str], by_key
) -> dict[str, str]:
    """
    Любой Ref_Key структуры -> одна из 19 карточек ФОТ, если узел лежит в её поддереве.
    Это нужно для fact: проводки часто приходят на дочерние подразделения, а не на
    саму "карточку" 4.1.
    """
    spec_keys = frozenset(name_to_key.values())
    ref_to_display = {v: k for k, v in name_to_key.items()}
    out: dict[str, str] = {}
    for k in by_key:
        cur = k
        seen: set[str] = set()
        while cur and cur not in seen and cur != EMPTY:
            seen.add(cur)
            if cur in spec_keys:
                out[k] = ref_to_display[cur]
                break
            row = by_key.get(cur)
            if not row:
                break
            par = row.get("Parent_Key")
            if not par or par == EMPTY:
                break
            cur = par
    return out


def collect_plan_subtrees(by_parent, plan_group_roots: dict[str, list[str]]):
    result = {}
    for display_name, root_keys in plan_group_roots.items():
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
        result[display_name] = keys
    return result


def load_plan_group_subtrees_for_spec(session, name_to_structure_label: dict[str, str]):
    url = (
        f"{BASE}/{quote('Catalog_ПодразделенияОрганизаций')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,Owner_Key"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url, timeout=60)
    by_parent: defaultdict = defaultdict(list)
    for row in rows:
        by_parent[row.get("Parent_Key")].append(row)
    exact_index = defaultdict(list)
    for row in rows:
        exact_index[normalize_name(row.get("Description", ""))].append(row)

    plan_group_roots: dict[str, list[str]] = {}
    org_unresolved: list[str] = []
    for display_name, aliases in FOT_SPEC:
        override = FOT_PLAN_ORG_ROOT_OVERRIDES.get(display_name)
        if override:
            plan_group_roots[display_name] = [override]
            continue
        extra: tuple[str, ...] = ()
        lab = name_to_structure_label.get(display_name)
        if lab:
            extra = (lab,)
        found = resolve_department_row(
            rows, exact_index, display_name, aliases + extra
        )
        if not found:
            org_unresolved.append(display_name)
            plan_group_roots[display_name] = []
            continue
        plan_group_roots[display_name] = [found["Ref_Key"]]
    if org_unresolved:
        print(
            f"  План: не найдено в «ПодразделенияОрганизаций» ({len(org_unresolved)}/19), "
            f"по этим строкам план=0; Ref_Key → FOT_PLAN_ORG_ROOT_OVERRIDES в fot_techdir_fact.py"
        )
        print("   ", "; ".join(org_unresolved))
    return collect_plan_subtrees(by_parent, plan_group_roots)


def get_subaccounts(session, parent_guid: str):
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


def load_cost_articles(session):
    url = (
        f"{BASE}/{quote('ChartOfCharacteristicTypes_СтатьиРасходов')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description"
        f"&$orderby=Ref_Key"
    )
    rows = fetch_all(session, url)
    return {
        row["Ref_Key"]: (row.get("Description") or "").strip()
        for row in rows
        if row.get("Ref_Key")
    }


def fact_bucket_spec(article_key_norm: str):
    if article_key_norm not in _FOT_SPEC_ARTICLE_NORMS:
        return None
    return "salary" if article_key_norm == _FOT_SALARY_NORM else "insurance"


def calc_techdir_spec_reg_fact(
    session, p_start: str, p_end: str, name_to_key: dict[str, str]
):
    """Проводки: сч. 26, статьи п. 4.2, 19 п/п по дереву структуры."""
    from collections import defaultdict as dd

    key_to_name = {v: k for k, v in name_to_key.items()}
    _rows, by_key, _, _ = load_structure(session)
    struct_map = build_struct_key_to_fot_group(name_to_key, by_key)

    target_accounts: set = set()
    for guid in ACCOUNT_ROOTS_FACT.values():
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
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = fetch_all(session, url)

    totals = dd(
        lambda: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
    )
    article_totals = dd(float)

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        if not dept_key or dept_key == EMPTY:
            continue
        if rec.get("ExtDimensionTypeDr1_Key") != SUBCONTO_TYPE_COST:
            continue
        group_name = struct_map.get(dept_key) or key_to_name.get(dept_key)
        if not group_name:
            continue

        article_name = article_names.get(rec.get("ExtDimensionDr1"), "")
        tnorm = normalize_name(article_name)
        bucket = fact_bucket_spec(tnorm)
        if not bucket:
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        article_totals[article_name] += amount
        if bucket == "salary":
            totals[group_name]["fact_salary"] += amount
        else:
            totals[group_name]["fact_insurance"] += amount
        totals[group_name]["fact_total"] += amount

    for gn in FOT_GROUP_ORDER:
        _ = totals[gn]

    return totals, article_totals


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def compute_td_fot_fact_monthly(year: int, month: int) -> dict:
    """Факт ФОТ по 19 п/п (сч. 26, статьи п. 4.2) за календарный месяц."""
    try:
        p_start, p_end = _month_period_bounds(year, month)
        session = requests.Session()
        session.auth = AUTH
        name_to_key, _labels = load_fot_spec_structure_map(session)
        totals, article_totals = calc_techdir_spec_reg_fact(
            session, p_start, p_end, name_to_key
        )
        total_fact = 0.0
        groups_out: dict[str, dict] = {}
        for n in FOT_GROUP_ORDER:
            row = totals.get(n, {})
            s = float(row.get("fact_salary", 0) or 0)
            ins = float(row.get("fact_insurance", 0) or 0)
            t = s + ins
            total_fact += t
            groups_out[n] = {
                "fact_salary": s,
                "fact_insurance": ins,
                "fact_total": t,
            }
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "groups": groups_out,
            "total_fact": round(total_fact, 2),
            "article_totals": dict(article_totals),
            "debug": {
                "status": "ok",
                "kpi_id": "TD-M4-FACT",
                "source": "fot_techdir_fact.py",
            },
        }
    except Exception as exc:
        print(f"  ⚠ TD-M4 fact fallback for {year}-{month:02d}: {exc}")
        groups_out = {
            n: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
            for n in FOT_GROUP_ORDER
        }
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "groups": groups_out,
            "total_fact": 0.0,
            "article_totals": {},
            "debug": {
                "status": "error",
                "kpi_id": "TD-M4-FACT",
                "source": "fot_techdir_fact.py",
                "error": str(exc),
            },
        }


def get_td_fot_fact_monthly(year: int | None = None, month: int | None = None) -> dict:
    if year is None or month is None:
        year, month = _last_full_month()
    return compute_td_fot_fact_monthly(year, month)


# ----- CLI: печать факта (было calc_fot_techdir_depts26) -----------------------

A1 = FOT_SPEC_ARTICLES[0]
A2 = FOT_SPEC_ARTICLES[1]


def print_fact_report(
    year: int,
    month: int,
    p_start: str,
    p_end: str,
    totals: dict,
    t0: float,
) -> None:
    mru = MONTH_RU.get(month, str(month))
    col_w = 18
    print(f"\n{'=' * 110}")
    print(f"  ФОТ технического директора (факт) · п. 4.1–4.2, п. 5 · {mru} {year}")
    print(
        f"  Период: {p_start[:10]} — {p_end[:10]} (lt) · Счёт 26 · Регистр «Хозрасчётный»"
    )
    print(f"{'=' * 110}")

    hdr = (
        f"  {'Подразделение (п. 4.1)':<44}"
        f" {'Оплата труда (26) НПО':>{col_w}}"
        f" {'Страх.взносы (26) НПО':>{col_w}}"
        f" {'Итого п/п':>{col_w}}"
        f" {'% ФОТ':>9}"
    )
    print(hdr)
    print("  " + "-" * 44 + (f" {'-' * col_w}" * 3) + " " + "-" * 8)

    grand = 0.0
    sum_sal = 0.0
    sum_ins = 0.0
    missing: list[tuple[str, str]] = []

    for name in FOT_GROUP_ORDER:
        row = totals.get(name, {})
        s = float(row.get("fact_salary", 0) or 0)
        ins = float(row.get("fact_insurance", 0) or 0)
        rtot = s + ins
        grand += rtot
        sum_sal += s
        sum_ins += ins
        if s == 0.0:
            missing.append((name, A1))
        if ins == 0.0:
            missing.append((name, A2))

    rows_out: list[tuple[str, float, float, float, float]] = []
    for name in FOT_GROUP_ORDER:
        row = totals.get(name, {})
        s = float(row.get("fact_salary", 0) or 0)
        ins = float(row.get("fact_insurance", 0) or 0)
        rtot = s + ins
        pct = (100.0 * rtot / grand) if grand else 0.0
        rows_out.append((name, s, ins, rtot, pct))

    for name, s, ins, rtot, pct in rows_out:
        print(
            f"  {name[:44]:<44}"
            f" {s:>{col_w},.2f} {ins:>{col_w},.2f} {rtot:>{col_w},.2f} {pct:>8.1f}%"
        )

    print("  " + "-" * 44 + (f" {'-' * col_w}" * 3) + " " + "-" * 8)
    print(
        f"  {'ИТОГО по столбцам':<44}"
        f" {sum_sal:>{col_w},.2f} {sum_ins:>{col_w},.2f} {grand:>{col_w},.2f} {'100.0':>8}%"
    )
    print(
        f"\n  ФОТ технического директора (сумма по {len(FOT_GROUP_ORDER)} п/п, п. 5 п.6) = {grand:,.2f} руб."
    )
    if missing:
        print(
            f"\n  Нулевой дебет по статье (комбинации п/п × статья), шт. {len(missing)}:"
        )
        for dname, art in missing:
            print(f"    — {dname} × {art}")
    print(f"\n  Готово за {time.time() - t0:.1f}с")


def _parse_period_fact() -> tuple[int, int, str, str]:
    y, m = 2026, 1
    for a in sys.argv[1:]:
        if len(a) == 7 and a[4] == "-":
            y, m = int(a[:4]), int(a[5:7])
            break
    if m == 12:
        p0 = f"{y}-12-01T00:00:00"
        p1 = f"{y + 1}-01-01T00:00:00"
    else:
        p0 = f"{y}-{m:02d}-01T00:00:00"
        p1 = f"{y}-{m + 1:02d}-01T00:00:00"
    return y, m, p0, p1


def _main_fact() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
    t0 = time.time()
    y, m, p_start, p_end = _parse_period_fact()
    session = requests.Session()
    session.auth = AUTH

    name_to_key, _labels = load_fot_spec_structure_map(session)
    totals, _article_totals = calc_techdir_spec_reg_fact(
        session, p_start, p_end, name_to_key
    )
    print_fact_report(y, m, p_start, p_end, totals, t0)


if __name__ == "__main__":
    _main_fact()
