# -*- coding: utf-8 -*-
"""
calc_tekuchest_opdir.py — Текучесть персонала компании для операционного директора.

Тот же принцип, что у коммерческого директора (calc_tekuchest / KD-M11):
  текучесть, % = уволенные за месяц / штатные единицы на конец месяца × 100.
Источник — кадровая история и штатное расписание в SQL erp_pm.

Периметр — организация «ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО».
На обороте плитки — службы внутри «Обособленного подразделения»
(и прочие корни НПО, где есть штат): процент и сколько уволено из штата.
Штат вложенных отделов входит в свою службу, без двойного счёта.

Вывод:
  • Итог по всем отделам периметра
  • «Дети»: каждое подразделение — строка (план/факт за выбранный месяц)
  • Опционально --json: tekuchest_opdir_ГГГГ-ММ.json

Запуск:
  python calc_tekuchest_opdir.py              # текущий месяц
  python calc_tekuchest_opdir.py 2026-03
  python calc_tekuchest_opdir.py 2026-03 --json
  python calc_tekuchest_opdir.py 2026 --json  # сводка по месяцам + детализация в JSON
"""
from __future__ import annotations

import functools
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from hr_turnover_sql import (
    ORG_TABLE,
    DeptSpec,
    SqlConnection,
    bin_to_guid,
    build_report,
    guid_to_1c,
    turnover_percent,
)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ENTITY = "Document_ТД_ТекучестьПерсонала"
CAT_STRUKTURA = "Catalog_СтруктураПредприятия"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "tekuchest_opdir_hr_sql_v1"
CACHE_VERSION = 4

# Catalog_Организации «ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО» — владелец подразделений.
NPO_OWNER = "fbca2148-6cfd-11e7-812d-001e67112509"
# Корень действующей оргструктуры НПО в Catalog_ПодразделенияОрганизаций.
COMPANY_ROOT = "572c1ca9-e6f3-11e7-826b-ac1f6b05524d"

OD_Q2_SOURCE = (
    "1С: кадровая история и штатное расписание, "
    "все подразделения НПО «Турбулентность»"
)
OD_Q2_FORMULA = (
    "уволенные за месяц / штатные единицы на последний день месяца × 100"
)
OD_Q2_DESCRIPTION = (
    "Факт:\n"
    "Текучесть, % = уволенные за месяц / штатные единицы "
    "на последний день месяца × 100.\n\n"
    "Периметр — организация «ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО». "
    "Штат вложенных отделов суммируется в службу, к которой они относятся.\n\n"
    "Увольнение — событие «Увольнение» в кадровой истории за календарный месяц. "
    "Один сотрудник в одном подразделении считается один раз. "
    "Сотрудники с признаком «не учитывать при текучести» не входят.\n\n"
    "На развороте — служба и её текучесть: процент и сколько уволено из штата."
)

# Корни поддеревьев, которые не входят в периметр операционного директора (Турбулентность-Дон).
EXCLUDE_SUBTREE_ROOTS: frozenset[str] = frozenset({
    "d29dd492-df8a-11e9-829b-ac1f6b05524d",  # _Ликвидированные
    "88cbfc9b-83ed-11e6-8121-001e67112509",  # ПРОИЗВОДСТВО  АЛМАЗ
})

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"tekuchest_opdir_hr_{year}_{ref_month:02d}.json"


def _normalize_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    if month is None:
        ref_month = today.month if ref_year == today.year else 12
    else:
        ref_month = int(month)
    if ref_month < 1:
        ref_month = 1
    if ref_year == today.year and ref_month > today.month:
        ref_month = today.month
    if ref_month > 12:
        ref_month = 12
    return ref_year, ref_month


def load_opdir_struktura_departments(session: requests.Session) -> dict[str, str]:
    """
    Элементы Catalog_СтруктураПредприятия (реквизит Подразделение в документе текучести),
    за вычетом поддеревьев EXCLUDE_SUBTREE_ROOTS.
    """
    flt = quote("DeletionMark eq false", safe="")
    sel = quote("Ref_Key,Description,Parent_Key", safe=",_")
    url = f"{BASE}/{quote(CAT_STRUKTURA)}?$format=json&$filter={flt}&$select={sel}"
    rows: list[dict] = []
    skip = 0
    PAGE = 500
    while True:
        r = session.get(f"{url}&$top={PAGE}&$skip={skip}", timeout=120)
        if not r.ok:
            raise SystemExit(f"HTTP {r.status_code} структура: {r.text[:400]}")
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break

    by_parent: dict[str, list[str]] = {}
    all_keys: set[str] = set()
    for row in rows:
        rk = row.get("Ref_Key")
        if not rk:
            continue
        all_keys.add(rk)
        pk = row.get("Parent_Key") or EMPTY
        if pk == EMPTY:
            pk = ""
        by_parent.setdefault(pk, []).append(rk)

    banned: set[str] = set()

    def dfs_excl(root: str) -> None:
        if root in banned:
            return
        banned.add(root)
        for ch in by_parent.get(root, []):
            dfs_excl(ch)

    for ex_root in EXCLUDE_SUBTREE_ROOTS:
        if ex_root in all_keys:
            dfs_excl(ex_root)

    out: dict[str, str] = {}
    for row in rows:
        rk = row.get("Ref_Key")
        if not rk or rk in banned:
            continue
        out[rk] = row.get("Description") or rk
    return out


def load_all_tekuchest_docs(session: requests.Session) -> list:
    flt = "DeletionMark eq false"
    docs: list = []
    skip = 0
    PAGE = 500
    while True:
        u = (
            f"{BASE}/{quote(ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$orderby=Ref_Key"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = session.get(u, timeout=120)
        if not r.ok:
            raise SystemExit(f"HTTP {r.status_code} текучесть: {r.text[:400]}")
        batch = r.json().get("value", [])
        if not batch:
            break
        docs.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break
    return docs


def month_key_from_cell(raw) -> str | None:
    """Приводит Месяц из ТЧ к 'YYYY-MM'."""
    if not raw:
        return None
    s = str(raw)
    if len(s) >= 7 and s[4] == "-":
        return s[:7]
    return None


def aggregate_month(
    docs_dept: list,
    dept_names: dict[str, str],
    year: int,
    month: int,
) -> tuple[dict, list[dict]]:
    target = f"{year}-{month:02d}"
    result = defaultdict(lambda: {"plan": 0.0, "fact": 0.0, "plan_rows": 0, "fact_rows": 0})

    for doc in docs_dept:
        dept_key = doc.get("Подразделение_Key", EMPTY)
        if dept_key not in dept_names:
            continue
        vid = str(doc.get("ВидДокумента", ""))
        for row in doc.get("Текучесть", []) or []:
            mk = month_key_from_cell(row.get("Месяц"))
            if mk != target:
                continue
            plan_val = float(row.get("План", 0) or 0)
            fact_val = float(row.get("Факт", 0) or 0)
            if vid == "0":
                result[dept_key]["plan"] += plan_val
                result[dept_key]["plan_rows"] += 1
            elif vid == "1":
                result[dept_key]["fact"] += fact_val
                result[dept_key]["fact_rows"] += 1

    children: list[dict] = []
    total_plan = 0.0
    total_fact = 0.0
    for dk in sorted(dept_names.keys(), key=lambda k: dept_names[k]):
        p = result[dk]["plan"]
        f = result[dk]["fact"]
        total_plan += p
        total_fact += f
        children.append({
            "department_key": dk,
            "name": dept_names[dk],
            "plan": round(p, 4),
            "fact": round(f, 4),
            "plan_rows": result[dk]["plan_rows"],
            "fact_rows": result[dk]["fact_rows"],
        })

    summary = {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month],
        "plan": round(total_plan, 4),
        "fact": round(total_fact, 4),
        "departments_with_data": sum(1 for c in children if c["plan"] or c["fact"]),
    }
    return summary, children


def yearly_month_totals(docs_dept: list, dept_names: dict[str, str], year: int) -> list[dict]:
    """Сводка по месяцам (сумма по всем отделам периметра)."""
    rows = []
    for m in range(1, 13):
        summ, _ = aggregate_month(docs_dept, dept_names, year, m)
        rows.append({
            "month": m,
            "month_name": MONTH_RU[m],
            "plan": summ["plan"],
            "fact": summ["fact"],
        })
    return rows


def _usable_cache(path: Path) -> dict | None:
    cached = _load_json(path)
    if (
        isinstance(cached, dict)
        and cached.get("source") == SOURCE_TAG
        and cached.get("cache_version") == CACHE_VERSION
    ):
        return cached
    return None


def _empty_monthly_payload(ref_year: int, ref_month: int) -> dict:
    return {
        "cache_date": None,
        "source": SOURCE_TAG,
        "cache_version": CACHE_VERSION,
        "year": ref_year,
        "ref_month": ref_month,
        "months": [],
        "last_full_month_row": None,
        "ytd": {},
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
        "cache_refresh_status": "computing",
    }


def _company_turnover_rag(pct: float | None) -> str:
    """Пороги плитки OD-Q2: ≤5 зелёный, 5,1–7 жёлтый, >7 красный."""
    if pct is None:
        return "unknown"
    if pct <= 5:
        return "green"
    if pct <= 7:
        return "yellow"
    return "red"


def _load_npo_tree(sql: SqlConnection) -> tuple[dict[str, str], dict[str, list[str]]]:
    owner = guid_to_1c(NPO_OWNER)
    names: dict[str, str] = {}
    children: dict[str, list[str]] = defaultdict(list)
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT _IDRRef, _Description, _ParentIDRRef
            FROM dbo.[{ORG_TABLE}] WITH (NOLOCK)
            WHERE _Marked = 0x00 AND _OwnerIDRRef = ?
            """,
            [owner],
        )
        for ref, desc, parent in cur.fetchall():
            key = bin_to_guid(bytes(ref))
            parent_key = bin_to_guid(bytes(parent)) if parent else ""
            if parent_key == EMPTY:
                parent_key = ""
            names[key] = str(desc or "").strip() or key
            children[parent_key].append(key)
    return names, children


def _descendants(key: str, children: dict[str, list[str]]) -> list[str]:
    out = [key]
    for child in children.get(key, []):
        out.extend(_descendants(child, children))
    return out


def _company_groups(
    names: dict[str, str],
    children: dict[str, list[str]],
) -> list[tuple[str, list[str]]]:
    """Службы НПО и подразделения, которые в них входят."""
    groups: list[tuple[str, list[str]]] = []
    seen: set[str] = set()
    for child in children.get(COMPANY_ROOT, []):
        keys = _descendants(child, children)
        groups.append((names.get(child) or child, keys))
        seen.update(keys)
    own = COMPANY_ROOT
    if own in names and own not in seen:
        groups.append((names[own], [own]))
        seen.add(own)
    for root in children.get("", []):
        if root in seen or root == COMPANY_ROOT:
            continue
        if (names.get(root) or "").casefold() == "удалить":
            continue
        keys = [key for key in _descendants(root, children) if key not in seen]
        if not keys:
            continue
        groups.append((names.get(root) or root, keys))
        seen.update(keys)
    return groups


def _month_from_report(year: int, month: int, month_payload: dict, org_to_group: dict[str, str]) -> dict:
    buckets: dict[str, dict[str, float | int]] = {}
    for row in month_payload.get("rows") or []:
        org_key = str(row.get("org_key") or "").lower()
        group = org_to_group.get(org_key)
        if not group:
            continue
        bucket = buckets.get(group)
        if bucket is None:
            bucket = {"staff": 0.0, "dismissed": 0}
            buckets[group] = bucket
        bucket["staff"] += float(row.get("staff_units") or 0)
        bucket["dismissed"] += int(row.get("dismissed") or 0)

    turnover_rows = []
    for name in sorted(buckets, key=lambda item: item.casefold()):
        staff = round(float(buckets[name]["staff"]), 2)
        dismissed = int(buckets[name]["dismissed"])
        if staff <= 0 and dismissed <= 0:
            continue
        turnover_rows.append({
            "name": name,
            "staff": staff,
            "dismissed": dismissed,
            "fact": turnover_percent(staff, dismissed),
            "navigable": False,
        })
    staff_total = round(sum(row["staff"] for row in turnover_rows), 2)
    dismissed_total = sum(row["dismissed"] for row in turnover_rows)
    fact = turnover_percent(staff_total, dismissed_total) if staff_total > 0 or dismissed_total else None
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month].lower(),
        "plan": None,
        "fact": fact,
        "kpi_pct": fact,
        "staff_units": staff_total,
        "dismissed": dismissed_total,
        "has_data": staff_total > 0 or dismissed_total > 0,
        "values_unit": "%",
        "color": _company_turnover_rag(fact),
        "turnover_rows": turnover_rows,
    }


def _calc_company_year(year: int, ref_month: int) -> list[dict]:
    sql = SqlConnection()
    names, children = _load_npo_tree(sql)
    groups = _company_groups(names, children)
    org_to_group: dict[str, str] = {}
    specs: list[DeptSpec] = []
    for group_name, keys in groups:
        for key in keys:
            org_to_group[key.lower()] = group_name
            specs.append(DeptSpec(
                group=key,
                org_key=key,
                structure_name=names.get(key) or group_name,
                note=group_name,
            ))
    report = build_report(
        specs,
        (year, 1),
        (year, ref_month),
        sql=sql,
        hierarchy_mode="listed_only_no_auto_children",
        kpi_label="OD-Q2",
    )
    by_month = {row["month"]: row for row in report.get("months") or []}
    return [
        _month_from_report(year, month, by_month.get(month) or {"rows": []}, org_to_group)
        for month in range(1, ref_month + 1)
    ]


def get_tekuchest_opdir_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path_monthly(ref_year, ref_month)
    cache_key = f"od_q2_turnover_{ref_year}_{ref_month}"
    from . import cache_manager

    cache_manager.register_cache_path(cache_key, cache_path)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _usable_cache(cache_path)
    if cached is not None and (
        not is_current_month or cached.get("cache_date") == today.isoformat()
    ):
        return cached

    if not cache_manager.is_force_compute_context():
        stale = cached
        if stale is None and ref_month > 1:
            stale = _usable_cache(_cache_path_monthly(ref_year, ref_month - 1))
        cache_manager.schedule_background_refresh(
            cache_key,
            get_tekuchest_opdir_monthly,
            year=ref_year,
            month=ref_month,
        )
        if stale is not None:
            out = dict(stale)
            out["cache_refresh_status"] = "stale"
            return out
        return _empty_monthly_payload(ref_year, ref_month)

    logger.info("calc_tekuchest_opdir: HR SQL %s-%02d", ref_year, ref_month)
    months_out = _calc_company_year(ref_year, ref_month)
    last = months_out[-1] if months_out else None
    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "cache_version": CACHE_VERSION,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(last) if last else None,
        "ytd": {
            "total_plan": None,
            "total_fact": last.get("fact") if last else None,
            "kpi_pct": last.get("kpi_pct") if last else None,
            "months_with_data": sum(1 for row in months_out if row.get("has_data")),
            "months_total": len(months_out),
            "values_unit": "%" if months_out else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
    }
    _save_json(cache_path, payload)
    return payload


def main() -> None:
    save_json = "--json" in sys.argv
    argv = [a for a in sys.argv[1:] if not a.startswith("--")]

    now = datetime.now()
    if argv and len(argv[0]) == 7 and argv[0][4] == "-":
        year, month = int(argv[0][:4]), int(argv[0][5:7])
        mode = "month"
    elif argv and len(argv[0]) == 4 and argv[0].isdigit():
        year, month = int(argv[0]), now.month if int(argv[0]) == now.year else 12
        mode = "year"
    else:
        year, month = now.year, now.month
        mode = "month"

    t0 = time.time()
    session = requests.Session()
    session.auth = AUTH

    print(f"\n{'=' * 72}")
    print(f"  ТЕКУЧЕСТЬ — операционный директор (структура предприятия, без исключённых веток)")
    print(f"{'=' * 72}")

    dept_names = load_opdir_struktura_departments(session)
    print(f"\n  Подразделений в периметре: {len(dept_names)}")

    all_docs = load_all_tekuchest_docs(session)
    docs_dept = [d for d in all_docs if d.get("Подразделение_Key", EMPTY) in dept_names]
    print(f"  Документов текучести (все): {len(all_docs)}")
    print(f"  Документов после отбора по периметру: {len(docs_dept)}")

    if mode == "month":
        summ, children = aggregate_month(docs_dept, dept_names, year, month)
        print(f"\n{'=' * 72}")
        print(f"  {summ['month_name']} {year} · итого: план {summ['plan']:.2f}, факт {summ['fact']:.2f}")
        print(f"  Отделов с ненулевыми данными: {summ['departments_with_data']}")
        print(f"{'=' * 72}")
        print(f"\n  {'Подразделение':<52} {'План':>10} {'Факт':>10}")
        print(f"  {'-' * 52} {'-' * 10} {'-' * 10}")
        for c in children:
            print(f"  {c['name'][:52]:<52} {c['plan']:>10.2f} {c['fact']:>10.2f}")
        print(f"  {'-' * 52} {'-' * 10} {'-' * 10}")
        print(f"  {'ИТОГО':<52} {summ['plan']:>10.2f} {summ['fact']:>10.2f}")

        if save_json:
            path = os.path.join(
                os.path.dirname(__file__),
                f"tekuchest_opdir_{year}-{month:02d}.json",
            )
            payload = {
                "generated": datetime.now().isoformat(timespec="seconds"),
                "scope": "Catalog_СтруктураПредприятия \\ исключённые поддеревья",
                "excluded_subtree_roots": list(EXCLUDE_SUBTREE_ROOTS),
                "summary": summ,
                "children": children,
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            print(f"\n  → {path}")
    else:
        # год: таблица итого по месяцам; полная детализация только в JSON
        ym = yearly_month_totals(docs_dept, dept_names, year)
        last_m = now.month if year == now.year else 12
        print(f"\n  Сводка {year} г. (январь — {MONTH_RU[last_m]}):")
        print(f"\n  {'Месяц':<12} {'План':>12} {'Факт':>12}")
        print(f"  {'-' * 12} {'-' * 12} {'-' * 12}")
        for item in ym[:last_m]:
            print(f"  {item['month_name']:<12} {item['plan']:>12.2f} {item['fact']:>12.2f}")

        if save_json:
            months_payload = []
            for item in ym[:last_m]:
                m = item["month"]
                summ, ch = aggregate_month(docs_dept, dept_names, year, m)
                months_payload.append({
                    "month": m,
                    "month_name": item["month_name"],
                    "summary": summ,
                    "children": ch,
                })
            path = os.path.join(os.path.dirname(__file__), f"tekuchest_opdir_{year}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "generated": datetime.now().isoformat(timespec="seconds"),
                    "scope": "Catalog_СтруктураПредприятия \\ исключённые поддеревья",
                    "excluded_subtree_roots": list(EXCLUDE_SUBTREE_ROOTS),
                    "year": year,
                    "months": months_payload,
                }, f, ensure_ascii=False, indent=2)
            print(f"\n  → {path}")

    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
