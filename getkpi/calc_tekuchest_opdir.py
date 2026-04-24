# -*- coding: utf-8 -*-
"""
calc_tekuchest_opdir.py — Текучесть для операционного директора (вся «Турбулентность-Дон»).

Логика как в calc_tekuchest.py (коммерческий директор):
  Document_ТД_ТекучестьПерсонала, ВидДокумента 0 = план, 1 = факт,
  табличная часть «Текучесть»: Месяц, План, Факт.

Периметр: Catalog_СтруктураПредприятия (как в реквизите Подразделение документа).
  Все элементы справочника, кроме поддеревьев:
    • «_Ликвидированные»
    • «ПРОИЗВОДСТВО  АЛМАЗ» (и все дочерние — дочерние юрлица / участки Алмаза)
  (аналог «вся Турбулентность-Дон» без типичных дочерних контуров; при необходимости
   список исключений расширяется константами EXCLUDE_SUBTREE_ROOTS).

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

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ENTITY = "Document_ТД_ТекучестьПерсонала"
CAT_STRUKTURA = "Catalog_СтруктураПредприятия"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "tekuchest_opdir_monthly_v1"

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
    return CACHE_DIR / f"tekuchest_opdir_monthly_{year}_{ref_month:02d}.json"


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


def get_tekuchest_opdir_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path_monthly(ref_year, ref_month)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    session = requests.Session()
    session.auth = AUTH

    dept_names = load_opdir_struktura_departments(session)
    all_docs = load_all_tekuchest_docs(session)
    docs_dept = [d for d in all_docs if d.get("Подразделение_Key", EMPTY) in dept_names]

    months_out: list[dict] = []
    total_plan = 0.0
    total_fact = 0.0
    for mm in range(1, ref_month + 1):
        summary, children = aggregate_month(docs_dept, dept_names, ref_year, mm)
        plan_total = round(float(summary.get("plan") or 0), 2)
        fact_total = round(float(summary.get("fact") or 0), 2)
        row = {
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": plan_total,
            "fact": fact_total,
            "kpi_pct": round(fact_total / plan_total * 100, 1) if plan_total > 0 else None,
            "has_data": abs(plan_total) > 0 or abs(fact_total) > 0,
            "values_unit": "чел.",
            "plan_by_dept": {
                child["name"]: round(float(child.get("plan") or 0), 2)
                for child in children
                if float(child.get("plan") or 0) != 0
            },
            "fact_by_dept": {
                child["name"]: round(float(child.get("fact") or 0), 2)
                for child in children
                if float(child.get("fact") or 0) != 0
            },
        }
        months_out.append(row)
        total_plan += plan_total
        total_fact += fact_total

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(months_out[-1]) if months_out else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if months_out else None,
            "total_fact": round(total_fact, 2) if months_out else None,
            "kpi_pct": round(total_fact / total_plan * 100, 1) if total_plan > 0 else None,
            "months_with_data": sum(1 for row in months_out if row.get("has_data")),
            "months_total": len(months_out),
            "values_unit": "чел." if months_out else None,
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
