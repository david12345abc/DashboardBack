"""
calc_fot_management.py - OD-M3.2 "FOT operational director".

Fact method:
  Source       : AccountingRegister_Хозрасчетный / RecordsWithExtDimensions
  Account      : debit turnover of account 26 and its subaccounts
  Departments  : subtree "ОПЕРАЦИОННЫЙ ДИРЕКТОР" in enterprise structure
  Cost articles: "Оплата труда (26 сч) НПО АУП!",
                 "Страховые взносы (26 сч) НПО АУП!"

Accumulation registers are not used for FOT fact.
"""
from __future__ import annotations

import json
import re
import sys
import time
from collections import defaultdict
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ACCOUNT_26_ROOT = "fb2bde43-6250-11e7-812d-001e67112509"
SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

OPDIR_ROOT_NAME = "ОПЕРАЦИОННЫЙ ДИРЕКТОР"

COST_ARTICLES = {
    "524cd9aa-215a-11e0-b91c-00248c26ee57": "Оплата труда (26 сч) НПО АУП!",
    "b2913ba1-768e-11e7-812e-001e67112509": "Страховые взносы (26 сч) НПО АУП!",
}
ARTICLE_SET = frozenset(COST_ARTICLES)
ARTICLE_ORDER = list(COST_ARTICLES.keys())

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "fot_management_opdir_account26_v2_daily_refresh"

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def normalize_name(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    value = re.sub(r"[^0-9a-zа-я]+", " ", value)
    return " ".join(value.split())


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
    return CACHE_DIR / f"fot_management_monthly_{year}_{ref_month:02d}.json"


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


def period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _fetch_all(session: requests.Session, url: str, page: int = 5000) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        r = session.get(f"{url}{sep}$top={page}&$skip={skip}", timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def _get_plan_total(month: int) -> float:
    try:
        from . import calc_fot
    except ImportError:
        import calc_fot
    return float(calc_fot.get_fot_plan(month) or 0.0)


def _prorate_if_current(plan: float | None, year: int, month: int) -> float | None:
    if plan is None:
        return None
    today = date.today()
    if year == today.year and month == today.month:
        total_days = monthrange(year, month)[1]
        return round(float(plan) * today.day / total_days, 2)
    return round(float(plan), 2)


def _load_structure(session: requests.Session) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$format=json"
        f"&$select=Ref_Key,Description,Parent_Key,DeletionMark"
        f"&$orderby=Ref_Key"
    )
    rows = _fetch_all(session, url)
    by_key: dict[str, dict] = {}
    by_parent: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        by_parent[row.get("Parent_Key")].append(row)
    return by_key, by_parent


def _resolve_department_root(by_key: dict[str, dict], name: str) -> dict:
    target = normalize_name(name)
    candidates = [
        row for row in by_key.values()
        if normalize_name(row.get("Description")) == target
    ]
    if not candidates:
        candidates = [
            row for row in by_key.values()
            if target in normalize_name(row.get("Description"))
        ]
    if not candidates:
        raise RuntimeError(f"Не найдено подразделение: {name}")
    candidates.sort(key=lambda row: (1 if row.get("DeletionMark") else 0, row.get("Description") or ""))
    return candidates[0]


def _collect_subtree_ordered(root_key: str,
                             by_key: dict[str, dict],
                             by_parent: dict[str, list[dict]]) -> list[dict]:
    result: list[dict] = []
    stack = [root_key]
    seen: set[str] = set()
    while stack:
        key = stack.pop(0)
        if key in seen:
            continue
        seen.add(key)
        row = by_key.get(key)
        if row and not row.get("DeletionMark"):
            result.append(row)
        children = sorted(
            by_parent.get(key, []),
            key=lambda item: normalize_name(item.get("Description")),
        )
        stack[0:0] = [child["Ref_Key"] for child in children if child.get("Ref_Key")]
    return result


def _get_subaccounts(session: requests.Session, parent_guid: str) -> set[str]:
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
        for row in _fetch_all(session, url, page=200):
            key = row.get("Ref_Key")
            if key and key not in collected:
                collected.add(key)
                frontier.append(key)
    return collected


def _blank_dept_row() -> dict:
    return {
        "salary": 0.0,
        "insurance": 0.0,
        "total": 0.0,
        "rows": 0,
        "by_article": {article: 0.0 for article in ARTICLE_ORDER},
    }


def calc_fact(session: requests.Session, year: int, month: int) -> dict:
    p_start, p_end = period_bounds(year, month)
    by_key, by_parent = _load_structure(session)
    root = _resolve_department_root(by_key, OPDIR_ROOT_NAME)
    departments = _collect_subtree_ordered(root["Ref_Key"], by_key, by_parent)
    dept_keys = {row["Ref_Key"] for row in departments}

    target_accounts = _get_subaccounts(session, ACCOUNT_26_ROOT)
    acc_or = " or ".join(f"AccountDr_Key eq guid'{a}'" for a in sorted(target_accounts))
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and ({acc_or})"
    )
    sel = ",".join([
        "Period", "AccountDr_Key", "ПодразделениеDr_Key",
        "Сумма", "Сторно", "ExtDimensionDr1", "ExtDimensionTypeDr1_Key",
    ])
    url = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    records = _fetch_all(session, url)

    by_dept = {row["Ref_Key"]: _blank_dept_row() for row in departments}
    by_article = {article: 0.0 for article in ARTICLE_ORDER}
    skipped_no_dept = 0
    skipped_not_target_dept = 0
    skipped_not_target_article = 0
    taken = 0

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
        if dept_key == EMPTY:
            skipped_no_dept += 1
            continue
        if dept_key not in dept_keys:
            skipped_not_target_dept += 1
            continue

        article_key = None
        if rec.get("ExtDimensionTypeDr1_Key") == SUBCONTO_TYPE_COST:
            article_key = rec.get("ExtDimensionDr1")
        if article_key not in ARTICLE_SET:
            skipped_not_target_article += 1
            continue

        amount = float(rec.get("Сумма", 0) or 0)
        if rec.get("Сторно"):
            amount = -amount

        row = by_dept[dept_key]
        row["by_article"][article_key] += amount
        row["total"] += amount
        row["rows"] += 1
        if article_key == ARTICLE_ORDER[0]:
            row["salary"] += amount
        elif article_key == ARTICLE_ORDER[1]:
            row["insurance"] += amount
        by_article[article_key] += amount
        taken += 1

    matrix = []
    missing_combinations = []
    for dept in departments:
        key = dept["Ref_Key"]
        row = by_dept[key]
        cells = {article: round(value, 2) for article, value in row["by_article"].items()}
        for article, value in cells.items():
            if abs(value) == 0:
                missing_combinations.append({
                    "department_key": key,
                    "department": dept.get("Description") or "",
                    "article_key": article,
                    "article": COST_ARTICLES[article],
                })
        matrix.append({
            "department_key": key,
            "department": dept.get("Description") or "",
            "salary": round(row["salary"], 2),
            "insurance": round(row["insurance"], 2),
            "total": round(row["total"], 2),
            "rows": row["rows"],
            "by_article": cells,
        })

    total = round(sum(float(row["total"] or 0) for row in by_dept.values()), 2)
    return {
        "total": total,
        "by_article": {key: round(value, 2) for key, value in by_article.items()},
        "by_dept": {row["department_key"]: row["total"] for row in matrix},
        "matrix": matrix,
        "missing_combinations": missing_combinations,
        "departments_count": len(departments),
        "records_total": len(records),
        "records_taken": taken,
        "skipped_no_dept": skipped_no_dept,
        "skipped_not_target_dept": skipped_not_target_dept,
        "skipped_not_target_article": skipped_not_target_article,
    }


def calc_month(session: requests.Session, year: int, month: int) -> dict:
    t = time.time()
    fact = calc_fact(session, year, month)
    plan_total = _get_plan_total(month)
    return {
        "year": year,
        "month": month,
        "period": {"start": period_bounds(year, month)[0][:10],
                   "end": period_bounds(year, month)[1][:10]},
        "fact": fact,
        "plan_total": plan_total,
        "elapsed_sec": round(time.time() - t, 2),
    }


def get_fot_management_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path_monthly(ref_year, ref_month)

    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        if cached.get("cache_date") == today.isoformat():
            return cached

    session = requests.Session()
    session.auth = AUTH

    months_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        row = calc_month(session, ref_year, mm)
        fact_payload = row.get("fact") or {}
        plan_total = row.get("plan_total")
        fact_total = float(fact_payload.get("total") or 0)
        plan_numeric = _prorate_if_current(plan_total, ref_year, mm)
        months_out.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan_numeric, 2) if plan_numeric is not None else None,
            "fact": round(fact_total, 2),
            "kpi_pct": round(fact_total / plan_numeric * 100, 1) if plan_numeric and plan_numeric > 0 else None,
            "has_data": (plan_numeric is not None) or abs(fact_total) > 0,
            "values_unit": "руб.",
            "fact_by_article": fact_payload.get("by_article") or {},
            "fact_by_dept": fact_payload.get("by_dept") or {},
            "fact_matrix": fact_payload.get("matrix") or [],
            "missing_combinations": fact_payload.get("missing_combinations") or [],
            "departments_count": fact_payload.get("departments_count"),
            "records_taken": fact_payload.get("records_taken"),
            "cost_articles": COST_ARTICLES,
        })

    with_data = [row for row in months_out if row.get("has_data")]
    last_data_row = with_data[-1] if with_data else (months_out[-1] if months_out else None)
    total_plan = sum(float(row.get("plan") or 0) for row in months_out if row.get("plan") is not None)
    total_fact = sum(float(row.get("fact") or 0) for row in months_out)
    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(last_data_row) if last_data_row else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if months_out else None,
            "total_fact": round(total_fact, 2) if months_out else None,
            "kpi_pct": round(total_fact / total_plan * 100, 1) if total_plan > 0 else None,
            "months_with_data": len(with_data),
            "months_total": len(months_out),
            "values_unit": "руб." if months_out else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": (last_data_row or {}).get("year", ref_year),
            "month": (last_data_row or {}).get("month", ref_month),
            "month_name": (last_data_row or {}).get("month_name", MONTH_RU[ref_month].lower()),
        },
    }
    _save_json(cache_path, payload)
    return payload


def fmt(v) -> str:
    if v is None:
        return "-"
    return f"{v:,.2f}"


def print_month(res: dict) -> None:
    y, m = res["year"], res["month"]
    fact = res["fact"]
    plan = res["plan_total"]
    delta = None if plan is None else (fact["total"] - plan)

    print("=" * 100)
    print(f"  ФОТ операционного директора · {y}-{m:02d} ({MONTH_RU[m]})")
    print("  Источник факта: Дт 26, AccountingRegister_Хозрасчетный / RecordsWithExtDimensions")
    print("=" * 100)
    print(f"\n  {'Показатель':<52s} {'Значение':>20s}")
    print(f"  {'-'*52} {'-'*20}")
    print(f"  {'ФАКТ ФОТ (сч.26)':<52s} {fact['total']:>20,.2f}")
    print(f"  {'ПЛАН':<52s} {fmt(plan):>20s}")
    if delta is not None:
        print(f"  {'Delta (факт - план)':<52s} {delta:>20,.2f}")
    print(f"  {'Подразделений в периметре':<52s} {fact['departments_count']:>20}")
    print(f"  {'Проводок взято':<52s} {fact['records_taken']:>20}")
    print(f"  Время: {res['elapsed_sec']:.1f}с")


def main():
    save_json = "--json" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: calc_fot_management.py YYYY-MM | YYYY [--json]")

    arg = args[0]
    today = datetime.now()
    t0 = time.time()
    session = requests.Session()
    session.auth = AUTH

    if len(arg) == 7 and arg[4] == "-":
        year, month = int(arg[:4]), int(arg[5:7])
        res = calc_month(session, year, month)
        print_month(res)
        if save_json:
            res["cost_articles"] = COST_ARTICLES
            res["generated"] = datetime.now().isoformat(timespec="seconds")
            out = Path(__file__).resolve().parent / f"fot_mgmt_{year}-{month:02d}.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            print(f"  -> {out}")
        return

    if len(arg) != 4 or not arg.isdigit():
        raise SystemExit("Ожидался YYYY-MM или YYYY")

    year = int(arg)
    last_month = today.month if year == today.year else 12
    print("=" * 100)
    print(f"  ФОТ операционного директора · {year} (январь - {MONTH_RU[last_month]})")
    print("=" * 100)
    print(f"\n  {'Мес':<10s} {'ФАКТ':>18s} {'ПЛАН':>16s} {'Delta':>18s}")
    print(f"  {'-'*10} {'-'*18} {'-'*16} {'-'*18}")
    months_out = []
    for m in range(1, last_month + 1):
        res = calc_month(session, year, m)
        months_out.append(res)
        plan = res["plan_total"]
        delta = None if plan is None else (res["fact"]["total"] - plan)
        print(f"  {MONTH_RU[m]:<10s} {res['fact']['total']:>18,.2f} "
              f"{fmt(plan):>16s} {fmt(delta):>18s}")
    print(f"\n  Время: {time.time()-t0:.1f}с")

    if save_json:
        result = {
            "year": year,
            "generated": datetime.now().isoformat(timespec="seconds"),
            "cost_articles": COST_ARTICLES,
            "metric": "ФОТ операционного директора",
            "months": months_out,
        }
        out = Path(__file__).resolve().parent / f"fot_mgmt_{year}.json"
        with out.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  -> {out}")


if __name__ == "__main__":
    main()
