"""
calc_budget_techdir_26.py — бюджет техдиректора по счёту 26.

Алгоритм:
  1. Берём обороты регистра бухгалтерии `AccountingRegister_Хозрасчетный / RecordsWithExtDimensions`
     по счёту 26.
  2. Оставляем только 19 целевых подразделений техдиректора.
  3. Оставляем только 5 целевых статей затрат.
  4. Для каждой пары "подразделение + статья" считаем дебетовый оборот за период.
  5. Бюджет по подразделению = сумма 5 статей.
  6. Бюджет технического директора = сумма по всем 19 подразделениям.
  7. Выводим матрицу: строки — подразделения, столбцы — статьи.

Запуск:
  python calc_budget_techdir_26.py 2026-02
  python calc_budget_techdir_26.py          # последний полный месяц
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from getkpi import fot_techdir_fact as fts  # noqa: E402
from getkpi import techdir_fot_fact as tdreg  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")

BASE = fts.BASE
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = fts.EMPTY

TARGET_DEPARTMENTS = [name for name, _aliases in fts.FOT_SPEC]
TARGET_ARTICLES = (
    "Оплата труда ГАРАНТИИ (26 сч) пр-во НПО!",
    "Гарантийное обслуживание, поверка, ремонт (26 сч) НПО!",
    "Гарантийное обслуживание, ТМЦ (26) НПО!",
    "Гарантийное обслуживание, транспорт и ГСМ (26 сч) НПО!",
    "Гарантийное обслуживание, командировка (26 сч) НПО!",
)
TARGET_ARTICLE_NORMS = {fts.normalize_name(name): name for name in TARGET_ARTICLES}
ACCOUNT_26_ROOT = "fb2bde43-6250-11e7-812d-001e67112509"


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_period() -> tuple[int, int, str, str]:
    period_arg = None
    for arg in sys.argv[1:]:
        if len(arg) == 7 and arg[4] == "-":
            period_arg = arg
            break

    if period_arg:
        year, month = int(period_arg[:4]), int(period_arg[5:7])
    else:
        year, month = _last_full_month()

    if month == 12:
        p_start = f"{year}-12-01T00:00:00"
        p_end = f"{year + 1}-01-01T00:00:00"
    else:
        p_start = f"{year}-{month:02d}-01T00:00:00"
        p_end = f"{year}-{month + 1:02d}-01T00:00:00"
    return year, month, p_start, p_end


def _fetch_all_rows(session: requests.Session, url: str, *, page: int = 5000, timeout: int = 120) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        r = session.get(page_url, timeout=timeout)
        r.raise_for_status()
        batch = r.json().get("value", []) or []
        if not batch:
            return rows
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


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
        rows = _fetch_all_rows(session, url, page=200, timeout=60)
        for row in rows:
            key = row.get("Ref_Key")
            if key and key not in collected:
                collected.add(key)
                frontier.append(key)
    return collected


def _load_register_rows(session: requests.Session, year: int, month: int) -> list[dict]:
    p_start, p_end = _month_period_bounds(year, month)
    target_accounts = _get_subaccounts(session, ACCOUNT_26_ROOT)
    acc_filter = " or ".join(f"AccountDr_Key eq guid'{key}'" for key in sorted(target_accounts))
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and ({acc_filter})"
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
    return _fetch_all_rows(session, url)


def calc_month(session: requests.Session, year: int, month: int) -> dict:
    p_start, p_end = _month_period_bounds(year, month)
    structure_rows, by_key, _, exact_index = fts.load_structure(session)
    name_to_key, name_to_structure_label = fts.load_fot_spec_structure_map(session)
    struct_map = fts.build_struct_key_to_fot_group(name_to_key, by_key)
    article_names = fts.load_cost_articles(session)

    records = _load_register_rows(session, year, month)

    matrix = {
        dept: {"by_article": defaultdict(float), "total": 0.0, "rows": 0}
        for dept in TARGET_DEPARTMENTS
    }
    article_totals = defaultdict(float)
    counts = {
        "records_total": len(records),
        "records_taken": 0,
        "skipped_no_dept": 0,
        "skipped_not_target_dept": 0,
        "skipped_not_target_article": 0,
    }
    total = 0.0

    for rec in records:
        dept_key = rec.get("ПодразделениеDr_Key") or EMPTY
        if not dept_key or dept_key == EMPTY:
            counts["skipped_no_dept"] += 1
            continue

        group_name = struct_map.get(dept_key)
        if not group_name:
            counts["skipped_not_target_dept"] += 1
            continue

        if rec.get("ExtDimensionTypeDr1_Key") != tdreg.SUBCONTO_TYPE_COST:
            counts["skipped_not_target_article"] += 1
            continue

        article_key = rec.get("ExtDimensionDr1") or ""
        article_name = article_names.get(article_key, "")
        article_norm = fts.normalize_name(article_name)
        canonical_article = TARGET_ARTICLE_NORMS.get(article_norm)
        if not canonical_article:
            counts["skipped_not_target_article"] += 1
            continue

        try:
            amount = float(rec.get("Сумма") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if rec.get("Сторно"):
            amount = -amount
        if amount == 0:
            continue

        row = matrix[group_name]
        row["by_article"][canonical_article] += amount
        row["total"] += amount
        row["rows"] += 1
        article_totals[canonical_article] += amount
        total += amount
        counts["records_taken"] += 1

    matrix_rows = []
    for dept in TARGET_DEPARTMENTS:
        row = matrix[dept]
        by_article = {article: round(float(row["by_article"][article]), 2) for article in TARGET_ARTICLES}
        matrix_rows.append(
            {
                "department": dept,
                "structure_label": name_to_structure_label.get(dept, ""),
                "by_article": by_article,
                "total": round(float(row["total"]), 2),
                "rows": int(row["rows"]),
            }
        )

    return {
        "year": year,
        "month": month,
        "month_name": (
            "Январь",
            "Февраль",
            "Март",
            "Апрель",
            "Май",
            "Июнь",
            "Июль",
            "Август",
            "Сентябрь",
            "Октябрь",
            "Ноябрь",
            "Декабрь",
        )[month - 1],
        "period": {"start": p_start[:10], "end": p_end[:10]},
        "departments": matrix_rows,
        "article_totals": {article: round(float(amount), 2) for article, amount in article_totals.items()},
        "total": round(total, 2),
        "counts": counts,
        "departments_count": len(TARGET_DEPARTMENTS),
        "records_total": len(records),
        "name_to_structure_label": name_to_structure_label,
    }


def _fmt(v: float) -> str:
    return f"{v:,.2f}"


def print_month(result: dict) -> None:
    print("=" * 140)
    print(
        f"  БЮДЖЕТ ТЕХДИРЕКЦИИ ПО СЧЁТУ 26 · {result['month_name']} {result['year']}"
    )
    print(
        f"  Период: {result['period']['start']} — {result['period']['end']} (lt)"
    )
    print("=" * 140)
    print(
        f"  Документов/записей: {result['records_total']} | "
        f"включено: {result['counts']['records_taken']} | "
        f"без подразделения: {result['counts']['skipped_no_dept']} | "
        f"нецелевые подразделения: {result['counts']['skipped_not_target_dept']} | "
        f"нецелевые статьи: {result['counts']['skipped_not_target_article']}"
    )

    col_width = 20
    dept_width = 44
    total_width = 16
    print()
    header = f"  {'Подразделение':<{dept_width}}"
    for article in TARGET_ARTICLES:
        header += f" {article[:col_width]:>{col_width}}"
    header += f" {'Итого':>{total_width}}"
    print(header)
    print(f"  {'-' * dept_width} {'-' * col_width * len(TARGET_ARTICLES)} {'-' * total_width}")

    column_totals = {article: 0.0 for article in TARGET_ARTICLES}
    for row in result["departments"]:
        line = f"  {row['department']:<{dept_width}}"
        for article in TARGET_ARTICLES:
            val = float(row["by_article"].get(article, 0.0))
            column_totals[article] += val
            line += f" {_fmt(val):>{col_width}}"
        line += f" {_fmt(row['total']):>{total_width}}"
        print(line)

    print(f"  {'-' * dept_width} {'-' * col_width * len(TARGET_ARTICLES)} {'-' * total_width}")
    total_line = f"  {'ИТОГО по статьям':<{dept_width}}"
    for article in TARGET_ARTICLES:
        total_line += f" {_fmt(column_totals[article]):>{col_width}}"
    total_line += f" {_fmt(result['total']):>{total_width}}"
    print(total_line)
    print()
    print(f"  Общий итог по техдиректору: {_fmt(result['total'])}")


def main() -> None:
    year, month, p_start, p_end = parse_period()
    session = requests.Session()
    session.auth = AUTH
    t0 = time.time()

    print(f"\n{'=' * 140}")
    print(f"  БЮДЖЕТ ТЕХДИРЕКЦИИ ПО СЧЁТУ 26 · {month:02d}.{year}")
    print(f"  Период: {p_start[:10]} — {p_end[:10]} (lt)")
    print(f"{'=' * 140}")

    result = calc_month(session, year, month)
    print_month(result)
    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
