"""
Бюджет факт контура «Служба развития» (RD-M3) за месяц.

Источник:
  Document_ЗаявкаНаРасходованиеДенежныхСредств

Критерии (сверено с эталоном 1С, март–май 2026):
  - Подразделение: «Служба развития» и подчинённые узлы Catalog_СтруктураПредприятия
  - Период: дата документа (Date) в выбранном месяце
  - Сумма: СуммаДокумента
  - DeletionMark eq false

CLI:
  python devdir/calc_budj_dev_service_fact.py
  python devdir/calc_budj_dev_service_fact.py 2026-03
  python devdir/calc_budj_dev_service_fact.py --check
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import date
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
STRUCTURE_ENTITY = "Catalog_СтруктураПредприятия"
DOC_ENTITY = "Document_ЗаявкаНаРасходованиеДенежныхСредств"
DEPARTMENT_NAME = "Служба развития"
EMPTY = "00000000-0000-0000-0000-000000000000"
ROUND_TOLERANCE = 0.01

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

REFERENCE_FACT_2026: dict[int, float] = {
    3: 842_818.70,
    4: 867_262.30,
    5: 295_364.55,
}


def normalize_odata_base(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    if base.endswith("/odata/standard.odata"):
        return base
    return f"{base}/odata/standard.odata"


BASE = normalize_odata_base(os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL))
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)


def money(value: float) -> str:
    return f"{value:,.2f}".replace(",", " ")


def month_bounds(year: int, month: int) -> tuple[str, str]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return f"{start.isoformat()}T00:00:00", f"{end.isoformat()}T00:00:00"


def open_budget_fact_session() -> requests.Session:
    session = requests.Session()
    session.auth = AUTH
    return session


def fetch_all(
    session: requests.Session,
    url: str,
    page: int = 500,
    timeout: int = 120,
) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        response = session.get(page_url, timeout=timeout)
        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

        batch = response.json().get("value", [])
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)

    return rows


def load_structure(session: requests.Session) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    url = (
        f"{BASE}/{quote(STRUCTURE_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote('DeletionMark eq false', safe='')}"
        f"&$select={quote('Ref_Key,Description,Parent_Key', safe=',_')}"
    )
    rows = fetch_all(session, url, page=1000)
    by_key: dict[str, dict] = {}
    by_parent: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        by_parent[row.get("Parent_Key") or EMPTY].append(row)
    return by_key, by_parent


def find_department_key(by_key: dict[str, dict]) -> str:
    matches = [
        key
        for key, row in by_key.items()
        if (row.get("Description") or "").strip().lower() == DEPARTMENT_NAME.lower()
    ]
    if not matches:
        raise RuntimeError(f"Подразделение не найдено: {DEPARTMENT_NAME}")
    return sorted(matches)[0]


def collect_subtree(root_key: str, by_parent: dict[str, list[dict]]) -> set[str]:
    result: set[str] = set()
    stack = [root_key]
    while stack:
        key = stack.pop()
        if key in result:
            continue
        result.add(key)
        for child in by_parent.get(key, []):
            child_key = child.get("Ref_Key")
            if child_key:
                stack.append(child_key)
    return result


def load_dev_service_department_keys(session: requests.Session) -> set[str]:
    by_key, by_parent = load_structure(session)
    root_key = find_department_key(by_key)
    return collect_subtree(root_key, by_parent)


def load_zayavki(
    session: requests.Session,
    year: int,
    month: int,
    dept_keys: set[str],
) -> list[dict]:
    period_start, period_end = month_bounds(year, month)
    dept_filter = " or ".join(f"Подразделение_Key eq guid'{key}'" for key in sorted(dept_keys))
    flt = (
        "DeletionMark eq false"
        f" and Date ge datetime'{period_start}'"
        f" and Date lt datetime'{period_end}'"
        f" and ({dept_filter})"
    )
    select = quote("Ref_Key,Number,Date,СуммаДокумента,Статус,Подразделение_Key", safe=",_")
    url = (
        f"{BASE}/{quote(DOC_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={select}"
        f"&$orderby={quote('Date desc', safe=' ')}"
    )
    return fetch_all(session, url)


def calc_fact_total(rows: list[dict]) -> float:
    return sum(float(row.get("СуммаДокумента") or 0) for row in rows)


def compute_dev_service_budget_fact_monthly(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
    dept_keys: set[str] | None = None,
) -> dict[str, Any]:
    """Факт бюджета RD-M3 за календарный месяц (руб.)."""
    owns_session = session is None
    try:
        if owns_session:
            session = open_budget_fact_session()
        keys = dept_keys if dept_keys is not None else load_dev_service_department_keys(session)
        rows = load_zayavki(session, year, month, keys)
        total = round(calc_fact_total(rows), 2)
        period_start, period_end = month_bounds(year, month)
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": total,
            "counts": {
                "requests_in_period": len(rows),
                "department_subtree_size": len(keys),
            },
            "debug": {
                "status": "ok",
                "kpi_id": "RD-M3-FACT",
                "component": "devdir.calc_budj_dev_service_fact",
                "department_root": DEPARTMENT_NAME,
                "period_start": period_start[:19],
                "period_end": period_end[:19],
                "date_basis": "document_date",
                "amount_basis": "СуммаДокумента",
            },
        }
    except Exception as exc:
        logger.exception("RD-M3 budget fact: ошибка за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": None,
            "counts": {},
            "debug": {
                "status": "error",
                "kpi_id": "RD-M3-FACT",
                "component": "devdir.calc_budj_dev_service_fact",
                "error": str(exc),
            },
        }


def parse_period(value: str) -> tuple[int, int]:
    text = value.strip()
    if len(text) == 7 and text[4] == "-":
        year = int(text[:4])
        month = int(text[5:7])
    elif " " in text:
        parts = text.split()
        if len(parts) != 2:
            raise ValueError(f"не удалось разобрать период: {value}")
        year = int(parts[0])
        month = int(parts[1])
    else:
        raise ValueError(f"не удалось разобрать период: {value}")

    if not 1 <= month <= 12:
        raise ValueError("месяц должен быть от 1 до 12")
    return year, month


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Бюджет факт по службе развития (заявки на расходование ДС)."
    )
    parser.add_argument(
        "period",
        nargs="?",
        help="Период: ГГГГ-ММ или «ГГГГ ММ»",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Сверка с эталонными значениями (март–май 2026)",
    )
    parser.add_argument(
        "--detail",
        action="store_true",
        help="Показать список заявок",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Вывести только число",
    )
    return parser


def print_month_report(
    year: int,
    month: int,
    total: float,
    rows: list[dict],
    *,
    detail: bool,
    reference: float | None = None,
) -> None:
    label = f"{MONTH_RU[month]} {year}"
    print(f"\nБюджет факт · {DEPARTMENT_NAME} · {label}")
    print(f"  Документов: {len(rows)}")
    print(f"  Бюджет факт: {money(total)}")
    if reference is not None:
        diff = total - reference
        mark = "OK" if abs(diff) <= ROUND_TOLERANCE else "РАСХОЖДЕНИЕ"
        print(f"  Эталон:      {money(reference)}")
        print(f"  Δ:           {money(diff)} ({mark})")

    if detail and rows:
        print("  ─────────────────────────────────────────────────────────────")
        for index, row in enumerate(rows, start=1):
            doc_date = (row.get("Date") or "")[:10]
            number = row.get("Number") or "—"
            amount = float(row.get("СуммаДокумента") or 0)
            status = row.get("Статус") or "—"
            print(
                f"  {index:>2}. №{number} от {doc_date} · {money(amount)} · {status}"
            )


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args()

    try:
        session = open_budget_fact_session()
        dept_keys = load_dev_service_department_keys(session)

        if args.check or not args.period:
            months = sorted(REFERENCE_FACT_2026)
            print(f"Сверка бюджета факт · {DEPARTMENT_NAME} · 2026")
            all_ok = True
            for month in months:
                rows = load_zayavki(session, 2026, month, dept_keys)
                total = calc_fact_total(rows)
                reference = REFERENCE_FACT_2026[month]
                print_month_report(
                    2026,
                    month,
                    total,
                    rows,
                    detail=args.detail,
                    reference=reference,
                )
                if abs(total - reference) > ROUND_TOLERANCE:
                    all_ok = False
            if not all_ok:
                sys.exit(2)
            return

        year, month = parse_period(args.period)
        rows = load_zayavki(session, year, month, dept_keys)
        total = calc_fact_total(rows)

        if args.raw:
            print(f"{total:.2f}")
            return

        reference = REFERENCE_FACT_2026.get(month) if year == 2026 else None
        print_month_report(
            year,
            month,
            total,
            rows,
            detail=args.detail,
            reference=reference,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
