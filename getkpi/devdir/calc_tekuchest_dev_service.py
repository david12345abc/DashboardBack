"""
Текучесть персонала по подразделению "Служба развития" по месяцам.

Источник:
  Document_ТД_ТекучестьПерсонала

Логика как в david/dashboard/dashboard/факт/calc_tekuchest.py:
  - план берется из 1С: ВидДокумента = "0", поле "План" в табличной части "Текучесть";
  - для 2026 есть fallback, если в 1С план по подразделению пока не заполнен;
  - ВидДокумента = "1" -> факт из 1С, поле "Факт" в табличной части "Текучесть";
  - подразделение берется из шапки документа: Подразделение_Key.

Использование:
  python devdir/calc_tekuchest_dev_service.py 2026-03

Вывод:
  один месяц: План / Факт.
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
STRUCTURE_ENTITY = "Catalog_СтруктураПредприятия"
TEKUCHEST_ENTITY = "Document_ТД_ТекучестьПерсонала"
DEPARTMENT_NAME = "Служба развития"
EMPTY = "00000000-0000-0000-0000-000000000000"

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

FIXED_PLAN_BY_YEAR = {
    2026: {
        1: 1.3,
        2: 2.5,
        3: 3.8,
        4: 5.0,
        5: 6.5,
        6: 7.5,
        7: 8.8,
        8: 10.0,
        9: 11.3,
        10: 12.5,
        11: 13.8,
        12: 15.0,
    }
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


def parse_period_arg() -> tuple[int, int]:
    args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    if len(args) == 1 and len(args[0]) == 7 and args[0][4] == "-":
        year = int(args[0][:4])
        month = int(args[0][5:7])
        if not 1 <= month <= 12:
            raise ValueError("Месяц должен быть от 1 до 12")
        return year, month
    raise ValueError("Период нужно указать одним аргументом в формате ГГГГ-ММ")


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


def _apply_fixed_plan_fallback_if_needed(
    result: dict[int, dict[str, float]],
    year: int,
) -> None:
    """Используем утвержденный план 2026, пока в 1С для этого контура нет строк плана."""
    fallback = FIXED_PLAN_BY_YEAR.get(year)
    if not fallback:
        return
    has_1c_plan = any(float(result[month]["plan"] or 0) != 0 for month in range(1, 13))
    if has_1c_plan:
        return
    for month, plan in fallback.items():
        result[month]["plan"] = plan


def load_tekuchest_docs(session: requests.Session) -> list[dict]:
    url = (
        f"{BASE}/{quote(TEKUCHEST_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote('DeletionMark eq false', safe='')}"
        f"&$orderby=Ref_Key"
    )
    return fetch_all(session, url, page=500)


def calculate_by_month(docs: list[dict], dept_keys: set[str], year: int) -> dict[int, dict[str, float]]:
    result = defaultdict(lambda: {"plan": 0.0, "fact": 0.0})

    for doc in docs:
        if doc.get("Подразделение_Key", EMPTY) not in dept_keys:
            continue

        doc_type = str(doc.get("ВидДокумента", ""))
        for row in doc.get("Текучесть", []):
            month_value = row.get("Месяц") or ""
            if len(month_value) < 7 or int(month_value[:4]) != year:
                continue

            month = int(month_value[5:7])
            if doc_type == "0":
                result[month]["plan"] += float(row.get("План", 0) or 0)
            elif doc_type == "1":
                result[month]["fact"] += float(row.get("Факт", 0) or 0)

    _apply_fixed_plan_fallback_if_needed(result, year)
    return result


def fetch_yearly_monthly_totals(session: requests.Session, year: int) -> dict[int, dict[str, float]]:
    """План/факт по месяцам 1–12 за год (один проход структуры и один — документов текучести)."""
    by_key, by_parent = load_structure(session)
    root_key = find_department_key(by_key)
    dept_keys = collect_subtree(root_key, by_parent)
    docs = load_tekuchest_docs(session)
    raw = calculate_by_month(docs, dept_keys, year)
    return {m: {"plan": float(raw[m]["plan"]), "fact": float(raw[m]["fact"])} for m in range(1, 13)}


def print_month_result(year: int, month: int, result: dict[int, dict[str, float]]) -> None:
    plan = result[month]["plan"]
    fact = result[month]["fact"]
    print(f"\nТекучесть персонала: {DEPARTMENT_NAME}, {MONTH_RU[month]} {year}")
    print(f"  {'План':<10} {plan:.2f}")
    print(f"  {'Факт':<10} {fact:.2f}")


def main() -> None:
    try:
        year, month = parse_period_arg()
        session = requests.Session()
        session.auth = AUTH

        by_key, by_parent = load_structure(session)
        root_key = find_department_key(by_key)
        dept_keys = collect_subtree(root_key, by_parent)

        docs = load_tekuchest_docs(session)
        result = calculate_by_month(docs, dept_keys, year)
        print_month_result(year, month, result)
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
