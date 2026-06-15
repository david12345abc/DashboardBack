"""
Факт ЗПР за месяц.

Источник:
  Document_ТД_ЗаявкаОПотребностиРынка

Критерии:
  - ДатаПриемаВРазработку попадает в выбранный год и месяц;
  - Статус: "Закрыта" или "Приостановлена";
  - исключается Автор = "Робот регламентных заданий";
  - исключаются статусы "Аннулирована" и "Не согласована в работу".

Использование:
  python devdir/calc_zpr_fact.py
  python devdir/calc_zpr_fact.py 2026 04
  python devdir/calc_zpr_fact.py 2026-04

Вывод:
  одно число — количество ЗПР.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
DOC_ENTITY = "Document_ТД_ЗаявкаОПотребностиРынка"
ROBOT_AUTHOR_KEY = "eeb3b4f9-8418-11e8-827b-ac1f6b05524d"


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


def parse_period_args() -> tuple[int, int]:
    args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]

    if len(args) == 1 and len(args[0]) == 7 and args[0][4] == "-":
        year = int(args[0][:4])
        month = int(args[0][5:7])
    elif len(args) == 2:
        year = int(args[0])
        month = int(args[1])
    elif not args:
        year = int(input("Введите год: ").strip())
        month = int(input("Введите месяц: ").strip())
    else:
        raise ValueError("Период нужно указать как ГГГГ-ММ или как два аргумента: ГГГГ ММ")

    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 1 до 12")

    return year, month


def month_bounds(year: int, month: int) -> tuple[str, str]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return f"{start.isoformat()}T00:00:00", f"{end.isoformat()}T00:00:00"


def count_zpr_fact(session: requests.Session, year: int, month: int) -> int:
    period_start, period_end = month_bounds(year, month)
    flt = (
        "DeletionMark eq false"
        f" and ДатаПриемаВРазработку ge datetime'{period_start}'"
        f" and ДатаПриемаВРазработку lt datetime'{period_end}'"
        " and (Статус eq 'Закрыта' or Статус eq 'Приостановлена')"
        f" and Автор_Key ne guid'{ROBOT_AUTHOR_KEY}'"
    )
    url = (
        f"{BASE}/{quote(DOC_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select=Ref_Key"
    )

    total = 0
    skip = 0
    page = 1000
    while True:
        page_url = f"{url}&$top={page}&$skip={skip}"
        response = session.get(page_url, timeout=120)
        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

        rows = response.json().get("value", [])
        if not rows:
            break

        total += len(rows)
        if len(rows) < page:
            break
        skip += len(rows)

    return total


def main() -> None:
    try:
        year, month = parse_period_args()
        session = requests.Session()
        session.auth = AUTH
        print(count_zpr_fact(session, year, month))
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
