"""
calc_budget_techdir_sum.py — сумма бюджетов техдиректора.

Складывает:
  1. `calc_budget_techdir_26.py` — бюджет по счёту 26 и 5 статьям.
  2. `calc_budget_techdir_m3.py` — бюджетный контур TD-M3 (берём `total_plan`).

Запуск:
  python calc_budget_techdir_sum.py 2026-02
  python calc_budget_techdir_sum.py          # последний полный месяц
"""

from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from getkpi import calc_budget_techdir_26 as budget26  # noqa: E402
from getkpi import calc_budget_techdir_m3 as budget_m3  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")

AUTH = HTTPBasicAuth("odata.user", "npo852456")


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_period() -> tuple[int, int]:
    period_arg = None
    for arg in sys.argv[1:]:
        if len(arg) == 7 and arg[4] == "-":
            period_arg = arg
            break

    if period_arg:
        return int(period_arg[:4]), int(period_arg[5:7])
    return _last_full_month()


def calc_month(year: int, month: int) -> dict:
    session = requests.Session()
    session.auth = AUTH

    result_26 = budget26.calc_month(session, year, month)
    result_m3 = budget_m3.compute_td_m3_costs_monthly(year, month)

    total_26 = float(result_26.get("total") or 0)
    total_m3_plan = float(result_m3.get("total_plan") or 0)
    total_m3_fact = float(result_m3.get("total_fact") or 0)

    return {
        "year": year,
        "month": month,
        "month_name": result_26.get("month_name") or result_m3.get("month_name"),
        "budget_26": result_26,
        "budget_m3": result_m3,
        "total_26": round(total_26, 2),
        "total_m3_plan": round(total_m3_plan, 2),
        "total_m3_fact": round(total_m3_fact, 2),
        "total": round(total_26 + total_m3_plan, 2),
    }


def _fmt(value: float) -> str:
    return f"{value:,.2f}"


def print_month(result: dict) -> None:
    year = result["year"]
    month = result["month"]
    month_name = result["month_name"]

    print("=" * 110)
    print(f"  СУММА БЮДЖЕТОВ ТЕХДИРЕКЦИИ · {month_name} {year}")
    print("=" * 110)
    print(f"  Бюджет сч. 26:            {_fmt(result['total_26'])}")
    print(f"  Бюджет TD-M3 (план):      {_fmt(result['total_m3_plan'])}")
    print(f"  TD-M3 (факт, инфо):       {_fmt(result['total_m3_fact'])}")
    print(f"  {'-' * 48}")
    print(f"  ИТОГО:                    {_fmt(result['total'])}")
    print()

    print("  Детализация сч. 26:")
    budget26.print_month(result["budget_26"])

    print("\n  Детализация TD-M3:")
    print(
        f"  План TD-M3: {_fmt(result['total_m3_plan'])} | "
        f"Факт TD-M3: {_fmt(result['total_m3_fact'])}"
    )


def main() -> None:
    year, month = parse_period()
    t0 = time.time()
    result = calc_month(year, month)
    print_month(result)
    print(f"  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
