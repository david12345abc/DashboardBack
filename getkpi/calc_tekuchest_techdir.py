# -*- coding: utf-8 -*-
"""calc_tekuchest_techdir.py — консольный расчёт текучести техдирекции."""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from getkpi.techdir_tekuchet import compute_td_turnover_month


def main() -> None:
    from datetime import date

    if len(sys.argv) > 1 and len(sys.argv[1]) == 7 and sys.argv[1][4] == "-":
        year, month = map(int, sys.argv[1].split("-"))
    elif len(sys.argv) > 1 and len(sys.argv[1]) == 4 and sys.argv[1].isdigit():
        year = int(sys.argv[1])
        today = date.today()
        month = today.month if year == today.year else 12
    else:
        today = date.today()
        year, month = today.year, today.month

    snapshot = compute_td_turnover_month(year, month)
    print(f"{snapshot['total_plan']:.2f} {snapshot['total_fact']:.2f}")


if __name__ == "__main__":
    main()
