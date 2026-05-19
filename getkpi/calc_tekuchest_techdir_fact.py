# -*- coding: utf-8 -*-
"""Консольный расчёт факта текучести TD-Q2 (уволено / штат × 100 %)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from getkpi.techdir_tekuchet_fact import compute_turnover_fact_percent


def main() -> None:
    args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    now = date.today()

    if not args:
        year, month = now.year, now.month
    elif len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        year, month = int(args[0]), int(args[1])
    elif len(args) == 1 and len(args[0]) == 7 and args[0][4] == "-":
        year, month = int(args[0][:4]), int(args[0][5:7])
    else:
        print("Используйте: ГОД МЕСЯЦ   например: 2026 3", file=sys.stderr)
        sys.exit(1)

    if not 1 <= month <= 12:
        print("Месяц должен быть от 1 до 12", file=sys.stderr)
        sys.exit(1)

    payload = compute_turnover_fact_percent(year, month)
    print(f"{payload['total_fact']:.1f}")


if __name__ == "__main__":
    main()
