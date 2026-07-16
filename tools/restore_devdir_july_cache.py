"""Пересчитать отсутствующие devdir-плитки за указанный месяц."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Dashbord.settings")
django.setup()

from devdir import rd_m3_budget, rd_m4_fot, rd_q2_tekuchest
from devdir import turboproject_projects_by_resources as tp_pr


def main() -> None:
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    month = int(sys.argv[2]) if len(sys.argv) > 2 else 7

    tiles = [
        ("RD-M3", rd_m3_budget.get_rd_m3_budget_ytd),
        ("RD-M4", rd_m4_fot.get_rd_m4_fot_ytd),
        ("RD-Q2", rd_q2_tekuchest.get_rd_q2_tekuchest_ytd),
        ("RD-M3-1", tp_pr.get_rd_m3_1_ytd),
    ]

    for name, fn in tiles:
        print(f"Computing {name} for {year}-{month:02d}...", flush=True)
        payload = fn(year=year, month=month)
        status = "ok" if payload else "FAILED"
        print(f"  {name}: {status}", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
