"""Rebuild RD-M3-1 snapshot and caches for Apr-Jun 2026."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Dashbord.settings")

import django

django.setup()

from getkpi.devdir.turboproject_projects_by_resources import (
    CACHE_DIR,
    CACHE_PATH,
    TABLE_CACHE_PREFIX,
    TILE_CACHE_PREFIX,
    get_projects_deviation_table,
    get_rd_m3_1_ytd,
)

YEAR = 2026
MONTHS = (4, 5, 6)


def main() -> None:
    if CACHE_PATH.exists():
        CACHE_PATH.unlink()

    for month in MONTHS:
        for prefix in (TILE_CACHE_PREFIX, TABLE_CACHE_PREFIX):
            path = CACHE_DIR / f"{prefix}_{YEAR}_{month:02d}.json"
            if path.exists():
                path.unlink()

    for month in MONTHS:
        payload = get_rd_m3_1_ytd(year=YEAR, month=month)
        if payload is None:
            print(f"Month {month:02d}: FAILED (TurboProject unavailable?)")
            continue
        row = payload["monthly_data"][-1]
        table = get_projects_deviation_table(year=YEAR, month=month)
        names = [r["project_name"] for r in table["rows"]]
        print(
            f"Month {month:02d}: plan={row['plan']} fact={row['fact']} "
            f"kpi={row['kpi_pct']}% dev_rows={len(names)}"
        )
        for key in ("Стандарт", "Группы экспертов"):
            hit = any(key in n for n in names)
            print(f"  {key} in deviations: {'YES' if hit else 'no'}")


if __name__ == "__main__":
    main()
