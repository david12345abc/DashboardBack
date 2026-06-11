"""Force-rebuild RD-M4 and RD-M3-1 caches for given months."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Dashbord.settings")

import django

django.setup()

from devdir import rd_m4_fot
from devdir.turboproject_projects_by_resources import (
    CACHE_PATH as M3_SNAPSHOT_PATH,
    get_projects_deviation_table,
    get_rd_m3_1_ytd,
)

DASHBOARD = ROOT / "getkpi" / "dashboard"
YEAR = 2026
MONTHS = (4, 5, 6)


def delete_paths(*paths: Path) -> None:
    for path in paths:
        if path.exists():
            path.unlink()
            print(f"deleted: {path.name}")


def main() -> None:
    delete_paths(M3_SNAPSHOT_PATH)

    for month in MONTHS:
        delete_paths(
            DASHBOARD / f"devdir_rd_m4_fot_{YEAR}_{month:02d}.json",
            DASHBOARD / f"devdir_rd_m3_1_turboproject_projects_by_resources_{YEAR}_{month:02d}.json",
            DASHBOARD / f"devdir_turboproject_projects_by_resources_deviations_{YEAR}_{month:02d}.json",
        )

    print("\n=== RD-M3-1 ===")
    for month in MONTHS:
        payload = get_rd_m3_1_ytd(year=YEAR, month=month)
        row = (payload or {}).get("last_full_month_row") or {}
        print(
            f"  {month:02d}: plan={row.get('plan')} fact={row.get('fact')} "
            f"kpi={row.get('kpi_pct')}%"
        )
        table = get_projects_deviation_table(year=YEAR, month=month)
        summary = (table or {}).get("summary") or {}
        print(
            f"       deviations table: plan={summary.get('plan')} "
            f"fact={summary.get('fact')} rows={len((table or {}).get('rows') or [])}"
        )

    print("\n=== RD-M4 ===")
    for month in MONTHS:
        payload = rd_m4_fot.get_rd_m4_fot_ytd(year=YEAR, month=month)
        row = (payload or {}).get("last_full_month_row") or {}
        print(
            f"  {month:02d}: plan={row.get('plan')} fact={row.get('fact')} "
            f"kpi={row.get('kpi_pct')}%"
        )


if __name__ == "__main__":
    main()
