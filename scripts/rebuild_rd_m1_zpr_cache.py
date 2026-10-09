"""Force-rebuild RD-M1 (ЗПР) YTD caches.

Usage: python scripts/rebuild_rd_m1_zpr_cache.py [YEAR] [MONTH ...]
Without months — all months of the year up to the current one.
"""
from __future__ import annotations

import os
import shutil
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Dashbord.settings")

import django

django.setup()

from devdir import rd_m1_zpr


def rebuild(year: int, month: int) -> bool:
    path = rd_m1_zpr.cache_file_path_for_period(year, month)
    backup = path.with_suffix(".json.bak")
    if path.exists():
        shutil.move(path, backup)

    payload = rd_m1_zpr.get_rd_m1_zpr_ytd(year=year, month=month) or {}
    failed = (payload.get("debug") or {}).get("status") == "error" or not path.exists()

    if failed:
        if backup.exists():
            shutil.move(backup, path)
        err = (payload.get("debug") or {}).get("error")
        print(f"  {year}-{month:02d}: FAILED, old cache restored. {err or ''}")
        return False

    backup.unlink(missing_ok=True)
    row = payload.get("last_full_month_row") or {}
    ytd = payload.get("ytd") or {}
    print(
        f"  {year}-{month:02d}: plan={row.get('plan')} fact={row.get('fact')} "
        f"kpi={row.get('kpi_pct')}% | ytd kpi={ytd.get('kpi_pct')}%"
    )
    return True


def main() -> None:
    today = date.today()
    year = int(sys.argv[1]) if len(sys.argv) > 1 else today.year
    months = [int(m) for m in sys.argv[2:]] or list(
        range(1, (today.month if year == today.year else 12) + 1)
    )

    print(f"=== RD-M1 ЗПР {year} ===")
    ok = sum(rebuild(year, m) for m in months)
    print(f"done: {ok}/{len(months)}")
    sys.exit(0 if ok == len(months) else 1)


if __name__ == "__main__":
    main()
