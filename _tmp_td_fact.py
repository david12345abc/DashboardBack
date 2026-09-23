import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / "getkpi"))

from getkpi.td_m3 import compute_td_m3_fact_monthly

for month in range(1, 10):
    snap = compute_td_m3_fact_monthly(2026, month)
    print(f"{month:02d} {snap['total_fact']:.2f} lines={snap['counts']['docs_included']}")
    if month == 9:
        for name, bucket in sorted(
            snap["groups"].items(),
            key=lambda kv: -float(kv[1]["fact_total"]),
        ):
            print(f"  {bucket['fact_total']:12.2f}  n={bucket['docs']}  {name}")
