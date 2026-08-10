"""Find OData entities related to EDO document execution tasks."""
from __future__ import annotations

import functools
import re
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

KEYS = (
    "эдо",
    "корреспонденц",
    "входящ",
    "исполнен",
    "поручен",
    "задач",
    "соглашени",
    "разноглас",
)


def main() -> None:
    session = requests.Session()
    session.auth = AUTH
    print(f"GET {BASE}/$metadata")
    r = session.get(f"{BASE}/$metadata", timeout=180)
    print(f"HTTP {r.status_code} bytes={len(r.text)}")
    names = sorted(set(re.findall(r'Name="([^"]+)"', r.text)))
    matched = [
        n
        for n in names
        if any(k in n.lower() for k in KEYS)
        and (
            n.startswith("Document_")
            or n.startswith("Task_")
            or n.startswith("BusinessProcess_")
            or n.startswith("InformationRegister_")
            or n.startswith("Catalog_ТД")
        )
    ]
    print(f"Matched entity-ish names: {len(matched)}")
    for n in matched:
        print(n)

    # Also search for report-like names containing ЭДО + задач/исполн
    print("\n--- broader ---")
    for n in names:
        low = n.lower()
        if "эдо" in low and ("задач" in low or "исполн" in low or "поруч" in low):
            print(n)


if __name__ == "__main__":
    main()
