import json
from pathlib import Path

snap = json.loads(Path("getkpi/dashboard/debitorka_2026-11-17.json").read_text(encoding="utf-8"))
print("snap na", snap.get("na_datu"), "total", snap.get("total_dz"), "alias", snap.get("dept_alias_source"), "scope", snap.get("kz_scope"))
by = snap.get("by_dept") or {}
for name, row in sorted(by.items(), key=lambda x: -float((x[1] or {}).get("dz") or 0)):
    dz = float((row or {}).get("dz") or 0)
    if abs(dz) < 1:
        continue
    print(f"{dz:15,.2f}  {name}")
print("--- guid ---")
for g, row in (snap.get("by_dept_guid") or {}).items():
    print(g[:8], row.get("name"), row.get("dz"))
