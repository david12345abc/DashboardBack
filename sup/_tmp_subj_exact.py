"""Exact ПредметСтрокой search for VK docs from report."""
from __future__ import annotations

import functools
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

REF = "a312e193-d7b7-11ee-94a5-6cb31113810e"


def main() -> None:
    session = requests.Session()
    session.auth = AUTH
    needles = [
        "НП00-002870 от 24.04.2026",
        "НП00-002937 от 04.05.2026",
        "НП00-003094 от 18.05.2026",
        "НП00-003101 от 19.05.2026",
        "МПГ00010733",
        "05dc41cd-3fb7-11f1-97ed-6cb31113810e",
    ]
    for needle in needles:
        filt = f"substringof('{needle}', ПредметСтрокой)"
        url = (
            f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
            f"?$format=json&$filter={quote(filt, safe='')}&$top=20"
        )
        r = session.get(url, timeout=90)
        print(f"\n[{needle}] HTTP {r.status_code} n={len(r.json().get('value',[]) if r.ok else [])}")
        if not r.ok:
            print(r.text[:150])
            continue
        for t in r.json().get("value", []):
            print(
                f"  {t.get('Number')} Date={str(t.get('Date') or '')[:16]} "
                f"срок={str(t.get('СрокИсполнения') or '')[:10]} "
                f"Ex={t.get('Executed')} Исп={t.get('Исполнитель')} "
                f"Роль={t.get('РольИсполнителя_Key')} Группа={t.get('ГруппаИсполнителейЗадач_Key')}"
            )
            print(f"    {(t.get('ПредметСтрокой') or '')[:160]}")
            print(f"    desc={(t.get('Description') or '')[:100]}")
            # is it Voronchikhina?
            if t.get("Исполнитель") == REF:
                print("    *** EXECUTOR = VORONCHIKHINA ***")


if __name__ == "__main__":
    main()
