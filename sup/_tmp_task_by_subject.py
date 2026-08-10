"""Find Task_ЗадачаИсполнителя by subject numbers from EDO report."""
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
NUMBERS = ["002870", "002937", "003094", "003101", "МПГ00010733", "10733"]


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    # Tasks for Voronchikhina in May by Date - we know 1. Search all her tasks mentioning numbers
    filt = (
        f"Исполнитель eq cast(guid'{REF}','Catalog_Пользователи') "
        f"and DeletionMark eq false "
        f"and Date ge datetime'2026-04-01T00:00:00' "
        f"and Date lt datetime'2026-07-01T00:00:00'"
    )
    url = (
        f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
        f"?$format=json&$filter={quote(filt, safe='')}"
        f"&$top=200&$orderby={quote('Date asc')}"
    )
    r = session.get(url, timeout=180)
    print("HTTP", r.status_code)
    rows = r.json().get("value", []) if r.ok else []
    print(f"Her tasks Apr-Jun: {len(rows)}")
    for row in rows:
        blob = " ".join(
            [
                str(row.get("Number") or ""),
                str(row.get("Description") or ""),
                str(row.get("ПредметСтрокой") or ""),
                str(row.get("Предмет_Type") or ""),
            ]
        )
        print(
            f"  {row.get('Number')} Date={str(row.get('Date') or '')[:16]} "
            f"срок={str(row.get('СрокИсполнения') or '')[:10]} "
            f"Ex={row.get('Executed')} type={(row.get('Предмет_Type') or '').replace('StandardODATA.','')}"
        )
        print(f"    desc={(row.get('Description') or '')[:100]}")
        print(f"    subj={(row.get('ПредметСтрокой') or '')[:120]}")

    print("\n=== Search tasks by ПредметСтрокой contains numbers (any executor) ===")
    for num in NUMBERS:
        filt = f"substringof('{num}', ПредметСтрокой) and DeletionMark eq false"
        url = (
            f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
            f"?$format=json&$filter={quote(filt, safe='')}"
            f"&$top=20&$orderby={quote('Date desc')}"
        )
        r = session.get(url, timeout=120)
        if not r.ok:
            print(num, "HTTP", r.status_code, r.text[:150])
            continue
        found = r.json().get("value", [])
        print(f"\n{num}: {len(found)}")
        for row in found[:10]:
            print(
                f"  {row.get('Number')} Date={str(row.get('Date') or '')[:16]} "
                f"срок={str(row.get('СрокИсполнения') or '')[:10]} "
                f"исп={str(row.get('ДатаИсполнения') or '')[:10]} Ex={row.get('Executed')}"
            )
            print(f"    Исполнитель={row.get('Исполнитель')} type={row.get('Исполнитель_Type')}")
            print(f"    {(row.get('ПредметСтрокой') or '')[:140]}")
            print(f"    {(row.get('Description') or '')[:120]}")


if __name__ == "__main__":
    main()
