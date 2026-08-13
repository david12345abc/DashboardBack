"""Probe BusinessProcess_Задание and Task via role for Voronchikhina May."""
from __future__ import annotations

import functools
import json
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE, fetch_all

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

REF = "a312e193-d7b7-11ee-94a5-6cb31113810e"


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    # BusinessProcess_Задание
    for label, filt in [
        (
            "BP Задание Исполнитель May Date",
            f"Исполнитель eq cast(guid'{REF}','Catalog_Пользователи') "
            f"and Date ge datetime'2026-05-01T00:00:00' "
            f"and Date lt datetime'2026-06-01T00:00:00' and DeletionMark eq false",
        ),
        (
            "BP Задание Started May",
            f"Исполнитель eq cast(guid'{REF}','Catalog_Пользователи') "
            f"and Started eq true and DeletionMark eq false "
            f"and Date ge datetime'2026-04-01T00:00:00' "
            f"and Date lt datetime'2026-07-01T00:00:00'",
        ),
    ]:
        url = (
            f"{BASE}/{quote('BusinessProcess_Задание')}"
            f"?$format=json&$filter={quote(filt, safe='')}&$top=50"
        )
        r = session.get(url, timeout=120)
        print(f"\n[{label}] HTTP {r.status_code}")
        if not r.ok:
            print(r.text[:250])
            continue
        rows = r.json().get("value", [])
        print("count", len(rows))
        for row in rows[:20]:
            print(
                f"  {row.get('Number')} Date={str(row.get('Date') or '')[:16]} "
                f"срок={str(row.get('СрокИсполнения') or '')[:10]} "
                f"Completed={row.get('Completed')} Выполнено={row.get('Выполнено')} "
                f"{(row.get('Наименование') or '')[:80]}"
            )
            print(f"    Предмет={row.get('Предмет_Type')} {(row.get('Содержание') or '')[:80]}")

    # Tasks assigned via РольИсполнителя - list roles, find HR-related
    print("\n=== Roles with персонал/кадр/суд/делопроизв ===")
    url = (
        f"{BASE}/{quote('Catalog_РолиИсполнителей')}"
        f"?$format=json&$top=500"
        f"&$select={quote('Ref_Key,Description,КраткоеПредставление', safe=',_')}"
    )
    roles = fetch_all(session, url)
    for role in roles:
        desc = (role.get("Description") or "") + " " + (role.get("КраткоеПредставление") or "")
        low = desc.lower().replace("ё", "е")
        if any(x in low for x in ("персонал", "кадр", "суд", "делопроиз", "входящ", "корреспонден")):
            print(f"  {role.get('Description')} | {role.get('КраткоеПредставление')} | {role.get('Ref_Key')}")

    # InformationRegister_ИсполнителиЗадачByAddress - try common BSP names
    for name in [
        "InformationRegister_ИсполнителиЗадач",
        "InformationRegister_ИсполнителиЗадачПоАдресации",
        "InformationRegister_АдресацияЗадач",
        "Catalog_ИсполнителиЗадач",
        "InformationRegister_CRM_ЗадачиПользователей",
    ]:
        url = f"{BASE}/{quote(name)}?$format=json&$top=1"
        r = session.get(url, timeout=30)
        if r.status_code != 404:
            print(f"{name}: HTTP {r.status_code}")


if __name__ == "__main__":
    main()
