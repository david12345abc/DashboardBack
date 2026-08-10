"""Probe BusinessProcess_Задание — likely source of EDO execution report."""
from __future__ import annotations

import functools
import json
import re
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

VOR = "a312e193-d7b7-11ee-94a5-6cb31113810e"
BP = "BusinessProcess_Задание"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})

    meta = s.get(f"{BASE}/$metadata", timeout=180).text
    m = re.search(rf'<EntityType Name="{BP}"[^>]*>(.*?)</EntityType>', meta, re.S)
    if not m:
        print("no entity type")
        return
    props = re.findall(r'<Property Name="([^"]+)"', m.group(1))
    navs = re.findall(r'<NavigationProperty Name="([^"]+)"', m.group(1))
    print("props:", props)
    print("navs:", navs)

    # sample
    url = f"{BASE}/{quote(BP)}?$top=1&$format=json"
    resp = s.get(url, timeout=60)
    print("sample", resp.status_code)
    if resp.ok and resp.json().get("value"):
        print(json.dumps(resp.json()["value"][0], ensure_ascii=False, indent=2)[:2500])

    # filter by subject / number / date May / executor
    selects = ",".join(
        p
        for p in props
        if p
        in {
            "Ref_Key",
            "Number",
            "Date",
            "Started",
            "Completed",
            "HeadTask_Key",
            "Наименование",
            "Содержание",
            "СрокИсполнения",
            "Дата",
            "ПредметСтрокой",
            "Предмет",
            "Предмет_Type",
            "Исполнитель",
            "Исполнитель_Type",
            "Проверяющий",
            "Проверяющий_Type",
            "Автор",
            "Автор_Type",
            "Важность",
            "DeletionMark",
            "Стартован",
            "Завершен",
            "ГлавнаяЗадача_Key",
        }
        or any(x in p for x in ("Исполн", "Провер", "Предмет", "Срок", "Содерж", "Наимен", "Автор", "Дата", "Number", "Started", "Completed"))
    )
    # simpler: no select first for one filter
    for filt, label in (
        (f"substringof('НП00-002870', ПредметСтрокой)", "subj870"),
        (f"substringof('НП00-002870', Наименование)", "name870"),
        (f"substringof('НП00-002870', Содержание)", "cont870"),
        (f"substringof('Входящая корреспонденция НП00-002870', ПредметСтрокой)", "fullsubj"),
        (
            f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи') and year(Date) eq 2026 and month(Date) eq 5",
            "vor_may_date",
        ),
        (
            f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи') and year(СрокИсполнения) eq 2026 and month(СрокИсполнения) eq 5",
            "vor_may_deadline",
        ),
        (
            f"year(Date) eq 2026 and month(Date) eq 5 and substringof('Входящая корреспонденция', ПредметСтрокой)",
            "may_vk",
        ),
    ):
        url = f"{BASE}/{quote(BP)}?$filter={quote(filt)}&$top=20&$format=json"
        resp = s.get(url, timeout=90)
        print(f"\n{label}: {resp.status_code}", end=" ")
        if not resp.ok:
            print(resp.text[:220].replace("\n", " "))
            continue
        rows = resp.json().get("value") or []
        print(f"rows={len(rows)}")
        for r in rows[:8]:
            keys_interest = {
                k: r.get(k)
                for k in r
                if any(
                    x in k
                    for x in (
                        "Date",
                        "Number",
                        "Срок",
                        "Предмет",
                        "Исполн",
                        "Провер",
                        "Наимен",
                        "Содерж",
                        "Started",
                        "Completed",
                        "Стартован",
                        "Завершен",
                        "Ref_Key",
                    )
                )
            }
            print(json.dumps(keys_interest, ensure_ascii=False)[:500])

    # Also Task linked via BusinessProcess
    # Search Task where BusinessProcess points to Задание
    # Get May BP for Vor and list
    filt = f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи') and year(Date) eq 2026"
    url = f"{BASE}/{quote(BP)}?$filter={quote(filt)}&$top=50&$format=json&$orderby=Date"
    resp = s.get(url, timeout=90)
    print(f"\nVOR 2026 all: {resp.status_code}", end=" ")
    if resp.ok:
        rows = resp.json().get("value") or []
        print(f"rows={len(rows)}")
        for r in rows:
            print(
                r.get("Date"),
                r.get("СрокИсполнения"),
                (r.get("ПредметСтрокой") or r.get("Наименование") or "")[:100],
            )


if __name__ == "__main__":
    main()
