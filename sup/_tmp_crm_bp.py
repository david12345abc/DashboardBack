"""Probe CRM_БизнесПроцесс and Document_ТД_Поручения for Vor / VK."""
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
VK870 = "05dc41cd-3fb7-11f1-97ed-6cb31113810e"


def probe(ent: str, filters: list[tuple[str, str]]) -> None:
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})
    meta = s.get(f"{BASE}/$metadata", timeout=180).text
    m = re.search(rf'<EntityType Name="{re.escape(ent)}"[^>]*>(.*?)</EntityType>', meta, re.S)
    if not m:
        # try without exact - EntitySet exists?
        url = f"{BASE}/{quote(ent)}?$top=0&$format=json"
        resp = s.get(url, timeout=30)
        print(f"\n{ent}: entitytype miss, set={resp.status_code}")
        return
    props = re.findall(r'<Property Name="([^"]+)"', m.group(1))
    print(f"\n=== {ent} props({len(props)}) ===")
    interesting = [
        p
        for p in props
        if any(
            x in p.lower()
            for x in (
                "исполн",
                "провер",
                "предмет",
                "срок",
                "дата",
                "автор",
                "описан",
                "содерж",
                "наимен",
                "статус",
                "состоян",
                "number",
                "date",
                "started",
                "completed",
            )
        )
    ]
    print("interesting:", interesting[:40])

    url = f"{BASE}/{quote(ent)}?$top=1&$format=json"
    resp = s.get(url, timeout=60)
    print("sample", resp.status_code, end=" ")
    if resp.ok:
        val = resp.json().get("value") or []
        if val:
            print(list(val[0].keys())[:40])
        else:
            print("empty")
    else:
        print(resp.text[:150].replace("\n", " "))
        return

    for filt, label in filters:
        url = f"{BASE}/{quote(ent)}?$filter={quote(filt)}&$top=20&$format=json"
        resp = s.get(url, timeout=90)
        print(f"  {label}: {resp.status_code}", end=" ")
        if not resp.ok:
            print(resp.text[:180].replace("\n", " "))
            continue
        rows = resp.json().get("value") or []
        print(f"rows={len(rows)}")
        for r in rows[:6]:
            slim = {
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
                        "Описан",
                        "Содерж",
                        "Статус",
                        "Состоян",
                        "Автор",
                        "Ref_Key",
                        "Started",
                        "Completed",
                    )
                )
            }
            print("   ", json.dumps(slim, ensure_ascii=False)[:450])


def main() -> None:
    entities = [
        "BusinessProcess_CRM_БизнесПроцесс",
        "Document_ТД_Поручения",
        "Document_CRM_Взаимодействие",
        "Task_CRM_Задача",
        "BusinessProcess_CRM_Согласование",
    ]
    # discover CRM BP / Task entity sets
    s = requests.Session()
    s.auth = AUTH
    meta = s.get(f"{BASE}/$metadata", timeout=180).text
    crm = sorted(
        set(
            re.findall(
                r'EntitySet Name="((?:BusinessProcess|Task|Document|InformationRegister)_CRM_[^"]+)"',
                meta,
            )
        )
    )
    print("CRM entity sets:", len(crm))
    for n in crm:
        if any(x in n for x in ("Бизнес", "Задач", "Поруч", "Исполн", "Соглас", "Взаимод")):
            print(" ", n)

    td = sorted(
        set(
            re.findall(
                r'EntitySet Name="((?:BusinessProcess|Task|Document|InformationRegister)_ТД_[^"]*(?:Поруч|Исполн|Задач|Маршрут|Корреспонденц)[^"]*)"',
                meta,
            )
        )
    )
    print("\nTD related:", td)

    filters_common = [
        (f"substringof('НП00-002870', Наименование)", "name870"),
        (f"substringof('НП00-002870', Описание)", "opis870"),
        (f"substringof('НП00-002870', Содержание)", "cont870"),
        (f"Предмет eq cast(guid'{VK870}','Document_ТД_ВходящаяКорреспонденция')", "predmet"),
        (
            f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи') and year(Date) eq 2026 and month(Date) eq 5",
            "vor_may",
        ),
    ]

    for ent in [
        "BusinessProcess_CRM_БизнесПроцесс",
        "Document_ТД_Поручения",
        "Document_CRM_Взаимодействие",
    ] + [n for n in crm if "Задач" in n or "БизнесПроцесс" in n][:8]:
        probe(ent, filters_common)


if __name__ == "__main__":
    main()
