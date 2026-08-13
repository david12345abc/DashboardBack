"""Probe VK CRM_Исполнители tabular entity and related."""
from __future__ import annotations

import functools
import json
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

REF_DOC = "05dc41cd-3fb7-11f1-97ed-6cb31113810e"  # НП00-002870
REF_USER = "a312e193-d7b7-11ee-94a5-6cb31113810e"


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    entities = [
        "Document_ТД_ВходящаяКорреспонденция_CRM_Исполнители",
        "Document_ТД_ВходящаяКорреспонденция_CRM_Исполнители_RowType",
        "InformationRegister_CRM_Задачи",
        "InformationRegister_CRM_СостоянияИнтересов",
        "InformationRegister_ИсполнителиЗадач",
        "InformationRegister_ДанныеБизнесПроцессов",
        "InformationRegister_ИсторияВыполненияЗадач",
        "InformationRegister_ИсторияСроковЗадач",
        "InformationRegister_СрокиИсполненияПроцессов",
        "InformationRegister_ТекущиеИсполнителиЗадач",
        "Catalog_РолиИсполнителей",
    ]
    for name in entities:
        url = f"{BASE}/{quote(name)}?$format=json&$top=2"
        r = session.get(url, timeout=60)
        print(f"\n{name}: HTTP {r.status_code}")
        if not r.ok:
            print(r.text[:180])
            continue
        rows = r.json().get("value", [])
        print(f"  rows={len(rows)}")
        if rows:
            keys = [k for k in rows[0] if "@" not in k]
            print("  keys:", keys)
            print(json.dumps(rows[0], ensure_ascii=False, indent=2, default=str)[:1200])

    # Filter tabular by Ref_Key = doc
    name = "Document_ТД_ВходящаяКорреспонденция_CRM_Исполнители"
    for filt in [
        f"Ref_Key eq guid'{REF_DOC}'",
        f"Исполнитель_Key eq guid'{REF_USER}'",
    ]:
        url = (
            f"{BASE}/{quote(name)}?$format=json&$filter={quote(filt, safe='')}"
            f"&$top=20"
        )
        r = session.get(url, timeout=60)
        print(f"\nfilter {filt[:50]}... HTTP {r.status_code}")
        if r.ok:
            rows = r.json().get("value", [])
            print("count", len(rows))
            for row in rows[:10]:
                print(json.dumps(row, ensure_ascii=False, default=str)[:500])
        else:
            print(r.text[:200])

    # All VK where Кому contains Ворончих or Ответственный - already know Отв is Akinina
    # Search metadata for EntityType containing Исполнение
    print("\n=== $metadata names with Исполнен/Задач/Поруч/ЭДО/Корреспонденц ===")
    r = session.get(f"{BASE}/$metadata", timeout=180)
    import re
    names = sorted(set(re.findall(r'Name="([^"]+)"', r.text)))
    for n in names:
        low = n.lower()
        if any(x in low for x in ("корреспонденц", "исполнениедокумент", "задачэдо", "эдозадач")):
            print(n)


if __name__ == "__main__":
    main()
