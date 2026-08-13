"""Probe VK CRM_Исполнители and related OData for May Voronchikhina EDO report docs."""
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

VK = "Document_ТД_ВходящаяКорреспонденция"
USER = "a312e193-d7b7-11ee-94a5-6cb31113810e"
NUMS = ["НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101"]


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})

    # metadata snippet for VK entity properties
    # fetch docs by number in 2026
    for num in NUMS:
        filt = f"Number eq '{num}' and year(Date) eq 2026"
        url = (
            f"{BASE}/{quote(VK)}?$filter={quote(filt)}"
            f"&$select={quote('Ref_Key,Number,Date,Date_Key,DeletionMark', safe=',_')}"
            f"&$format=json"
        )
        rows = fetch_all(s, url)
        print(f"\n=== {num}: {len(rows)} ===")
        for r in rows:
            ref = r["Ref_Key"]
            print("doc", r.get("Number"), r.get("Date"), ref)

            # expand CRM_Исполнители
            for nav in (
                "CRM_Исполнители",
                "ПодразделениеИсполнитель",
            ):
                u2 = f"{BASE}/{quote(VK)}(guid'{ref}')/{quote(nav)}?$format=json"
                resp = s.get(u2, timeout=60)
                print(f"  {nav}: {resp.status_code}", end="")
                if resp.ok:
                    data = resp.json().get("value", resp.json())
                    if isinstance(data, dict) and "value" in data:
                        data = data["value"]
                    if isinstance(data, list):
                        print(f" rows={len(data)}")
                        for row in data[:10]:
                            print("   ", json.dumps(row, ensure_ascii=False)[:400])
                    else:
                        print(" ", json.dumps(data, ensure_ascii=False)[:300])
                else:
                    print(" ", resp.text[:120])

            # Try $expand
            u3 = (
                f"{BASE}/{quote(VK)}(guid'{ref}')"
                f"?$expand={quote('CRM_Исполнители')}"
                f"&$format=json"
            )
            resp = s.get(u3, timeout=60)
            if resp.ok:
                js = resp.json()
                execs = js.get("CRM_Исполнители") or []
                print(f"  expand CRM_Исполнители: {len(execs)}")
                # print all keys of doc briefly
                keys = [k for k in js if not k.startswith("odata") and k != "CRM_Исполнители"]
                interesting = [k for k in keys if any(x in k.lower() for x in ("исполн", "срок", "постанов", "ответ", "задач", "дата"))]
                print("  interesting keys:", interesting)
                for k in interesting:
                    print(f"    {k}={js.get(k)}")

    # Search entity set names containing Исполн and ТД
    meta = s.get(f"{BASE}/$metadata", timeout=120)
    text = meta.text if meta.ok else ""
    print("\nmetadata size", len(text))
    for needle in (
        "ИсполнениеДокументов",
        "ИсполнениеДокумента",
        "ДатаПостановки",
        "ТД_Задач",
        "ЗадачаПо",
        "CRM_Задач",
        "ПереносСрока",
        "СрокИсполненияДокумента",
        "ТД_Исполнение",
    ):
        if needle in text:
            # find EntityType / EntitySet around
            idx = 0
            n = 0
            while n < 8:
                i = text.find(needle, idx)
                if i < 0:
                    break
                snippet = text[max(0, i - 80) : i + 80].replace("\n", " ")
                print(f"META {needle}: ...{snippet}...")
                idx = i + len(needle)
                n += 1
        else:
            print(f"META miss {needle}")

    # Also list EntitySets with Исполн
    import re
    sets = re.findall(r'EntitySet Name="([^"]*Исполн[^"]*)"', text)
    print("\nEntitySets Исполн:", sets[:80])
    sets2 = re.findall(r'EntitySet Name="([^"]*ТД_[^"]*Задач[^"]*)"', text)
    print("EntitySets ТД_Задач:", sets2)
    sets3 = re.findall(r'EntitySet Name="([^"]*Постанов[^"]*)"', text)
    print("EntitySets Постанов:", sets3)


if __name__ == "__main__":
    main()
