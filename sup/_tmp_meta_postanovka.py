"""Find EntityType containing ДатаПостановкиЗадачи."""
from __future__ import annotations

import functools
import re
import sys

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    r = s.get(f"{BASE}/$metadata", timeout=180)
    print("status", r.status_code, "len", len(r.text))
    text = r.text

    for needle in (
        "ДатаПостановкиЗадачи",
        "ИсполнениеДокументов",
        "ПереносСрокаПоЗадаче",
        "ПереносСрока",
        "ДатаПостановки",
    ):
        idx = 0
        n = 0
        while n < 10:
            i = text.find(needle, idx)
            if i < 0:
                if n == 0:
                    print("MISS", needle)
                break
            chunk = text[max(0, i - 2500) : i + 120]
            ets = re.findall(r'<EntityType Name="([^"]+)"', chunk)
            props = re.findall(r'<Property Name="([^"]+)"', chunk[-800:])
            print(f"\nHIT {needle} near EntityType={ets[-1] if ets else '?'} props_near={props[-8:]}")
            # also EntitySet linking
            et = ets[-1] if ets else None
            if et:
                sets = re.findall(rf'EntitySet Name="([^"]+)" EntityType="[^"]*\.{re.escape(et)}"', text)
                print("  EntitySets:", sets[:5])
            idx = i + len(needle)
            n += 1

    # EntitySets with Исполнение
    for pat in [
        r'EntitySet Name="([^"]*Исполнение[^"]*)"',
        r'EntitySet Name="([^"]*ДокументовЭДО[^"]*)"',
        r'EntitySet Name="([^"]*Постановк[^"]*)"',
        r'EntitySet Name="([^"]*CRM_Исполнители[^"]*)"',
    ]:
        print(pat, re.findall(pat, text)[:50])

    # Fix VK probe quickly
    from urllib.parse import quote
    from getkpi.list_enterprise_positions import fetch_all

    VK = "Document_ТД_ВходящаяКорреспонденция"
    for num in ("НП00-002870", "НП00-002937"):
        filt = f"Number eq '{num}' and year(Date) eq 2026"
        url = f"{BASE}/{quote(VK)}?$filter={quote(filt)}&$format=json&$top=5"
        rows = fetch_all(s, url)
        print(f"\nVK {num}: {len(rows)}")
        for row in rows:
            ref = row["Ref_Key"]
            print(" ", row.get("Date"), ref)
            u = f"{BASE}/{quote(VK)}(guid'{ref}')/{quote('CRM_Исполнители')}?$format=json"
            resp = s.get(u, timeout=60)
            print("  CRM_Исполнители", resp.status_code, resp.text[:500] if resp.ok else resp.text[:200])
            # dump keys with исполн/срок
            u2 = f"{BASE}/{quote(VK)}(guid'{ref}')?$format=json"
            resp2 = s.get(u2, timeout=60)
            if resp2.ok:
                js = resp2.json()
                for k, v in js.items():
                    if any(x in k.lower() for x in ("исполн", "срок", "постанов", "ответ", "задач")):
                        print(f"   {k}={v}")


if __name__ == "__main__":
    main()
