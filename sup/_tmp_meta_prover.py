"""Find entities with Проверяющий / CRM поручения / доп соглашения."""
from __future__ import annotations

import functools
import re
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE, fetch_all

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    text = s.get(f"{BASE}/$metadata", timeout=180).text

    for needle in (
        "Проверяющий",
        "ДатаПостановки",
        "CRM_Поручение",
        "Поручение",
        "ДополнительныеСоглашения",
        "ПротоколыРазногласий",
        "ИсполнениеДокументовЭДО",
        "ДокументовЭДО",
    ):
        sets = []
        # EntityTypes containing property
        for m in re.finditer(rf'<Property Name="{needle}"', text):
            chunk = text[max(0, m.start() - 3000) : m.start()]
            ets = re.findall(r'<EntityType Name="([^"]+)"', chunk)
            if ets:
                sets.append(ets[-1])
        # EntitySet names
        es = re.findall(rf'EntitySet Name="([^"]*{needle}[^"]*)"', text)
        print(f"\n{needle}: props_in={sorted(set(sets))[:20]} sets={es[:20]}")

    # Document entity containing Дополнительн
    docs = re.findall(r'EntitySet Name="(Document_[^"]*Доп[^"]*)"', text)
    print("\nDocs Доп:", docs[:40])
    docs2 = re.findall(r'EntitySet Name="(Document_ТД_[^"]*Соглаш[^"]*)"', text)
    print("Docs ТД Соглаш:", docs2)
    docs3 = re.findall(r'EntitySet Name="(Document_ТД_[^"]*Разноглас[^"]*)"', text)
    print("Docs Разноглас:", docs3)

    # Try find доп agreement МПГ00010733/1
    for ent in docs2 + docs3 + [
        "Document_ТД_ДополнительныеСоглашенияИПротоколыРазногласий",
        "Document_ТД_ДополнительноеСоглашение",
    ]:
        # check exists
        url = f"{BASE}/{quote(ent)}?$top=0&$format=json"
        resp = s.get(url, timeout=30)
        if resp.status_code == 404:
            continue
        print(f"EXISTS {ent} {resp.status_code}")
        filt = "substringof('МПГ00010733', Number)"
        url = f"{BASE}/{quote(ent)}?$filter={quote(filt)}&$top=5&$format=json"
        resp = s.get(url, timeout=60)
        if resp.ok:
            rows = resp.json().get("value") or []
            print("  rows", len(rows))
            for r in rows:
                print(" ", r.get("Number"), r.get("Date"), r.get("Ref_Key"))


if __name__ == "__main__":
    main()
