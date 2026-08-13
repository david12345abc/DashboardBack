"""Probe Document_ТД_ВходящаяКорреспонденция and related tasks for Voronchikhina May."""
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

REF = "a312e193-d7b7-11ee-94a5-6cb31113810e"


def probe(session, name, filt=None, top=3, expand=None):
    parts = [f"{BASE}/{quote(name)}?$format=json&$top={top}"]
    if filt:
        parts[0] += f"&$filter={quote(filt, safe='')}"
    if expand:
        parts[0] += f"&$expand={quote(expand, safe=',')}"
    url = parts[0]
    print(f"\n=== {name} ===")
    r = session.get(url, timeout=120)
    print(f"HTTP {r.status_code}")
    if not r.ok:
        print(r.text[:300])
        return
    rows = r.json().get("value", [])
    print(f"rows={len(rows)}")
    if not rows:
        return
    sample = rows[0]
    keys = [k for k in sorted(sample.keys()) if "@" not in k and "Base64" not in k]
    print("keys:", ", ".join(keys[:60]))
    # interesting scalars
    interesting = {}
    for k in keys:
        v = sample.get(k)
        if isinstance(v, (list, dict)):
            continue
        if v in (None, "", False, 0):
            continue
        interesting[k] = v
    print(json.dumps(interesting, ensure_ascii=False, indent=2, default=str)[:2500])


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    for name in [
        "Document_ТД_ВходящаяКорреспонденция",
        "Document_ТД_Поручения",
        "Document_ТД_ДополнительныеСоглашения",
        "Document_ТД_ДополнительноеСоглашение",
        "Document_ТД_ПротоколРазногласий",
        "Document_ТД_ПротоколыРазногласий",
        "InformationRegister_ТД_ИсполнениеДокументовЭДО",
        "InformationRegister_ТД_ЗадачиПоДокументамЭДО",
        "InformationRegister_ТД_ЗадачиЭДО",
        "BusinessProcess_ТД_ИсполнениеДокумента",
        "BusinessProcess_Исполнение",
        "Task_ЗадачаИсполнителя",
    ]:
        probe(session, name, top=1)

    # Try VK by number from screenshot
    print("\n\n=== Search VK by Number contains 002870 ===")
    for name in ["Document_ТД_ВходящаяКорреспонденция"]:
        filt = "substringof('002870', Number)"
        url = (
            f"{BASE}/{quote(name)}?$format=json&$filter={quote(filt, safe='')}"
            f"&$top=5"
        )
        r = session.get(url, timeout=120)
        print(name, r.status_code)
        if r.ok:
            for row in r.json().get("value", []):
                print(
                    " ",
                    row.get("Number"),
                    str(row.get("Date") or "")[:19],
                    row.get("Ref_Key"),
                )
                # dump more fields with deadline-like names
                for k, v in row.items():
                    if "@" in k or "Base64" in k:
                        continue
                    kl = k.lower()
                    if any(
                        x in kl
                        for x in (
                            "срок",
                            "исполн",
                            "постанов",
                            "ответств",
                            "автор",
                            "статус",
                            "тема",
                            "номер",
                            "date",
                            "number",
                        )
                    ):
                        if not isinstance(v, (list, dict)):
                            print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
