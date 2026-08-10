"""Exact search for VK docs from screenshot + CRM_Исполнители."""
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
VK = "Document_ТД_ВходящаяКорреспонденция"
NUMBERS = ["НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101", "МПГ00010733/1", "МПГ00010733"]


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    for num in NUMBERS:
        filt = f"Number eq '{num}' and DeletionMark eq false"
        url = (
            f"{BASE}/{quote(VK)}?$format=json&$filter={quote(filt, safe='')}"
            f"&$orderby={quote('Date desc')}&$top=20"
            f"&$expand={quote('CRM_Исполнители', safe=',')}"
        )
        r = session.get(url, timeout=120)
        print(f"\n=== {num} HTTP {r.status_code} ===")
        if not r.ok:
            print(r.text[:200])
            continue
        rows = r.json().get("value", [])
        print(f"count={len(rows)}")
        for row in rows:
            print(
                f"  Date={str(row.get('Date') or '')[:19]} Status={row.get('Статус')} "
                f"Posted={row.get('Posted')} Отв={row.get('Ответственный_Key')} "
                f"Ref={row.get('Ref_Key')}"
            )
            print(f"  Кому={row.get('Кому')!r} Тема={row.get('ТемаСлужебнойЗаписки')!r}")
            execs = row.get("CRM_Исполнители") or []
            print(f"  CRM_Исполнители ({len(execs)}):")
            for e in execs:
                print(json.dumps(e, ensure_ascii=False, default=str)[:500])

    # Also Date = 2026-04-24 specifically
    print("\n=== VK Date on 2026-04-24 ===")
    filt = (
        "Date ge datetime'2026-04-24T00:00:00' "
        "and Date lt datetime'2026-04-25T00:00:00' "
        "and DeletionMark eq false"
    )
    url = (
        f"{BASE}/{quote(VK)}?$format=json&$filter={quote(filt, safe='')}"
        f"&$top=50&$orderby={quote('Number asc')}"
        f"&$select={quote('Number,Date,Статус,Ответственный_Key,Кому,Ref_Key', safe=',_')}"
    )
    r = session.get(url, timeout=120)
    print("HTTP", r.status_code)
    for row in (r.json().get("value", []) if r.ok else []):
        print(
            f"  {row.get('Number')} {str(row.get('Date') or '')[:16]} "
            f"{row.get('Статус')} Отв={row.get('Ответственный_Key')}"
        )

    # Search Document_ТД_СлужебнаяЗаписка with same numbers
    print("\n=== СлужебнаяЗаписка by numbers ===")
    SZ = "Document_ТД_СлужебнаяЗаписка"
    for num in NUMBERS[:4]:
        filt = f"substringof('{num.split('/')[0]}', Number) and Date ge datetime'2026-04-01T00:00:00'"
        url = f"{BASE}/{quote(SZ)}?$format=json&$filter={quote(filt, safe='')}&$top=5"
        r = session.get(url, timeout=60)
        print(num, "HTTP", r.status_code, "n=", len(r.json().get("value", []) if r.ok else []), (r.text[:80] if not r.ok else ""))

    # Metadata property names containing Перенос / Срок on Task
    print("\n=== Task fields with Перенос/Срок for a sample with CRM_Перенести ===")
    # Search any task with CRM_ПоследнееДействиеСтрокой containing Перенос
    filt = "substringof('Перенос', CRM_ПоследнееДействиеСтрокой) and Date ge datetime'2026-05-01T00:00:00' and Date lt datetime'2026-06-01T00:00:00'"
    url = (
        f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
        f"?$format=json&$filter={quote(filt, safe='')}&$top=20"
    )
    r = session.get(url, timeout=120)
    print("HTTP", r.status_code)
    if r.ok:
        rows = r.json().get("value", [])
        print("count", len(rows))
        for row in rows[:10]:
            print(
                f"  {row.get('Number')} Date={str(row.get('Date') or '')[:16]} "
                f"срок={str(row.get('СрокИсполнения') or '')[:10]} "
                f"Исполнитель={row.get('Исполнитель')} "
                f"Ex={row.get('Executed')}"
            )
            print(f"    last={(row.get('CRM_ПоследнееДействиеСтрокой') or '')[:160]}")
            print(f"    subj={(row.get('ПредметСтрокой') or '')[:120]}")


if __name__ == "__main__":
    main()
