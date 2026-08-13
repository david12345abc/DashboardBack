"""Find May 2026 EDO docs/tasks for Voronchikhina."""
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


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    # 1) VK docs dated Apr-May 2026 with numbers from screenshot
    print("=== VK Date Apr-Jun 2026 ===")
    filt = (
        "Date ge datetime'2026-04-01T00:00:00' "
        "and Date lt datetime'2026-07-01T00:00:00' "
        "and DeletionMark eq false"
    )
    url = (
        f"{BASE}/{quote(VK)}?$format=json&$filter={quote(filt, safe='')}"
        f"&$orderby={quote('Date desc')}&$top=200"
        f"&$select={quote('Ref_Key,Number,Date,Статус,Ответственный_Key,Кому,ТемаСлужебнойЗаписки,Posted', safe=',_')}"
    )
    r = session.get(url, timeout=180)
    print("HTTP", r.status_code)
    rows = r.json().get("value", []) if r.ok else []
    print("count", len(rows))
    nums = {"002870", "002937", "003094", "003101", "10733"}
    for row in rows:
        num = str(row.get("Number") or "")
        hit = any(n in num for n in nums) or row.get("Ответственный_Key") == REF
        if hit or True:
            # print all briefly, highlight hits
            mark = "*" if any(n in num for n in nums) else ("R" if row.get("Ответственный_Key") == REF else " ")
            if mark != " " or len(rows) < 40:
                print(
                    f" {mark} {num} Date={str(row.get('Date') or '')[:16]} "
                    f"Статус={row.get('Статус')} Отв={str(row.get('Ответственный_Key') or '')[:8]} "
                    f"Кому={(row.get('Кому') or '')[:40]}"
                )

    print("\n=== Hits by number among all 2026 VK ===")
    filt = (
        "Date ge datetime'2026-01-01T00:00:00' "
        "and Date lt datetime'2027-01-01T00:00:00' "
        "and DeletionMark eq false"
    )
    url = (
        f"{BASE}/{quote(VK)}?$format=json&$filter={quote(filt, safe='')}"
        f"&$orderby={quote('Date asc')}&$top=1000"
        f"&$select={quote('Ref_Key,Number,Date,Статус,Ответственный_Key,Кому', safe=',_')}"
    )
    r = session.get(url, timeout=240)
    rows = r.json().get("value", []) if r.ok else []
    print("VK 2026 total", len(rows))
    for row in rows:
        num = str(row.get("Number") or "")
        if any(n in num for n in nums):
            print(
                f"  {num} Date={str(row.get('Date') or '')[:16]} "
                f"Статус={row.get('Статус')} Отв={row.get('Ответственный_Key')} "
                f"Кому={(row.get('Кому') or '')[:50]} Ref={row.get('Ref_Key')}"
            )

    # 2) Tasks with Предмет_Type = VK
    print("\n=== Tasks where subject type is VK, Date in May, any executor ===")
    # OData may not filter by Предмет_Type easily; try substringof on ПредметСтрокой
    for needle in ["Входящая корреспонденция", "НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101", "МПГ00010733"]:
        filt = (
            f"substringof('{needle}', ПредметСтрокой) and DeletionMark eq false "
            f"and Date ge datetime'2026-04-01T00:00:00' "
            f"and Date lt datetime'2026-07-01T00:00:00'"
        )
        url = (
            f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
            f"?$format=json&$filter={quote(filt, safe='')}"
            f"&$top=50&$orderby={quote('Date desc')}"
        )
        r = session.get(url, timeout=120)
        if not r.ok:
            print(needle, "HTTP", r.status_code, r.text[:120])
            continue
        found = r.json().get("value", [])
        print(f"\n[{needle}] {len(found)}")
        for row in found[:15]:
            print(
                f"  {row.get('Number')} Date={str(row.get('Date') or '')[:16]} "
                f"срок={str(row.get('СрокИсполнения') or '')[:10]} "
                f"Ex={row.get('Executed')} Исполнитель={row.get('Исполнитель')}"
            )
            print(f"    {(row.get('ПредметСтрокой') or '')[:140]}")
            print(f"    {(row.get('Description') or '')[:100]}")

    # 3) Catalog / enum for roles - maybe tasks assigned via РольИсполнителя
    print("\n=== Tasks for Voronchikhina via Автор May ===")
    filt = (
        f"Автор eq cast(guid'{REF}','Catalog_Пользователи') "
        f"and DeletionMark eq false "
        f"and Date ge datetime'2026-05-01T00:00:00' "
        f"and Date lt datetime'2026-06-01T00:00:00'"
    )
    url = (
        f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
        f"?$format=json&$filter={quote(filt, safe='')}"
        f"&$top=50"
    )
    r = session.get(url, timeout=120)
    print("HTTP", r.status_code, "count", len(r.json().get("value", []) if r.ok else []))

    # 4) Look for BusinessProcess entities with Исполнение in name from earlier list
    print("\n=== Probe BusinessProcess_CRM and related ===")
    for name in [
        "BusinessProcess_CRM_БизнесПроцесс",
        "BusinessProcess_Задание",
        "Document_ЭлектронныйДокументИсходящийЭДО",
        "Document_СообщениеЭДО",
    ]:
        url = f"{BASE}/{quote(name)}?$format=json&$top=1"
        r = session.get(url, timeout=60)
        print(name, r.status_code, "empty" if r.ok and not r.json().get("value") else "")


if __name__ == "__main__":
    main()
