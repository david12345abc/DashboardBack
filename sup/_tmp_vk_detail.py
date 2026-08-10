"""Load VK НП00-002870 from 2026-04-24 and linked executor tasks."""
from __future__ import annotations

import functools
import json
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE, fetch_all, normalize_text

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

REF = "a312e193-d7b7-11ee-94a5-6cb31113810e"
VK = "Document_ТД_ВходящаяКорреспонденция"
NUMBERS = ["НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101"]


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    # Who is 7a3fa603...?
    users = fetch_all(
        session,
        f"{BASE}/{quote('Catalog_Пользователи')}"
        f"?$select={quote('Ref_Key,Description', safe=',_')}&$format=json",
    )
    by_key = {u["Ref_Key"]: (u.get("Description") or "").strip() for u in users}
    print("Отв on VK sample:", by_key.get("7a3fa603-0899-11f0-9637-6cb31113810e"))
    print("Voronchikhina:", REF, by_key.get(REF))

    docs = []
    for num in NUMBERS:
        filt = (
            f"Number eq '{num}' and Date ge datetime'2026-04-01T00:00:00' "
            f"and Date lt datetime'2026-07-01T00:00:00' and DeletionMark eq false"
        )
        url = f"{BASE}/{quote(VK)}?$format=json&$filter={quote(filt, safe='')}&$top=5"
        r = session.get(url, timeout=120)
        rows = r.json().get("value", []) if r.ok else []
        print(f"\n=== {num} rows={len(rows)} ===")
        for row in rows:
            docs.append(row)
            print(json.dumps({k: v for k, v in row.items() if "@" not in k and "Base64" not in k and not isinstance(v, (list, dict))}, ensure_ascii=False, indent=2, default=str)[:2000])
            # fetch CRM_Исполнители navigation
            nav = row.get("CRM_Исполнители@navigationLinkUrl") or (
                f"{VK}(guid'{row['Ref_Key']}')/CRM_Исполнители"
            )
            if not nav.startswith("http"):
                nav = f"{BASE}/{nav}" if not nav.startswith(VK) else f"{BASE}/{nav}"
            # try standard path
            nav = f"{BASE}/{quote(VK)}(guid'{row['Ref_Key']}')/CRM_Исполнители?$format=json"
            rr = session.get(nav, timeout=60)
            print(f"CRM_Исполнители HTTP {rr.status_code}")
            if rr.ok:
                for e in rr.json().get("value", []):
                    print("  exec row:", json.dumps(e, ensure_ascii=False, default=str)[:600])

    # Tasks by Предмет guid
    print("\n=== Tasks by Предмет = VK Ref_Key ===")
    for row in docs:
        ref = row["Ref_Key"]
        num = row.get("Number")
        filt = f"Предмет eq cast(guid'{ref}','{VK}') and DeletionMark eq false"
        url = (
            f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
            f"?$format=json&$filter={quote(filt, safe='')}"
            f"&$top=50&$orderby={quote('Date asc')}"
        )
        r = session.get(url, timeout=120)
        print(f"\n{num} / {ref}: HTTP {r.status_code}")
        if not r.ok:
            # try without cast
            filt2 = f"Предмет eq guid'{ref}' and DeletionMark eq false"
            url = (
                f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
                f"?$format=json&$filter={quote(filt2, safe='')}"
                f"&$top=50"
            )
            r = session.get(url, timeout=120)
            print("  fallback HTTP", r.status_code, r.text[:200] if not r.ok else "")
        if r.ok:
            tasks = r.json().get("value", [])
            print(f"  tasks={len(tasks)}")
            for t in tasks:
                ex = t.get("Исполнитель")
                ex_name = by_key.get(ex, ex) if isinstance(ex, str) else ex
                print(
                    f"  {t.get('Number')} Date={str(t.get('Date') or '')[:16]} "
                    f"срок={str(t.get('СрокИсполнения') or '')[:10]} "
                    f"исп={str(t.get('ДатаИсполнения') or '')[:10]} "
                    f"Ex={t.get('Executed')} Исполнитель={ex_name}"
                )
                print(f"    desc={(t.get('Description') or '')[:120]}")
                print(f"    last={(t.get('CRM_ПоследнееДействиеСтрокой') or '')[:160]}")
                print(f"    Результат={(t.get('РезультатВыполнения') or '')[:160]}")
                print(f"    Описание={(t.get('Описание') or '')[:160]}")


if __name__ == "__main__":
    main()
