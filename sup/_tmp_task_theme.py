"""Search tasks by theme text from VK docs; probe CRM BP."""
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
NEEDLES = [
    "системообразующих",
    "прохождение опроса",
    "предоставить ответ на запрос",
    "реклама",
    "Перенос срока по задаче",
    "Входящая корреспонденция НП00",
]


def main() -> None:
    session = requests.Session()
    session.auth = AUTH

    for needle in NEEDLES:
        for field in ("Описание", "Description", "РезультатВыполнения", "CRM_ПоследнееДействиеСтрокой", "ПредметСтрокой"):
            filt = (
                f"substringof('{needle}', {field}) and DeletionMark eq false "
                f"and Date ge datetime'2026-04-01T00:00:00'"
            )
            url = (
                f"{BASE}/{quote('Task_ЗадачаИсполнителя')}"
                f"?$format=json&$filter={quote(filt, safe='')}&$top=10"
            )
            r = session.get(url, timeout=90)
            if not r.ok:
                continue
            rows = r.json().get("value", [])
            if not rows:
                continue
            print(f"\n[{field} ~ {needle}] {len(rows)}")
            for t in rows:
                print(
                    f"  {t.get('Number')} Date={str(t.get('Date') or '')[:16]} "
                    f"срок={str(t.get('СрокИсполнения') or '')[:10]} "
                    f"Ex={t.get('Executed')} Исп={t.get('Исполнитель')}"
                )
                print(f"    {(t.get(field) or '')[:180]}")

    # Группа исполнителей - maybe Voronchikhina is in a group
    print("\n=== Catalog_ГруппыИсполнителейЗадач containing? ===")
    url = f"{BASE}/{quote('Catalog_ГруппыИсполнителейЗадач')}?$format=json&$top=5"
    r = session.get(url, timeout=60)
    print("HTTP", r.status_code)
    if r.ok and r.json().get("value"):
        print(json.dumps({k: v for k, v in r.json()["value"][0].items() if "@" not in k}, ensure_ascii=False, indent=2, default=str)[:1500])

    # BusinessProcess_CRM May with Ответственный = Voronchikhina
    print("\n=== BusinessProcess_CRM_БизнесПроцесс sample keys ===")
    url = f"{BASE}/{quote('BusinessProcess_CRM_БизнесПроцесс')}?$format=json&$top=1"
    r = session.get(url, timeout=60)
    if r.ok and r.json().get("value"):
        keys = [k for k in r.json()["value"][0] if "@" not in k]
        print(keys)


if __name__ == "__main__":
    main()
