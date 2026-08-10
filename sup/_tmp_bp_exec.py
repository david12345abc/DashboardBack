"""Find BusinessProcess Исполнение / tasks by subject VK for May."""
from __future__ import annotations

import functools
import json
import re
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE, EMPTY, fetch_all

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

VOR = "a312e193-d7b7-11ee-94a5-6cb31113810e"
ROLE = "6345f269-528e-11f1-9805-6cb31113810e"  # from prior context
VK_REFS = {
    "НП00-002870": "05dc41cd-3fb7-11f1-97ed-6cb31113810e",
    "НП00-002937": "dd5272e4-478c-11f1-97f7-6cb31113810e",
}


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})

    meta = s.get(f"{BASE}/$metadata", timeout=180).text
    bp = sorted(set(re.findall(r'EntitySet Name="(BusinessProcess_[^"]+)"', meta)))
    print("BP count", len(bp))
    for name in bp:
        if any(x in name.lower() for x in ("исполн", "поруч", "ознаком", "соглас", "рассмотр", "утвержд", "edo", "эдо", "тд_")):
            print(" ", name)

    task_props = []
    # extract Task_ЗадачаИсполнителя properties
    m = re.search(r'<EntityType Name="Task_ЗадачаИсполнителя"[^>]*>(.*?)</EntityType>', meta, re.S)
    if m:
        task_props = re.findall(r'<Property Name="([^"]+)"', m.group(1))
        navs = re.findall(r'<NavigationProperty Name="([^"]+)"', m.group(1))
        print("\nTask props sample:", [p for p in task_props if any(x in p.lower() for x in ("исполн", "срок", "предмет", "роль", "групп", "провер", "автор", "дата", "описан", "результат"))][:40])
        print("Task navs:", [n for n in navs if any(x in n.lower() for x in ("исполн", "предмет", "роль", "провер", "бизнес"))][:30])

    # Fetch tasks by ПредметСтрокой containing numbers
    TASK = "Task_ЗадачаИсполнителя"
    for num, ref in VK_REFS.items():
        for filt, label in (
            (f"substringof('{num}', ПредметСтрокой)", "subj"),
            (f"substringof('{num}', Description)", "desc"),
            (f"Предмет eq cast(guid'{ref}', 'Document_ТД_ВходящаяКорреспонденция')", "predmet_cast"),
            (f"year(Date) eq 2026 and month(Date) eq 5 and substringof('Входящая', ПредметСтрокой)", "may_vk"),
        ):
            url = (
                f"{BASE}/{quote(TASK)}?$filter={quote(filt)}"
                f"&$select={quote('Ref_Key,Number,Date,Description,Executed,СрокИсполнения,ДатаИсполнения,ПредметСтрокой,Исполнитель_Key,РольИсполнителя_Key,DeletionMark', safe=',_')}"
                f"&$top=20&$format=json"
            )
            resp = s.get(url, timeout=90)
            print(f"\n{num} {label}: {resp.status_code}", end="")
            if not resp.ok:
                print(" ", resp.text[:180].replace("\n", " "))
                continue
            rows = resp.json().get("value") or []
            print(f" rows={len(rows)}")
            for r in rows[:5]:
                print(
                    " ",
                    r.get("Date"),
                    r.get("СрокИсполнения"),
                    r.get("Исполнитель_Key"),
                    r.get("РольИсполнителя_Key"),
                    (r.get("ПредметСтрокой") or "")[:80],
                    (r.get("Description") or "")[:60],
                )

    # Role tasks in May
    filt = (
        f"РольИсполнителя_Key eq guid'{ROLE}' and year(Date) eq 2026 and month(Date) eq 5"
    )
    url = (
        f"{BASE}/{quote(TASK)}?$filter={quote(filt)}"
        f"&$select={quote('Ref_Key,Number,Date,Description,Executed,СрокИсполнения,ПредметСтрокой,Исполнитель_Key,РольИсполнителя_Key', safe=',_')}"
        f"&$top=50&$format=json"
    )
    resp = s.get(url, timeout=90)
    print(f"\nROLE May tasks: {resp.status_code}", end="")
    if resp.ok:
        rows = resp.json().get("value") or []
        print(f" rows={len(rows)}")
        for r in rows[:20]:
            print(" ", r.get("Date"), r.get("СрокИсполнения"), (r.get("ПредметСтрокой") or "")[:100])
    else:
        print(" ", resp.text[:250].replace("\n", " "))

    # Personal May with all fields including role
    filt = f"Исполнитель_Key eq guid'{VOR}' and year(Date) eq 2026 and month(Date) eq 5"
    url = (
        f"{BASE}/{quote(TASK)}?$filter={quote(filt)}"
        f"&$format=json&$top=20"
    )
    resp = s.get(url, timeout=90)
    if resp.ok:
        rows = resp.json().get("value") or []
        print(f"\nVOR May personal full rows={len(rows)}")
        if rows:
            print("keys", sorted(rows[0].keys())[:60])
            print(json.dumps(rows[0], ensure_ascii=False, indent=2)[:2000])

    # Try BusinessProcess_Исполнение if exists
    for ent in bp:
        if "Исполнение" in ent and "Заявк" not in ent:
            print("\nTrying", ent)
            url = f"{BASE}/{quote(ent)}?$top=1&$format=json"
            resp = s.get(url, timeout=60)
            print(resp.status_code, list((resp.json().get("value") or [{}])[0].keys())[:40] if resp.ok else resp.text[:150])


if __name__ == "__main__":
    main()
