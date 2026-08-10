"""Find tasks by subject string / group / CRM deadline move for VK docs."""
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

VOR = "a312e193-d7b7-11ee-94a5-6cb31113810e"
TASK = "Task_ЗадачаИсполнителя"
SELECT = (
    "Ref_Key,Number,Date,Description,Executed,СрокИсполнения,ДатаИсполнения,"
    "ПредметСтрокой,Предмет_Type,РольИсполнителя_Key,ГруппаИсполнителейЗадач_Key,"
    "CRM_ПеренестиСрокИсполненияНа,Описание,РезультатВыполнения,DeletionMark"
)


def q(filt: str, top: int = 30) -> list:
    url = (
        f"{BASE}/{quote(TASK)}?$filter={quote(filt)}"
        f"&$select={quote(SELECT, safe=',_')}"
        f"&$top={top}&$format=json"
    )
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})
    resp = s.get(url, timeout=120)
    if not resp.ok:
        print("ERR", resp.status_code, resp.text[:250].replace("\n", " "))
        return []
    return resp.json().get("value") or []


def show(rows: list, title: str) -> None:
    print(f"\n=== {title}: {len(rows)} ===")
    for r in rows[:15]:
        print(
            r.get("Date"),
            "| срок",
            r.get("СрокИсполнения"),
            "| CRM",
            r.get("CRM_ПеренестиСрокИсполненияНа"),
            "| role",
            r.get("РольИсполнителя_Key"),
            "| grp",
            r.get("ГруппаИсполнителейЗадач_Key"),
        )
        print("  subj:", (r.get("ПредметСтрокой") or "")[:120])
        print("  desc:", (r.get("Description") or "")[:100])
        print("  opi:", (r.get("Описание") or "")[:150].replace("\n", " | "))


def main() -> None:
    for num in ("НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101", "МПГ00010733"):
        rows = q(f"substringof('{num}', ПредметСтрокой)")
        show(rows, f"subj {num}")
        rows = q(f"substringof('{num}', Description)")
        show(rows, f"desc {num}")
        rows = q(f"substringof('{num}', Описание)")
        show(rows, f"opis {num}")

    # personal via cast
    rows = q(
        f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи') "
        f"and year(Date) eq 2026 and month(Date) eq 5 and DeletionMark eq false"
    )
    show(rows, "VOR May by Date")

    rows = q(
        f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи') "
        f"and year(СрокИсполнения) eq 2026 and month(СрокИсполнения) eq 5 "
        f"and DeletionMark eq false"
    )
    show(rows, "VOR May by deadline")

    # any May 2026 with Входящая корреспонденция in subject
    rows = q(
        "substringof('Входящая корреспонденция НП00-002', ПредметСтрокой) "
        "and year(Date) eq 2026",
        top=50,
    )
    show(rows, "VK NP00-002* Date 2026")

    # Groups containing Vor?
    s = requests.Session()
    s.auth = AUTH
    # probe catalog names
    for ent in (
        "Catalog_ГруппыИсполнителейЗадач",
        "Catalog_РолиИсполнителей",
        "InformationRegister_ИсполнителиЗадач",
        "InformationRegister_CRM_Исполнители",
    ):
        url = f"{BASE}/{quote(ent)}?$top=1&$format=json"
        resp = s.get(url, timeout=60)
        print(f"\n{ent}: {resp.status_code}", end=" ")
        if resp.ok:
            val = resp.json().get("value") or []
            print("ok", list(val[0].keys())[:25] if val else "empty")
        else:
            print(resp.text[:160].replace("\n", " "))

    # ИсполнителиЗадач register for Vor
    for ent in ("InformationRegister_ИсполнителиЗадач",):
        filt = f"Исполнитель_Key eq guid'{VOR}'"
        url = f"{BASE}/{quote(ent)}?$filter={quote(filt)}&$top=20&$format=json"
        resp = s.get(url, timeout=60)
        print(f"\nreg filter: {resp.status_code}")
        if resp.ok:
            rows = resp.json().get("value") or []
            print("rows", len(rows))
            for r in rows[:10]:
                print(json.dumps(r, ensure_ascii=False)[:300])
        else:
            # try without _Key
            filt = f"Исполнитель eq cast(guid'{VOR}','Catalog_Пользователи')"
            url = f"{BASE}/{quote(ent)}?$filter={quote(filt)}&$top=20&$format=json"
            resp = s.get(url, timeout=60)
            print("alt", resp.status_code, resp.text[:300].replace("\n", " "))


if __name__ == "__main__":
    main()
