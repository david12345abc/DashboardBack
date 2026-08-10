"""BP_Задание by Предмет cast to VK; role as executor; search report name in SQL."""
from __future__ import annotations

import functools
import json
import sys
import uuid
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

VOR = "a312e193-d7b7-11ee-94a5-6cb31113810e"
ROLE = "6345f269-528e-11f1-9805-6cb31113810e"
VK = {
    "НП00-002870": "05dc41cd-3fb7-11f1-97ed-6cb31113810e",
    "НП00-002937": "dd5272e4-478c-11f1-97f7-6cb31113810e",
}
BP = "BusinessProcess_Задание"
TASK = "Task_ЗадачаИсполнителя"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})

    for num, ref in VK.items():
        filt = f"Предмет eq cast(guid'{ref}','Document_ТД_ВходящаяКорреспонденция')"
        for ent in (BP, TASK):
            url = f"{BASE}/{quote(ent)}?$filter={quote(filt)}&$top=20&$format=json"
            resp = s.get(url, timeout=90)
            print(f"{ent} predmet {num}: {resp.status_code}", end=" ")
            if not resp.ok:
                print(resp.text[:200].replace("\n", " "))
                continue
            rows = resp.json().get("value") or []
            print(f"rows={len(rows)}")
            for r in rows[:5]:
                print(
                    " ",
                    r.get("Date"),
                    r.get("СрокИсполнения"),
                    r.get("Исполнитель"),
                    r.get("Исполнитель_Type"),
                    r.get("Проверяющий"),
                    (r.get("Наименование") or r.get("Description") or "")[:80],
                )

    # Role as executor on BP
    for cast in (
        f"Исполнитель eq cast(guid'{ROLE}','Catalog_РолиИсполнителей')",
        f"Исполнитель eq cast(guid'{ROLE}','Catalog_Пользователи')",
    ):
        filt = f"{cast} and year(Date) eq 2026 and month(Date) eq 5"
        url = f"{BASE}/{quote(BP)}?$filter={quote(filt)}&$top=30&$format=json"
        resp = s.get(url, timeout=90)
        print(f"\nBP role filter: {resp.status_code}", resp.text[:120].replace("\n", " ") if not resp.ok else f"rows={len(resp.json().get('value') or [])}")
        if resp.ok:
            for r in (resp.json().get("value") or [])[:10]:
                print(" ", r.get("Date"), r.get("СрокИсполнения"), r.get("Предмет"), r.get("Предмет_Type"), (r.get("Наименование") or "")[:80])

    # Role on Task
    filt = (
        f"РольИсполнителя_Key eq guid'{ROLE}' and year(Date) eq 2026 and month(Date) eq 5"
    )
    url = (
        f"{BASE}/{quote(TASK)}?$filter={quote(filt)}"
        f"&$select=Ref_Key,Number,Date,Description,СрокИсполнения,ПредметСтрокой,РольИсполнителя_Key,Executed"
        f"&$top=30&$format=json"
    )
    resp = s.get(url, timeout=90)
    print(f"\nTask role May: {resp.status_code}", end=" ")
    if resp.ok:
        rows = resp.json().get("value") or []
        print(f"rows={len(rows)}")
        for r in rows[:15]:
            print(" ", r.get("Date"), r.get("СрокИсполнения"), (r.get("ПредметСтрокой") or "")[:100])
    else:
        print(resp.text[:200].replace("\n", " "))

    # Resolve role name
    url = f"{BASE}/{quote('Catalog_РолиИсполнителей')}(guid'{ROLE}')?$format=json"
    resp = s.get(url, timeout=30)
    print("\nROLE", resp.status_code, (resp.json() if resp.ok else resp.text) if resp.ok else resp.text[:200])
    if resp.ok:
        print(resp.json().get("Description"), resp.json().get("Code"))

    # SQL search report title
    print("\n=== SQL report title ===")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        # Prefer smaller tables: _Reference* (catalogs), _InfoRg with report variants
        cur.execute(
            """
            SELECT t.name, c.name, c.max_length
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE ty.name = 'nvarchar'
              AND c.max_length > 20
              AND (
                t.name LIKE '_Reference%'
                OR t.name LIKE '_InfoRg%Report%'
                OR t.name LIKE '_InfoRg%Вариант%'
                OR t.name LIKE '_Sched%'
              )
            """
        )
        cols = cur.fetchall()
        print("candidate cols", len(cols))
        for tname, cname, _ in cols:
            try:
                cur.execute(
                    f"""
                    SELECT TOP 1 [{cname}] FROM [{tname}] WITH (NOLOCK)
                    WHERE [{cname}] LIKE N'%Исполнение документов ЭДО%'
                    """
                )
                row = cur.fetchone()
                if row:
                    print(f"FOUND {tname}.{cname}: {str(row[0])[:200]}")
            except Exception:
                pass


if __name__ == "__main__":
    main()
