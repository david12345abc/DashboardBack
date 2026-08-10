"""Locate ТД_COMИнтеграцияИБ / ИсполнениеДокументовЭДО in OData + SQL."""
from __future__ import annotations

import functools
import re
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

NEEDLES = (
    "COMИнтеграция",
    "ИсполнениеДокументовЭДО",
    "ТД_COM",
    "COMИнтеграцияИБ",
    "ИнтеграцияИБ",
)


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    print("GET metadata", BASE)
    text = s.get(f"{BASE}/$metadata", timeout=180).text
    print("bytes", len(text))

    for n in NEEDLES:
        hits = [m.start() for m in re.finditer(re.escape(n), text)]
        print(f"meta '{n}': {len(hits)}")
        for i in hits[:5]:
            print(" ", text[max(0, i - 60) : i + 80].replace("\n", " "))

    # EntitySets with COM / Интеграц / ЭДО + Исполн
    sets = re.findall(r'EntitySet Name="([^"]+)"', text)
    for pat in ("COM", "Интеграц", "ИсполнениеДокумент", "ДокументовЭДО"):
        m = [x for x in sets if pat.lower() in x.lower()]
        print(f"sets ~{pat}:", m[:40])

    # ExternalDataSource?
    ext = [x for x in sets if x.startswith("ExternalDataSource") or "Внешн" in x]
    print("external-ish sets:", ext[:40])

    print("\n=== SQL ===")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        # table names
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '%COM%'
               OR t.name LIKE '%Интеграц%'
               OR t.name LIKE '%ЭДО%'
            ORDER BY t.name
            """
        )
        print("tables name match:", [r[0] for r in cur.fetchall()][:50])

        # nvarchar containing processing/form name
        cur.execute(
            """
            SELECT t.name, c.name
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE ty.name IN ('nvarchar', 'nchar')
              AND (
                t.name LIKE '_Reference%'
                OR t.name LIKE '_InfoRg%'
                OR t.name LIKE '_Document%'
                OR t.name LIKE '_Sched%'
                OR t.name LIKE '_Const%'
              )
              AND t.name NOT LIKE '%ChngR%'
              AND c.max_length BETWEEN 20 AND 500
            """
        )
        cols = cur.fetchall()
        print("text cols to scan", len(cols))
        phrases = (
            "ИсполнениеДокументовЭДО",
            "ТД_COMИнтеграцияИБ",
            "COMИнтеграцияИБ",
            "Исполнение документов ЭДО",
        )
        found = 0
        for tname, cname in cols:
            for ph in phrases:
                try:
                    cur.execute(
                        f"""
                        SELECT TOP 1 [{cname}] FROM [{tname}] WITH (NOLOCK)
                        WHERE [{cname}] LIKE ?
                        """,
                        f"%{ph}%",
                    )
                    row = cur.fetchone()
                except Exception:
                    continue
                if row:
                    print(f"FOUND {tname}.{cname} ~ {ph}: {str(row[0])[:220]}")
                    found += 1
                    break
            if found >= 20:
                break
        print("found", found)

        # Connection strings / COM server params often in constants
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '_Const%'
            ORDER BY t.name
            """
        )
        consts = [r[0] for r in cur.fetchall()]
        print("const tables", len(consts), consts[:20])


if __name__ == "__main__":
    main()
