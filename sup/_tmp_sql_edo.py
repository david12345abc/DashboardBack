"""Search SQL erp_pm for EDO execution / VK executor tables."""
from __future__ import annotations

import functools
import sys

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def main() -> None:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        # Search extended properties / table names
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE t.name LIKE '%Корреспонденц%'
               OR t.name LIKE '%ЭДО%'
               OR t.name LIKE '%Исполнен%'
               OR t.name LIKE '%Поручен%'
            ORDER BY t.name
            """
        )
        names = [r[0] for r in cur.fetchall()]
        print(f"tables matched: {len(names)}")
        for n in names[:80]:
            print(n)

        # Also search columns comments if any - look for Document767 or similar
        # Find physical table for Document_ТД_ВходящаяКорреспонденция via known Ref
        # GUID 05dc41cd-3fb7-11f1-97ed-6cb31113810e
        guid = "05dc41cd-3fb7-11f1-97ed-6cb31113810e"
        # 1C stores RRef as reversed binary - try searching config
        print("\nLooking for InfoRg with task-like fields...")
        cur.execute(
            """
            SELECT c.TABLE_NAME, c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.COLUMN_NAME LIKE '%Срок%'
               OR c.COLUMN_NAME LIKE '%Fld%'
            GROUP BY c.TABLE_NAME, c.COLUMN_NAME
            HAVING c.TABLE_NAME LIKE '_InfoRg%'
            """
        )
        # too broad - skip

        # Search sys.extended_properties for description containing Корреспонденц / Исполнение
        cur.execute(
            """
            SELECT TOP 100
                OBJECT_NAME(ep.major_id) AS obj,
                ep.name,
                CAST(ep.value AS nvarchar(400)) AS val
            FROM sys.extended_properties ep
            WHERE CAST(ep.value AS nvarchar(400)) LIKE N'%Входящая корреспонденц%'
               OR CAST(ep.value AS nvarchar(400)) LIKE N'%Исполнение документов%'
               OR CAST(ep.value AS nvarchar(400)) LIKE N'%документов ЭДО%'
            """
        )
        rows = cur.fetchall()
        print(f"\nextended props: {len(rows)}")
        for r in rows[:40]:
            print(r)


if __name__ == "__main__":
    main()
