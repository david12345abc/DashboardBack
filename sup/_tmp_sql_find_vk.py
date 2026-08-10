"""Find SQL table for Document_ТД_ВходящаяКорреспонденция by number."""
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
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            JOIN sys.columns c1 ON c1.object_id = t.object_id AND c1.name = '_Number'
            JOIN sys.columns c2 ON c2.object_id = t.object_id AND c2.name LIKE '_Date%'
            WHERE t.name LIKE '_Document%'
            ORDER BY t.name
            """
        )
        docs = [r[0] for r in cur.fetchall()]
        print("document-like tables", len(docs))
        found = 0
        for tname in docs:
            try:
                cur.execute(
                    f"""
                    SELECT TOP 1 _Number, _Date_Time
                    FROM [{tname}] WITH (NOLOCK)
                    WHERE _Number LIKE N'%002870%'
                      AND YEAR(_Date_Time) IN (2026, 4026)
                    """
                )
                row = cur.fetchone()
            except Exception:
                continue
            if row:
                print("HIT", tname, row[0], row[1])
                found += 1
                if found >= 15:
                    break
        print("done found", found)

        # Also search Task tables
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE t.name LIKE '_Task%' OR t.name LIKE '_BPr%'
            ORDER BY t.name
            """
        )
        print("\nTask/BPr tables:")
        for r in cur.fetchall():
            print(" ", r[0])


if __name__ == "__main__":
    main()
