# -*- coding: utf-8 -*-
"""Resolve top ObjCalc department hexes to names via Catalog_СтруктураПредприятия."""
from __future__ import annotations

from comdir.common import connect

# Try common reference table numbers for structure
CANDS = [
    "_Reference96",
    "_Reference99",
    "_Reference102",
    "_Reference128",
    "_Reference132",
    "_Reference138",
    "_Reference142",
    "_Reference150",
    "_Reference239",
    "_Reference258",
    "_Reference291",
    "_Reference351",
    "_Reference380",
    "_Reference420",
    "_Reference473",
    "_Reference530",
    "_Reference560",
]


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # Get a known dept id
    gaz = bytes.fromhex("80da001e6711250911e49f9cbd7b5184")
    dept_table = None
    for t in CANDS:
        cur.execute(
            f"""
            SELECT 1 FROM sys.tables WHERE name=?
            """,
            t,
        )
        if not cur.fetchone():
            continue
        cur.execute(
            f"SELECT TOP 1 _Description FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?",
            gaz,
        )
        row = cur.fetchone()
        if row:
            print("DEPT TABLE", t, "gaz=", row[0])
            dept_table = t
            break

    if not dept_table:
        # brute: find any reference containing gazprom guid
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '_Reference%'
              AND t.name NOT LIKE '%VT%'
              AND EXISTS (
                SELECT 1 FROM sys.columns c
                WHERE c.object_id=t.object_id AND c.name='_IDRRef'
              )
              AND EXISTS (
                SELECT 1 FROM sys.columns c
                WHERE c.object_id=t.object_id AND c.name='_Description'
              )
            """
        )
        tables = [r[0] for r in cur.fetchall()]
        print("searching", len(tables), "refs")
        for t in tables:
            cur.execute(
                f"SELECT TOP 1 _Description FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?",
                gaz,
            )
            row = cur.fetchone()
            if row:
                print("DEPT TABLE", t, "gaz=", row[0])
                dept_table = t
                break

    if not dept_table:
        print("not found")
        return

    cur.execute(
        """
        SELECT TOP 40
          o._Fld138169RRef AS dept,
          COUNT(*) AS cnt
        FROM _Reference134945 o WITH (NOLOCK)
        WHERE o._Fld138169RRef <> 0x00000000000000000000000000000000
        GROUP BY o._Fld138169RRef
        ORDER BY COUNT(*) DESC
        """
    )
    rows = cur.fetchall()
    for dept, cnt in rows:
        cur.execute(
            f"SELECT _Description FROM [{dept_table}] WITH (NOLOCK) WHERE _IDRRef=?",
            dept,
        )
        name = cur.fetchone()
        print(f"{cnt:6d}  {name[0] if name else '?'}  {dept.hex()}")

    cn.close()


if __name__ == "__main__":
    main()
