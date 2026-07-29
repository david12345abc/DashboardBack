# -*- coding: utf-8 -*-
"""Check if analytics has department; attribute empty-dept debt via analytics."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
# analytics is likely _Fld107663RRef (first RRef after RecordKind)
ANAL = "_Fld107663RRef"
DOLG = "_Fld107672"
TARGET = 373_930_180.89

COMM = [
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
]
LIQ = [
    "4edcf3a0-9f99-11e4-80da-001e67112509",
    "ff740269-d71e-11e6-8127-001e67112509",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e",
]


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    p_end = to_1c_dt(date(2026, 7, 1))

    # Find analytics catalog: sample ANAL ref and find which _Reference contains it
    cur.execute(
        f"""
        SELECT TOP 1 s.[{ANAL}]
        FROM [{T}] s WITH (NOLOCK)
        WHERE s.[{ANAL}] <> 0x00000000000000000000000000000000
        """
    )
    sample = cur.fetchone()[0]
    print("anal sample", sample.hex())

    cur.execute(
        """
        SELECT t.name FROM sys.tables t
        WHERE t.name LIKE '_Reference%'
          AND t.name NOT LIKE '%VT%'
          AND EXISTS (
            SELECT 1 FROM sys.columns c
            WHERE c.object_id=t.object_id AND c.name='_IDRRef'
          )
        """
    )
    refs = [r[0] for r in cur.fetchall()]
    anal_table = None
    for t in refs:
        cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?", sample)
        if cur.fetchone():
            # count columns
            cur.execute(
                """
                SELECT COUNT(*) FROM sys.columns WHERE object_id=OBJECT_ID(?)
                """,
                t,
            )
            print("analytics table", t, "ncols", cur.fetchone()[0])
            anal_table = t
            cur.execute(
                """
                SELECT c.name, ty.name
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id=c.user_type_id
                WHERE c.object_id=OBJECT_ID(?)
                ORDER BY c.column_id
                """,
                t,
            )
            for n, ty in cur.fetchall():
                print(f"  {n} {ty}")
            break

    if not anal_table:
        print("analytics catalog not found")
        cn.close()
        return

    # Find which RRef on analytics points to СтруктураПредприятия (_Reference513)
    cur.execute(
        f"""
        SELECT TOP 1 * FROM [{anal_table}] WITH (NOLOCK) WHERE _IDRRef=?
        """,
        sample,
    )
    # better: test each RRef col
    cur.execute(
        """
        SELECT c.name FROM sys.columns c
        WHERE c.object_id=OBJECT_ID(?) AND c.name LIKE '%RRef'
        """,
        anal_table,
    )
    rrefs = [r[0] for r in cur.fetchall()]
    dept_col = None
    for col in rrefs:
        cur.execute(
            f"""
            SELECT TOP 1 1
            FROM [{anal_table}] a WITH (NOLOCK)
            INNER JOIN _Reference513 d WITH (NOLOCK) ON d._IDRRef = a.[{col}]
            WHERE a._IDRRef=?
            """,
            sample,
        )
        if cur.fetchone():
            # verify more broadly
            cur.execute(
                f"""
                SELECT TOP 1 1
                FROM [{anal_table}] a WITH (NOLOCK)
                INNER JOIN _Reference513 d WITH (NOLOCK) ON d._IDRRef = a.[{col}]
                """
            )
            if cur.fetchone():
                print("analytics dept col", col)
                dept_col = col
                break

    if not dept_col:
        print("no dept on analytics")
        cn.close()
        return

    # Sum debt by analytics.dept for commercial+liq
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in COMM + LIQ:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN [{anal_table}] a WITH (NOLOCK) ON a._IDRRef = s.[{ANAL}]
        INNER JOIN #d d ON d.id = a.[{dept_col}]
        WHERE s._Period < ? AND s._Active = 0x01
        """,
        p_end,
    )
    total = float(cur.fetchone()[0] or 0)
    print(f"by analytics dept COMM+LIQ: {total:,.2f}  d={total-TARGET:,.2f}")

    # Also: coalesce analytics dept / object dept
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        LEFT JOIN [{anal_table}] a WITH (NOLOCK) ON a._IDRRef = s.[{ANAL}]
        WHERE s._Period < ? AND s._Active = 0x01
          AND (
            o._Fld138169RRef IN (SELECT id FROM #d)
            OR a.[{dept_col}] IN (SELECT id FROM #d)
          )
        """,
        p_end,
    )
    total2 = float(cur.fetchone()[0] or 0)
    print(f"object OR analytics dept: {total2:,.2f}  d={total2-TARGET:,.2f}")

    # Empty object dept but analytics in commercial
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN [{anal_table}] a WITH (NOLOCK) ON a._IDRRef = s.[{ANAL}]
        INNER JOIN #d d ON d.id = a.[{dept_col}]
        WHERE s._Period < ? AND s._Active = 0x01
          AND o._Fld138169RRef = 0x00000000000000000000000000000000
        """,
        p_end,
    )
    print(f"empty obj dept, analytics commercial: {float(cur.fetchone()[0] or 0):,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
