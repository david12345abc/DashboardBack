# -*- coding: utf-8 -*-
from datetime import date
from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
cur.execute(
    """
    SELECT c.name, ty.name FROM sys.columns c
    JOIN sys.types ty ON ty.user_type_id=c.user_type_id
    WHERE c.object_id=OBJECT_ID('_AccumRg107629')
    ORDER BY c.column_id
    """
)
print("107629 schema:")
for n, ty in cur.fetchall():
    print(f"  {n} {ty}")

# Does 107629 join ObjCalc?
p_end = to_1c_dt(date(2026, 7, 1))
cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
for g in [
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
    "4edcf3a0-9f99-11e4-80da-001e67112509",
    "ff740269-d71e-11e6-8127-001e67112509",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e",
]:
    cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

# find obj col
cur.execute(
    """
    SELECT c.name FROM sys.columns c
    WHERE c.object_id=OBJECT_ID('_AccumRg107629') AND c.name LIKE '%RRef'
    """
)
for (col,) in cur.fetchall():
    cur.execute(
        f"""
        SELECT TOP 1 1 FROM _AccumRg107629 s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef=s.[{col}]
        INNER JOIN #d d ON d.id=o._Fld138169RRef
        """
    )
    if cur.fetchone():
        print("obj", col)
        # sum nums
        cur.execute(
            """
            SELECT c.name FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id=c.user_type_id
            WHERE c.object_id=OBJECT_ID('_AccumRg107629')
              AND ty.name IN ('numeric','decimal')
              AND c.name NOT IN ('_LineNo','_RecordKind','_Fld1952')
            """
        )
        for (ncol,) in cur.fetchall():
            cur.execute(
                f"""
                SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                FROM _AccumRg107629 s WITH (NOLOCK)
                INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef=s.[{col}]
                INNER JOIN #d d ON d.id=o._Fld138169RRef
                WHERE s._Period < ? AND s._Active=0x01
                """,
                p_end,
            )
            v = float(cur.fetchone()[0] or 0)
            if abs(v) > 10000:
                print(f"  {ncol}: {v:,.2f}")
cn.close()
