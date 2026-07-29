# -*- coding: utf-8 -*-
from datetime import date

from comdir.common import connect, to_1c_dt

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
p_end = to_1c_dt(date(2026, 7, 1))
na = to_1c_dt(date(2026, 6, 30))

# Exact / near 50M at object or object+plan level
for label, sql in [
    (
        "obj",
        f"""
        SELECT TOP 10 s.[{OBJ}],
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
        FROM [{T}] s WITH (NOLOCK)
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY s.[{OBJ}]
        HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)) BETWEEN 49900000 AND 50100000
        """,
    ),
    (
        "obj+plan",
        f"""
        SELECT TOP 10 s.[{OBJ}], s.[{PLAN}],
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
        FROM [{T}] s WITH (NOLOCK)
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY s.[{OBJ}], s.[{PLAN}]
        HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)) BETWEEN 49900000 AND 50100000
        """,
    ),
]:
    cur.execute(sql, p_end)
    rows = cur.fetchall()
    print(label, "hits", len(rows))
    for row in rows:
        obj = row[0]
        amt = float(row[-1])
        cur.execute(
            "SELECT _Description, _Fld138169RRef FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?",
            obj,
        )
        r = cur.fetchone()
        dept = "?"
        if r and r[1] != bytes(16):
            cur.execute(
                "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
                r[1],
            )
            d = cur.fetchone()
            dept = d[0] if d else "?"
        print(f"  {amt:,.2f}  {(r[0] if r else '?')[:60]}  dept={dept}")

# Try COMM only + tender
cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
from comdir.common import uuid_to_1c_bytes

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
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d",  # tender
]:
    cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

cur.execute(
    f"""
    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    INNER JOIN #d d ON d.id = o._Fld138169RRef
    WHERE s._Period < ? AND s._Active = 0x01
    """,
    p_end,
)
print("COMM+LIQ+TENDER", float(cur.fetchone()[0] or 0))

# overdue with only positive order nets for COMM+LIQ, and also try
# overdue = sum of overdue terms where order total dz > 0
cur.execute(
    f"""
    SELECT
      s.[{OBJ}] AS obj,
      s.[{PLAN}] AS plan_dt,
      SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    INNER JOIN #d d ON d.id = o._Fld138169RRef
    WHERE s._Period < ? AND s._Active = 0x01
      AND o._Fld138169RRef <> 0x8129cd2988c3db2d11e0d91b1c9f9419
    GROUP BY s.[{OBJ}], s.[{PLAN}]
    """,
    p_end,
)
# skip - tender hex wrong in binary. Just use commercial without tender from earlier numbers.

cn.close()
