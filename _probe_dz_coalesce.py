# -*- coding: utf-8 -*-
"""Debt with empty ObjCalc but commercial analytics dept; find exact +50M."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
ANAL = "_Fld107663RRef"
DOLG = "_Fld107672"
TARGET = 373_930_180.89

DEPTS = [
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
]

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
p_end = to_1c_dt(date(2026, 7, 1))

# Find analytics table - try common refs near 107xxx era / partner keys
cur.execute(f"SELECT TOP 1 [{ANAL}] FROM [{T}] WITH (NOLOCK) WHERE [{ANAL}]<>0x00000000000000000000000000000000")
sample = cur.fetchone()[0]

anal = None
for n in list(range(220, 260)) + list(range(480, 520)) + list(range(100, 150)):
    t = f"_Reference{n}"
    cur.execute("SELECT 1 FROM sys.tables WHERE name=?", t)
    if not cur.fetchone():
        continue
    try:
        cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?", sample)
    except Exception:
        continue
    if cur.fetchone():
        anal = t
        print("analytics", t)
        break

if not anal:
    print("analytics not found quickly")
    cn.close()
    raise SystemExit

# Find dept column on analytics
cur.execute(
    f"""
    SELECT c.name FROM sys.columns c
    WHERE c.object_id=OBJECT_ID(?) AND c.name LIKE '%RRef'
    """,
    anal,
)
dept_col = None
for (col,) in cur.fetchall():
    cur.execute(
        f"""
        SELECT TOP 1 1 FROM [{anal}] a WITH (NOLOCK)
        INNER JOIN _Reference513 d WITH (NOLOCK) ON d._IDRRef=a.[{col}]
        """
    )
    if cur.fetchone():
        # verify sample
        cur.execute(
            f"""
            SELECT TOP 1 d._Description FROM [{anal}] a WITH (NOLOCK)
            INNER JOIN _Reference513 d WITH (NOLOCK) ON d._IDRRef=a.[{col}]
            WHERE a._IDRRef=?
            """,
            sample,
        )
        r = cur.fetchone()
        if r:
            print("dept col", col, "sample dept", r[0])
            dept_col = col
            break

if not dept_col:
    print("no dept on analytics")
    cn.close()
    raise SystemExit

cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
for g in DEPTS:
    cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

# Coalesce: use object dept if set else analytics dept
cur.execute(
    f"""
    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
    FROM [{T}] s WITH (NOLOCK)
    LEFT JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    LEFT JOIN [{anal}] a WITH (NOLOCK) ON a._IDRRef = s.[{ANAL}]
    WHERE s._Period < ? AND s._Active = 0x01
      AND COALESCE(
            NULLIF(o._Fld138169RRef, 0x00000000000000000000000000000000),
            a.[{dept_col}]
          ) IN (SELECT id FROM #d)
    """,
    p_end,
)
total = float(cur.fetchone()[0] or 0)
print(f"coalesce obj/anal dept: {total:,.2f}  d={total-TARGET:,.2f}")

# Only analytics dept
cur.execute(
    f"""
    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN [{anal}] a WITH (NOLOCK) ON a._IDRRef = s.[{ANAL}]
    INNER JOIN #d d ON d.id = a.[{dept_col}]
    WHERE s._Period < ? AND s._Active = 0x01
    """,
    p_end,
)
total2 = float(cur.fetchone()[0] or 0)
print(f"analytics dept only: {total2:,.2f}  d={total2-TARGET:,.2f}")

cn.close()
