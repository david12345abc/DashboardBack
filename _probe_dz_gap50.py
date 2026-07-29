# -*- coding: utf-8 -*-
"""Hunt exact 50,000,020.01 gap."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
GAP = 50_000_020.01
TARGET = 373_930_180.89

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
p_end = to_1c_dt(date(2026, 7, 1))

# Any single movement of ~50M?
cur.execute(
    f"""
    SELECT TOP 20 s.[{OBJ}], s.[{DOLG}], s._RecordKind, s._Period, s.[{PLAN}]
    FROM [{T}] s WITH (NOLOCK)
    WHERE s._Period < ? AND s._Active = 0x01
      AND ABS(s.[{DOLG}] - 50000000) < 100
    """,
    p_end,
)
print("movements near 50M:")
for row in cur.fetchall():
    print(row[1], row[2], row[3], row[0].hex()[:16])

# Object balances near gap
cur.execute(
    f"""
    SELECT TOP 20 s.[{OBJ}],
      SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
    FROM [{T}] s WITH (NOLOCK)
    WHERE s._Period < ? AND s._Active = 0x01
    GROUP BY s.[{OBJ}]
    HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?) < 5000
        OR ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - 50000000) < 5000
    ORDER BY ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?)
    """,
    (p_end, GAP, GAP),
)
print("\nobjects near gap:")
for obj, amt in cur.fetchall():
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
    print(f"  {float(amt):15,.2f}  {(r[0] if r else '?')[:70]}  [{dept}]")

# Two-object combinations from top empty + tender etc that make gap?
# empty top: 78.2M - too big
# Check: COMM+LIQ + empty where description contains certain pattern

# Maybe Period should be <= end of day June 30 inclusive with Active, AND also
# include inactive? 
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

# without Active filter
cur.execute(
    f"""
    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    INNER JOIN #d d ON d.id = o._Fld138169RRef
    WHERE s._Period < ?
    """,
    p_end,
)
print(f"\nno Active filter: {float(cur.fetchone()[0] or 0):,.2f}")

# Period <= June 30 23:59:59
p_incl = to_1c_dt(date(2026, 6, 30)).replace(hour=23, minute=59, second=59)
cur.execute(
    f"""
    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    INNER JOIN #d d ON d.id = o._Fld138169RRef
    WHERE s._Period <= ? AND s._Active = 0x01
    """,
    p_incl,
)
print(f"period<=jun30eod: {float(cur.fetchone()[0] or 0):,.2f}")

# Invert RecordKind sign
cur.execute(
    f"""
    SELECT SUM(CASE WHEN s._RecordKind=1 THEN s.[{DOLG}] ELSE -s.[{DOLG}] END)
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    INNER JOIN #d d ON d.id = o._Fld138169RRef
    WHERE s._Period < ? AND s._Active = 0x01
    """,
    p_end,
)
print(f"inverted sign: {float(cur.fetchone()[0] or 0):,.2f}")

cn.close()
