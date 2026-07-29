# -*- coding: utf-8 -*-
"""Check alternate object dims; sum all debt for org via analytics."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
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

cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
for g in DEPTS:
    cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

# Which other RRef cols join ObjCalc?
rrefs = [
    "_Fld107663RRef",
    "_Fld140445RRef",
    "_Fld107665RRef",
    "_Fld107677RRef",
    "_Fld107679RRef",
    "_Fld107681RRef",
    "_Fld140448RRef",
    "_Fld111758RRef",
    "_Fld140446RRef",
    "_Fld170985RRef",
    "_Fld170986RRef",
    "_Fld170987RRef",
    "_Fld184033RRef",
]
for col in rrefs:
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{col}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
        """,
        p_end,
    )
    total = float(cur.fetchone()[0] or 0)
    if abs(total) > 1000:
        print(f"{col}: {total:,.2f}  d={total-TARGET:,.2f}")

# Find analytics table quickly: sample from _Fld107663RRef
cur.execute(f"SELECT TOP 1 _Fld107663RRef FROM [{T}] WITH (NOLOCK) WHERE _Fld107663RRef<>0x00000000000000000000000000000000")
sample = cur.fetchone()[0]
# Known ERP: often _Reference228 or similar - try nearby numbers from dengi era
for t in [f"_Reference{i}" for i in range(200, 280)]:
    cur.execute("SELECT 1 FROM sys.tables WHERE name=?", t)
    if not cur.fetchone():
        continue
    cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?", sample)
    if cur.fetchone():
        print("analytics", t)
        cur.execute(
            """
            SELECT c.name FROM sys.columns c
            WHERE c.object_id=OBJECT_ID(?) AND c.name LIKE '%RRef'
            """,
            t,
        )
        print("  rrefs", [r[0] for r in cur.fetchall()])
        break

cn.close()
