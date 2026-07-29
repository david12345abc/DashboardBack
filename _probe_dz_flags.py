# -*- coding: utf-8 -*-
"""Explore filters: storno, type strings; try to hit 373.93 / 142.77."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PRED = "_Fld107669"
PLAN = "_Fld107667"
TARGET_DZ = 373_930_180.89
TARGET_OD = 142_773_571.76

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


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    p_end = to_1c_dt(date(2026, 7, 1))
    na = to_1c_dt(date(2026, 6, 30))

    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    # Distinct string field samples
    for col in ("_Fld107675", "_Fld107676", "_Fld140447"):
        cur.execute(
            f"""
            SELECT TOP 15 [{col}], COUNT(*)
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01 AND [{col}] <> N''
            GROUP BY [{col}]
            ORDER BY COUNT(*) DESC
            """,
            p_end,
        )
        print(f"\n{col}:")
        for v, c in cur.fetchall():
            print(f"  {c:6d}  {v}")

    # Binary flags
    for col in ("_Fld170989", "_Fld140449"):
        cur.execute(
            f"""
            SELECT [{col}],
              SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END),
              COUNT(*)
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01
            GROUP BY [{col}]
            """,
            p_end,
        )
        print(f"\n{col}:")
        for v, amt, c in cur.fetchall():
            print(f"  {v.hex() if isinstance(v, bytes) else v}  amt={float(amt or 0):,.2f}  n={c}")

    # Exclude storno if _Fld170989 or _Fld140449 is storno flag
    # Try amt where flag = 0x00 only
    for col in ("_Fld170989", "_Fld140449"):
        cur.execute(
            f"""
            SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01 AND s.[{col}] = 0x00
            """,
            p_end,
        )
        print(f"dolg where {col}=0: {float(cur.fetchone()[0] or 0):,.2f}")

    # Net debt - prepaid (client debt net of prepay) at object level, positive only
    cur.execute(
        f"""
        SELECT
          s.[{OBJ}] AS obj,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dolg,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{PRED}] ELSE s.[{PRED}] END) AS pred
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY s.[{OBJ}]
        """,
        p_end,
    )
    net_pos = 0.0
    dolg_pos = 0.0
    for obj, dolg, pred in cur.fetchall():
        dolg = float(dolg or 0)
        pred = float(pred or 0)
        if dolg > 0.01:
            dolg_pos += dolg
        net = dolg - pred
        if net > 0.01:
            net_pos += net
    print(f"\ndolg_pos orders: {dolg_pos:,.2f}")
    print(f"net(dolg-pred) pos orders: {net_pos:,.2f}  d={net_pos-TARGET_DZ:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
