# -*- coding: utf-8 -*-
"""Filter by РасчетныйДокумент type (RTRef) for Только фактическую."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
TARGET_DZ = 323_930_180.89
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

    # RTRef of settlement doc / registrar types
    for col in ("_Fld107666_RTRef", "_RecorderTRef", "_Fld107678_RTRef", "_Fld107682_RTRef"):
        cur.execute(
            f"""
            SELECT s.[{col}],
              SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dz
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01
            GROUP BY s.[{col}]
            ORDER BY 2 DESC
            """,
            p_end,
        )
        print(f"\nDZ by {col}:")
        for v, amt in cur.fetchall()[:15]:
            print(f"  {float(amt or 0):15,.2f}  {(v.hex() if v else None)}")

        cur.execute(
            f"""
            SELECT s.[{col}],
              SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS od
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01
              AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
            GROUP BY s.[{col}]
            ORDER BY 2 DESC
            """,
            (p_end, na),
        )
        print(f"OD by {col}:")
        rows = cur.fetchall()
        for v, amt in rows[:15]:
            print(f"  {float(amt or 0):15,.2f}  {(v.hex() if v else None)}")
        # Try each type alone against target
        for v, amt in rows:
            amt = float(amt or 0)
            if abs(amt - TARGET_OD) < 100_000:
                print(f"  *** CLOSE OD type {v.hex() if v else None}: {amt:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
