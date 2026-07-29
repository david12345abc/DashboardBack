# -*- coding: utf-8 -*-
"""Find which 22M of overdue 1C excludes (Только фактическую)."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
ANAL = "_Fld107663RRef"
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

    # Overdue by dept
    cur.execute(
        f"""
        SELECT o._Fld138169RRef,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
        GROUP BY o._Fld138169RRef
        ORDER BY 2 DESC
        """,
        (p_end, na),
    )
    print("Overdue by dept:")
    total = 0.0
    for dept, amt in cur.fetchall():
        amt = float(amt or 0)
        total += amt
        cur.execute(
            "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
            dept,
        )
        name = cur.fetchone()
        print(f"  {amt:15,.2f}  {name[0] if name else '?'}")
    print(f"  TOTAL {total:,.2f}")

    # Distinct _Fld140447 (possible ТипЗаписиВзаиморасчетов)
    cur.execute(
        f"""
        SELECT s._Fld140447,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dolg,
          SUM(CASE WHEN s._RecordKind=1 AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
                   THEN -s.[{DOLG}] ELSE CASE WHEN s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
                   THEN s.[{DOLG}] ELSE 0 END END) AS od_wrong
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY s._Fld140447
        ORDER BY 2 DESC
        """,
        (na, na, p_end),
    )
    # simpler overdue by type:
    cur.execute(
        f"""
        SELECT ISNULL(s._Fld140447, N''),
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
        GROUP BY ISNULL(s._Fld140447, N'')
        ORDER BY 2 DESC
        """,
        (p_end, na),
    )
    print("\nOverdue by _Fld140447:")
    for v, amt in cur.fetchall():
        print(f"  {float(amt or 0):15,.2f}  [{v}]")

    # Ref enum field _Fld184033RRef
    cur.execute(
        f"""
        SELECT s._Fld184033RRef,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
        GROUP BY s._Fld184033RRef
        ORDER BY 2 DESC
        """,
        (p_end, na),
    )
    print("\nOverdue by _Fld184033RRef:")
    for v, amt in cur.fetchall():
        print(f"  {float(amt or 0):15,.2f}  {v.hex() if v else None}")

    # Try excluding liquidated from overdue only
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
          AND o._Fld138169RRef IN (
            SELECT id FROM #d
          )
          AND o._Fld138169RRef NOT IN (
            ?,?,?,?,?
          )
        """,
        (
            p_end,
            na,
            uuid_to_1c_bytes("4edcf3a0-9f99-11e4-80da-001e67112509"),
            uuid_to_1c_bytes("ff740269-d71e-11e6-8127-001e67112509"),
            uuid_to_1c_bytes("c6810cc3-cf32-11ef-95e8-6cb31113810e"),
            uuid_to_1c_bytes("ebd2d511-cf38-11ef-95e8-6cb31113810e"),
            uuid_to_1c_bytes("ad83f8bd-cf39-11ef-95e8-6cb31113810e"),
        ),
    )
    print(f"\nod COMM only (no liq): {float(cur.fetchone()[0] or 0):,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
