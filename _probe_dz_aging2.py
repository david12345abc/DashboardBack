# -*- coding: utf-8 -*-
"""Refine 180+ overdue toward 142.77M."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
TARGET_OD = 142_773_571.76

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


def od_for(cur, depts, p_end, na, min_days: int) -> float:
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in depts:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
          AND DATEDIFF(day, s.[{PLAN}], ?) >= ?
        """,
        (p_end, na, na, min_days),
    )
    return float(cur.fetchone()[0] or 0)


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    p_end = to_1c_dt(date(2026, 7, 1))
    na = to_1c_dt(date(2026, 6, 30))

    for label, depts in [
        ("COMM+LIQ", COMM + LIQ),
        ("COMM", COMM),
        ("COMM+LIQ dealers only", COMM + LIQ[:2]),
    ]:
        for md in (0, 30, 60, 180):
            v = od_for(cur, depts, p_end, na, md)
            print(f"{label:30s} days>={md}: {v:,.2f}  d={v-TARGET_OD:,.2f}")

    # 180+ by dept
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in COMM + LIQ:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))
    cur.execute(
        f"""
        SELECT o._Fld138169RRef,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
          AND DATEDIFF(day, s.[{PLAN}], ?) >= 180
        GROUP BY o._Fld138169RRef
        ORDER BY 2 DESC
        """,
        (p_end, na, na),
    )
    print("\n180+ by dept:")
    for dept, amt in cur.fetchall():
        cur.execute(
            "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
            dept,
        )
        name = cur.fetchone()
        print(f"  {float(amt or 0):15,.2f}  {name[0] if name else '?'}")

    cn.close()


if __name__ == "__main__":
    main()
