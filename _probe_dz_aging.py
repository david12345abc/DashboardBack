# -*- coding: utf-8 -*-
"""Try aging-bucket filters for overdue total 142.77M."""
from __future__ import annotations

from datetime import date, datetime

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
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
    # НаДату in 1C space
    na = to_1c_dt(date(2026, 6, 30))
    # real calendar date for day-diff: plan dates are also +2000
    # days = (na - plan).days works in 1C space the same

    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    cur.execute(
        f"""
        SELECT
          s.[{PLAN}] AS plan_dt,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
        GROUP BY s.[{PLAN}]
        """,
        (p_end, na),
    )
    buckets = {"1-29": 0.0, "30-59": 0.0, "60-179": 0.0, "180+": 0.0}
    total = 0.0
    for plan_dt, amt in cur.fetchall():
        amt = float(amt or 0)
        total += amt
        days = (na - plan_dt).days
        if days <= 29:
            buckets["1-29"] += amt
        elif days <= 59:
            buckets["30-59"] += amt
        elif days <= 179:
            buckets["60-179"] += amt
        else:
            buckets["180+"] += amt

    print(f"total od {total:,.2f}")
    for k, v in buckets.items():
        print(f"  {k}: {v:,.2f}")

    # exclude 1-29
    without_129 = total - buckets["1-29"]
    print(f"without 1-29: {without_129:,.2f}  d={without_129-TARGET_OD:,.2f}")

    # only 180+
    print(f"only 180+: {buckets['180+']:,.2f}  d={buckets['180+']-TARGET_OD:,.2f}")

    # 30+ 
    print(f"30+: {total-buckets['1-29']:,.2f}")

    # Try days > 7, 14, etc to hit target
    for min_days in (0, 1, 7, 14, 30, 45, 60, 90, 180):
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
        od = float(cur.fetchone()[0] or 0)
        print(f"days>={min_days}: {od:,.2f}  d={od-TARGET_OD:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
