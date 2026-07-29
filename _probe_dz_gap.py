# -*- coding: utf-8 -*-
"""Find the missing 50,000,020.01 and refine overdue to 142.77M."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

TARGET_DZ = 373_930_180.89
TARGET_OD = 142_773_571.76
MISSING = 50_000_020.01

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"  # Долг* (same for 672/673/674)
PLAN = "_Fld107667"  # ДатаПлановогоПогашения

COMMERCIAL = [
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


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    p_end = to_1c_dt(date(2026, 7, 1))
    na = to_1c_dt(date(2026, 6, 30))

    # 1) Company-wide net debt (all depts)
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        WHERE s._Period < ? AND s._Active = 0x01
        """,
        p_end,
    )
    print("ALL orgs/depts net dolg", float(cur.fetchone()[0] or 0))

    # 2) By department: top debt
    cur.execute(
        f"""
        SELECT TOP 30
          o._Fld138169RRef AS dept,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY o._Fld138169RRef
        ORDER BY ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)) DESC
        """,
        p_end,
    )
    print("\nTop depts by |debt|:")
    for dept, amt in cur.fetchall():
        cur.execute(
            "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
            dept,
        )
        name = cur.fetchone()
        label = name[0] if name else "?"
        print(f"  {float(amt):15,.2f}  {label}")

    # 3) Find balances near 50M
    cur.execute(
        f"""
        SELECT TOP 20
          s.[{OBJ}] AS obj,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
        FROM [{T}] s WITH (NOLOCK)
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY s.[{OBJ}]
        HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?) < 1.0
            OR ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - 50000000) < 100
        ORDER BY ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?) 
        """,
        (p_end, MISSING, MISSING),
    )
    print("\nObjects near 50M:")
    for obj, amt in cur.fetchall():
        cur.execute(
            "SELECT _Description, _Fld138169RRef FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?",
            obj,
        )
        row = cur.fetchone()
        dept_name = "?"
        if row:
            cur.execute(
                "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
                row[1],
            )
            dn = cur.fetchone()
            dept_name = dn[0] if dn else "?"
        print(f"  {float(amt):15,.2f}  obj={obj.hex()[:16]}  {row[0] if row else '?'}  dept={dept_name}")

    # 4) Positive-only per (obj, plan) then sum — commercial+liq
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in COMMERCIAL + LIQ:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

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
        GROUP BY s.[{OBJ}], s.[{PLAN}]
        """,
        p_end,
    )
    rows = cur.fetchall()
    pos_term = 0.0
    neg_term = 0.0
    overdue_pos = 0.0
    overdue_raw = 0.0
    per_order = defaultdict(float)
    per_order_od = defaultdict(float)
    for obj, plan_dt, amt in rows:
        amt = float(amt or 0)
        per_order[obj] += amt
        if amt > 0:
            pos_term += amt
        else:
            neg_term += amt
        if plan_dt and plan_dt < na and plan_dt.year > 2000:
            overdue_raw += amt
            if amt > 0:
                overdue_pos += amt
            per_order_od[obj] += amt

    order_pos = sum(v for v in per_order.values() if v > 0.01)
    order_od_pos = sum(v for v in per_order_od.values() if v > 0.01)
    print(f"\npos terms: {pos_term:,.2f}")
    print(f"neg terms: {neg_term:,.2f}")
    print(f"order pos: {order_pos:,.2f}  dDZ={order_pos-TARGET_DZ:,.2f}")
    print(f"overdue raw: {overdue_raw:,.2f}")
    print(f"overdue pos terms: {overdue_pos:,.2f}  dOD={overdue_pos-TARGET_OD:,.2f}")
    print(f"overdue order pos: {order_od_pos:,.2f}  dOD={order_od_pos-TARGET_OD:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
