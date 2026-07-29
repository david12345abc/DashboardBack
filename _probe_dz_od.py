# -*- coding: utf-8 -*-
"""Match overdue 142,773,571.76 with COMM+LIQ on _AccumRg107662."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PRED = "_Fld107669"
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

    cur.execute(
        f"""
        SELECT
          s.[{OBJ}] AS obj,
          o._Fld138169RRef AS dept,
          s.[{PLAN}] AS plan_dt,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dolg,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{PRED}] ELSE s.[{PRED}] END) AS pred
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY s.[{OBJ}], o._Fld138169RRef, s.[{PLAN}]
        """,
        p_end,
    )
    rows = cur.fetchall()

    # variants
    dz_raw = 0.0
    dz_pos_order = 0.0
    od_raw = 0.0
    od_pos_terms = 0.0
    od_capped = 0.0  # min(dz, od) per order
    od_only_if_dz_pos = 0.0
    od_net_pred = 0.0

    per_order = defaultdict(lambda: {"dz": 0.0, "pred": 0.0, "od": 0.0, "od_pos": 0.0})

    for obj, dept, plan_dt, dolg, pred in rows:
        dolg = float(dolg or 0)
        pred = float(pred or 0)
        dz_raw += dolg
        e = per_order[obj]
        e["dz"] += dolg
        e["pred"] += pred
        if plan_dt and plan_dt < na and getattr(plan_dt, "year", 2001) > 2000:
            od_raw += dolg
            e["od"] += dolg
            if dolg > 0:
                od_pos_terms += dolg
                e["od_pos"] += dolg

    for e in per_order.values():
        if e["dz"] > 0.01:
            dz_pos_order += e["dz"]
            od = e["od"]
            if od > 0.01:
                od_only_if_dz_pos += od
                od_capped += min(e["dz"], od)
            # net of prepaid
            net = e["dz"] - e["pred"]
            if net > 0.01 and od > 0.01:
                od_net_pred += min(net, max(od, 0))

    # Also: sum positive overdue terms only for orders with positive dz
    od_pos_terms_dzpos = 0.0
    for e in per_order.values():
        if e["dz"] > 0.01:
            od_pos_terms_dzpos += e["od_pos"]

    print(f"dz_raw            {dz_raw:,.2f}  d={dz_raw-TARGET_DZ:,.2f}")
    print(f"dz_pos_order      {dz_pos_order:,.2f}  d={dz_pos_order-TARGET_DZ:,.2f}")
    print(f"od_raw            {od_raw:,.2f}  d={od_raw-TARGET_OD:,.2f}")
    print(f"od_pos_terms      {od_pos_terms:,.2f}  d={od_pos_terms-TARGET_OD:,.2f}")
    print(f"od_only_if_dz_pos {od_only_if_dz_pos:,.2f}  d={od_only_if_dz_pos-TARGET_OD:,.2f}")
    print(f"od_capped         {od_capped:,.2f}  d={od_capped-TARGET_OD:,.2f}")
    print(f"od_net_pred       {od_net_pred:,.2f}  d={od_net_pred-TARGET_OD:,.2f}")
    print(f"od_pos_terms_dzpos{od_pos_terms_dzpos:,.2f}  d={od_pos_terms_dzpos-TARGET_OD:,.2f}")

    # Try planned date <= na (inclusive end of day before? or <= НаДату)
    # already using plan < na (start of June 30). Try plan_dt.date() <= June 29 vs <= June 30
    from datetime import datetime as dt

    od_le_jun30 = 0.0
    od_lt_jun30 = 0.0
    for obj, dept, plan_dt, dolg, pred in rows:
        dolg = float(dolg or 0)
        if not plan_dt or getattr(plan_dt, "year", 1) < 2000:
            continue
        # plan_dt is already 1C datetime with +2000 year offset!
        # to_1c_dt(June 30) = 4026-06-30. So compare in 1C space.
        if plan_dt < na:
            od_lt_jun30 += dolg
        if plan_dt <= na:
            od_le_jun30 += dolg
    print(f"od plan<Jun30     {od_lt_jun30:,.2f}")
    print(f"od plan<=Jun30    {od_le_jun30:,.2f}")

    # Exclude storno binary fields
    for col in ("_Fld170989", "_Fld140449"):
        cur.execute(
            f"""
            SELECT
              SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01
              AND s.[{col}] = 0x00
              AND s.[{PLAN}] < ?
              AND s.[{PLAN}] > '20000101'
            """,
            (p_end, na),
        )
        print(f"od {col}=0: {float(cur.fetchone()[0] or 0):,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
