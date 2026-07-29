# -*- coding: utf-8 -*-
"""Map _AccumRg107662 fields and match June 1C totals."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

TARGET_DZ = 373_930_180.89
TARGET_OD = 142_773_571.76
T = "_AccumRg107662"

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

    cur.execute(
        """
        SELECT c.name, ty.name
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id=c.user_type_id
        WHERE c.object_id=OBJECT_ID(?)
        ORDER BY c.column_id
        """,
        T,
    )
    cols = cur.fetchall()
    print("SCHEMA", T)
    for n, ty in cols:
        print(f"  {n} {ty}")

    names = {n: ty for n, ty in cols}
    dts = [n for n, ty in names.items() if ty.startswith("datetime") and n != "_Period"]
    nums = [
        n
        for n, ty in names.items()
        if ty in ("numeric", "decimal") and n not in ("_LineNo", "_RecordKind", "_Fld1952")
    ]
    rrefs = [n for n in names if n.endswith("RRef") and n != "_RecorderRRef"]
    print("dts", dts)
    print("nums", nums)

    # ObjCalc join
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    cur.execute("IF OBJECT_ID('tempdb..#obj') IS NOT NULL DROP TABLE #obj")
    cur.execute(
        """
        SELECT o._IDRRef AS id, o._Fld138169RRef AS dept
        INTO #obj
        FROM _Reference134945 o WITH (NOLOCK)
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        """
    )
    cur.execute("CREATE UNIQUE CLUSTERED INDEX ix ON #obj(id)")
    cur.execute("SELECT COUNT(*) FROM #obj")
    print("objs", cur.fetchone()[0])

    p_end = to_1c_dt(date(2026, 7, 1))
    na = to_1c_dt(date(2026, 6, 30))

    obj_col = None
    for col in rrefs:
        cur.execute(
            f"""
            SELECT TOP 1 1 FROM [{T}] s WITH (NOLOCK)
            INNER JOIN #obj o ON o.id = s.[{col}]
            WHERE s._Period < ?
            """,
            p_end,
        )
        if cur.fetchone():
            obj_col = col
            print("obj_col", col)
            break
    if not obj_col:
        print("no obj col")
        cn.close()
        return

    for ncol in nums:
        cur.execute(
            f"""
            SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
            FROM [{T}] s WITH (NOLOCK)
            INNER JOIN #obj o ON o.id = s.[{obj_col}]
            WHERE s._Period < ? AND s._Active = 0x01
            """,
            p_end,
        )
        total = float(cur.fetchone()[0] or 0)
        print(f"net {ncol}: {total:,.2f}  dDZ={total-TARGET_DZ:,.2f}")
        for dcol in dts:
            cur.execute(
                f"""
                SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                FROM [{T}] s WITH (NOLOCK)
                INNER JOIN #obj o ON o.id = s.[{obj_col}]
                WHERE s._Period < ? AND s._Active = 0x01
                  AND s.[{dcol}] < ? AND s.[{dcol}] > '20000101'
                """,
                (p_end, na),
            )
            od = float(cur.fetchone()[0] or 0)
            print(f"  od {dcol}: {od:,.2f}  dOD={od-TARGET_OD:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
