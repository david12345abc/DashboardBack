# -*- coding: utf-8 -*-
"""Find ПоСрокам AccumRg and match 1C June totals."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

TARGET_DZ = 373_930_180.89
TARGET_OD = 142_773_571.76

DEPTS = [
    ("VED", "49480c10-e401-11e8-8283-ac1f6b05524d"),
    ("ETALON", "34497ef7-810f-11e4-80d6-001e67112509"),
    ("BMI", "9edaa7d4-37a5-11ee-93d3-6cb31113810e"),
    ("KEY", "639ec87b-67b6-11eb-8523-ac1f6b05524d"),
    ("ODP", "7587c178-92f6-11f0-96f9-6cb31113810e"),
    ("GAZ", "bd7b5184-9f9c-11e4-80da-001e67112509"),
    ("byt", "4edcf3a0-9f99-11e4-80da-001e67112509"),
    ("prom", "ff740269-d71e-11e6-8127-001e67112509"),
    ("h1", "c6810cc3-cf32-11ef-95e8-6cb31113810e"),
    ("h2", "ebd2d511-cf38-11ef-95e8-6cb31113810e"),
    ("h3", "ad83f8bd-cf39-11ef-95e8-6cb31113810e"),
]


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    p_end = to_1c_dt(date(2026, 7, 1))
    na = to_1c_dt(date(2026, 6, 30))

    cur.execute(
        """
        SELECT t.name FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%[_]VT%'
          AND t.name NOT LIKE '%[_]ChngR%'
          AND t.name NOT LIKE '%[_]Buf%'
        ORDER BY t.name
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("accum tables", len(tables))

    hits = []
    for t in tables:
        cur.execute(
            """
            SELECT c.name, ty.name
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
            """,
            t,
        )
        cols = cur.fetchall()
        names = {c[0]: c[1] for c in cols}
        if "_Period" not in names or "_Active" not in names or "_RecordKind" not in names:
            continue
        dts = [n for n, ty in names.items() if ty.startswith("datetime") and n != "_Period"]
        nums = [n for n, ty in names.items() if ty in ("numeric", "decimal")]
        rrefs = [n for n in names if n.endswith("RRef")]
        if len(dts) >= 2 and len(nums) >= 2:
            hits.append((t, dts, nums, rrefs))

    print("date-dim candidates", len(hits))
    for t, dts, nums, rrefs in hits:
        print(f"\n=== {t} ===")
        print("  dts", dts)
        print("  nums", nums)
        print("  rrefs", rrefs)

        # which RRef joins to ObjCalc
        obj_col = None
        for col in rrefs:
            cur.execute(
                f"""
                SELECT TOP 1 1
                FROM [{t}] s WITH (NOLOCK)
                INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{col}]
                WHERE s._Period < ?
                """,
                p_end,
            )
            if cur.fetchone():
                obj_col = col
                print("  obj_col", col)
                break
        if not obj_col:
            print("  no ObjCalc join")
            continue

        # dept temp
        cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
        cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
        for _, guid in DEPTS:
            cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(guid))

        # Try each numeric resource: net debt with RecordKind
        for ncol in nums:
            # total net for commercial depts
            cur.execute(
                f"""
                SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                FROM [{t}] s WITH (NOLOCK)
                INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
                INNER JOIN #d d ON d.id = o._Fld138169RRef
                WHERE s._Period < ? AND s._Active = 0x01
                """,
                p_end,
            )
            total = float(cur.fetchone()[0] or 0)
            if abs(total) < 1_000_000:
                continue
            print(f"  net {ncol}: {total:,.2f}  d={total-TARGET_DZ:,.2f}")

            # overdue: need a planned-date column — try each datetime dim
            for dcol in dts:
                cur.execute(
                    f"""
                    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                    FROM [{t}] s WITH (NOLOCK)
                    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
                    INNER JOIN #d d ON d.id = o._Fld138169RRef
                    WHERE s._Period < ? AND s._Active = 0x01
                      AND s.[{dcol}] < ?
                      AND s.[{dcol}] > '2000-01-01'
                    """,
                    (p_end, na),
                )
                od = float(cur.fetchone()[0] or 0)
                if abs(od - TARGET_OD) < 5_000_000 or abs(total - TARGET_DZ) < 5_000_000:
                    print(f"    overdue via {dcol}: {od:,.2f}  d={od-TARGET_OD:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
