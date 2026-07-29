# -*- coding: utf-8 -*-
"""Fast probe: find ПоСрокам table via ObjCalc sample join."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

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

    # Candidate tables: AccumRg with >=2 extra datetime cols near money register numbering
    cur.execute(
        """
        SELECT t.name
        FROM sys.tables t
        WHERE t.name LIKE '_AccumRg5%'
          AND t.name NOT LIKE '%[_]%'
             OR (t.name LIKE '_AccumRg5%' AND t.name NOT LIKE '%VT%'
                 AND t.name NOT LIKE '%ChngR%' AND t.name NOT LIKE '%Buf%'
                 AND t.name NOT LIKE '%Opts%' AND t.name NOT LIKE '%T%')
        """
    )
    # simpler:
    cur.execute(
        """
        SELECT t.name FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND LEN(t.name) BETWEEN 10 AND 14
          AND t.name NOT LIKE '%VT'
        ORDER BY t.name
        """
    )
    raw = [r[0] for r in cur.fetchall()]
    # filter out totals/change tables by name patterns
    tables = [
        t for t in raw
        if not any(x in t for x in ("VT", "ChngR", "Buf", "Opts", "Bf"))
        and not t.endswith("T")  # keep? AccumRgT is totals - actually ends with digit usually
    ]
    # AccumRgT tables end with T after number like _AccumRg53835T
    tables = [t for t in raw if t[9:].isdigit()]
    print("plain AccumRg count", len(tables))

    # Prefetch sample ObjCalc ids that belong to commercial depts
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    cur.execute("IF OBJECT_ID('tempdb..#obj') IS NOT NULL DROP TABLE #obj")
    cur.execute(
        """
        SELECT TOP 2000 o._IDRRef AS id
        INTO #obj
        FROM _Reference134945 o WITH (NOLOCK)
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        """
    )
    cur.execute("SELECT COUNT(*) FROM #obj")
    print("sample objs", cur.fetchone()[0])

    # Narrow candidates: tables with Period+Active+RecordKind+>=2 datetime dims
    cands = []
    for t in tables:
        cur.execute(
            """
            SELECT c.name, ty.name
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id=c.user_type_id
            WHERE c.object_id=OBJECT_ID(?)
            """,
            t,
        )
        cols = {r[0]: r[1] for r in cur.fetchall()}
        if not {"_Period", "_Active", "_RecordKind"} <= set(cols):
            continue
        dts = [n for n, ty in cols.items() if ty.startswith("datetime") and n != "_Period"]
        nums = [
            n for n, ty in cols.items()
            if ty in ("numeric", "decimal") and n not in ("_LineNo", "_RecordKind")
        ]
        rrefs = [n for n in cols if n.endswith("RRef") and not n.startswith("_Recorder")]
        if len(dts) >= 2 and len(nums) >= 2:
            cands.append((t, dts, nums, rrefs))
    print("cands", len(cands), [c[0] for c in cands])

    for t, dts, nums, rrefs in cands:
        print(f"\n=== {t} === dts={dts} nums={nums}")
        obj_col = None
        for col in rrefs:
            # fast: any sample obj appears in this column
            cur.execute(
                f"""
                SELECT TOP 1 1
                FROM [{t}] s WITH (NOLOCK)
                INNER JOIN #obj o ON o.id = s.[{col}]
                """
            )
            if cur.fetchone():
                obj_col = col
                break
        print("  obj_col", obj_col)
        if not obj_col:
            continue

        for ncol in nums:
            cur.execute(
                f"""
                SELECT
                  SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                FROM [{t}] s WITH (NOLOCK)
                INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
                INNER JOIN #d d ON d.id = o._Fld138169RRef
                WHERE s._Period < ? AND s._Active = 0x01
                """,
                p_end,
            )
            total = float(cur.fetchone()[0] or 0)
            if abs(total) < 50_000_000:
                continue
            print(f"  {ncol}: {total:,.2f}  deltaDZ={total-TARGET_DZ:,.2f}")
            for dcol in dts:
                cur.execute(
                    f"""
                    SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                    FROM [{t}] s WITH (NOLOCK)
                    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
                    INNER JOIN #d d ON d.id = o._Fld138169RRef
                    WHERE s._Period < ? AND s._Active = 0x01
                      AND s.[{dcol}] < ? AND s.[{dcol}] > '20000101'
                    """,
                    (p_end, na),
                )
                od = float(cur.fetchone()[0] or 0)
                print(f"    od {dcol}: {od:,.2f}  deltaOD={od-TARGET_OD:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
