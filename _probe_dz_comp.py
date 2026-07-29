# -*- coding: utf-8 -*-
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

    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    tests = [
        (
            "_AccumRg53835",
            [
                "_Fld53845_RRRef",
                "_Fld53846_RRRef",
                "_Fld53836RRef",
                "_Fld53837RRef",
                "_Fld53838RRef",
                "_Fld53840RRef",
                "_Fld53841RRef",
                "_Fld53842RRef",
                "_Fld53843RRef",
                "_Fld75086RRef",
            ],
            ["_Fld53839", "_Fld53853", "_Fld75088", "_Fld125806"],
            [
                "_Fld53844",
                "_Fld129437",
                "_Fld131787",
                "_Fld170971",
                "_Fld170972",
                "_Fld170973",
                "_Fld170974",
                "_Fld170975",
                "_Fld179174",
                "_Fld179175",
                "_Fld179176",
                "_Fld179177",
                "_Fld53851",
            ],
        ),
        (
            "_AccumRg53936",
            [
                "_Fld53947_RRRef",
                "_Fld75091_RRRef",
                "_Fld109211_RRRef",
                "_Fld53937RRef",
                "_Fld140452RRef",
                "_Fld53939RRef",
                "_Fld53944RRef",
                "_Fld53945RRef",
                "_Fld53946RRef",
            ],
            ["_Fld53948", "_Fld53949"],
            [
                "_Fld53940",
                "_Fld53941",
                "_Fld53942",
                "_Fld53943",
                "_Fld53950",
                "_Fld53951",
                "_Fld53952",
                "_Fld170994",
            ],
        ),
    ]

    for t, obj_cols, dts, nums in tests:
        print(f"\n=== {t} ===")
        obj_col = None
        for col in obj_cols:
            cur.execute(
                f"""
                SELECT TOP 1 1
                FROM [{t}] s WITH (NOLOCK)
                INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{col}]
                """
            )
            hit = bool(cur.fetchone())
            print(f"  {col} ObjCalc={hit}")
            if hit and not obj_col:
                obj_col = col
        if not obj_col:
            continue
        print("  USING", obj_col)

        for ncol in nums:
            cur.execute(
                f"""
                SELECT SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
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
            print(f"  net {ncol}: {total:,.2f}  dDZ={total - TARGET_DZ:,.2f}")
            for dcol in dts:
                cur.execute(
                    f"""
                    SELECT SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{ncol}] ELSE s.[{ncol}] END)
                    FROM [{t}] s WITH (NOLOCK)
                    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
                    INNER JOIN #d d ON d.id = o._Fld138169RRef
                    WHERE s._Period < ? AND s._Active = 0x01
                      AND s.[{dcol}] < ? AND s.[{dcol}] > '20000101'
                    """,
                    (p_end, na),
                )
                od = float(cur.fetchone()[0] or 0)
                close = abs(total - TARGET_DZ) < 3_000_000 or abs(od - TARGET_OD) < 3_000_000
                if close or abs(od) > 50_000_000:
                    print(f"    od via {dcol}: {od:,.2f}  dOD={od - TARGET_OD:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
