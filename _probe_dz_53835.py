# -*- coding: utf-8 -*-
"""Probe _AccumRg53835 as ПоСрокам vs 1C totals."""
from __future__ import annotations

from datetime import date, datetime

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
    na = to_1c_dt(date(2026, 6, 30))  # start of day for overdue compare

    # Which RRef on 53835 is ОбъектРасчетов (_Reference134945)?
    cur.execute("SELECT TOP 1 _Fld53836RRef FROM _AccumRg53835 WITH (NOLOCK) WHERE _Fld53836RRef IS NOT NULL")
    sample = cur.fetchone()[0]
    for col in (
        "_Fld53836RRef",
        "_Fld53837RRef",
        "_Fld53838RRef",
        "_Fld53840RRef",
        "_Fld53841RRef",
        "_Fld53842RRef",
        "_Fld53843RRef",
        "_Fld75086RRef",
    ):
        cur.execute(f"SELECT TOP 1 [{col}] FROM _AccumRg53835 WITH (NOLOCK) WHERE [{col}] <> 0x00000000000000000000000000000000")
        v = cur.fetchone()
        if not v:
            continue
        cur.execute("SELECT 1 FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?", v[0])
        hit = cur.fetchone()
        print(col, "in ObjCalc", bool(hit), "sample", v[0].hex()[:16])

    # Try sum of numeric fields with RecordKind sign, join dept
    # First find obj field
    obj_col = None
    for col in (
        "_Fld53836RRef",
        "_Fld53837RRef",
        "_Fld53838RRef",
        "_Fld53840RRef",
        "_Fld53841RRef",
        "_Fld53842RRef",
        "_Fld53843RRef",
        "_Fld75086RRef",
    ):
        cur.execute(
            f"""
            SELECT TOP 1 1
            FROM _AccumRg53835 s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{col}]
            """
        )
        if cur.fetchone():
            obj_col = col
            break
    print("obj_col", obj_col)
    if not obj_col:
        cn.close()
        return

    # Load dept ids
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY, name nvarchar(64))")
    for name, guid in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?, ?)", uuid_to_1c_bytes(guid), name)

    # Dept field on objects is _Fld138169RRef (from dengi fact)
    num_cols = [
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
    ]
    for ncol in num_cols:
        cur.execute(
            f"""
            SELECT SUM(
              CASE WHEN s._RecordKind = 1 THEN -s.[{ncol}] ELSE s.[{ncol}] END
            )
            FROM _AccumRg53835 s WITH (NOLOCK)
            INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
            INNER JOIN #d d ON d.id = o._Fld138169RRef
            WHERE s._Period < ? AND s._Active = 0x01 AND s.[{ncol}] <> 0
            """,
            p_end,
        )
        total = float(cur.fetchone()[0] or 0)
        print(f"  net {ncol}: {total:,.2f}  d_dz={total-TARGET_DZ:,.2f}")

    cn.close()


if __name__ == "__main__":
    main()
