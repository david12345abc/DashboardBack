# -*- coding: utf-8 -*-
"""Inspect ObjCalc columns for empty-dept objects; find 50M subset."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # ObjCalc schema - find RRef fields
    cur.execute(
        """
        SELECT c.name, ty.name
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id=c.user_type_id
        WHERE c.object_id=OBJECT_ID('_Reference134945')
        ORDER BY c.column_id
        """
    )
    cols = cur.fetchall()
    rrefs = [n for n, ty in cols if n.endswith("RRef")]
    print("ObjCalc RRefs", len(rrefs))

    # Pick top empty-dept object and show all its RRef targets' descriptions if in 513
    p_end = to_1c_dt(date(2026, 7, 1))
    cur.execute(
        f"""
        SELECT TOP 1 s.[{OBJ}]
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        WHERE s._Period < ? AND s._Active = 0x01
          AND o._Fld138169RRef = 0x00000000000000000000000000000000
        GROUP BY s.[{OBJ}]
        ORDER BY SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) DESC
        """,
        p_end,
    )
    top = cur.fetchone()[0]
    print("top empty obj", top.hex())
    cur.execute(
        "SELECT _Description FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?",
        top,
    )
    print("desc", cur.fetchone()[0])

    for col in rrefs:
        cur.execute(
            f"SELECT [{col}] FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?",
            top,
        )
        val = cur.fetchone()[0]
        if not val or val == bytes(16):
            continue
        cur.execute(
            "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
            val,
        )
        d = cur.fetchone()
        if d:
            print(f"  {col} -> DEPT {d[0]}")
        else:
            # try partners?
            print(f"  {col} -> {val.hex()[:20]} (not dept)")

    # Sum of empty-dept objects whose Description looks like order (НП00)
    cur.execute(
        f"""
        SELECT
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        WHERE s._Period < ? AND s._Active = 0x01
          AND o._Fld138169RRef = 0x00000000000000000000000000000000
          AND o._Description LIKE N'%НП00%'
        """,
        p_end,
    )
    print("empty+NP00", float(cur.fetchone()[0] or 0))

    cn.close()


if __name__ == "__main__":
    main()
