# -*- coding: utf-8 -*-
"""Scan ALL AccumRg for ObjCalc key from ПоСрокам OData sample."""
from __future__ import annotations

from comdir.common import connect, uuid_to_1c_bytes

OBJ = "fe35043c-c873-11ec-8899-ac1f6b05524d"


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    b = uuid_to_1c_bytes(OBJ)

    cur.execute(
        """
        SELECT t.name
        FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND t.name NOT LIKE '%Buf%'
        ORDER BY t.name
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("tables", len(tables), flush=True)

    # Prefer tables that also have >=2 datetime dims besides Period
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
        dts = [n for n, ty in names.items() if ty.startswith("datetime") and n != "_Period"]
        rrefs = [n for n in names if n.endswith("RRef")]
        if len(dts) < 2:
            continue
        for col in rrefs:
            cur.execute(
                f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{col}]=?",
                b,
            )
            if cur.fetchone():
                nums = [
                    n
                    for n, ty in names.items()
                    if ty in ("numeric", "decimal") and n not in ("_LineNo", "_RecordKind", "_Fld1952")
                ]
                print(f"HIT {t}.{col} dts={dts} nums={nums[:12]}", flush=True)
                break

    cn.close()
    print("done", flush=True)


if __name__ == "__main__":
    main()
