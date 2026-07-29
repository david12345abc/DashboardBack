# -*- coding: utf-8 -*-
from __future__ import annotations

from comdir.common import connect

NEEDLE = "20260804200000039НП00-0011421"


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # Tables with RecordKind + >=2 datetime + nvarchar
    cur.execute(
        """
        SELECT t.name
        FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND t.name NOT LIKE '%Buf%'
          AND EXISTS (
            SELECT 1 FROM sys.columns c WHERE c.object_id=t.object_id AND c.name='_RecordKind'
          )
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("recordkind tables", len(tables), flush=True)

    for t in tables:
        cur.execute(
            """
            SELECT c.name, ty.name, c.max_length
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id=c.user_type_id
            WHERE c.object_id=OBJECT_ID(?)
            """,
            t,
        )
        cols = cur.fetchall()
        dts = [n for n, ty, ml in cols if ty.startswith("datetime") and n != "_Period"]
        strs = [n for n, ty, ml in cols if ty in ("nvarchar", "varchar") and (ml < 0 or ml >= 40)]
        if len(dts) < 2 or not strs:
            continue
        print(f"candidate {t} dts={dts} strs={strs}", flush=True)
        for sc in strs:
            cur.execute(
                f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{sc}]=?",
                NEEDLE,
            )
            if cur.fetchone():
                print("FOUND", t, sc, flush=True)
                for n, ty, ml in cols:
                    print(f"  {n} {ty}({ml})")
                cn.close()
                return
    print("not found among filtered", flush=True)
    cn.close()


if __name__ == "__main__":
    main()
