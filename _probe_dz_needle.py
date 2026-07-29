# -*- coding: utf-8 -*-
"""Find register by unique ПорядокЗачета string from OData sample."""
from __future__ import annotations

from comdir.common import connect

NEEDLE = "20260804200000039НП00-0011421"
NEEDLE2 = "20260728216325339НП00-0011421"


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    cur.execute(
        """
        SELECT t.name, c.name
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND ty.name IN ('nvarchar', 'varchar')
          AND c.max_length >= 40
        ORDER BY t.name, c.column_id
        """
    )
    cols = cur.fetchall()
    print("nvarchar cols", len(cols), flush=True)

    for t, c in cols:
        for needle in (NEEDLE, NEEDLE2):
            try:
                cur.execute(
                    f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{c}]=?",
                    needle,
                )
            except Exception as e:
                print("err", t, c, e)
                continue
            if cur.fetchone():
                print("FOUND", t, c, needle, flush=True)
                cur.execute(
                    """
                    SELECT col.name, ty.name
                    FROM sys.columns col
                    JOIN sys.types ty ON ty.user_type_id=col.user_type_id
                    WHERE col.object_id=OBJECT_ID(?)
                    ORDER BY col.column_id
                    """,
                    t,
                )
                for n, ty in cur.fetchall():
                    print(f"  {n} {ty}")
                cn.close()
                return
    print("not found")
    cn.close()


if __name__ == "__main__":
    main()
