# -*- coding: utf-8 -*-
"""Find Document_РегистраторРасчетов table and its AccumRg writes."""
from __future__ import annotations

from datetime import date, timedelta

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

REC = "f8f20bf7-8a88-11f1-9850-6cb31113810e"
OBJ = "25416d4b-875f-11f1-984c-6cb31113810e"
AMOUNT = 726046


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    rb = uuid_to_1c_bytes(REC)
    ob = uuid_to_1c_bytes(OBJ)
    p0 = to_1c_dt(date(2026, 7, 28))
    p1 = to_1c_dt(date(2026, 7, 29))

    # Find document table containing this recorder
    cur.execute(
        """
        SELECT t.name FROM sys.tables t
        WHERE t.name LIKE '_Document%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
        ORDER BY t.name
        """
    )
    docs = [r[0] for r in cur.fetchall()]
    print("doc tables", len(docs), flush=True)
    for t in docs:
        cur.execute(
            """
            SELECT 1 FROM sys.columns
            WHERE object_id=OBJECT_ID(?) AND name='_IDRRef'
            """,
            t,
        )
        if not cur.fetchone():
            continue
        cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?", rb)
        if cur.fetchone():
            print("DOC HIT", t, flush=True)

    # Narrow AccumRg search: tables with Period between and Recorder match,
    # only those with string field (ПорядокЗачета) or many resources
    cur.execute(
        """
        SELECT t.name
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id=t.object_id AND c.name='_RecorderRRef'
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND t.name NOT LIKE '%Buf%'
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("accum with recorder", len(tables), flush=True)

    # Prefer tables that have nvarchar field like ПорядокЗачета
    for t in tables:
        cur.execute(
            """
            SELECT COUNT(*) FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id=c.user_type_id
            WHERE c.object_id=OBJECT_ID(?) AND ty.name LIKE 'nvarchar'
            """,
            t,
        )
        n_str = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*) FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id=c.user_type_id
            WHERE c.object_id=OBJECT_ID(?) AND ty.name LIKE 'datetime%'
            """,
            t,
        )
        n_dt = cur.fetchone()[0]
        if n_str < 1 or n_dt < 3:
            continue
        cur.execute(
            f"""
            SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK)
            WHERE _RecorderRRef=? AND _Period>=? AND _Period<?
            """,
            (rb, p0, p1),
        )
        if cur.fetchone():
            print("HIT", t, "nvarchar", n_str, "dt", n_dt, flush=True)
            cur.execute(
                """
                SELECT c.name, ty.name
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id=c.user_type_id
                WHERE c.object_id=OBJECT_ID(?)
                ORDER BY c.column_id
                """,
                t,
            )
            for n, ty in cur.fetchall():
                print(f"  {n} {ty}")

    cn.close()


if __name__ == "__main__":
    main()
