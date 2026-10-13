# -*- coding: utf-8 -*-
"""August payroll date vs the 35k supplement. No OData."""
from __future__ import annotations

import sys

from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")

EMP = bytes.fromhex("979e6cb31113810c11f107d4d3e8a703")


def main() -> None:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cur.execute(
            """
            SELECT DISTINCT r._RecorderTRef, r._RecorderRRef, r._Period
            FROM dbo._CRg2301 r WITH (NOLOCK)
            WHERE r._Fld2302RRef = ? AND r._Period >= '4026-08-01' AND r._Period < '4026-10-01'
              AND r._Active = 0x01
            """,
            EMP,
        )
        for tref, rref, period in cur.fetchall():
            type_no = int.from_bytes(bytes(tref)[:4], "big")
            table = f"_Document{type_no}"
            cur.execute(
                f"SELECT _Number, _Date_Time FROM dbo.[{table}] WITH (NOLOCK) WHERE _IDRRef = ?",
                bytes(rref),
            )
            print(period, table, cur.fetchone())

        print("\n== doc 828 lines with numbers")
        doc = bytes.fromhex("98856cb31113810e11f1b28f45a114fd")
        cur.execute(
            """
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE '_Document828_VT%'
            """
        )
        for (vt,) in cur.fetchall():
            cur.execute(
                """
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ? AND DATA_TYPE IN ('numeric','decimal','datetime2','nvarchar')
                """,
                vt,
            )
            cols = [r[0] for r in cur.fetchall() if r[0] != "_Fld1952"]
            parent = "_Document828_IDRRef"
            cur.execute(
                f"SELECT COUNT(*) FROM dbo.[{vt}] WITH (NOLOCK) WHERE [{parent}] = ?",
                doc,
            )
            n = cur.fetchone()[0]
            if not n:
                continue
            sel = ", ".join(f"[{c}]" for c in cols[:12])
            cur.execute(
                f"SELECT TOP 8 {sel} FROM dbo.[{vt}] WITH (NOLOCK) WHERE [{parent}] = ?",
                doc,
            )
            print(vt, "rows", n)
            for row in cur.fetchall():
                print(" ", row)


if __name__ == "__main__":
    main()
