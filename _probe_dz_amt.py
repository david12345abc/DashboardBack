# -*- coding: utf-8 -*-
"""Find register row by amount 726046 near period 2026-07-28."""
from __future__ import annotations

from datetime import date, datetime

from comdir.common import connect, to_1c_dt

AMOUNT = 726046
P0 = to_1c_dt(date(2026, 7, 28))
P1 = to_1c_dt(date(2026, 7, 29))

CANDS = [
    "_AccumRg53835",
    "_AccumRg53936",
    "_AccumRg53885",
    "_AccumRg54090",
    "_AccumRg53819",
    "_AccumRg54533",
    "_AccumRg52263",
    "_AccumRg50853",
    "_AccumRg50866",
    "_AccumRg182567",
    "_AccumRg182617",
    "_AccumRg121975",
]


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    print("period", P0, P1, flush=True)

    for t in CANDS:
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
        nums = [
            n
            for n, ty in cols.items()
            if ty in ("numeric", "decimal") and n not in ("_LineNo", "_RecordKind", "_Fld1952")
        ]
        print(f"\n{t} nums={len(nums)}", flush=True)
        for ncol in nums:
            cur.execute(
                f"""
                SELECT TOP 1 [{ncol}], _Period, _RecorderRRef
                FROM [{t}] WITH (NOLOCK)
                WHERE _Period >= ? AND _Period < ? AND [{ncol}] = ?
                """,
                (P0, P1, AMOUNT),
            )
            row = cur.fetchone()
            if row:
                print(f"  FOUND amount in {ncol} period={row[1]}", flush=True)
                # print all datetime cols of this table
                dts = [n for n, ty in cols.items() if ty.startswith("datetime")]
                print(f"  dts={dts}", flush=True)
                break
            # also try abs match with float tolerance via between
            cur.execute(
                f"""
                SELECT TOP 1 [{ncol}], _Period
                FROM [{t}] WITH (NOLOCK)
                WHERE _Period >= ? AND _Period < ?
                  AND [{ncol}] BETWEEN ? AND ?
                """,
                (P0, P1, AMOUNT - 0.01, AMOUNT + 0.01),
            )
            row = cur.fetchone()
            if row:
                print(f"  FOUND~ amount in {ncol} val={row[0]} period={row[1]}", flush=True)
                break

    cn.close()


if __name__ == "__main__":
    main()
