# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

REC = "f8f20bf7-8a88-11f1-9850-6cb31113810e"
OBJ = "25416d4b-875f-11f1-984c-6cb31113810e"
AMOUNT = 726046.0


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    rb = uuid_to_1c_bytes(REC)
    ob = uuid_to_1c_bytes(OBJ)
    p0 = to_1c_dt(date(2026, 7, 28))
    p1 = to_1c_dt(date(2026, 7, 29))

    t = "_AccumRg54090"
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
    cols = cur.fetchall()
    print("cols", len(cols))
    dts = [n for n, ty in cols if ty.startswith("datetime")]
    nums = [n for n, ty in cols if ty in ("numeric", "decimal")]
    rrefs = [n for n, ty in cols if n.endswith("RRef")]
    print("dts", dts)
    print("nums", nums)
    print("rrefs count", len(rrefs))

    print("lookup recorder...", flush=True)
    cur.execute(
        f"SELECT TOP 3 _Period, _LineNo, _RecordKind FROM [{t}] WITH (NOLOCK) WHERE _RecorderRRef=?",
        rb,
    )
    rows = cur.fetchall()
    print("by recorder", rows)

    print("lookup recorder+period...", flush=True)
    cur.execute(
        f"""
        SELECT TOP 3 _Period, _LineNo, _RecordKind
        FROM [{t}] WITH (NOLOCK)
        WHERE _RecorderRRef=? AND _Period>=? AND _Period<?
        """,
        (rb, p0, p1),
    )
    print("by rec+period", cur.fetchall())

    # Try obj in each rref with period filter (day)
    print("lookup obj in rrefs on day...", flush=True)
    for col in rrefs:
        cur.execute(
            f"""
            SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK)
            WHERE [{col}]=? AND _Period>=? AND _Period<?
            """,
            (ob, p0, p1),
        )
        if cur.fetchone():
            print("OBJ COL", col)
            # dump numeric nonzeros for this obj that day
            cur.execute(
                f"""
                SELECT TOP 1 *
                FROM [{t}] WITH (NOLOCK)
                WHERE [{col}]=? AND _Period>=? AND _Period<?
                """,
                (ob, p0, p1),
            )
            # better: select nums
            num_sel = ", ".join(f"[{n}]" for n in nums[:20])
            cur.execute(
                f"""
                SELECT TOP 5 _Period, _RecordKind, _LineNo, {num_sel}
                FROM [{t}] WITH (NOLOCK)
                WHERE [{col}]=? AND _Period>=? AND _Period<?
                """,
                (ob, p0, p1),
            )
            for rr in cur.fetchall():
                print(" row", rr[:10], "...")
            break

    # amount on day any column
    print("lookup amount on day...", flush=True)
    for ncol in nums:
        cur.execute(
            f"""
            SELECT TOP 1 _Period, _RecorderRRef, _LineNo
            FROM [{t}] WITH (NOLOCK)
            WHERE _Period>=? AND _Period<? AND [{ncol}]=?
            """,
            (p0, p1, AMOUNT),
        )
        hit = cur.fetchone()
        if hit:
            print("AMOUNT in", ncol, "period", hit[0], "line", hit[2])

    cn.close()
    print("done")


if __name__ == "__main__":
    main()
