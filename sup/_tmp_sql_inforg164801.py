"""Deep dive _InfoRg164801 — likely EDO document execution register."""
from __future__ import annotations

import functools
import sys
import uuid

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def guid_to_1c_a(guid_str: str) -> bytes:
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


def bin_to_guid(b: bytes) -> str:
    if not b or len(b) != 16:
        return ""
    orig = b[12:16] + b[10:12] + b[8:10] + b[0:8]
    try:
        return str(uuid.UUID(bytes=orig))
    except Exception:
        return b.hex()


def main() -> None:
    user_bin = guid_to_1c_a("a312e193-d7b7-11ee-94a5-6cb31113810e")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        tname = "_InfoRg164801"

        # rows for НП00-002870
        cur.execute(
            f"""
            SELECT TOP 20 *
            FROM [{tname}] WITH (NOLOCK)
            WHERE _Fld164814 = N'НП00-002870'
               OR _Fld164807 LIKE N'%НП00-002870%'
               OR _Fld164816 LIKE N'%НП00-002870%'
            ORDER BY _Fld164812 DESC
            """
        )
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"rows for NП00-002870: {len(rows)}")
        for row in rows:
            d = dict(zip(names, row))
            print("---")
            for k, v in d.items():
                if isinstance(v, (bytes, bytearray, memoryview)):
                    vb = bytes(v)
                    if len(vb) == 16:
                        print(f"  {k}={vb.hex()} guid={bin_to_guid(vb)}")
                    else:
                        print(f"  {k}=bin({len(vb)}) {vb.hex() if len(vb)<=8 else ''}")
                else:
                    sv = str(v)
                    if len(sv) > 300:
                        sv = sv[:300] + "..."
                    print(f"  {k}={sv}")

        # Which columns = user?
        cur.execute(
            """
            SELECT c.name FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
              AND ty.name IN ('binary','varbinary') AND c.max_length=16
            """,
            tname,
        )
        bin16 = [r[0] for r in cur.fetchall()]
        print("\nuser hits by col (all time):")
        for bcol in bin16:
            cur.execute(
                f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                user_bin,
            )
            print(f"  {bcol}: {cur.fetchone()[0]}")

        # May by each date for user on each bin col
        dates = ["_Fld164808", "_Fld164812", "_Fld164813"]
        print("\nuser x date May:")
        for bcol in bin16:
            for dcol in dates:
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                    WHERE [{bcol}]=? AND YEAR([{dcol}])=4026 AND MONTH([{dcol}])=5
                    """,
                    user_bin,
                )
                cnt = cur.fetchone()[0]
                if cnt:
                    print(f"  {bcol}/{dcol}: {cnt}")

        # All Voronchikhina rows in May by assignment-like dates
        # Try each user col that has hits
        for bcol in bin16:
            cur.execute(
                f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                user_bin,
            )
            if cur.fetchone()[0] == 0:
                continue
            for dcol in dates:
                cur.execute(
                    f"""
                    SELECT _Fld164814, _Fld164808, _Fld164812, _Fld164813,
                           _Fld164809, _Fld164810, _Fld164811, _Fld164815,
                           _Fld164807, _Fld164816,
                           _Fld164802RRef, _Fld164803_RRRef, _Fld164804RRef,
                           _Fld164805RRef, _Fld164806RRef
                    FROM [{tname}] WITH (NOLOCK)
                    WHERE [{bcol}]=? AND YEAR([{dcol}])=4026 AND MONTH([{dcol}])=5
                    ORDER BY [{dcol}]
                    """,
                    user_bin,
                )
                rows = cur.fetchall()
                if not rows:
                    continue
                print(f"\n=== Vor May via {bcol}/{dcol}: {len(rows)} ===")
                for r in rows:
                    print(
                        f"  num={r[0]} d808={r[1]} d812={r[2]} d813={r[3]} "
                        f"f809={bytes(r[4]).hex()} f810={bytes(r[5]).hex()} "
                        f"f811={bytes(r[6]).hex()} f815={bytes(r[7]).hex()}"
                    )
                    txt = (r[8] or "")[:100]
                    hist = (r[9] or "")[:150]
                    print(f"    txt={txt}")
                    print(f"    hist={hist}")
                    print(
                        f"    refs 802={bin_to_guid(bytes(r[10]))} "
                        f"803={bin_to_guid(bytes(r[11]))} "
                        f"804={bin_to_guid(bytes(r[12]))} "
                        f"805={bin_to_guid(bytes(r[13]))} "
                        f"806={bin_to_guid(bytes(r[14]))}"
                    )

        # Map OData name: try match by looking at metadata? Or Config
        # Count total rows
        cur.execute(f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)")
        print("\ntotal rows", cur.fetchone()[0])

        # Distinct _Fld164814 prefixes for Vor in 2026
        for bcol in bin16:
            cur.execute(
                f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                user_bin,
            )
            if not cur.fetchone()[0]:
                continue
            cur.execute(
                f"""
                SELECT LEFT(_Fld164814, 4) pref, COUNT(*)
                FROM [{tname}] WITH (NOLOCK)
                WHERE [{bcol}]=? AND YEAR(_Fld164812)=4026
                GROUP BY LEFT(_Fld164814, 4)
                ORDER BY COUNT(*) DESC
                """,
                user_bin,
            )
            print(f"prefixes via {bcol}:", cur.fetchall()[:20])


if __name__ == "__main__":
    main()
