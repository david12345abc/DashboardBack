"""Find tables with exact assignment timestamps from EDO report."""
from __future__ import annotations

import functools
import sys

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

TIMES = [
    ("4026-05-04 21:09:00", "4026-05-04 21:09:59"),
    ("4026-05-04 10:58:00", "4026-05-04 10:58:59"),
    ("4026-05-18 14:41:00", "4026-05-18 14:41:59"),
    ("4026-05-20 11:08:00", "4026-05-20 11:08:59"),
    ("4026-05-23 07:03:00", "4026-05-23 07:03:59"),
]


def main() -> None:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        # Candidate tables: have datetime + binary16 + not huge change logs
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE (
                t.name LIKE '_InfoRg%'
                OR t.name LIKE '_Task%'
                OR t.name LIKE '_BPr%'
                OR t.name LIKE '_Document%'
                OR t.name LIKE '_Reference%'
            )
            AND t.name NOT LIKE '%ChngR%'
            ORDER BY t.name
            """
        )
        tables = [r[0] for r in cur.fetchall()]
        print("tables", len(tables))

        hits_total = 0
        for tname in tables:
            cur.execute(
                """
                SELECT c.name
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                  AND ty.name LIKE 'datetime%'
                """,
                tname,
            )
            dcols = [r[0] for r in cur.fetchall()]
            if not dcols:
                continue

            for dcol in dcols:
                for t0, t1 in TIMES:
                    try:
                        cur.execute(
                            f"""
                            SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                            WHERE [{dcol}] >= ? AND [{dcol}] <= ?
                            """,
                            t0,
                            t1,
                        )
                        cnt = cur.fetchone()[0]
                    except Exception:
                        continue
                    if not cnt:
                        continue
                    print(f"HIT {tname}.{dcol} {t0[11:16]} cnt={cnt}")
                    hits_total += 1
                    # dump one row condensed
                    try:
                        cur.execute(
                            f"""
                            SELECT TOP 1 * FROM [{tname}] WITH (NOLOCK)
                            WHERE [{dcol}] >= ? AND [{dcol}] <= ?
                            """,
                            t0,
                            t1,
                        )
                        names = [d[0] for d in cur.description]
                        row = cur.fetchone()
                        parts = []
                        for k, v in zip(names, row):
                            if isinstance(v, (bytes, bytearray, memoryview)):
                                continue
                            if v is None:
                                continue
                            sv = str(v).replace("\n", " ")
                            if not sv.strip() or sv in ("0",):
                                continue
                            if len(sv) > 80:
                                sv = sv[:80] + "..."
                            parts.append(f"{k}={sv}")
                        print("  ", " | ".join(parts[:20]))
                    except Exception as e:
                        print("  dump err", e)
        print("done hits", hits_total)


if __name__ == "__main__":
    main()
