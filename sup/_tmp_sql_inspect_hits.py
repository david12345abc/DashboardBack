"""Inspect promising InfoRg hits for Voronchikhina May + VK VT."""
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


def main() -> None:
    user_bin = guid_to_1c_a("a312e193-d7b7-11ee-94a5-6cb31113810e")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT TOP 1 _IDRRef FROM [_Document86498] WITH (NOLOCK)
            WHERE _Number=N'НП00-002870' AND YEAR(_Date_Time)=4026 AND MONTH(_Date_Time)=4
            """
        )
        doc_bin = bytes(cur.fetchone()[0])

        # VT of VK
        cur.execute(
            """
            SELECT * FROM [_Document86498_VT166661] WITH (NOLOCK)
            WHERE _Document86498_IDRRef = ?
            """,
            doc_bin,
        )
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"VK VT rows: {len(rows)} cols={names}")
        for row in rows:
            d = dict(zip(names, row))
            print({k: (bytes(v).hex() if isinstance(v, (bytes, bytearray, memoryview)) else v) for k, v in d.items()})

        targets = [
            "_InfoRg137298",
            "_InfoRg164801",
            "_InfoRg43471X1",
            "_InfoRg50700",
        ]
        for tname in targets:
            cur.execute(
                """
                SELECT c.name, ty.name, c.max_length
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                ORDER BY c.column_id
                """,
                tname,
            )
            cols = cur.fetchall()
            print(f"\n=== {tname} ({len(cols)}) ===")
            for c in cols:
                print(f"  {c[0]} {c[1]}({c[2]})")

            bin16 = [c[0] for c in cols if c[1] in ("binary", "varbinary") and c[2] == 16]
            dates = [c[0] for c in cols if c[1].startswith("datetime")]
            texts = [c[0] for c in cols if c[1] in ("nvarchar", "nchar")]

            # find which bin = user
            user_cols = []
            for bcol in bin16:
                cur.execute(
                    f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=? AND YEAR(_Period)=4026",
                    user_bin,
                ) if False else None
                # generic without assuming _Period
                try:
                    # May via first date col
                    if not dates:
                        continue
                    dcol = dates[0]
                    cur.execute(
                        f"""
                        SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                        WHERE [{bcol}]=? AND YEAR([{dcol}])=4026 AND MONTH([{dcol}])=5
                        """,
                        user_bin,
                    )
                    cnt = cur.fetchone()[0]
                    if cnt:
                        user_cols.append((bcol, dcol, cnt))
                except Exception as e:
                    print(" ", bcol, e)

            print("user May cols:", user_cols)

            for bcol, dcol, cnt in user_cols[:2]:
                sel = ", ".join(f"[{c[0]}]" for c in cols[:40])
                cur.execute(
                    f"""
                    SELECT TOP 10 {sel}
                    FROM [{tname}] WITH (NOLOCK)
                    WHERE [{bcol}]=? AND YEAR([{dcol}])=4026 AND MONTH([{dcol}])=5
                    ORDER BY [{dcol}]
                    """,
                    user_bin,
                )
                rnames = [d[0] for d in cur.description]
                print(f"\n sample via {bcol}/{dcol} ({cnt})")
                for row in cur.fetchall():
                    d = dict(zip(rnames, row))
                    parts = []
                    for k, v in d.items():
                        if isinstance(v, (bytes, bytearray, memoryview)):
                            vb = bytes(v)
                            if len(vb) == 16 and vb != b"\x00" * 16:
                                parts.append(f"{k}={vb.hex()}")
                        elif v is not None and str(v).strip() not in ("", "0"):
                            sv = str(v)
                            if len(sv) > 120:
                                sv = sv[:120] + "..."
                            parts.append(f"{k}={sv}")
                    print("  -", " | ".join(parts[:30]))

            # Does any row reference our VK doc?
            for bcol in bin16:
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                        doc_bin,
                    )
                    cnt = cur.fetchone()[0]
                    if cnt:
                        print(f" DOC via {bcol}: {cnt}")
                except Exception:
                    pass

            # text contains НП00
            for tcol in texts:
                try:
                    cur.execute(
                        f"""
                        SELECT TOP 3 [{tcol}] FROM [{tname}] WITH (NOLOCK)
                        WHERE [{tcol}] LIKE N'%НП00-002870%' OR [{tcol}] LIKE N'%Перенос срока%'
                        """
                    )
                    hits = cur.fetchall()
                    if hits:
                        print(f" TEXT {tcol}:")
                        for h in hits:
                            print("   ", str(h[0])[:200])
                except Exception:
                    pass

        # Search BPr for user May
        print("\n=== BPr user May ===")
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '_BPr%' AND t.name NOT LIKE '%ChngR%' AND t.name NOT LIKE '%Points%'
              AND t.name NOT LIKE '%VT%'
            ORDER BY t.name
            """
        )
        bprs = [r[0] for r in cur.fetchall()]
        for tname in bprs:
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
            has_date = False
            cur.execute(
                """
                SELECT COUNT(*) FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?) AND ty.name LIKE 'datetime%'
                """,
                tname,
            )
            has_date = cur.fetchone()[0] > 0
            if not has_date:
                continue
            for bcol in bin16:
                try:
                    cur.execute(
                        f"""
                        SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                        WHERE [{bcol}]=? AND YEAR(_Date_Time)=4026 AND MONTH(_Date_Time)=5
                        """,
                        user_bin,
                    )
                    cnt = cur.fetchone()[0]
                    if cnt:
                        print(f"HIT {tname}.{bcol}: {cnt}")
                        cur.execute(
                            f"""
                            SELECT TOP 3 _Date_Time, _Number, _Head
                            FROM [{tname}] WITH (NOLOCK)
                            WHERE [{bcol}]=? AND YEAR(_Date_Time)=4026 AND MONTH(_Date_Time)=5
                            """,
                            user_bin,
                        )
                        # _Head may not exist
                except Exception:
                    # try without number/head
                    try:
                        cur.execute(
                            f"""
                            SELECT TOP 1 * FROM [{tname}] WITH (NOLOCK)
                            WHERE [{bcol}]=? AND YEAR(_Date_Time)=4026 AND MONTH(_Date_Time)=5
                            """,
                            user_bin,
                        )
                        names = [d[0] for d in cur.description]
                        row = cur.fetchone()
                        if row:
                            d = dict(zip(names, row))
                            print(f"HIT {tname}.{bcol}")
                            for k in names[:20]:
                                v = d[k]
                                if isinstance(v, (bytes, bytearray, memoryview)):
                                    continue
                                print(f"   {k}={v}")
                    except Exception:
                        pass


if __name__ == "__main__":
    main()
