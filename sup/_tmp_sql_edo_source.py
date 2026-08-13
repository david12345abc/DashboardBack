"""Find EDO execution source: VK tabular parts + InfoRg mentioning executor/deadline."""
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

        # VK doc id
        cur.execute(
            """
            SELECT TOP 1 _IDRRef
            FROM [_Document86498] WITH (NOLOCK)
            WHERE _Number = N'НП00-002870' AND YEAR(_Date_Time)=4026 AND MONTH(_Date_Time)=4
            """
        )
        doc_bin = bytes(cur.fetchone()[0])
        print("doc", doc_bin.hex())

        # All tables related to Document86498
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE t.name LIKE '_Document86498%'
               OR t.name LIKE '%86498%'
            ORDER BY t.name
            """
        )
        related = [r[0] for r in cur.fetchall()]
        print("related tables:", related)

        for tname in related:
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
            print(f"\n{tname} ({len(cols)} cols)")
            for c in cols[:60]:
                print(f"  {c[0]} {c[1]}({c[2]})")

            # If has Document_RRRef / _Document86498_IDRRef / _Fld*RRRef matching doc
            bin_cols = [c[0] for c in cols if c[1] in ("binary", "varbinary") and c[2] == 16]
            # also owner ref often _Document86498_IDRRef
            owner_cols = [c[0] for c in cols if "86498" in c[0] or c[0] in ("_Document86498_IDRRef",)]
            print("owner-like", owner_cols)

            for bcol in (owner_cols + bin_cols)[:25]:
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}] = ?",
                        doc_bin,
                    )
                    cnt = cur.fetchone()[0]
                    if cnt:
                        print(f"  DOC via {bcol}: {cnt}")
                        cur.execute(
                            f"SELECT TOP 5 * FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}] = ?",
                            doc_bin,
                        )
                        # get colnames
                        names = [d[0] for d in cur.description]
                        for row in cur.fetchall():
                            d = dict(zip(names, row))
                            # print interesting
                            parts = []
                            for k, v in d.items():
                                if isinstance(v, (bytes, bytearray, memoryview)):
                                    vb = bytes(v)
                                    if len(vb) == 16:
                                        parts.append(f"{k}={vb.hex()[:16]}...")
                                    else:
                                        parts.append(f"{k}=bin({len(vb)})")
                                elif v is not None and str(v).strip() not in ("", "0"):
                                    parts.append(f"{k}={v}")
                            print("   ", " | ".join(parts[:25]))
                except Exception as e:
                    print(f"  err {bcol}: {e}")

        # Search InfoRg / AccumRg for user_bin in May 2026 with many date cols
        print("\n=== scanning InfoRg for user in May ===")
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE t.name LIKE '_InfoRg%'
              AND t.name NOT LIKE '%ChngR%'
            ORDER BY t.name
            """
        )
        infos = [r[0] for r in cur.fetchall()]
        print("InfoRg count", len(infos))

        may_hits = []
        for tname in infos:
            cur.execute(
                """
                SELECT c.name, ty.name, c.max_length
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                """,
                tname,
            )
            cols = cur.fetchall()
            bin16 = [c[0] for c in cols if c[1] in ("binary", "varbinary") and c[2] == 16]
            dates = [c[0] for c in cols if c[1].startswith("datetime")]
            if not bin16 or not dates:
                continue
            # quick: any bin = user and any date May
            for bcol in bin16:
                for dcol in dates[:3]:
                    try:
                        cur.execute(
                            f"""
                            SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                            WHERE [{bcol}] = ?
                              AND YEAR([{dcol}])=4026 AND MONTH([{dcol}])=5
                            """,
                            user_bin,
                        )
                        cnt = cur.fetchone()[0]
                        if cnt:
                            may_hits.append((tname, bcol, dcol, cnt))
                            print(f"HIT {tname} {bcol}/{dcol}: {cnt}")
                    except Exception:
                        pass
        print("may_hits total", len(may_hits))
        for h in may_hits[:40]:
            print(" ", h)

        # Also look for tables with nvarchar containing 'Перенос срока'
        print("\n=== search Перенос срока in nvarchar cols of Task/Info/Document VT ===")
        cur.execute(
            """
            SELECT t.name, c.name
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE ty.name IN ('nvarchar','nchar')
              AND (
                t.name LIKE '_Task%'
                OR t.name LIKE '_InfoRg%'
                OR t.name LIKE '_Document86498%'
                OR t.name LIKE '_BPr%'
              )
            """
        )
        text_cols = cur.fetchall()
        print("text cols to scan", len(text_cols))
        found = 0
        for tname, cname in text_cols:
            try:
                cur.execute(
                    f"""
                    SELECT TOP 1 [{cname}] FROM [{tname}] WITH (NOLOCK)
                    WHERE [{cname}] LIKE N'%Перенос срока%'
                    """
                )
                row = cur.fetchone()
                if row:
                    print(f"FOUND {tname}.{cname}: {str(row[0])[:160]}")
                    found += 1
                    if found >= 15:
                        break
            except Exception:
                pass
        print("found", found)


if __name__ == "__main__":
    main()
