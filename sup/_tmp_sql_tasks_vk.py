"""Inspect SQL Task tables for VK doc and Voronchikhina."""
from __future__ import annotations

import functools
import sys
import uuid

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def guid_to_1c_a(guid_str: str) -> bytes:
    """qualdir.qd_m6 style."""
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


def guid_to_1c_b(guid_str: str) -> bytes:
    """Classic little-endian UUID reorder."""
    b = uuid.UUID(guid_str).bytes
    return bytes(
        [
            b[3], b[2], b[1], b[0],
            b[5], b[4],
            b[7], b[6],
            b[8], b[9],
            b[10], b[11], b[12], b[13], b[14], b[15],
        ]
    )


def main() -> None:
    doc_guid = "05dc41cd-3fb7-11f1-97ed-6cb31113810e"
    user_guid = "a312e193-d7b7-11ee-94a5-6cb31113810e"

    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()

        # Confirm VK table row and get real binary id
        cur.execute(
            """
            SELECT TOP 3 _IDRRef, _Number, _Date_Time, _Marked
            FROM [_Document86498] WITH (NOLOCK)
            WHERE _Number = N'НП00-002870'
              AND YEAR(_Date_Time) = 4026
            """
        )
        vk_rows = cur.fetchall()
        for row in vk_rows:
            print("VK row", bytes(row[0]).hex(), row[1], row[2])
        if not vk_rows:
            print("VK not found")
            return
        doc_bin = bytes(vk_rows[0][0])

        # Find user binary from InfoRg / Catalog of users if possible
        for enc_name, enc in (("a", guid_to_1c_a), ("b", guid_to_1c_b)):
            ub = enc(user_guid)
            print(f"user_bin_{enc_name}", ub.hex())

        user_bins = [guid_to_1c_a(user_guid), guid_to_1c_b(user_guid)]

        for tname in ("_Task38", "_Task39", "_Task39X1"):
            cur.execute(
                """
                SELECT c.name, ty.name AS typ
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                ORDER BY c.column_id
                """,
                tname,
            )
            cols = cur.fetchall()
            print(f"\n{tname} columns ({len(cols)}):")
            for c in cols[:50]:
                print(f"  {c[0]} {c[1]}")

            bin_cols = [c[0] for c in cols if c[1] in ("binary", "varbinary", "image")]
            date_cols = [c[0] for c in cols if "date" in c[0].lower() or c[0].startswith("_Date")]
            print("bin_cols", len(bin_cols), "date_cols", date_cols[:10])

            for user_bin in user_bins:
                for bcol in bin_cols:
                    try:
                        cur.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM [{tname}] WITH (NOLOCK)
                            WHERE [{bcol}] = ?
                              AND YEAR(_Date_Time) = 4026
                              AND MONTH(_Date_Time) = 5
                            """,
                            user_bin,
                        )
                        cnt = cur.fetchone()[0]
                        if cnt:
                            print(f"  USER hits via {bcol} in May: {cnt} bin={user_bin.hex()[:16]}...")
                    except Exception:
                        continue

            for bcol in bin_cols:
                try:
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM [{tname}] WITH (NOLOCK)
                        WHERE [{bcol}] = ?
                        """,
                        doc_bin,
                    )
                    cnt = cur.fetchone()[0]
                    if cnt:
                        print(f"  DOC hits via {bcol}: {cnt}")
                except Exception:
                    continue


if __name__ == "__main__":
    main()
