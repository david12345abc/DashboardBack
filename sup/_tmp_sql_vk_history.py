"""Inspect InfoRg43471X1 rows for VK doc; find any table linking doc+user+May dates."""
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
        cur.execute(
            """
            SELECT TOP 1 _IDRRef FROM [_Document86498] WITH (NOLOCK)
            WHERE _Number=N'НП00-002870' AND YEAR(_Date_Time)=4026
            """
        )
        doc_bin = bytes(cur.fetchone()[0])

        # Also get other VK docs from report
        nums = ["НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101"]
        docs = {}
        for num in nums:
            cur.execute(
                """
                SELECT TOP 1 _IDRRef, _Date_Time FROM [_Document86498] WITH (NOLOCK)
                WHERE _Number=? AND YEAR(_Date_Time)=4026
                ORDER BY _Date_Time DESC
                """,
                num,
            )
            row = cur.fetchone()
            if row:
                docs[num] = bytes(row[0])
                print(num, row[1], bin_to_guid(bytes(row[0])))
            else:
                print(num, "NOT FOUND")

        print("\n=== _InfoRg43471X1 for docs ===")
        for num, dbin in docs.items():
            cur.execute(
                """
                SELECT TOP 20 _Fld43472_RRRef, _Fld43473, _Fld43476_RRRef, _Fld43477,
                       _Fld43478, _Fld43479RRef, _Fld43481, _Fld43482, _Fld43485, _Fld134221
                FROM [_InfoRg43471X1] WITH (NOLOCK)
                WHERE _Fld43472_RRRef = ?
                ORDER BY _Fld43477
                """,
                dbin,
            )
            rows = cur.fetchall()
            print(f"\n{num}: {len(rows)} rows")
            for r in rows:
                print(
                    " ",
                    r[3],
                    "user?",
                    bin_to_guid(bytes(r[2])),
                    "n=",
                    r[1],
                    "txt=",
                    (str(r[4] or "")[:80]),
                    "t85=",
                    (str(r[8] or "")[:60]),
                    "t134=",
                    (str(r[9] or "")[:60]),
                )

        # Find tables where BOTH doc_bin and user_bin appear in same row
        print("\n=== tables with both doc and user ===")
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE (t.name LIKE '_InfoRg%' OR t.name LIKE '_BPr%' OR t.name LIKE '_Task%' OR t.name LIKE '_Document%VT%')
              AND t.name NOT LIKE '%ChngR%'
            ORDER BY t.name
            """
        )
        tables = [r[0] for r in cur.fetchall()]
        both_hits = []
        for tname in tables:
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
            if len(bin16) < 2:
                continue
            # presence checks first
            doc_cols = []
            user_cols = []
            for bcol in bin16:
                try:
                    cur.execute(
                        f"SELECT TOP 1 1 FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                        doc_bin,
                    )
                    if cur.fetchone():
                        doc_cols.append(bcol)
                except Exception:
                    pass
            if not doc_cols:
                continue
            for bcol in bin16:
                try:
                    cur.execute(
                        f"SELECT TOP 1 1 FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                        user_bin,
                    )
                    if cur.fetchone():
                        user_cols.append(bcol)
                except Exception:
                    pass
            if not user_cols:
                continue
            # same-row
            for dc in doc_cols:
                for uc in user_cols:
                    if dc == uc:
                        continue
                    try:
                        cur.execute(
                            f"""
                            SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                            WHERE [{dc}]=? AND [{uc}]=?
                            """,
                            doc_bin,
                            user_bin,
                        )
                        cnt = cur.fetchone()[0]
                        if cnt:
                            print(f"BOTH {tname} doc={dc} user={uc} cnt={cnt}")
                            both_hits.append((tname, dc, uc, cnt))
                            cur.execute(
                                f"SELECT TOP 3 * FROM [{tname}] WITH (NOLOCK) WHERE [{dc}]=? AND [{uc}]=?",
                                doc_bin,
                                user_bin,
                            )
                            names = [d[0] for d in cur.description]
                            for row in cur.fetchall():
                                parts = []
                                for k, v in zip(names, row):
                                    if isinstance(v, (bytes, bytearray, memoryview)):
                                        vb = bytes(v)
                                        if len(vb) == 16 and vb != b"\x00" * 16:
                                            parts.append(f"{k}={bin_to_guid(vb)}")
                                    elif v is not None and str(v).strip() not in ("", "0"):
                                        sv = str(v).replace("\n", " ")[:70]
                                        parts.append(f"{k}={sv}")
                                print("  ", " | ".join(parts[:25]))
                    except Exception:
                        pass
        print("both_hits", both_hits)

        # Find доп agreement doc table by number
        print("\n=== find МПГ00010733 ===")
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            WHERE t.name LIKE '_Document%'
              AND t.name NOT LIKE '%VT%'
              AND t.name NOT LIKE '%ChngR%'
            """
        )
        doctables = [r[0] for r in cur.fetchall()]
        for tname in doctables:
            # has _Number?
            cur.execute(
                """
                SELECT 1 FROM sys.columns
                WHERE object_id=OBJECT_ID(?) AND name='_Number'
                """,
                tname,
            )
            if not cur.fetchone():
                continue
            try:
                cur.execute(
                    f"""
                    SELECT TOP 1 _Number, _Date_Time, _IDRRef
                    FROM [{tname}] WITH (NOLOCK)
                    WHERE _Number LIKE N'%МПГ00010733%'
                    """
                )
                row = cur.fetchone()
                if row:
                    print(f"FOUND {tname}: {row[0]} {row[1]} {bin_to_guid(bytes(row[2]))}")
            except Exception:
                pass


if __name__ == "__main__":
    main()
