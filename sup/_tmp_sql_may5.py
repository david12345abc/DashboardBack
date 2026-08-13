"""Inspect InfoRg with ~5 May hits for Vor; search deadline 18.05.2026 + VK numbers."""
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


def dump_table(cur, tname: str, user_bin: bytes) -> None:
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
    print(f"\n===== {tname} ({len(cols)}) =====")
    for c in cols:
        print(f"  {c[0]} {c[1]}({c[2]})")

    bin16 = [c[0] for c in cols if c[1] in ("binary", "varbinary") and c[2] == 16]
    dates = [c[0] for c in cols if c[1].startswith("datetime")]
    texts = [c[0] for c in cols if c[1] in ("nvarchar", "nchar")]

    user_cols = []
    for bcol in bin16:
        for dcol in dates:
            try:
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
            except Exception:
                pass
    print("user May:", user_cols)

    for bcol, dcol, cnt in user_cols:
        sel = ", ".join(f"[{c[0]}]" for c in cols)
        cur.execute(
            f"""
            SELECT TOP 20 {sel} FROM [{tname}] WITH (NOLOCK)
            WHERE [{bcol}]=? AND YEAR([{dcol}])=4026 AND MONTH([{dcol}])=5
            ORDER BY [{dcol}]
            """,
            user_bin,
        )
        names = [d[0] for d in cur.description]
        print(f"\nsamples {bcol}/{dcol} ({cnt})")
        for row in cur.fetchall():
            d = dict(zip(names, row))
            parts = []
            for k, v in d.items():
                if isinstance(v, (bytes, bytearray, memoryview)):
                    vb = bytes(v)
                    if len(vb) == 16 and vb != b"\x00" * 16:
                        parts.append(f"{k}={bin_to_guid(vb)}")
                    elif len(vb) == 1:
                        parts.append(f"{k}={vb.hex()}")
                elif v is not None and str(v).strip() not in ("", "0"):
                    sv = str(v).replace("\n", " ")
                    if len(sv) > 100:
                        sv = sv[:100] + "..."
                    parts.append(f"{k}={sv}")
            print(" -", " | ".join(parts[:35]))

    for tcol in texts:
        try:
            cur.execute(
                f"""
                SELECT TOP 3 [{tcol}] FROM [{tname}] WITH (NOLOCK)
                WHERE [{tcol}] LIKE N'%НП00-002870%'
                   OR [{tcol}] LIKE N'%НП00-002937%'
                   OR [{tcol}] LIKE N'%МПГ00010733%'
                   OR [{tcol}] LIKE N'%Перенос срока по задаче%'
                """
            )
            hits = cur.fetchall()
            if hits:
                print(f"TEXT {tcol}:")
                for h in hits:
                    print(" ", str(h[0])[:220])
        except Exception:
            pass


def main() -> None:
    user_bin = guid_to_1c_a("a312e193-d7b7-11ee-94a5-6cb31113810e")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        for t in ("_InfoRg63065", "_InfoRg72945", "_InfoRg98657", "_InfoRg50700"):
            dump_table(cur, t, user_bin)

        # Global search: nvarchar = exact doc numbers from screenshot in any InfoRg
        print("\n===== global number search =====")
        numbers = ("НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101", "МПГ00010733/1")
        cur.execute(
            """
            SELECT t.name, c.name
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE ty.name IN ('nvarchar','nchar')
              AND c.max_length BETWEEN 10 AND 80
              AND (
                t.name LIKE '_InfoRg%'
                OR t.name LIKE '_Document%'
                OR t.name LIKE '_AccumRg%'
              )
              AND t.name NOT LIKE '%ChngR%'
            """
        )
        candidates = cur.fetchall()
        print("candidate text cols", len(candidates))
        found = 0
        for tname, cname in candidates:
            try:
                # quick equality on first number
                cur.execute(
                    f"SELECT TOP 1 1 FROM [{tname}] WITH (NOLOCK) WHERE [{cname}] = N'НП00-002937'"
                )
                if cur.fetchone():
                    print(f"EQ HIT {tname}.{cname} = НП00-002937")
                    found += 1
                    # count May user if possible
            except Exception:
                pass
        print("eq found", found)

        # Search datetime = 4026-05-18 near user in InfoRg with text fields mentioning Перенос
        print("\n===== rows with deadline 18.05.2026 containing Перенос =====")
        # already found in Task - search InfoRg164801 hist
        cur.execute(
            """
            SELECT TOP 20 _Fld164814, _Fld164802RRef, _Fld164808, _Fld164812, _Fld164813, _Fld164816
            FROM [_InfoRg164801] WITH (NOLOCK)
            WHERE _Fld164816 LIKE N'%18.05.2026%'
               OR _Fld164816 LIKE N'%Перенос срока по задаче%'
            """
        )
        for r in cur.fetchall():
            print(r[0], bin_to_guid(bytes(r[1])), r[2], r[3], r[4])
            print(" ", str(r[5])[:250])


if __name__ == "__main__":
    main()
