"""Dump Voronchikhina May 2026 tasks from _Task39 / _Task39X1."""
from __future__ import annotations

import functools
import sys
import uuid
from datetime import datetime

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def guid_to_1c_a(guid_str: str) -> bytes:
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


def bin_to_guid(b: bytes) -> str:
    """Inverse of guid_to_1c_a."""
    if not b or len(b) != 16:
        return ""
    # a: b[8:16]+b[6:8]+b[4:6]+b[0:4]  => reverse
    # stored = orig[8:16] + orig[6:8] + orig[4:6] + orig[0:4]
    orig = b[12:16] + b[10:12] + b[8:10] + b[0:8]
    try:
        return str(uuid.UUID(bytes=orig))
    except Exception:
        return b.hex()


def main() -> None:
    user_guid = "a312e193-d7b7-11ee-94a5-6cb31113810e"
    user_bin = guid_to_1c_a(user_guid)
    print("user", user_guid, user_bin.hex())

    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()

        for tname in ("_Task39", "_Task39X1"):
            # find executor-like columns that match user in 2026
            cur.execute(
                """
                SELECT c.name
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                  AND ty.name IN ('binary','varbinary')
                  AND c.max_length = 16
                ORDER BY c.column_id
                """,
                tname,
            )
            bin16 = [r[0] for r in cur.fetchall()]

            hit_cols = []
            for col in bin16:
                try:
                    cur.execute(
                        f"""
                        SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                        WHERE [{col}] = ?
                          AND YEAR(_Date_Time) = 4026
                        """,
                        user_bin,
                    )
                    cnt = cur.fetchone()[0]
                    if cnt:
                        hit_cols.append((col, cnt))
                except Exception:
                    pass
            print(f"\n=== {tname} user-hit cols in 2026 ===")
            for col, cnt in hit_cols:
                print(f"  {col}: {cnt}")

            # Prefer Fld2518 if present else first hit
            exec_col = None
            for col, _ in hit_cols:
                if "2518" in col:
                    exec_col = col
                    break
            if not exec_col and hit_cols:
                exec_col = hit_cols[0][0]
            if not exec_col:
                print("no executor col")
                continue

            # All columns for SELECT
            cur.execute(
                """
                SELECT c.name, ty.name
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE c.object_id = OBJECT_ID(?)
                ORDER BY c.column_id
                """,
                tname,
            )
            cols = [(r[0], r[1]) for r in cur.fetchall()]
            date_flds = [c for c, t in cols if t.startswith("datetime")]
            text_flds = [c for c, t in cols if t in ("nvarchar", "nchar", "varchar")]
            print("date fields:", date_flds)
            print("text fields:", text_flds)

            # May by Date_Time (постановка?)
            select_cols = [
                "_IDRRef", "_Date_Time", "_Number", "_Name", "_Executed", "_Marked",
            ] + date_flds + text_flds[:8]
            # unique preserve
            seen = set()
            sel = []
            for c in select_cols:
                if c not in seen:
                    seen.add(c)
                    sel.append(c)
            sel_sql = ", ".join(f"[{c}]" for c in sel)

            cur.execute(
                f"""
                SELECT {sel_sql}
                FROM [{tname}] WITH (NOLOCK)
                WHERE [{exec_col}] = ?
                  AND YEAR(_Date_Time) = 4026
                  AND MONTH(_Date_Time) = 5
                ORDER BY _Date_Time
                """,
                user_bin,
            )
            rows = cur.fetchall()
            print(f"\n--- {tname} by Date_Time May ({len(rows)}) via {exec_col} ---")
            for row in rows:
                d = dict(zip(sel, row))
                print("---")
                print("id", bin_to_guid(bytes(d["_IDRRef"])) if d.get("_IDRRef") else None)
                print("date", d.get("_Date_Time"), "num", (d.get("_Number") or "").strip(), "exec", d.get("_Executed"))
                print("name", d.get("_Name"))
                for df in date_flds:
                    if df == "_Date_Time":
                        continue
                    v = d.get(df)
                    if v and str(v)[:4] not in ("0001", "1753", "2001"):
                        # 1C empty often year 0001 or 2001-01-01 after shift? actual is +2000 so empty ~0001 -> 2001?
                        y = getattr(v, "year", None)
                        if y and y > 2005:
                            print(f"  {df}={v}")
                for tf in text_flds[:8]:
                    v = d.get(tf)
                    if v and str(v).strip():
                        print(f"  {tf}={(str(v)[:200])}")

            # Also: any task where ANY date field is May 2026 and executor=user
            # Build OR of date fields in May
            or_parts = []
            for df in date_flds:
                or_parts.append(f"(YEAR([{df}])=4026 AND MONTH([{df}])=5)")
            or_sql = " OR ".join(or_parts)
            cur.execute(
                f"""
                SELECT {sel_sql}
                FROM [{tname}] WITH (NOLOCK)
                WHERE [{exec_col}] = ?
                  AND ({or_sql})
                ORDER BY _Date_Time
                """,
                user_bin,
            )
            rows2 = cur.fetchall()
            print(f"\n--- {tname} ANY date in May ({len(rows2)}) ---")
            for row in rows2:
                d = dict(zip(sel, row))
                print("---")
                print("date", d.get("_Date_Time"), "name", d.get("_Name"))
                for df in date_flds:
                    v = d.get(df)
                    y = getattr(v, "year", None) if v else None
                    if y and y == 4026 and getattr(v, "month", None) == 5:
                        print(f"  MAY {df}={v}")

            # Search name/subject containing НП00-002870
            for tf in text_flds:
                try:
                    cur.execute(
                        f"""
                        SELECT TOP 5 _Date_Time, _Name, [{tf}]
                        FROM [{tname}] WITH (NOLOCK)
                        WHERE [{tf}] LIKE N'%НП00-002870%'
                        """,
                    )
                    hits = cur.fetchall()
                    if hits:
                        print(f"\nTEXT HIT {tname}.{tf}: {len(hits)}")
                        for h in hits:
                            print(" ", h[0], h[1], str(h[2])[:120])
                except Exception:
                    pass


if __name__ == "__main__":
    main()
