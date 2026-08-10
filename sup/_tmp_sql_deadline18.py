"""Find rows with 18.05.2026 deadline and VK assignment dates from report."""
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
    doc_bin = None
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
        print("doc", doc_bin.hex())

        # Scan Task39X1 for subject RRRef = doc
        cur.execute(
            """
            SELECT c.name FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID('_Task39X1')
              AND ty.name IN ('binary','varbinary') AND c.max_length=16
            """
        )
        bin16 = [r[0] for r in cur.fetchall()]
        for bcol in bin16:
            cur.execute(
                f"SELECT COUNT(*) FROM [_Task39X1] WITH (NOLOCK) WHERE [{bcol}]=?",
                doc_bin,
            )
            cnt = cur.fetchone()[0]
            if cnt:
                print(f"Task39X1 DOC via {bcol}: {cnt}")
                cur.execute(
                    f"""
                    SELECT TOP 5 _Date_Time, _Name, _Fld2509, _Fld2511, _Fld2506, _Fld2507, _Fld2508,
                           _Fld2518_RRRef, _Executed
                    FROM [_Task39X1] WITH (NOLOCK) WHERE [{bcol}]=?
                    ORDER BY _Date_Time DESC
                    """,
                    doc_bin,
                )
                for r in cur.fetchall():
                    print(" ", r)

        # Search nvarchar for exact phrase from report
        phrases = (
            "Перенос срока по задаче",
            "Старый срок",
            "Новый срок",
            "Подготовить ответ",
        )
        cur.execute(
            """
            SELECT t.name, c.name
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE ty.name IN ('nvarchar')
              AND (
                t.name LIKE '_Task%'
                OR t.name LIKE '_BPr%'
                OR t.name LIKE '_InfoRg%'
                OR t.name LIKE '_Document%'
              )
              AND t.name NOT LIKE '%ChngR%'
            """
        )
        cols = cur.fetchall()
        print("text cols", len(cols))
        for phrase in phrases:
            print(f"\nsearch {phrase!r}")
            hits = 0
            for tname, cname in cols:
                try:
                    cur.execute(
                        f"""
                        SELECT TOP 1 [{cname}], _Date_Time
                        FROM [{tname}] WITH (NOLOCK)
                        WHERE [{cname}] LIKE ?
                        """,
                        f"%{phrase}%",
                    )
                except Exception:
                    # no _Date_Time
                    try:
                        cur.execute(
                            f"""
                            SELECT TOP 1 [{cname}]
                            FROM [{tname}] WITH (NOLOCK)
                            WHERE [{cname}] LIKE ?
                            """,
                            f"%{phrase}%",
                        )
                    except Exception:
                        continue
                row = cur.fetchone()
                if row:
                    print(f"  HIT {tname}.{cname}: {str(row[0])[:180]}")
                    hits += 1
                    if hits >= 8:
                        break

        # Exact datetime 4026-05-04 21:09 (assignment) near user - search Task dates
        print("\n=== tasks around 2026-05-04 21:09 ===")
        cur.execute(
            """
            SELECT TOP 20 _Date_Time, _Name, _Fld2509, _Fld2511, _Fld2506, _Fld2508,
                   _Fld2518_RRRef, _Executed
            FROM [_Task39X1] WITH (NOLOCK)
            WHERE _Date_Time BETWEEN '4026-05-04 21:00:00' AND '4026-05-04 21:20:00'
            ORDER BY _Date_Time
            """
        )
        for r in cur.fetchall():
            print(r[0], (r[1] or "")[:60], (r[3] or "")[:80], bytes(r[6]).hex()[:20] if r[6] else None)

        # Also 04.05.2026 10:58
        cur.execute(
            """
            SELECT TOP 20 _Date_Time, _Name, _Fld2511, _Fld2518_RRRef
            FROM [_Task39X1] WITH (NOLOCK)
            WHERE _Date_Time BETWEEN '4026-05-04 10:50:00' AND '4026-05-04 11:10:00'
            ORDER BY _Date_Time
            """
        )
        print("\n=== around 10:58 ===")
        for r in cur.fetchall():
            print(r[0], (r[1] or "")[:60], (r[2] or "")[:80], bytes(r[3]).hex()[:24] if r[3] else None)

        # Deadline field = 4026-05-18 for any task
        print("\n=== deadline 18.05.2026 ===")
        for dcol in ("_Fld2506", "_Fld2508", "_Fld2515", "_Fld166379"):
            cur.execute(
                f"""
                SELECT COUNT(*) FROM [_Task39X1] WITH (NOLOCK)
                WHERE CAST([{dcol}] AS date) = '4026-05-18'
                """
            )
            print(dcol, cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT TOP 10 _Date_Time, _Name, _Fld2511, [{dcol}], _Fld2518_RRRef
                FROM [_Task39X1] WITH (NOLOCK)
                WHERE CAST([{dcol}] AS date) = '4026-05-18'
                ORDER BY _Date_Time
                """
            )
            for r in cur.fetchall():
                print(" ", r[0], (r[1] or "")[:50], (r[2] or "")[:70], r[3], bytes(r[4]).hex()[:20] if r[4] else None)


if __name__ == "__main__":
    main()
