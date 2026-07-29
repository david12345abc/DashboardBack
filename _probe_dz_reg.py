# -*- coding: utf-8 -*-
"""Find AccumRg for РасчетыСКлиентамиПоСрокам and compare to 1C June totals."""
from __future__ import annotations

from datetime import date, datetime

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

# 1C report targets 30.06.2026
TARGET_DZ = 373_930_180.89
TARGET_OD = 142_773_571.76

COMMERCIAL = {
    "VED": "49480c10-e401-11e8-8283-ac1f6b05524d",
    "ETALON": "34497ef7-810f-11e4-80d6-001e67112509",
    "BMI": "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "KEY": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "ODP": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "GAZ": "bd7b5184-9f9c-11e4-80da-001e67112509",
}
LIQ = {
    "byt": "4edcf3a0-9f99-11e4-80da-001e67112509",
    "prom": "ff740269-d71e-11e6-8127-001e67112509",
    "h1": "c6810cc3-cf32-11ef-95e8-6cb31113810e",
    "h2": "ebd2d511-cf38-11ef-95e8-6cb31113810e",
    "h3": "ad83f8bd-cf39-11ef-95e8-6cb31113810e",
}


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # Find tables with many numeric debt-like columns + Period + Active
    cur.execute(
        """
        SELECT t.name, COUNT(*) AS ncols
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id = t.object_id
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%_VT%'
        GROUP BY t.name
        HAVING COUNT(*) BETWEEN 15 AND 40
        ORDER BY t.name
        """
    )
    cands = [r[0] for r in cur.fetchall()]
    print("accum candidates", len(cands))

    # Look for a table that has a date field for planned repayment + debt amount
    # Heuristic: has _Period, _Active, at least 2 datetime fields, decimal fields
    hits = []
    for t in cands:
        cur.execute(
            """
            SELECT c.name, ty.name, c.max_length
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
            ORDER BY c.column_id
            """,
            t,
        )
        cols = cur.fetchall()
        names = [c[0] for c in cols]
        types = {c[0]: c[1] for c in cols}
        if "_Period" not in names or "_Active" not in names:
            continue
        dt_cols = [n for n, ty in types.items() if ty.startswith("datetime")]
        num_cols = [n for n, ty in types.items() if ty in ("numeric", "decimal")]
        if len(dt_cols) >= 3 and len(num_cols) >= 4:
            hits.append((t, dt_cols, num_cols[:8]))
    print("hits with Period+Active+3dates+4nums:")
    for t, dts, nums in hits:
        print(" ", t, "dates", dts, "nums", nums)

    cn.close()


if __name__ == "__main__":
    main()
