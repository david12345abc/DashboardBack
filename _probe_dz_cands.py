# -*- coding: utf-8 -*-
"""Identify ПоСрокам among candidate AccumRg near 53885."""
from datetime import date
from comdir.common import connect, to_1c_dt

CANDS = [
    "_AccumRg53439",
    "_AccumRg53835",
    "_AccumRg53936",
    "_AccumRg54090",
    "_AccumRg54652",
    "_AccumRg52356",
]

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
p_end = to_1c_dt(date(2026, 7, 1))  # exclusive end after 30.06

for t in CANDS:
    cur.execute(
        f"""
        SELECT c.name, ty.name, c.max_length
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id=c.user_type_id
        WHERE c.object_id=OBJECT_ID('{t}')
        ORDER BY c.column_id
        """
    )
    cols = cur.fetchall()
    print(f"\n=== {t} ({len(cols)} cols) ===")
    for n, ty, ml in cols:
        print(f"  {n:30} {ty}({ml})")

    # row count with period < july
    try:
        cur.execute(f"SELECT COUNT_BIG(*) FROM {t} WITH (NOLOCK) WHERE _Period < ?", p_end)
        print("  rows < jul2026", cur.fetchone()[0])
    except Exception as e:
        print("  count err", e)

cn.close()
