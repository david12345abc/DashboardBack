# -*- coding: utf-8 -*-
from datetime import date

from comdir.common import connect, to_1c_dt

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
p_end = to_1c_dt(date(2026, 7, 1))
cur.execute(
    f"""
    SELECT TOP 20 s.[{OBJ}] AS obj,
      SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
    FROM [{T}] s WITH (NOLOCK)
    INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
    WHERE s._Period < ? AND s._Active = 0x01
      AND o._Fld138169RRef = 0x00000000000000000000000000000000
    GROUP BY s.[{OBJ}]
    ORDER BY SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) DESC
    """,
    p_end,
)
lines = []
for obj, amt in cur.fetchall():
    cur.execute(
        "SELECT _Description FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?",
        obj,
    )
    r = cur.fetchone()
    lines.append(f"{float(amt):15,.2f}\t{r[0] if r else '?'}\t{obj.hex()}")
text = "\n".join(lines)
open("_dz_empty_dept_objs.txt", "w", encoding="utf-8").write(text)
print(text)
cn.close()
