# -*- coding: utf-8 -*-
from comdir.common import connect

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
gaz = bytes.fromhex("80da001e6711250911e49f9cbd7b5184")
cur.execute("SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?", gaz)
print("gaz", cur.fetchone())
cur.execute(
    """
    SELECT TOP 40 o._Fld138169RRef AS dept, COUNT(*) AS cnt
    FROM _Reference134945 o WITH (NOLOCK)
    WHERE o._Fld138169RRef <> 0x00000000000000000000000000000000
    GROUP BY o._Fld138169RRef
    ORDER BY COUNT(*) DESC
    """
)
for dept, cnt in cur.fetchall():
    cur.execute(
        "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
        dept,
    )
    name = cur.fetchone()
    label = name[0] if name else "?"
    print(f"{cnt:6d}  {label}  {dept.hex()}")
cn.close()
