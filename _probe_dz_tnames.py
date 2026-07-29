# -*- coding: utf-8 -*-
"""Find AccumRg*T totals tables near 538xx with ObjCalc-like dims."""
from comdir.common import connect

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
cur.execute(
    """
    SELECT t.name FROM sys.tables t
    WHERE t.name LIKE '_AccumRg53%'
       OR t.name LIKE '_AccumRg54%'
    ORDER BY t.name
    """
)
for (name,) in cur.fetchall():
    if "53" in name or "54" in name:
        print(name)
cn.close()
