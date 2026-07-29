# -*- coding: utf-8 -*-
from comdir.common import connect

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
cur.execute("SELECT name FROM sys.tables WHERE name LIKE '%107662%' ORDER BY name")
print([r[0] for r in cur.fetchall()])
cur.execute("SELECT name FROM sys.tables WHERE name LIKE '%107629%' ORDER BY name")
print("107629", [r[0] for r in cur.fetchall()])
cn.close()
