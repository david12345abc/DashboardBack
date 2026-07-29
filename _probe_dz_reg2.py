# -*- coding: utf-8 -*-
from comdir.common import connect

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
cur.execute(
    """
    SELECT t.name,
      SUM(CASE WHEN ty.name LIKE 'datetime%' THEN 1 ELSE 0 END) AS ndt,
      SUM(CASE WHEN ty.name IN ('numeric','decimal') THEN 1 ELSE 0 END) AS nnum
    FROM sys.tables t
    JOIN sys.columns c ON c.object_id=t.object_id
    JOIN sys.types ty ON ty.user_type_id=c.user_type_id
    WHERE t.name LIKE '_AccumRg%'
      AND t.name NOT LIKE '%[_]VT%'
      AND t.name NOT LIKE '%ChngR%'
    GROUP BY t.name
    HAVING SUM(CASE WHEN ty.name LIKE 'datetime%' THEN 1 ELSE 0 END) >= 3
       AND SUM(CASE WHEN ty.name IN ('numeric','decimal') THEN 1 ELSE 0 END) >= 6
    ORDER BY t.name
    """
)
for r in cur.fetchall():
    print(r[0], "dt", r[1], "num", r[2])
cn.close()
