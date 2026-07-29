# -*- coding: utf-8 -*-
from comdir.common import connect

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
cur.execute(
    """
    SELECT _IDRRef, _Description
    FROM _Reference513 WITH (NOLOCK)
    WHERE _Description LIKE N'%холдинг%'
       OR _Description LIKE N'%Ключев%'
       OR _Description LIKE N'%БМИ%'
       OR _Description LIKE N'%тендер%'
       OR _Description LIKE N'%Коммерч%'
       OR _Description LIKE N'%продаж%'
    ORDER BY _Description
    """
)
rows = cur.fetchall()
open("_dz_sales_depts.txt", "w", encoding="utf-8").write(
    "\n".join(f"{r[1]}\t{r[0].hex()}" for r in rows)
)
print("wrote", len(rows))
cn.close()
