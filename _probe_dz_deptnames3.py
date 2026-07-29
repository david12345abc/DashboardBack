# -*- coding: utf-8 -*-
from comdir.common import connect, uuid_to_1c_hex

def bytes_to_odata(b: bytes) -> str:
    hx = b.hex()
    # reverse of uuid_to_1c_hex: u[16:20]+u[20:32]+u[12:16]+u[8:12]+u[0:8]
    # so hx[0:4]=u[16:20], hx[4:16]=u[20:32], hx[16:20]=u[12:16], hx[20:24]=u[8:12], hx[24:32]=u[0:8]
    u = hx[24:32] + hx[20:24] + hx[16:20] + hx[0:4] + hx[4:16]
    return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:32]}"

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
cur.execute(
    """
    SELECT TOP 50 o._Fld138169RRef AS dept, COUNT(*) AS cnt
    FROM _Reference134945 o WITH (NOLOCK)
    WHERE o._Fld138169RRef <> 0x00000000000000000000000000000000
    GROUP BY o._Fld138169RRef
    ORDER BY COUNT(*) DESC
    """
)
rows = cur.fetchall()
out = []
for dept, cnt in rows:
    cur.execute(
        "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
        dept,
    )
    name = cur.fetchone()
    label = name[0] if name else "?"
    out.append(f"{cnt:6d}\t{label}\t{dept.hex()}\t{bytes_to_odata(dept)}")

text = "\n".join(out)
with open("_dz_dept_names.txt", "w", encoding="utf-8") as f:
    f.write(text)
print("wrote", len(out), "rows")
# print commercial-ish lines
for line in out:
    if any(x in line.lower() for x in ("отдел", "ликв", "холдинг", "дилер", "бми", "вэд", "тендер", "ключев", "эталон", "газпром")):
        print(line)
cn.close()
