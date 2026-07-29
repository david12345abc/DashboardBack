# -*- coding: utf-8 -*-
from comdir.common import connect, uuid_to_1c_bytes

REC = "f8f20bf7-8a88-11f1-9850-6cb31113810e"
# Also try ДокументРегистратор (Реализация)
SHIP = "d3242d9c-8a88-11f1-9850-6cb31113810e"

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
rb = uuid_to_1c_bytes(REC)
sb = uuid_to_1c_bytes(SHIP)

# Documents with numbers near 54xxx / 53xxx
cur.execute(
    """
    SELECT t.name FROM sys.tables t
    WHERE t.name LIKE '_Document53%'
       OR t.name LIKE '_Document54%'
       OR t.name LIKE '_Document55%'
    ORDER BY t.name
    """
)
docs = [r[0] for r in cur.fetchall() if "VT" not in r[0] and "ChngR" not in r[0]]
print("doc candidates", len(docs))
for t in docs:
    cur.execute(
        "SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID(?) AND name='_IDRRef'",
        t,
    )
    if not cur.fetchone():
        continue
    cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?", rb)
    if cur.fetchone():
        print("RECORDER DOC", t)
    cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _IDRRef=?", sb)
    if cur.fetchone():
        print("SHIP DOC", t)

# Also search AccumRg* for SHIP as recorder (realization often posts directly)
print("search ship as recorder in AccumRg53/54...")
cur.execute(
    """
    SELECT t.name FROM sys.tables t
    WHERE (t.name LIKE '_AccumRg53%' OR t.name LIKE '_AccumRg54%')
      AND t.name NOT LIKE '%VT%'
      AND EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id=t.object_id AND c.name='_RecorderRRef')
    """
)
for (t,) in cur.fetchall():
    cur.execute(
        f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _RecorderRRef=?",
        sb,
    )
    if cur.fetchone():
        print("SHIP RECORDER IN", t)
    cur.execute(
        f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _RecorderRRef=?",
        rb,
    )
    if cur.fetchone():
        print("REG RECORDER IN", t)

cn.close()
print("done")
