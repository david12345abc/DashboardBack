# -*- coding: utf-8 -*-
from comdir.common import connect, uuid_to_1c_bytes

DEPTS = [
    ("VED", "49480c10-e401-11e8-8283-ac1f6b05524d"),
    ("ETALON", "34497ef7-810f-11e4-80d6-001e67112509"),
    ("BMI", "9edaa7d4-37a5-11ee-93d3-6cb31113810e"),
    ("KEY", "639ec87b-67b6-11eb-8523-ac1f6b05524d"),
    ("ODP", "7587c178-92f6-11f0-96f9-6cb31113810e"),
    ("GAZ", "bd7b5184-9f9c-11e4-80da-001e67112509"),
    ("byt", "4edcf3a0-9f99-11e4-80da-001e67112509"),
    ("prom", "ff740269-d71e-11e6-8127-001e67112509"),
    ("h1", "c6810cc3-cf32-11ef-95e8-6cb31113810e"),
    ("h2", "ebd2d511-cf38-11ef-95e8-6cb31113810e"),
    ("h3", "ad83f8bd-cf39-11ef-95e8-6cb31113810e"),
    ("tender", "1c9f9419-d91b-11e0-8129-cd2988c3db2d"),
]

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
for name, guid in DEPTS:
    b = uuid_to_1c_bytes(guid)
    cur.execute(
        "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
        b,
    )
    desc = cur.fetchone()
    cur.execute(
        "SELECT COUNT(*) FROM _Reference134945 WITH (NOLOCK) WHERE _Fld138169RRef=?",
        b,
    )
    cnt = cur.fetchone()[0]
    print(f"{name:8s} cnt={cnt:5d} desc={desc[0] if desc else '?'} guid={guid}")
cn.close()
