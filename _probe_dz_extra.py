# -*- coding: utf-8 -*-
"""Sum debt for extra commercial-related departments."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
TARGET = 373_930_180.89
TARGET_OD = 142_773_571.76

EXTRA = [
    ("КОММЕРЧЕСКИЙ ДИРЕКТОР", "afce001e6711250911e26eb14668a582"),
    ("Амурская легенда", "8288ac1f6b05524d11e92dc11184435b"),
    ("Сектор сопровождения продаж", "977e6cb31113810e11f0f049ec9b2bac"),
    ("Сектор сопр. пр-ва и продаж", "977e6cb31113810e11f0f04b4772c6d2"),
    ("Зам. комдира развитие", "94756cb31113810e11eeb43b98812712"),
    ("ликв холдинги НОВАТЭК", "8878ac1f6b05524d11ecbaf50492966f"),
    ("ликв развитие продаж", "826fac1f6b05524d11e812ef68bda2ff"),
    ("ликв сопровод продаж", "8267ac1f6b05524d11e7cb6a8a8e6050"),
    ("ликв директор ключ клиенты", "953b6cb31113810e11ef48207f832c31"),
    ("ликв зам комдира", "846fac1f6b05524d11eb2034dbc72a8b"),
]

BASE = [
    "80da001e6711250911e49f9cbd7b5184",  # gaz
    "93d36cb31113810e11ee37a59edaa7d4",  # bmi
    "8283ac1f6b05524d11e8e40149480c10",  # ved
    "8523ac1f6b05524d11eb67b6639ec87b",  # key
    "96f96cb31113810e11f092f67587c178",  # odp
    "80d6001e6711250911e4810f34497ef7",  # etalon
    "80da001e6711250911e49f994edcf3a0",  # byt
    "8127001e6711250911e6d71eff740269",  # prom
    "95e86cb31113810e11efcf32c6810cc3",  # h1
    "95e86cb31113810e11efcf38ebd2d511",  # h2
    "95e86cb31113810e11efcf39ad83f8bd",  # h3
]

cn = connect()
cur = cn.cursor()
cur.execute("SET NOCOUNT ON")
p_end = to_1c_dt(date(2026, 7, 1))
na = to_1c_dt(date(2026, 6, 30))


def sum_depts(hexes):
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for hx in hexes:
        cur.execute("INSERT INTO #d VALUES (?)", bytes.fromhex(hx))
    cur.execute(
        f"""
        SELECT
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END),
          SUM(CASE WHEN s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
                   THEN CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END
                   ELSE 0 END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
        """,
        (na, p_end),
    )
    return cur.fetchone()


for name, hx in EXTRA:
    dz, od = sum_depts([hx])
    print(f"{float(dz or 0):15,.2f}  od={float(od or 0):15,.2f}  {name}")

base_dz, base_od = sum_depts(BASE)
print(f"\nBASE dz={float(base_dz):,.2f} od={float(base_od):,.2f}")

# BASE + each extra
for name, hx in EXTRA:
    dz, od = sum_depts(BASE + [hx])
    print(f"BASE+{name}: dz={float(dz):,.2f} d={float(dz)-TARGET:,.2f}  od={float(od):,.2f} d={float(od)-TARGET_OD:,.2f}")

# BASE + all extras with nonzero
nonzero = [hx for name, hx in EXTRA]
dz, od = sum_depts(BASE + nonzero)
print(f"\nBASE+all extras: dz={float(dz):,.2f} d={float(dz)-TARGET:,.2f} od={float(od):,.2f} d={float(od)-TARGET_OD:,.2f}")

cn.close()
