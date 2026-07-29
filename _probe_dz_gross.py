# -*- coding: utf-8 -*-
from datetime import date
from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T="_AccumRg107662"; OBJ="_Fld140445RRef"; DOLG="_Fld107672"; PRED="_Fld107669"
TARGET=373_930_180.89
DEPTS=[
"49480c10-e401-11e8-8283-ac1f6b05524d","34497ef7-810f-11e4-80d6-001e67112509",
"9edaa7d4-37a5-11ee-93d3-6cb31113810e","639ec87b-67b6-11eb-8523-ac1f6b05524d",
"7587c178-92f6-11f0-96f9-6cb31113810e","bd7b5184-9f9c-11e4-80da-001e67112509",
"4edcf3a0-9f99-11e4-80da-001e67112509","ff740269-d71e-11e6-8127-001e67112509",
"c6810cc3-cf32-11ef-95e8-6cb31113810e","ebd2d511-cf38-11ef-95e8-6cb31113810e",
"ad83f8bd-cf39-11ef-95e8-6cb31113810e",
]
cn=connect(); cur=cn.cursor(); cur.execute("SET NOCOUNT ON")
p_end=to_1c_dt(date(2026,7,1))
cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
for g in DEPTS: cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

# sum max(dolg,0) + max(pred,0) as 'gross exposure'
cur.execute(f'''
SELECT s.[{OBJ}],
 SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dolg,
 SUM(CASE WHEN s._RecordKind=1 THEN -s.[{PRED}] ELSE s.[{PRED}] END) AS pred
FROM [{T}] s WITH (NOLOCK)
INNER JOIN _Reference134945 o ON o._IDRRef=s.[{OBJ}]
INNER JOIN #d d ON d.id=o._Fld138169RRef
WHERE s._Period<? AND s._Active=0x01
GROUP BY s.[{OBJ}]
''', p_end)
gross=0; dolg_only=0; both=0
for obj,dolg,pred in cur.fetchall():
  dolg=float(dolg or 0); pred=float(pred or 0)
  if dolg>0: dolg_only+=dolg
  if dolg>0: both+=dolg
  if pred>0: both+=pred
print(f"dolg_pos {dolg_only:,.2f}")
print(f"dolg_pos+pred_pos {both:,.2f} d={both-TARGET:,.2f}")

# Include tender
cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes("1c9f9419-d91b-11e0-8129-cd2988c3db2d"))
cur.execute(f'''
SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
FROM [{T}] s WITH (NOLOCK)
INNER JOIN _Reference134945 o ON o._IDRRef=s.[{OBJ}]
INNER JOIN #d d ON d.id=o._Fld138169RRef
WHERE s._Period<? AND s._Active=0x01
''', p_end)
print(f"with tender {float(cur.fetchone()[0] or 0):,.2f}")

# Include empty dept FULL
cur.execute(f'''
SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
FROM [{T}] s WITH (NOLOCK)
INNER JOIN _Reference134945 o ON o._IDRRef=s.[{OBJ}]
WHERE s._Period<? AND s._Active=0x01
  AND (
    o._Fld138169RRef IN (SELECT id FROM #d)
    OR o._Fld138169RRef = 0x00000000000000000000000000000000
  )
''', p_end)
print(f"comm+liq+tender+empty {float(cur.fetchone()[0] or 0):,.2f}")
cn.close()
