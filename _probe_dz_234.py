# -*- coding: utf-8 -*-
from datetime import date
from comdir.common import connect, to_1c_dt

T="_AccumRg107662"; OBJ="_Fld140445RRef"; DOLG="_Fld107672"
NEED=23_424_399.47
TARGET=373_930_180.89

cn=connect(); cur=cn.cursor(); cur.execute("SET NOCOUNT ON")
p_end=to_1c_dt(date(2026,7,1))

# objects near 23.42M
cur.execute(f'''
SELECT TOP 15 s.[{OBJ}],
 SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
FROM [{T}] s WITH (NOLOCK)
WHERE s._Period<? AND s._Active=0x01
GROUP BY s.[{OBJ}]
HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?) < 200000
   OR ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - 23400000) < 200000
ORDER BY ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?)
''', (p_end, NEED, NEED))
print("objects near 23.42M:")
for obj,amt in cur.fetchall():
  cur.execute("SELECT _Description, _Fld138169RRef FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?", obj)
  r=cur.fetchone()
  dept="?"
  if r and r[1]!=bytes(16):
    cur.execute("SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?", r[1])
    d=cur.fetchone(); dept=d[0] if d else "?"
  elif r and r[1]==bytes(16):
    dept="(empty)"
  print(f"  {float(amt):15,.2f}  {(r[0] if r else '?')[:65]}  [{dept}]")

# depts near 23.42M
cur.execute(f'''
SELECT TOP 15 o._Fld138169RRef,
 SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
FROM [{T}] s WITH (NOLOCK)
INNER JOIN _Reference134945 o ON o._IDRRef=s.[{OBJ}]
WHERE s._Period<? AND s._Active=0x01
GROUP BY o._Fld138169RRef
HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?) < 500000
ORDER BY ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) - ?)
''', (p_end, NEED, NEED))
print("\ndepts near 23.42M:")
for dept,amt in cur.fetchall():
  cur.execute("SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?", dept)
  d=cur.fetchone()
  print(f"  {float(amt):15,.2f}  {d[0] if d else '(empty/unknown)'}  {dept.hex()}")

# BASE (comm+liq hex) + tender + empty second object?
# Try BASE+tender+specific empty objs
from comdir.common import uuid_to_1c_bytes
BASE_GUIDS=[
"49480c10-e401-11e8-8283-ac1f6b05524d","34497ef7-810f-11e4-80d6-001e67112509",
"9edaa7d4-37a5-11ee-93d3-6cb31113810e","639ec87b-67b6-11eb-8523-ac1f6b05524d",
"7587c178-92f6-11f0-96f9-6cb31113810e","bd7b5184-9f9c-11e4-80da-001e67112509",
"4edcf3a0-9f99-11e4-80da-001e67112509","ff740269-d71e-11e6-8127-001e67112509",
"c6810cc3-cf32-11ef-95e8-6cb31113810e","ebd2d511-cf38-11ef-95e8-6cb31113810e",
"ad83f8bd-cf39-11ef-95e8-6cb31113810e","1c9f9419-d91b-11e0-8129-cd2988c3db2d",
]
cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
for g in BASE_GUIDS:
  cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

# get empty objs sorted
cur.execute(f'''
SELECT s.[{OBJ}],
 SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt
FROM [{T}] s WITH (NOLOCK)
INNER JOIN _Reference134945 o ON o._IDRRef=s.[{OBJ}]
WHERE s._Period<? AND s._Active=0x01 AND o._Fld138169RRef=0x00000000000000000000000000000000
GROUP BY s.[{OBJ}]
HAVING SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) > 1000
ORDER BY 2 DESC
''', p_end)
empty=cur.fetchall()
base=350_505_781.42
# greedy: add empty objs until we hit target
running=base
chosen=[]
for obj,amt in empty:
  amt=float(amt)
  if running + amt <= TARGET + 1:
    running += amt
    chosen.append((amt, obj))
  if abs(running - TARGET) < 1:
    break
print(f"\ngreedy empty add: {running:,.2f} d={running-TARGET:,.2f} n={len(chosen)}")
# also try: find subset of empty that sums to NEED
print(f"need from empty: {NEED:,.2f}")
# check if any single empty is close after adjusting
for obj,amt in empty[:10]:
  print(f"  empty {float(amt):15,.2f}  base+this={base+float(amt):,.2f} d={base+float(amt)-TARGET:,.2f}")

cn.close()
