# -*- coding: utf-8 -*-
"""Map RTRef 000003b6 / 0000028f to document tables; test actual-only overdue."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
PLAN = "_Fld107667"
DOC_TYPE = "_Fld107666_RTRef"
TARGET_OD = 142_773_571.76
TARGET_DZ = 323_930_180.89

DEPTS = [
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
    "4edcf3a0-9f99-11e4-80da-001e67112509",
    "ff740269-d71e-11e6-8127-001e67112509",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e",
]


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # Find document tables whose _IDRRef appears with these RTRefs
    for label, rt in [("3b6", bytes.fromhex("000003b6")), ("28f", bytes.fromhex("0000028f"))]:
        cur.execute(
            f"""
            SELECT TOP 1 s._Fld107666_RRRef
            FROM [{T}] s WITH (NOLOCK)
            WHERE s.[{DOC_TYPE}] = ? AND s._Fld107666_RRRef <> 0x00000000000000000000000000000000
            """,
            rt,
        )
        sample = cur.fetchone()
        if not sample:
            print(label, "no sample")
            continue
        ref = sample[0]
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '_Document%'
              AND t.name NOT LIKE '%VT%'
              AND EXISTS (
                SELECT 1 FROM sys.columns c
                WHERE c.object_id=t.object_id AND c.name='_IDRRef'
              )
            """
        )
        found = None
        for (tname,) in cur.fetchall():
            cur.execute(
                f"SELECT TOP 1 1 FROM [{tname}] WITH (NOLOCK) WHERE _IDRRef=?",
                ref,
            )
            if cur.fetchone():
                found = tname
                break
        print(label, "doc table", found, "sample", ref.hex())

    p_end = to_1c_dt(date(2026, 7, 1))
    na = to_1c_dt(date(2026, 6, 30))
    cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
    cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY)")
    for g in DEPTS:
        cur.execute("INSERT INTO #d VALUES (?)", uuid_to_1c_bytes(g))

    # Only type 3b6 (putative actual)
    rt_actual = bytes.fromhex("000003b6")
    cur.execute(
        f"""
        SELECT
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dz,
          SUM(CASE WHEN s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
                   THEN CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END
                   ELSE 0 END) AS od
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01 AND s.[{DOC_TYPE}] = ?
        """,
        (na, p_end, rt_actual),
    )
    dz, od = cur.fetchone()
    print(f"only 3b6: dz={float(dz or 0):,.2f} dDZ={float(dz or 0)-TARGET_DZ:,.2f}")
    print(f"         od={float(od or 0):,.2f} dOD={float(od or 0)-TARGET_OD:,.2f}")

    # Exclude type 28f only
    rt_order = bytes.fromhex("0000028f")
    cur.execute(
        f"""
        SELECT
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS dz,
          SUM(CASE WHEN s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
                   THEN CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END
                   ELSE 0 END) AS od
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01 AND s.[{DOC_TYPE}] <> ?
        """,
        (na, p_end, rt_order),
    )
    dz, od = cur.fetchone()
    print(f"excl 28f: dz={float(dz or 0):,.2f} dDZ={float(dz or 0)-TARGET_DZ:,.2f}")
    print(f"         od={float(od or 0):,.2f} dOD={float(od or 0)-TARGET_OD:,.2f}")

    # Find object-level overdue contributions that sum to ~22M
    cur.execute(
        f"""
        SELECT TOP 30
          s.[{OBJ}],
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS od
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        INNER JOIN #d d ON d.id = o._Fld138169RRef
        WHERE s._Period < ? AND s._Active = 0x01
          AND s.[{PLAN}] < ? AND s.[{PLAN}] > '20000101'
        GROUP BY s.[{OBJ}]
        HAVING SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) > 100000
        ORDER BY 2 DESC
        """,
        (p_end, na),
    )
    lines = []
    for obj, od in cur.fetchall():
        cur.execute(
            "SELECT _Description FROM _Reference134945 WITH (NOLOCK) WHERE _IDRRef=?",
            obj,
        )
        name = cur.fetchone()
        lines.append(f"{float(od):15,.2f}\t{name[0] if name else '?'}")
    open("_dz_top_overdue_objs.txt", "w", encoding="utf-8").write("\n".join(lines))
    print("wrote top overdue objs")

    cn.close()


if __name__ == "__main__":
    main()
