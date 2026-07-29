# -*- coding: utf-8 -*-
"""Identify unknown 120M dept and what makes +50M to hit 373.93."""
from __future__ import annotations

from datetime import date

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

T = "_AccumRg107662"
OBJ = "_Fld140445RRef"
DOLG = "_Fld107672"
TARGET = 373_930_180.89

COMMERCIAL = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
}
LIQ = {
    "4edcf3a0-9f99-11e4-80da-001e67112509",
    "ff740269-d71e-11e6-8127-001e67112509",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e",
}


def b2u(b: bytes) -> str:
    hx = b.hex()
    u = hx[24:32] + hx[20:24] + hx[16:20] + hx[0:4] + hx[4:16]
    return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:32]}"


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    p_end = to_1c_dt(date(2026, 7, 1))

    cur.execute(
        f"""
        SELECT
          o._Fld138169RRef AS dept,
          SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) AS amt,
          COUNT(DISTINCT s.[{OBJ}]) AS nobj
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        WHERE s._Period < ? AND s._Active = 0x01
        GROUP BY o._Fld138169RRef
        HAVING ABS(SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)) > 1000000
        ORDER BY SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END) DESC
        """,
        p_end,
    )
    rows = cur.fetchall()
    with open("_dz_depts_debt.txt", "w", encoding="utf-8") as f:
        total_comm = 0.0
        total_liq = 0.0
        total_other = 0.0
        for dept, amt, nobj in rows:
            amt = float(amt)
            cur.execute(
                "SELECT _Description, _Marked FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef=?",
                dept,
            )
            r = cur.fetchone()
            name = r[0] if r else "NOT_IN_513"
            marked = r[1].hex() if r and r[1] is not None else ""
            guid = b2u(dept)
            kind = "OTHER"
            if guid in COMMERCIAL:
                kind = "COMM"
                total_comm += amt
            elif guid in LIQ:
                kind = "LIQ"
                total_liq += amt
            else:
                total_other += max(amt, 0)
            line = f"{amt:15,.2f}\t{nobj:5d}\t{kind}\t{guid}\t{name}\tmarked={marked}\thex={dept.hex()}"
            f.write(line + "\n")
            print(line)
        print(f"\nCOMM {total_comm:,.2f}")
        print(f"LIQ  {total_liq:,.2f}")
        print(f"COMM+LIQ {total_comm+total_liq:,.2f}  need {TARGET:,.2f}  gap={TARGET-(total_comm+total_liq):,.2f}")
        print(f"OTHER positive {total_other:,.2f}")

    # Empty dept?
    empty = bytes(16)
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ}]
        WHERE s._Period < ? AND s._Active = 0x01 AND o._Fld138169RRef = ?
        """,
        (p_end, empty),
    )
    print("empty dept", float(cur.fetchone()[0] or 0))

    # Objects without join to ObjCalc
    cur.execute(
        f"""
        SELECT SUM(CASE WHEN s._RecordKind=1 THEN -s.[{DOLG}] ELSE s.[{DOLG}] END)
        FROM [{T}] s WITH (NOLOCK)
        WHERE s._Period < ? AND s._Active = 0x01
          AND NOT EXISTS (
            SELECT 1 FROM _Reference134945 o WITH (NOLOCK) WHERE o._IDRRef = s.[{OBJ}]
          )
        """,
        p_end,
    )
    print("no ObjCalc", float(cur.fetchone()[0] or 0))

    cn.close()


if __name__ == "__main__":
    main()
