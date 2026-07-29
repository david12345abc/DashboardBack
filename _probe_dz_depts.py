# -*- coding: utf-8 -*-
"""List distinct departments on ObjCalc that have any presence; compare to commercial set."""
from __future__ import annotations

from comdir.common import connect, hex_to_odata, uuid_to_1c_bytes

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


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # Department catalog
    # Find structure of _Reference for departments - usually _Reference96 or similar
    # We know depts via Fld138169RRef on ObjCalc
    cur.execute(
        """
        SELECT TOP 30
          CONVERT(varchar(32), o._Fld138169RRef, 2) AS dept_hex,
          COUNT(*) AS cnt
        FROM _Reference134945 o WITH (NOLOCK)
        WHERE o._Fld138169RRef <> 0x00000000000000000000000000000000
        GROUP BY o._Fld138169RRef
        ORDER BY COUNT(*) DESC
        """
    )
    print("top depts on ObjCalc:")
    for hx, cnt in cur.fetchall():
        # try reverse via known map
        odata = hex_to_odata(hx.lower())
        flag = ""
        if odata in COMMERCIAL:
            flag = "COMM"
        elif odata in LIQ:
            flag = "LIQ"
        print(f"  {hx} cnt={cnt} odata={odata} {flag}")

    cn.close()


if __name__ == "__main__":
    main()
