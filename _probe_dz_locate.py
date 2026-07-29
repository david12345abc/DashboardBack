# -*- coding: utf-8 -*-
"""Get one OData DZ record and locate its ObjCalc key in SQL AccumRg tables."""
from __future__ import annotations

from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from comdir.common import connect, uuid_to_1c_bytes

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

REG = "AccumulationRegister_" + "\u0420\u0430\u0441\u0447\u0435\u0442\u044b\u0421\u041a\u043b\u0438\u0435\u043d\u0442\u0430\u043c\u0438\u041f\u043e\u0421\u0440\u043e\u043a\u0430\u043c" + "_RecordType"
FLD_OBJ = "\u041e\u0431\u044a\u0435\u043a\u0442\u0420\u0430\u0441\u0447\u0435\u0442\u043e\u0432_Key"
FLD_PLAN = "\u0414\u0430\u0442\u0430\u041f\u043b\u0430\u043d\u043e\u0432\u043e\u0433\u043e\u041f\u043e\u0433\u0430\u0448\u0435\u043d\u0438\u044f"
FLD_OCC = "\u0414\u0430\u0442\u0430\u0412\u043e\u0437\u043d\u0438\u043a\u043d\u043e\u0432\u0435\u043d\u0438\u044f"
FLD_DOLG = "\u0414\u043e\u043b\u0433\u0423\u043f\u0440"
FLD_PRED = "\u041f\u0440\u0435\u0434\u043e\u043f\u043b\u0430\u0442\u0430\u0423\u043f\u0440"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    sel = ",".join([FLD_OBJ, FLD_PLAN, FLD_OCC, FLD_DOLG, FLD_PRED, "RecordType", "Period", "Active"])
    flt = f"{FLD_DOLG} gt 1000 and Active eq true"
    url = (
        f"{BASE}/{REG}?$format=json&$top=5"
        f"&$select={quote(sel, safe=',_')}"
        f"&$filter={quote(flt, safe='')}"
    )
    print("GET", REG)
    r = s.get(url, timeout=120)
    print("HTTP", r.status_code)
    if not r.ok:
        print(r.text[:500])
        return
    rows = r.json().get("value", [])
    for row in rows[:3]:
        print(row)

    if not rows:
        return
    obj = rows[0][FLD_OBJ]
    print("obj guid", obj)
    b = uuid_to_1c_bytes(obj)

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    cur.execute("SELECT TOP 1 1 FROM _Reference134945 WHERE _IDRRef=?", b)
    print("in ObjCalc", bool(cur.fetchone()))

    tables = [
        "_AccumRg53835",
        "_AccumRg53936",
        "_AccumRg53885",
        "_AccumRg54090",
        "_AccumRg53439",
        "_AccumRg53819",
        "_AccumRg54533",
        "_AccumRg52263",
        "_AccumRg50853",
        "_AccumRg50866",
    ]
    for t in tables:
        cur.execute(
            """
            SELECT c.name FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(?) AND c.name LIKE '%RRef'
            """,
            t,
        )
        cols = [r[0] for r in cur.fetchall()]
        if not cols:
            print(t, "missing")
            continue
        for col in cols:
            cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{col}]=?", b)
            if cur.fetchone():
                print(f"FOUND {t}.{col}")
    cn.close()


if __name__ == "__main__":
    main()
