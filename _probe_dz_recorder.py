# -*- coding: utf-8 -*-
"""Locate ПоСрокам by RecorderRRef from OData."""
from __future__ import annotations

from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from comdir.common import connect, uuid_to_1c_bytes

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

REG = (
    "AccumulationRegister_"
    + "\u0420\u0430\u0441\u0447\u0435\u0442\u044b\u0421\u041a\u043b\u0438\u0435\u043d\u0442\u0430\u043c\u0438\u041f\u043e\u0421\u0440\u043e\u043a\u0430\u043c"
    + "_RecordType"
)
FLD_OBJ = "\u041e\u0431\u044a\u0435\u043a\u0442\u0420\u0430\u0441\u0447\u0435\u0442\u043e\u0432_Key"
FLD_DOLG = "\u0414\u043e\u043b\u0433\u0423\u043f\u0440"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    # Recorder is usually available as Recorder / Recorder_Type
    sel = ",".join([FLD_OBJ, FLD_DOLG, "RecordType", "Period", "Recorder", "LineNumber"])
    flt = f"{FLD_DOLG} gt 10000 and Active eq true"
    url = (
        f"{BASE}/{REG}?$format=json&$top=3"
        f"&$select={quote(sel, safe=',_')}"
        f"&$filter={quote(flt, safe='')}"
        f"&$orderby=Period%20desc"
    )
    r = s.get(url, timeout=120)
    print("HTTP", r.status_code)
    rows = r.json().get("value", [])
    for row in rows:
        print({k: row[k] for k in row})

    if not rows:
        return

    # Recorder field may be like "Document_xxx_Key" style or just GUID
    rec = rows[0].get("Recorder")
    print("Recorder raw", rec, type(rec))

    # Also try dedicated Recorder_Key if present
    url2 = (
        f"{BASE}/{REG}?$format=json&$top=1"
        f"&$filter={quote(flt, safe='')}"
    )
    r2 = s.get(url2, timeout=120)
    full = r2.json().get("value", [{}])[0]
    rec_keys = {k: full[k] for k in full if "ecord" in k or "Recorder" in k}
    print("recorder-ish keys", rec_keys)

    # Try to parse GUID from Recorder
    guid = None
    if isinstance(rec, str) and len(rec) >= 36:
        # sometimes "Document_Xxx(guid'...')"
        if "guid'" in rec:
            guid = rec.split("guid'")[1].split("'")[0]
        elif len(rec) == 36:
            guid = rec
    print("recorder guid", guid)
    if not guid:
        return

    b = uuid_to_1c_bytes(guid)
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    cur.execute(
        """
        SELECT t.name FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND t.name NOT LIKE '%Buf%'
        ORDER BY t.name
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("scanning", len(tables), flush=True)
    for t in tables:
        cur.execute(
            """
            SELECT 1 FROM sys.columns
            WHERE object_id=OBJECT_ID(?) AND name='_RecorderRRef'
            """,
            t,
        )
        if not cur.fetchone():
            continue
        cur.execute(
            f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _RecorderRRef=?",
            b,
        )
        if cur.fetchone():
            # print schema briefly
            cur.execute(
                """
                SELECT c.name, ty.name
                FROM sys.columns c
                JOIN sys.types ty ON ty.user_type_id=c.user_type_id
                WHERE c.object_id=OBJECT_ID(?)
                ORDER BY c.column_id
                """,
                t,
            )
            cols = cur.fetchall()
            print(f"FOUND RECORDER IN {t}", flush=True)
            for n, ty in cols:
                print(f"  {n} {ty}")
    cn.close()


if __name__ == "__main__":
    main()
