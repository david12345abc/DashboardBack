# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes
from datetime import date

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

REG = (
    "AccumulationRegister_"
    + "\u0420\u0430\u0441\u0447\u0435\u0442\u044b\u0421\u041a\u043b\u0438\u0435\u043d\u0442\u0430\u043c\u0438\u041f\u043e\u0421\u0440\u043e\u043a\u0430\u043c"
    + "_RecordType"
)
FLD_DOLG = "\u0414\u043e\u043b\u0433\u0423\u043f\u0440"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    flt = f"{FLD_DOLG} gt 500000 and Active eq true"
    url = (
        f"{BASE}/{REG}?$format=json&$top=1"
        f"&$filter={quote(flt, safe='')}"
        f"&$orderby=Period%20desc"
    )
    r = s.get(url, timeout=120)
    row = r.json()["value"][0]
    # write keys as utf-8 json
    with open("_dz_sample_row.json", "w", encoding="utf-8") as f:
        json.dump(row, f, ensure_ascii=False, indent=2)
    print("saved _dz_sample_row.json")
    for k, v in row.items():
        if isinstance(v, (int, float)) and v not in (0, 0.0):
            print(f"NUM {k}={v}")
        if "ата" in k or "Date" in k or k in ("Period", "RecordType", "Recorder", "LineNumber"):
            print(f"DIM {k}={v}")
        if k.endswith("_Key") and str(v) != "00000000-0000-0000-0000-000000000000":
            print(f"KEY {k}={v}")
        if isinstance(v, bool):
            print(f"BOOL {k}={v}")

    amount = float(row[FLD_DOLG])
    period = row["Period"]  # 2026-...
    recorder = row["Recorder"]
    obj = row["\u041e\u0431\u044a\u0435\u043a\u0442\u0420\u0430\u0441\u0447\u0435\u0442\u043e\u0432_Key"]
    print("amount", amount, "period", period, "rec", recorder, "obj", obj)

    # Search SQL: all AccumRg that have Period in same day AND amount match
    p_day = date.fromisoformat(period[:10])
    p0 = to_1c_dt(p_day)
    # end of next day
    from datetime import timedelta

    p1 = to_1c_dt(p_day + timedelta(days=1))
    rb = uuid_to_1c_bytes(recorder)
    ob = uuid_to_1c_bytes(obj)

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # First: find by RecorderRRef among tables with >=2 extra datetimes
    cur.execute(
        """
        SELECT t.name
        FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND t.name NOT LIKE '%Buf%'
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("tables", len(tables))

    hits = 0
    for t in tables:
        cur.execute(
            """
            SELECT c.name, ty.name
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id=c.user_type_id
            WHERE c.object_id=OBJECT_ID(?)
            """,
            t,
        )
        cols = {a: b for a, b in cur.fetchall()}
        if "_RecorderRRef" not in cols or "_Period" not in cols:
            continue
        dts = [n for n, ty in cols.items() if ty.startswith("datetime") and n != "_Period"]
        if len(dts) < 2:
            continue
        cur.execute(
            f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _RecorderRRef=? AND _Period>=? AND _Period<?",
            (rb, p0, p1),
        )
        if cur.fetchone():
            print("RECORDER HIT", t, "dts", dts)
            hits += 1
            # show matching numeric amounts
            nums = [
                n
                for n, ty in cols.items()
                if ty in ("numeric", "decimal") and n not in ("_LineNo", "_RecordKind")
            ]
            for ncol in nums:
                cur.execute(
                    f"""
                    SELECT TOP 3 [{ncol}], _LineNo, _RecordKind
                    FROM [{t}] WITH (NOLOCK)
                    WHERE _RecorderRRef=? AND _Period>=? AND _Period<? AND [{ncol}]<>0
                    """,
                    (rb, p0, p1),
                )
                for rr in cur.fetchall():
                    print(f"  {ncol}={rr[0]} line={rr[1]} kind={rr[2]}")
        # also try obj
        rrefs = [n for n in cols if n.endswith("RRef") and n != "_RecorderRRef"]
        for col in rrefs:
            cur.execute(
                f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{col}]=? AND _Period>=? AND _Period<?",
                (ob, p0, p1),
            )
            if cur.fetchone():
                print("OBJ HIT", t, col)

    print("done hits", hits)
    cn.close()


if __name__ == "__main__":
    main()
