# -*- coding: utf-8 -*-
"""Locate ПоСрокам using a June-2026 OData movement (should exist in SQL copy)."""
from __future__ import annotations

import json
from datetime import date
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

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
    flt = (
        f"{FLD_DOLG} gt 100000 and Active eq true "
        f"and Period lt datetime'2026-07-01T00:00:00' "
        f"and Period ge datetime'2026-06-01T00:00:00'"
    )
    url = (
        f"{BASE}/{REG}?$format=json&$top=3"
        f"&$filter={quote(flt, safe='')}"
        f"&$orderby=Period%20desc"
    )
    r = s.get(url, timeout=120)
    print("HTTP", r.status_code)
    rows = r.json().get("value", [])
    with open("_dz_june_row.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    if not rows:
        print("no rows")
        return
    row = rows[0]
    print("Period", row["Period"], "Recorder", row["Recorder"], "Dolg", row[FLD_DOLG])
    print("PlanDate", row.get("\u0414\u0430\u0442\u0430\u041f\u043b\u0430\u043d\u043e\u0432\u043e\u0433\u043e\u041f\u043e\u0433\u0430\u0448\u0435\u043d\u0438\u044f"))
    print("Poryadok", row.get("\u041f\u043e\u0440\u044f\u0434\u043e\u043a\u0417\u0430\u0447\u0435\u0442\u0430"))

    rec = row["Recorder"]
    amount = float(row[FLD_DOLG])
    period = date.fromisoformat(row["Period"][:10])
    needle = row.get("\u041f\u043e\u0440\u044f\u0434\u043e\u043a\u0417\u0430\u0447\u0435\u0442\u0430") or ""
    rb = uuid_to_1c_bytes(rec)
    p0 = to_1c_dt(period)
    from datetime import timedelta

    p1 = to_1c_dt(period + timedelta(days=1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    # Search only AccumRg with RecordKind + >=2 datetime + nvarchar
    cur.execute(
        """
        SELECT t.name FROM sys.tables t
        WHERE t.name LIKE '_AccumRg%'
          AND t.name NOT LIKE '%VT%'
          AND t.name NOT LIKE '%ChngR%'
          AND EXISTS (
            SELECT 1 FROM sys.columns c
            WHERE c.object_id=t.object_id AND c.name='_RecordKind'
          )
        """
    )
    tables = [r[0] for r in cur.fetchall()]
    print("tables", len(tables))
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
        dts = [n for n, ty in cols.items() if ty.startswith("datetime") and n != "_Period"]
        strs = [n for n, ty in cols.items() if ty in ("nvarchar", "varchar")]
        if len(dts) < 2:
            continue
        if "_RecorderRRef" not in cols:
            continue
        cur.execute(
            f"""
            SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK)
            WHERE _RecorderRRef=? AND _Period>=? AND _Period<?
            """,
            (rb, p0, p1),
        )
        if cur.fetchone():
            print("FOUND BY RECORDER", t)
            nums = [
                n
                for n, ty in cols.items()
                if ty in ("numeric", "decimal") and n not in ("_LineNo", "_RecordKind", "_Fld1952")
            ]
            for ncol in nums:
                cur.execute(
                    f"""
                    SELECT TOP 3 [{ncol}], _LineNo, _RecordKind
                    FROM [{t}] WITH (NOLOCK)
                    WHERE _RecorderRRef=? AND _Period>=? AND _Period<? AND ABS([{ncol}]-?) < 0.01
                    """,
                    (rb, p0, p1, amount),
                )
                hits = cur.fetchall()
                if hits:
                    print(f"  amount field {ncol}: {hits}")
            print("  dts", dts)
            print("  strs", strs)
            if needle:
                for sc in strs:
                    cur.execute(
                        f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{sc}]=?",
                        needle,
                    )
                    if cur.fetchone():
                        print("  needle col", sc)
            # dump schema
            for n, ty in cols.items():
                print(f"  {n} {ty}")
            cn.close()
            return

    print("recorder not found in SQL")
    cn.close()


if __name__ == "__main__":
    main()
