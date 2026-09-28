# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from comdir.calc_plan_fact_dengi import (
    BANK_TREF,
    COMM_TREF,
    COMMERCIAL_DEPTS,
    EMPTY16,
    ORDER_TREF,
    RET_OP,
    load_depts,
    load_resale,
    to_1c_dt,
)
from comdir.common import connect_ctx
from comdir.resale import ORDER_SOPR_FIELD, _FALLBACK_MGS_HEX

MGS = bytes.fromhex(_FALLBACK_MGS_HEX)


def inner_sql() -> str:
    return f"""
        SELECT
          d.name,
          MONTH(DATEADD(year, -2000, x.period)) AS mon,
          CAST(x.amt AS float) AS amt,
          ISNULL(LTRIM(RTRIM(ord._Number)), N'') AS ord_num,
          CASE WHEN EXISTS (SELECT 1 FROM #resale r WHERE r.id = x.partner) THEN 1 ELSE 0 END AS in_resale
        FROM (
          SELECT o._Fld138169RRef AS dept,
                 CASE WHEN c._Fld51417RRef = ? THEN -c._Fld51434 ELSE c._Fld51434 END AS amt,
                 COALESCE(ord._Fld21180RRef, c._Fld51418RRef) AS partner,
                 c._Period AS period,
                 ord._IDRRef AS ord_id
          FROM _AccumRg51416 c WITH (NOLOCK)
          INNER JOIN _Reference134945 o WITH (NOLOCK)
            ON o._IDRRef = c._Fld140225_RRRef
          LEFT JOIN _Document704 ord WITH (NOLOCK)
            ON ord._IDRRef = o._Fld138162_RRRef AND o._Fld138162_RTRef = ?
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01
            AND ISNULL(c._Fld140228, 0x00) = 0x00
            AND c._Fld51434 <> 0
            AND o._Fld138169RRef IN (SELECT id FROM #fact_depts)
            AND o._Fld138169RRef <> ?
            AND (
                  ord._IDRRef IS NULL
                  OR (
                       o._Fld138193_RRRef <> ?
                       AND ISNULL(ord._Fld184301, 0x00) = 0x00
                       AND ISNULL(ord._Fld185210, 0x00) = 0x00
                       AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                     )
                )
          UNION ALL
          SELECT c._Fld51419RRef, c._Fld51437, c._Fld51418RRef, c._Period, NULL
          FROM _AccumRg51416 c WITH (NOLOCK)
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01 AND ISNULL(c._Fld140228, 0x00) = 0x00
            AND c._Fld51419RRef IN (SELECT id FROM #fact_depts)
            AND c._Fld51432_RTRef = ? AND c._RecorderTRef = ? AND c._Fld51437 <> 0
          UNION ALL
          SELECT o._Fld138169RRef, c._Fld51626, ord._Fld21180RRef, c._Period, ord._IDRRef
          FROM _AccumRg51608 c WITH (NOLOCK)
          INNER JOIN _Reference134945 o WITH (NOLOCK) ON o._IDRRef = c._Fld140249_RRRef
          INNER JOIN _Document704 ord WITH (NOLOCK)
            ON ord._IDRRef = o._Fld138162_RRRef AND o._Fld138162_RTRef = ?
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01
            AND o._Fld138169RRef IN (SELECT id FROM #fact_depts)
            AND o._Fld138193_RRRef <> ?
            AND ISNULL(ord._Fld184301, 0x00) = 0x00
            AND ISNULL(ord._Fld185210, 0x00) = 0x00
            AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
        ) x
        INNER JOIN #fact_depts d ON d.id = x.dept
        LEFT JOIN _Document704 ord WITH (NOLOCK) ON ord._IDRRef = x.ord_id
        WHERE x.partner = ?
    """


p0 = to_1c_dt(date(2026, 1, 1))
p1 = to_1c_dt(date(2026, 10, 1))

with connect_ctx() as cn:
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    load_depts(cur, COMMERCIAL_DEPTS, "#fact_depts")
    load_resale(cur)
    cur.execute("SELECT COUNT(*) FROM #resale WHERE id = ?", MGS)
    mgs_in_list = int(cur.fetchone()[0])
    cur.execute(
        inner_sql(),
        RET_OP,
        ORDER_TREF,
        p0,
        p1,
        EMPTY16,
        EMPTY16,
        p0,
        p1,
        COMM_TREF,
        BANK_TREF,
        ORDER_TREF,
        p0,
        p1,
        EMPTY16,
        MGS,
    )
    rows = []
    by_m: dict[int, dict] = {}
    for name, mon, amt, ord_num, in_resale in cur.fetchall():
        amt = float(amt or 0)
        rec = {
            "month": int(mon),
            "dept": name,
            "amt": amt,
            "ord": (ord_num or "").strip(),
            "in_resale_list": int(in_resale),
        }
        rows.append(rec)
        b = by_m.setdefault(int(mon), {"amt": 0.0, "n": 0, "depts": {}, "orders": []})
        b["amt"] = round(b["amt"] + amt, 2)
        b["n"] += 1
        b["depts"][name] = round(b["depts"].get(name, 0.0) + amt, 2)
        if rec["ord"]:
            b["orders"].append({"ord": rec["ord"], "amt": amt, "dept": name})

out = {
    "mgs_in_resale_list": mgs_in_list,
    "by_month": {str(m): by_m.get(m, {"amt": 0, "n": 0, "depts": {}, "orders": []}) for m in range(1, 10)},
    "total_jan_aug": round(sum(by_m.get(m, {}).get("amt", 0) for m in range(1, 9)), 2),
    "note": "это суммы, которые проходят все фильтры факта, кроме отсева перепродажи; в текущем calc_fact их нет",
}
Path(__file__).with_suffix(".json").write_text(
    json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
)
print("ok", mgs_in_list, out["total_jan_aug"])
