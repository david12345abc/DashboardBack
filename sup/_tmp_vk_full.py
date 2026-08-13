"""Dump full VK document fields + search related registers by RTRef 000151e2."""
from __future__ import annotations

import functools
import json
import sys
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

REF = "05dc41cd-3fb7-11f1-97ed-6cb31113810e"
VK = "Document_ТД_ВходящаяКорреспонденция"


def main() -> None:
    s = requests.Session()
    s.auth = AUTH
    s.headers.update({"Accept": "application/json"})
    url = f"{BASE}/{quote(VK)}(guid'{REF}')?$format=json"
    resp = s.get(url, timeout=60)
    print("status", resp.status_code)
    js = resp.json()
    for k, v in sorted(js.items()):
        if k.startswith("odata") or k.endswith("@navigationLinkUrl"):
            continue
        if isinstance(v, list):
            print(f"{k}: list[{len(v)}]")
            for row in v[:5]:
                print(" ", json.dumps(row, ensure_ascii=False)[:300])
        else:
            print(f"{k}={v}")

    # Expand all nav collections that look useful
    meta = s.get(f"{BASE}/$metadata", timeout=180).text
    import re
    m = re.search(rf'<EntityType Name="{re.escape(VK)}"[^>]*>(.*?)</EntityType>', meta, re.S)
    if m:
        navs = re.findall(r'<NavigationProperty Name="([^"]+)"', m.group(1))
        print("\nnavs:", navs)
        for nav in navs:
            u = f"{BASE}/{quote(VK)}(guid'{REF}')/{quote(nav)}?$format=json"
            r = s.get(u, timeout=60)
            if not r.ok:
                print(f"  {nav}: {r.status_code}")
                continue
            data = r.json().get("value", r.json())
            if isinstance(data, list):
                print(f"  {nav}: {len(data)} rows")
                for row in data[:8]:
                    print("   ", json.dumps(row, ensure_ascii=False)[:350])
            else:
                print(f"  {nav}:", json.dumps(data, ensure_ascii=False)[:250])

    # SQL: find InfoRg that reference RTRef 000151e2 (VK type) and have Vor
    print("\n=== SQL registers with VK type RTRef and Vor ===")
    user_bin = bytes.fromhex("94a56cb31113810e11eed7b7a312e193")
    doc_bin = bytes.fromhex("97ed6cb31113810e11f13fb705dc41cd")
    rtref = bytes.fromhex("000151e2")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT t.name, c.name
            FROM sys.tables t
            JOIN sys.columns c ON c.object_id = t.object_id
            WHERE t.name LIKE '_InfoRg%'
              AND t.name NOT LIKE '%ChngR%'
              AND c.name LIKE '%_RTRef'
            """
        )
        candidates = cur.fetchall()
        print("RTRef cols", len(candidates))
        hits = 0
        for tname, cname in candidates:
            # corresponding RRRef
            rr = cname.replace("_RTRef", "_RRRef")
            try:
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                    WHERE [{cname}] = ? AND [{rr}] = ?
                    """,
                    rtref,
                    doc_bin,
                )
                cnt = cur.fetchone()[0]
            except Exception:
                continue
            if not cnt:
                continue
            print(f"DOC-TYPE HIT {tname}.{cname}/{rr}: {cnt}")
            # check if Vor in same table
            cur.execute(
                """
                SELECT c2.name FROM sys.columns c2
                JOIN sys.types ty ON ty.user_type_id = c2.user_type_id
                WHERE c2.object_id = OBJECT_ID(?)
                  AND ty.name IN ('binary','varbinary') AND c2.max_length=16
                """,
                tname,
            )
            bins = [r[0] for r in cur.fetchall()]
            for bcol in bins:
                try:
                    cur.execute(
                        f"""
                        SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK)
                        WHERE [{cname}]=? AND [{rr}]=? AND [{bcol}]=?
                        """,
                        rtref,
                        doc_bin,
                        user_bin,
                    )
                    c2 = cur.fetchone()[0]
                    if c2:
                        print(f"  WITH VOR via {bcol}: {c2}")
                        hits += 1
                except Exception:
                    pass
        print("vor+doc hits", hits)


if __name__ == "__main__":
    main()
