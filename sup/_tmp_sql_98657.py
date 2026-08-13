"""Dump _InfoRg98657 for Vor May — candidate EDO execution register."""
from __future__ import annotations

import functools
import sys
import uuid
from urllib.parse import quote

import requests

sys.path.insert(0, r"c:\Users\a.komarkova\Documents\projects\DashboardBack\DashboardBack")
from getkpi.list_enterprise_positions import AUTH, BASE
from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)


def guid_to_1c_a(guid_str: str) -> bytes:
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


def bin_to_guid(b: bytes) -> str:
    if not b or len(b) != 16:
        return ""
    orig = b[12:16] + b[10:12] + b[8:10] + b[0:8]
    try:
        return str(uuid.UUID(bytes=orig))
    except Exception:
        return b.hex()


def main() -> None:
    user_bin = guid_to_1c_a("a312e193-d7b7-11ee-94a5-6cb31113810e")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        tname = "_InfoRg98657"
        cur.execute(
            """
            SELECT c.name, ty.name, c.max_length
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
            ORDER BY c.column_id
            """,
            tname,
        )
        cols = cur.fetchall()
        print("cols:")
        for c in cols:
            print(f"  {c[0]} {c[1]}({c[2]})")

        cur.execute(
            f"""
            SELECT * FROM [{tname}] WITH (NOLOCK)
            WHERE _Fld98670RRef = ?
              AND (
                (YEAR(_Fld98665)=4026 AND MONTH(_Fld98665)=5)
                OR (YEAR(_Fld98676)=4026 AND MONTH(_Fld98676)=5)
                OR (YEAR(_Fld98682)=4026 AND MONTH(_Fld98682)=5)
              )
            ORDER BY _Fld98665
            """,
            user_bin,
        )
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"\nrows={len(rows)}")
        for row in rows:
            d = dict(zip(names, row))
            print("\n---")
            for k, v in d.items():
                if isinstance(v, (bytes, bytearray, memoryview)):
                    vb = bytes(v)
                    if len(vb) == 16:
                        print(f"  {k}={bin_to_guid(vb)}")
                    elif len(vb) <= 4:
                        print(f"  {k}={vb.hex()}")
                elif v is not None and str(v).strip() not in ("", "0"):
                    sv = str(v).replace("\n", " | ")
                    print(f"  {k}={sv[:300]}")

        # Resolve refs that look like documents
        s = requests.Session()
        s.auth = AUTH
        # Collect unique guids from binary fields
        guids = set()
        for row in rows:
            d = dict(zip(names, row))
            for k, v in d.items():
                if isinstance(v, (bytes, bytearray, memoryview)) and len(bytes(v)) == 16:
                    g = bin_to_guid(bytes(v))
                    if g and g != "00000000-0000-0000-0000-000000000000":
                        guids.add(g)
        print("\nunique guids", len(guids))
        # try as users
        for g in sorted(guids):
            url = f"{BASE}/{quote('Catalog_Пользователи')}(guid'{g}')?$format=json&$select=Description"
            resp = s.get(url, timeout=20)
            if resp.ok and resp.json().get("Description"):
                print("USER", g, resp.json()["Description"])
                continue
            # try VK doc
            url = f"{BASE}/{quote('Document_ТД_ВходящаяКорреспонденция')}(guid'{g}')?$format=json&$select=Number,Date"
            resp = s.get(url, timeout=20)
            if resp.ok and resp.json().get("Number"):
                print("VK", g, resp.json().get("Number"), resp.json().get("Date"))
                continue
            print("UNK", g)

        # Cross-check: does any row reference our VK docs?
        for num in ("НП00-002870", "НП00-002937", "НП00-003094", "НП00-003101"):
            cur.execute(
                """
                SELECT TOP 1 _IDRRef FROM [_Document86498] WITH (NOLOCK)
                WHERE _Number=? AND YEAR(_Date_Time)=4026
                """,
                num,
            )
            r = cur.fetchone()
            if not r:
                continue
            dbin = bytes(r[0])
            bin16 = [c[0] for c in cols if c[1] in ("binary", "varbinary") and c[2] == 16]
            for bcol in bin16:
                cur.execute(
                    f"SELECT COUNT(*) FROM [{tname}] WITH (NOLOCK) WHERE [{bcol}]=?",
                    dbin,
                )
                cnt = cur.fetchone()[0]
                if cnt:
                    print(f"DOC {num} via {bcol}: {cnt}")


if __name__ == "__main__":
    main()
