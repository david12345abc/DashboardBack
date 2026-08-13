"""Decode _InfoRg43471X1 history rows at assignment times; map OData name."""
from __future__ import annotations

import functools
import re
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
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 _IDRRef FROM [_Document86498] WITH (NOLOCK)
            WHERE _Number=N'НП00-002870' AND YEAR(_Date_Time)=4026
            """
        )
        doc_bin = bytes(cur.fetchone()[0])

        cur.execute(
            """
            SELECT * FROM [_InfoRg43471X1] WITH (NOLOCK)
            WHERE _Fld43472_RRRef = ?
              AND (
                _Fld43477 BETWEEN '4026-05-04 21:09:00' AND '4026-05-04 21:09:59'
                OR _Fld43477 BETWEEN '4026-05-04 10:58:00' AND '4026-05-04 10:58:59'
                OR _Fld43473 IN (6,3)
              )
            ORDER BY _Fld43477
            """,
            doc_bin,
        )
        # Actually get all for doc and dump carefully
        cur.execute(
            """
            SELECT * FROM [_InfoRg43471X1] WITH (NOLOCK)
            WHERE _Fld43472_RRRef = ?
            ORDER BY _Fld43473
            """,
            doc_bin,
        )
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print("cols", names)
        for row in rows:
            d = dict(zip(names, row))
            print("\n--- ver", d.get("_Fld43473"), "at", d.get("_Fld43477"), "---")
            for k, v in d.items():
                if isinstance(v, (bytes, bytearray, memoryview)):
                    vb = bytes(v)
                    if len(vb) == 16:
                        print(f"  {k}={bin_to_guid(vb)} raw={vb.hex()}")
                    elif len(vb) <= 8:
                        print(f"  {k}={vb.hex()}")
                    else:
                        # try decode utf-8/utf-16
                        txt = None
                        for enc in ("utf-8", "utf-16-le", "cp1251"):
                            try:
                                t = vb.decode(enc)
                                if t.isprintable() or "\n" in t or "\r" in t:
                                    txt = t
                                    break
                            except Exception:
                                pass
                        if txt and any(ch.isalpha() for ch in txt):
                            print(f"  {k}=TEXT len={len(vb)}: {txt[:300]!r}")
                        else:
                            # show printable ascii fragments
                            frag = "".join(chr(b) if 32 <= b < 127 else "." for b in vb[:200])
                            print(f"  {k}=bin len={len(vb)} ascii={frag[:200]}")
                else:
                    print(f"  {k}={v}")

        # Who are the users at assignment moments
        users = {
            "f74842ae-4ca2-11ee-93e5-6cb31113810e",
            "5b2e1e74-a805-11eb-85c6-ac1f6b05524d",
            "a312e193-d7b7-11ee-94a5-6cb31113810e",
            "7a3fa603-0899-11f0-9637-6cb31113810e",
            "a11c3aa4-548f-11ef-954c-6cb31113810c",
        }
        # fix last guid - from earlier a11c3aa4-548f-11ef-954c-6cb31113810e
        users = {
            "f74842ae-4ca2-11ee-93e5-6cb31113810e",
            "5b2e1e74-a805-11eb-85c6-ac1f6b05524d",
            "a312e193-d7b7-11ee-94a5-6cb31113810e",
            "7a3fa603-0899-11f0-9637-6cb31113810e",
            "a11c3aa4-548f-11ef-954c-6cb31113810e",
        }
        s = requests.Session()
        s.auth = AUTH
        print("\n=== users ===")
        for u in users:
            url = f"{BASE}/{quote('Catalog_Пользователи')}(guid'{u}')?$format=json&$select=Description,Ref_Key"
            resp = s.get(url, timeout=30)
            if resp.ok:
                print(u, resp.json().get("Description"))
            else:
                print(u, resp.status_code)

        # Map InfoRg43471 to OData - search metadata for property patterns is hard.
        # Try InformationRegister entities, probe by reading one known key? 
        # Alternative: Config / ConfigCAS
        # Search _InfoRg43471 without X1
        cur.execute(
            """
            SELECT t.name FROM sys.tables t
            WHERE t.name LIKE '_InfoRg43471%'
            ORDER BY t.name
            """
        )
        print("\ntables", [r[0] for r in cur.fetchall()])

        # Try find RTRef of document type for Fld43472
        # Fld43472_TYPE/RTRef on a row
        cur.execute(
            """
            SELECT TOP 1 _Fld43472_TYPE, _Fld43472_RTRef
            FROM [_InfoRg43471X1] WITH (NOLOCK)
            WHERE _Fld43472_RRRef = ?
            """,
            doc_bin,
        )
        typ = cur.fetchone()
        print("type/rtref", typ[0].hex() if typ else None, typ[1].hex() if typ else None)


if __name__ == "__main__":
    main()
