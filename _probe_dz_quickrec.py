# -*- coding: utf-8 -*-
"""Quick check recorder in known date-dim AccumRg candidates."""
from __future__ import annotations

from comdir.common import connect, uuid_to_1c_bytes

REC = "f8f20bf7-8a88-11f1-9850-6cb31113810e"
OBJ = "25416d4b-875f-11f1-984c-6cb31113810e"

CANDS = [
    "_AccumRg107629",
    "_AccumRg107647",
    "_AccumRg107662",
    "_AccumRg107688",
    "_AccumRg107706",
    "_AccumRg107721",
    "_AccumRg121975",
    "_AccumRg182567",
    "_AccumRg182617",
    "_AccumRg50853",
    "_AccumRg50866",
    "_AccumRg52263",
    "_AccumRg53819",
    "_AccumRg53835",
    "_AccumRg53885",
    "_AccumRg53936",
    "_AccumRg54090",
    "_AccumRg54533",
]


def main() -> None:
    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")
    rb = uuid_to_1c_bytes(REC)
    ob = uuid_to_1c_bytes(OBJ)

    for t in CANDS:
        cur.execute(
            f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE _RecorderRRef=?",
            rb,
        )
        hit_r = bool(cur.fetchone())
        # any RRef = obj
        cur.execute(
            """
            SELECT c.name FROM sys.columns c
            WHERE c.object_id=OBJECT_ID(?) AND c.name LIKE '%RRef'
            """,
            t,
        )
        cols = [r[0] for r in cur.fetchall()]
        hit_o = None
        for col in cols:
            cur.execute(f"SELECT TOP 1 1 FROM [{t}] WITH (NOLOCK) WHERE [{col}]=?", ob)
            if cur.fetchone():
                hit_o = col
                break
        if hit_r or hit_o:
            print(t, "recorder", hit_r, "obj_col", hit_o)

    # Also try raw GUID bytes without swap
    import uuid as _uuid

    raw = _uuid.UUID(REC).bytes_le
    raw2 = _uuid.UUID(REC).bytes
    for label, b in [("bytes_le", raw), ("bytes", raw2), ("1c", rb)]:
        cur.execute(
            "SELECT TOP 1 1 FROM _AccumRg53835 WITH (NOLOCK) WHERE _RecorderRRef=?",
            b,
        )
        print("53835", label, bool(cur.fetchone()))
        cur.execute(
            "SELECT TOP 1 1 FROM _AccumRg53936 WITH (NOLOCK) WHERE _RecorderRRef=?",
            b,
        )
        print("53936", label, bool(cur.fetchone()))
        cur.execute(
            "SELECT TOP 1 1 FROM _AccumRg53885 WITH (NOLOCK) WHERE _RecorderRRef=?",
            b,
        )
        print("53885", label, bool(cur.fetchone()))

    cn.close()


if __name__ == "__main__":
    main()
