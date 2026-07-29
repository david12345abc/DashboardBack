"""Import autoit modules from TestKPIDump (strip CLI bootstrap)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = Path(r"c:\Users\a.komarkova\Documents\projects\TestKPIDump\autoit")
DST = ROOT / "getkpi" / "autoit"

ODATA_HEADER = '''import json
import logging as _logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

log = _logging.getLogger(__name__).info
SCRIPT_DIR = Path(__file__).resolve().parent

BASE = os.getenv(
    "ONEC_BASE_URL",
    os.getenv("ODATA_BASE", "http://192.168.2.229:81/erp_pm/odata/standard.odata"),
).rstrip("/")
if not BASE.endswith("/odata/standard.odata"):
    BASE = f"{BASE.rstrip('/')}/odata/standard.odata"
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)

'''

SQL_HEADER = '''import json
import logging as _logging
import re
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

log = _logging.getLogger(__name__).info
SCRIPT_DIR = Path(__file__).resolve().parent

'''


def _first_constant_line(text: str) -> int:
    markers = (
        "EXECUTOR_DEPARTMENT_LABEL = ",
        "DOC_ENTITY = ",
        "DOC_SQL = ",
        "YEAR_OFFSET = ",
        "IT_M4_FOT_PLAN",
        "IT_Q2_DEPARTMENTS",
        "REG = ",
    )
    best: int | None = None
    for i, line in enumerate(text.splitlines()):
        for m in markers:
            if line.startswith(m):
                if best is None or i < best:
                    best = i
                break
    if best is None:
        raise RuntimeError("Constant marker not found")
    return best


def _strip_bootstrap(text: str, *, odata: bool) -> str:
    text = text.lstrip("\n")
    text = re.sub(
        r"\ndef main\(\).*?\nif __name__ == \"__main__\":\n    main\(\)\n?\Z",
        "\n",
        text,
        flags=re.DOTALL,
    )

    m = re.match(
        r'(""".*?"""\n\nfrom __future__ import annotations\n\n)',
        text,
        flags=re.DOTALL,
    )
    if not m:
        raise RuntimeError("Unexpected module structure")
    prefix = m.group(1)
    rest = text[len(prefix) :]
    lines = rest.splitlines(keepends=True)
    idx = _first_constant_line(rest)
    body = "".join(lines[idx:])
    header = ODATA_HEADER if odata else SQL_HEADER
    return prefix + header + body


MAPPING = {
    "it_m1.py": ("it_m1_core.py", False),
    "it_m3.py": ("it_m3_core.py", False),
    "it_m4.py": ("it_m4_core.py", False),
    "it_q2.py": ("it_q2_core.py", True),
}


def main() -> None:
    DST.mkdir(parents=True, exist_ok=True)
    for src_name, (dst_name, odata) in MAPPING.items():
        src = SRC / src_name
        dst = DST / dst_name
        raw = src.read_text(encoding="utf-8")
        dst.write_text(_strip_bootstrap(raw, odata=odata), encoding="utf-8")
        print(f"Wrote {dst.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
