# -*- coding: utf-8 -*-
"""Compare ДолгУпр vs ДолгРегл via OData Balance; include liquidated depts."""
from __future__ import annotations

from collections import defaultdict
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

REG_BAL = (
    "AccumulationRegister_"
    + "\u0420\u0430\u0441\u0447\u0435\u0442\u044b\u0421\u041a\u043b\u0438\u0435\u043d\u0442\u0430\u043c\u0438\u041f\u043e\u0421\u0440\u043e\u043a\u0430\u043c"
    + "/Balance"
)
REG_REC = (
    "AccumulationRegister_"
    + "\u0420\u0430\u0441\u0447\u0435\u0442\u044b\u0421\u041a\u043b\u0438\u0435\u043d\u0442\u0430\u043c\u0438\u041f\u043e\u0421\u0440\u043e\u043a\u0430\u043c"
    + "_RecordType"
)

COMMERCIAL = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
}
LIQUIDATED = {
    "4edcf3a0-9f99-11e4-80da-001e67112509",
    "ff740269-d71e-11e6-8127-001e67112509",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e",
}
ALLOWED = COMMERCIAL | LIQUIDATED

TARGET_DZ = 373_930_180.89
TARGET_OD = 142_773_571.76


def main() -> None:
    s = requests.Session()
    s.auth = AUTH

    # Discover Balance properties
    url = f"{BASE}/$metadata"
    print("fetch metadata snippet...")
    # Try Balance with common field names
    for fields in [
        ["ДолгУпр", "ПредоплатаУпр", "ДолгРегл", "ПредоплатаРегл", "Долг", "Предоплата"],
        ["ДолгУпр", "ПредоплатаУпр"],
        ["ДолгРегл", "ПредоплатаРегл"],
    ]:
        # Balance(Period=datetime'...') 
        # OData standard for 1C: .../Balance(Datetime'2026-06-30T23:59:59')
        sel = ",".join(
            ["ОбъектРасчетов_Key", "ДатаПлановогоПогашения"] + fields
        )
        bal_url = (
            f"{BASE}/{REG_BAL}(datetime'2026-06-30T23:59:59')"
            f"?$format=json&$select={quote(sel, safe=',_')}&$top=5"
        )
        r = s.get(bal_url, timeout=60)
        print("Balance try fields", fields, "HTTP", r.status_code)
        if r.ok:
            val = r.json().get("value", [])
            print(" sample", val[:2])
            break
        else:
            print(" ", r.text[:200])

    # Fallback: inspect one RecordType row for all Долг* keys
    r = s.get(
        f"{BASE}/{REG_REC}?$format=json&$top=1"
        f"&$filter={quote('Active eq true', safe='')}",
        timeout=60,
    )
    if r.ok:
        row = r.json().get("value", [{}])[0]
        dolg_keys = sorted(k for k in row if "олг" in k.lower() or "олг" in k or "Dolg" in k or "Debt" in k)
        # also print all keys containing numeric-ish names
        keys = sorted(row.keys())
        print("record keys count", len(keys))
        for k in keys:
            if any(x in k for x in ("олг", "редопл", "ата", "бъект", "Record", "Period")):
                print(" ", k, "=", row.get(k))


if __name__ == "__main__":
    main()
