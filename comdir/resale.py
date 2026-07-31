# -*- coding: utf-8 -*-
"""Партнёры перепродажи и ОПБО из ТД_ПредопределенныеЗначения (без хардкода списков)."""
from __future__ import annotations

import logging
import os
import uuid
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

# Элементы справочника ТД_ПредопределенныеЗначения (не GUID партнёров/отделов).
PREDEFINED_RESALE_REF = "8180316b-7c73-11e9-828e-ac1f6b05524d"
PREDEFINED_MGS_REF = "5bd32178-cf94-11e9-829b-ac1f6b05524d"
PREDEFINED_OPBO_REF = "5bd32179-cf94-11e9-829b-ac1f6b05524d"  # Структура предприятия ОПБО
PREDEFINED_SALES_DEPTS_REF = "6c0a333b-762a-11e9-828e-ac1f6b05524d"

# Fallback только если OData недоступна.
_FALLBACK_RESALE_HEX = (
    "8266ac1f6b05524d11e7a8c56ff45495",  # АЛМАЗ ООО (рабочий)
    "812e001e6711250911e788a06ac41964",  # Турбулентность-Дон ООО
    "8266ac1f6b05524d11e7a8c46cdfe9f3",  # Турбулентность-ДОН ООО НПО
    "8266ac1f6b05524d11e7a8c74babc7a7",  # СКТБ Турбо-Дон ООО
    "8266ac1f6b05524d11e7a8c6d7f5ff44",  # Метрогазсервис ООО
)
_FALLBACK_MGS_HEX = "8266ac1f6b05524d11e7a8c6d7f5ff44"
_FALLBACK_OPBO_HEX = "96f96cb31113810e11f092f67587c178"  # ОДП (= ОПБО в предопределённых)

# Document.ЗаказКлиента.ТД_СопровождениеПродажи
ORDER_SOPR_FIELD = "_Fld123476"


def guid_to_1c_bytes(guid_str: str) -> bytes:
    u = uuid.UUID(str(guid_str))
    b = u.bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


def _session() -> requests.Session:
    base_auth = HTTPBasicAuth(
        os.getenv("ODATA_USER", "odata.user"),
        os.getenv("ODATA_PASSWORD", "npo852456"),
    )
    s = requests.Session()
    s.auth = base_auth
    return s


def _base() -> str:
    return os.getenv("ONEC_BASE_URL", "http://192.168.2.229:81/erp_pm/odata/standard.odata")


def _fetch_from_odata() -> tuple[list[bytes], bytes | None, bytes | None]:
    session = _session()
    base = _base()
    resale: list[bytes] = []
    mgs_bin: bytes | None = None
    opbo_bin: bytes | None = None

    flt = quote(f"Ref_Key eq guid'{PREDEFINED_RESALE_REF}'", safe="")
    url = (
        f"{base}/Catalog_ТД_ПредопределенныеЗначения_ДополнительныеЗначения"
        f"?$format=json&$filter={flt}&$select=Значение,Значение_Type&$top=5000"
    )
    r = session.get(url, timeout=45)
    r.raise_for_status()
    for row in r.json().get("value") or []:
        val = row.get("Значение")
        typ = str(row.get("Значение_Type") or "")
        if val and "Catalog_Партнеры" in typ:
            resale.append(guid_to_1c_bytes(val))

    flt_m = quote(f"Ref_Key eq guid'{PREDEFINED_MGS_REF}'", safe="")
    url_m = (
        f"{base}/Catalog_ТД_ПредопределенныеЗначения"
        f"?$format=json&$filter={flt_m}&$select=Значение,Значение_Type&$top=1"
    )
    rm = session.get(url_m, timeout=30)
    rm.raise_for_status()
    row = (rm.json().get("value") or [{}])[0]
    val = row.get("Значение")
    typ = str(row.get("Значение_Type") or "")
    if val and "Catalog_Партнеры" in typ:
        mgs_bin = guid_to_1c_bytes(val)
        if mgs_bin not in resale:
            resale.append(mgs_bin)

    flt_o = quote(f"Ref_Key eq guid'{PREDEFINED_OPBO_REF}'", safe="")
    url_o = (
        f"{base}/Catalog_ТД_ПредопределенныеЗначения"
        f"?$format=json&$filter={flt_o}&$select=Значение,Значение_Type&$top=1"
    )
    ro = session.get(url_o, timeout=30)
    ro.raise_for_status()
    row_o = (ro.json().get("value") or [{}])[0]
    val_o = row_o.get("Значение")
    typ_o = str(row_o.get("Значение_Type") or "")
    if val_o and "СтруктураПредприятия" in typ_o:
        opbo_bin = guid_to_1c_bytes(val_o)

    if not resale:
        raise RuntimeError("пустой список партнёров перепродажи из OData")
    return resale, mgs_bin, opbo_bin


def fetch_fx_rates() -> dict[str, float]:
    """Курсы Константы.ТД_ВалютаПланФакта_УЕ_*."""
    session = _session()
    base = _base()
    out: dict[str, float] = {}
    for name in ("USD", "EUR", "BYN", "KZT"):
        r = session.get(f"{base}/Constant_ТД_ВалютаПланФакта_УЕ_{name}?$format=json", timeout=20)
        r.raise_for_status()
        out[name] = float((r.json().get("value") or [{}])[0].get("Value") or 1.0)
    return out


def load_resale_temp(cur) -> None:
    """Заполнить #resale, #resale_nomgs, #dept_nomgs (ОПБО) для SQL факт/ожидаемо."""
    try:
        bins, mgs_bin, opbo_bin = _fetch_from_odata()
    except Exception:
        logger.exception("OData перепродажи/ОПБО недоступна — fallback")
        bins = [bytes.fromhex(h) for h in _FALLBACK_RESALE_HEX]
        mgs_bin = bytes.fromhex(_FALLBACK_MGS_HEX)
        opbo_bin = bytes.fromhex(_FALLBACK_OPBO_HEX)

    cur.execute("IF OBJECT_ID('tempdb..#resale') IS NOT NULL DROP TABLE #resale")
    cur.execute("CREATE TABLE #resale (id binary(16) PRIMARY KEY)")
    cur.execute("IF OBJECT_ID('tempdb..#resale_nomgs') IS NOT NULL DROP TABLE #resale_nomgs")
    cur.execute("CREATE TABLE #resale_nomgs (id binary(16) PRIMARY KEY)")
    cur.execute("IF OBJECT_ID('tempdb..#dept_nomgs') IS NOT NULL DROP TABLE #dept_nomgs")
    cur.execute("CREATE TABLE #dept_nomgs (id binary(16) PRIMARY KEY)")

    for b in bins:
        cur.execute("INSERT INTO #resale(id) VALUES (?)", b)
        if mgs_bin is None or b != mgs_bin:
            cur.execute("INSERT INTO #resale_nomgs(id) VALUES (?)", b)

    if opbo_bin is not None:
        cur.execute("INSERT INTO #dept_nomgs(id) VALUES (?)", opbo_bin)
