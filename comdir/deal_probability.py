"""Вероятность сделки с коммерческого предложения.

ТД_ВидВероятностиСделки = 0 — автоматическая, 1 — ручная.
У заказа берётся предложение, в котором указан этот заказ.
"""
from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from urllib.parse import quote

_CACHE = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard" / "comdir_deal_probability.json"
_NAV = re.compile(r"Документ\.([^?]+)\?ref=([0-9a-fA-F]{32})")
_EMPTY = "00000000-0000-0000-0000-000000000000"
_SELECT = (
    "Ref_Key,Date,ЗаказКлиента_Key,ТД_ВидВероятностиСделки,"
    "ТД_АвтоматическаяВероятностьСделки,ТД_РучнаяВероятностьСделки,ТД_ОсновноеТКПДляБМИ"
)


def ref_to_guid(ref: str) -> str:
    value = ref.lower()
    raw = value[24:32] + value[20:24] + value[16:20] + value[0:4] + value[4:16]
    return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"


def line_source(row: dict) -> tuple[str, str]:
    """(вид документа, guid) из навигационной ссылки строки."""
    match = _NAV.search(str(row.get("onec_nav") or ""))
    if not match:
        return "", ""
    return match.group(1), ref_to_guid(match.group(2))


def chosen_percent(row: dict) -> float | None:
    kind = str(row.get("ТД_ВидВероятностиСделки") or "0").strip()
    raw = row.get("ТД_РучнаяВероятностьСделки") if kind == "1" else row.get("ТД_АвтоматическаяВероятностьСделки")
    try:
        return round(float(raw or 0), 2)
    except (TypeError, ValueError):
        return None


def _load() -> dict[str, float | None]:
    if not _CACHE.exists():
        return {}
    try:
        data = json.loads(_CACHE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save(data: dict[str, float | None]) -> None:
    _CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CACHE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(_CACHE)


def _session():
    import requests
    from requests.auth import HTTPBasicAuth

    session = requests.Session()
    session.auth = HTTPBasicAuth("odata.user", "npo852456")
    return session


def _odata(session, url: str) -> list[dict] | None:
    response = session.get(url, timeout=60)
    if not response.ok:
        return None
    return response.json().get("value") or []


def _pick(rows: list[dict]) -> float | None:
    if not rows:
        return None

    def rank(row: dict) -> tuple:
        percent = chosen_percent(row)
        return (
            1 if row.get("ТД_ОсновноеТКПДляБМИ") else 0,
            1 if percent not in (None, 0) else 0,
            str(row.get("Date") or ""),
        )

    return chosen_percent(max(rows, key=rank))


def _fetch(proposals: set[str], orders: set[str]) -> dict[str, float | None]:
    if not proposals and not orders:
        return {}
    session = _session()
    base = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
    entity = quote("Document_КоммерческоеПредложениеКлиенту")
    select = quote(_SELECT, safe=",_")
    found: dict[str, float | None] = {}
    proposal_list = sorted(proposals)
    for index in range(0, len(proposal_list), 15):
        part = proposal_list[index:index + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{key}'" for key in part), safe="")
        rows = _odata(
            session,
            f"{base}/{entity}?$format=json&$filter={flt}&$select={select}&$top={len(part)}",
        )
        if rows is None:
            continue
        by_ref = {row.get("Ref_Key"): row for row in rows}
        for key in part:
            row = by_ref.get(key)
            found[key] = chosen_percent(row) if row else None
    by_order: dict[str, list[dict]] = {}
    fetched_orders: set[str] = set()
    order_list = sorted(orders)
    for index in range(0, len(order_list), 10):
        part = order_list[index:index + 10]
        flt = quote(" or ".join(f"ЗаказКлиента_Key eq guid'{key}'" for key in part), safe="")
        rows = _odata(
            session,
            f"{base}/{entity}?$format=json&$filter={flt}&$select={select}&$top=200",
        )
        if rows is None:
            continue
        fetched_orders.update(part)
        for row in rows:
            order_key = row.get("ЗаказКлиента_Key") or ""
            if order_key and order_key != _EMPTY:
                by_order.setdefault(order_key, []).append(row)
    for key in fetched_orders:
        found[key] = _pick(by_order.get(key) or [])
    return found


def _fresh_percent(entry: object, today: str) -> tuple[float | None, bool]:
    if not isinstance(entry, dict) or entry.get("day") != today or "pct" not in entry:
        return None, False
    pct = entry.get("pct")
    if pct is None:
        return None, True
    try:
        return float(pct), True
    except (TypeError, ValueError):
        return None, False


def probabilities_for_lines(rows: list[dict]) -> dict[str, float | None]:
    """guid документа строки -> процент. Значение из 1С обновляется раз в день."""
    needed: dict[str, str] = {}
    for row in rows:
        kind, guid = line_source(row)
        if not guid or guid == _EMPTY:
            continue
        needed[guid] = "proposal" if kind == "КоммерческоеПредложениеКлиенту" else "order" if kind == "ЗаказКлиента" else ""
    today = date.today().isoformat()
    cache = _load()
    fresh: dict[str, float | None] = {}
    missing_proposals: set[str] = set()
    missing_orders: set[str] = set()
    for guid, kind in needed.items():
        pct, is_fresh = _fresh_percent(cache.get(guid), today)
        if is_fresh:
            fresh[guid] = pct
        elif kind == "proposal":
            missing_proposals.add(guid)
        elif kind == "order":
            missing_orders.add(guid)
    if missing_proposals or missing_orders:
        fetched = _fetch(missing_proposals, missing_orders)
        for guid, pct in fetched.items():
            cache[guid] = {"pct": pct, "day": today}
            fresh[guid] = pct
        _save(cache)
    return fresh


def attach(rows: list[dict]) -> None:
    percents = probabilities_for_lines(rows)
    for row in rows:
        _kind, guid = line_source(row)
        if guid in percents and percents[guid] is not None:
            row["conversion_pct"] = percents[guid]
        elif "conversion_pct" not in row:
            row["conversion_pct"] = None
