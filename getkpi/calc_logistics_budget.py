from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from . import calc_budget_limit
from .cache_manager import CACHE_DIR
from .calc_budget_limit import AUTH, EMPTY, period_bounds

SOURCE_TAG = "logistics_budget_v5_month_plan_cumulative_fact"
REQUEST_DOC_ENTITY = "Document_ЗаявкаНаРасходованиеДенежныхСредств"
LOGISTICS_CFO_NAME = "Директор по логистике"

MONTH_NAMES = {
    1: "январь",
    2: "февраль",
    3: "март",
    4: "апрель",
    5: "май",
    6: "июнь",
    7: "июль",
    8: "август",
    9: "сентябрь",
    10: "октябрь",
    11: "ноябрь",
    12: "декабрь",
}

LOGISTICS_BUDGET_PLAN_ROWS = (
    (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 196_676),
    (69_972, 80_810, 98_654, 97_001, 91_626, 96_214, 106_302, 96_598, 115_877, 105_510, 83_624, 88_054),
    (20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 20_000),
    (3_500, 0, 0, 0, 0, 0, 0, 0, 10_500, 0, 0, 0),
    (82_913, 82_913, 82_913, 63_293, 63_293, 63_293, 63_293, 82_913, 82_913, 82_913, 82_913, 82_913),
    (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    (4_806, 4_806, 4_806, 4_806, 4_806, 4_806, 4_806, 4_806, 4_806, 4_806, 4_806, 4_806),
)


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_budget_{year}_{ref_month:02d}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month) if month is not None else (today.month if ref_year == today.year else 12)
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _normalize_name(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in raw).split())


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 1)


def _plan_by_month() -> list[int]:
    return [sum(row[idx] for row in LOGISTICS_BUDGET_PLAN_ROWS) for idx in range(12)]


def _fetch_all(session: requests.Session, url: str, page: int = 5000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        response = session.get(f"{url}{sep}$top={page}&$skip={skip}", timeout=120)
        response.raise_for_status()
        batch = response.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _load_request_docs(session: requests.Session, refs: set[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    ref_list = sorted(ref for ref in refs if ref and ref != EMPTY)
    select = "Ref_Key,ТД_ЦФО_Key,ТД_ЦФО,Posted,DeletionMark"
    for idx in range(0, len(ref_list), 20):
        batch = ref_list[idx:idx + 20]
        flt = " or ".join(f"Ref_Key eq guid'{ref}'" for ref in batch)
        url = (
            f"{calc_budget_limit.BASE}/{quote(REQUEST_DOC_ENTITY)}"
            f"?$format=json&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        for row in _fetch_all(session, url, page=100):
            ref = row.get("Ref_Key")
            if ref:
                out[ref] = row
    return out


def _request_cfo_values(
    doc: dict[str, Any],
    cfo_by_key: dict[str, dict],
) -> list[str]:
    values: list[str] = []
    for field in ("ТД_ЦФО", "ТД_ЦФО_Key"):
        value = doc.get(field)
        if value not in (None, ""):
            raw = str(value).strip()
            values.append(raw)
            row = cfo_by_key.get(raw)
            if row and row.get("Description"):
                values.append(str(row["Description"]).strip())
    return values


def _load_logistics_cfo_context(session: requests.Session) -> tuple[dict[str, dict], set[str]]:
    url = (
        f"{calc_budget_limit.BASE}/{quote('Catalog_ТД_ЦФО')}"
        "?$format=json&$select=Ref_Key,Description,DeletionMark&$top=1000"
    )
    rows = _fetch_all(session, url)
    cfo_by_key = {str(row.get("Ref_Key")): row for row in rows if row.get("Ref_Key")}
    target = _normalize_name(LOGISTICS_CFO_NAME)
    keys = {
        str(row.get("Ref_Key"))
        for row in rows
        if row.get("Ref_Key")
        and (
            _normalize_name(row.get("Description")) == target
            or target in _normalize_name(row.get("Description"))
        )
    }
    return cfo_by_key, keys


def _is_logistics_request(
    doc: dict[str, Any],
    cfo_by_key: dict[str, dict],
    cfo_keys: set[str],
) -> bool:
    if doc.get("DeletionMark") or doc.get("Posted") is False:
        return False
    cfo_key = str(doc.get("ТД_ЦФО_Key") or "").strip()
    if cfo_key and cfo_key in cfo_keys:
        return True
    target = _normalize_name(LOGISTICS_CFO_NAME)
    for value in _request_cfo_values(doc, cfo_by_key):
        normalized = _normalize_name(value)
        if normalized == target or target in normalized:
            return True
    return False


def _budget_fact_paid_requests(
    session: requests.Session,
    year: int,
    month: int,
    cfo_by_key: dict[str, dict],
    cfo_keys: set[str],
) -> float:
    p_start, p_end = period_bounds(year, month)
    rows = calc_budget_limit.load_records(session, p_start, p_end)
    paid_by_request: dict[str, float] = {}

    for row in rows:
        sign = -1 if row.get("Сторно") else 1
        paid = float(row.get("СуммаОплаты") or 0) * sign
        if paid <= 0:
            continue
        request_key = row.get("ЗаявкаНаРасходованиеДенежныхСредств_Key")
        if not request_key or request_key == EMPTY:
            continue
        paid_by_request[request_key] = paid_by_request.get(request_key, 0.0) + paid

    if not paid_by_request:
        return 0.0

    docs = _load_request_docs(session, set(paid_by_request))
    total = 0.0
    for ref, paid in paid_by_request.items():
        doc = docs.get(ref)
        if doc and _is_logistics_request(doc, cfo_by_key, cfo_keys):
            total += paid
    return round(total, 2)


def _month_row(
    year: int,
    month: int,
    period_plan: float,
    period_fact: float | None,
    cumulative_fact: float | None,
    total_plan: float,
) -> dict:
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "plan": round(float(period_plan), 2),
        "fact": round(float(cumulative_fact), 2) if cumulative_fact is not None else None,
        "period_plan": round(float(period_plan), 2),
        "period_fact": round(float(period_fact), 2) if period_fact is not None else None,
        "kpi_pct": _kpi_pct(float(total_plan), float(cumulative_fact)) if cumulative_fact is not None else None,
        "has_data": True,
        "values_unit": "руб.",
    }


def get_logistics_budget_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = _cache_path(ref_year, ref_month)
    cached = _load_json(path)
    if cached and cached.get("source") == SOURCE_TAG and cached.get("cache_date") == today.isoformat():
        return cached

    plans = _plan_by_month()
    facts_by_month: dict[int, float] = {}
    session = requests.Session()
    session.auth = AUTH
    cfo_by_key, cfo_keys = _load_logistics_cfo_context(session)
    for mm in range(1, ref_month + 1):
        facts_by_month[mm] = _budget_fact_paid_requests(session, ref_year, mm, cfo_by_key, cfo_keys)

    total_plan = sum(plans)
    running_fact = 0.0
    months = []
    for mm in range(1, 13):
        period_fact = facts_by_month.get(mm)
        if period_fact is not None:
            running_fact += float(period_fact or 0)
        months.append(
            _month_row(
                ref_year,
                mm,
                plans[mm - 1],
                period_fact,
                running_fact if mm <= ref_month else None,
                total_plan,
            )
        )
    total_fact = sum(float(value or 0) for value in facts_by_month.values())
    fact_months = [row for row in months[:ref_month] if row.get("fact") is not None]

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months,
        "last_full_month_row": months[ref_month - 1],
        "ytd": {
            "total_plan": round(total_plan, 2),
            "total_fact": round(total_fact, 2),
            "kpi_pct": _kpi_pct(float(total_plan), float(total_fact)),
            "months_with_data": len(fact_months),
            "months_total": 12,
            "values_unit": "руб.",
        },
        "kpi_period": {
            "type": "year",
            "year": ref_year,
        },
    }
    _save_json(path, payload)
    return payload


__all__ = ["get_logistics_budget_monthly"]
