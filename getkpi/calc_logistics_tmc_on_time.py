from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests

from .calc_budget_limit import AUTH, BASE
from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period

SOURCE_TAG = "logistics_tmc_on_time_v1"
RECEIPT_ENTITY = "Document_ПриобретениеТоваровУслуг"
ORDER_ENTITY = "Document_ЗаказПоставщику"
TABULAR_FIELD = "Товары"


def cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_tmc_on_time_{year}_{ref_month:02d}.json"


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


def _period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _fetch_all(session: requests.Session, url: str, page: int = 300) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        r = session.get(f"{url}{sep}$top={page}&$skip={skip}", timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _to_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", ""))
    except ValueError:
        return None


def _is_empty_guid(value) -> bool:
    return not value or str(value) == "00000000-0000-0000-0000-000000000000"


def _kpi_pct(plan: float, fact: float) -> float | None:
    return round(fact / plan * 100, 1) if plan > 0 else None


def _load_receipts(session: requests.Session, year: int, month: int) -> list[dict]:
    p_start, p_end = _period_bounds(year, month)
    flt = (
        "Posted eq true and DeletionMark eq false"
        f" and Date ge datetime'{p_start}' and Date lt datetime'{p_end}'"
    )
    url = f"{BASE}/{quote(RECEIPT_ENTITY)}?$format=json&$filter={quote(flt, safe='')}"
    return _fetch_all(session, url)


def _load_orders_batch(session: requests.Session, order_keys: set[str]) -> dict[str, dict | None]:
    result: dict[str, dict | None] = {
        key: None for key in order_keys if not _is_empty_guid(key)
    }
    keys = list(result.keys())
    chunk_size = 25
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i:i + chunk_size]
        if not chunk:
            continue
        refs = " or ".join(f"Ref_Key eq guid'{key}'" for key in chunk)
        flt = f"Posted eq true and DeletionMark eq false and ({refs})"
        url = f"{BASE}/{quote(ORDER_ENTITY)}?$format=json&$filter={quote(flt, safe='')}"
        for order in _fetch_all(session, url):
            key = order.get("Ref_Key")
            if key:
                result[str(key)] = order
    return result


def _receipt_order_key(doc: dict, row: dict) -> str | None:
    for src in (row, doc):
        for key in (
            "ЗаказПоставщику_Key",
            "Заказ_Key",
            "Распоряжение_Key",
            "Назначение_Key",
            "ДокументОснование_Key",
        ):
            value = src.get(key)
            if not _is_empty_guid(value):
                return str(value)
    return None


def _row_identity(row: dict) -> tuple[str, str]:
    return (
        str(row.get("Номенклатура_Key") or row.get("Номенклатура") or ""),
        str(row.get("Характеристика_Key") or row.get("Характеристика") or ""),
    )


def _planned_date_from_order(order: dict | None, receipt_row: dict) -> datetime | None:
    if not order:
        return None
    target_n, target_h = _row_identity(receipt_row)
    fallback = None
    for order_row in order.get(TABULAR_FIELD) or []:
        if not isinstance(order_row, dict):
            continue
        plan_dt = _parse_dt(order_row.get("ДатаПоступления"))
        if plan_dt and fallback is None:
            fallback = plan_dt
        order_n, order_h = _row_identity(order_row)
        if target_n and order_n == target_n and (not target_h or target_h == order_h):
            return plan_dt or fallback
    return fallback or _parse_dt(order.get("ЖелаемаяДатаПоступления")) or _parse_dt(order.get("ДатаПоступления"))


def _build_month(session: requests.Session, year: int, month: int) -> dict:
    receipts = _load_receipts(session, year, month)
    order_keys: set[str] = set()
    for doc in receipts:
        for row in doc.get(TABULAR_FIELD) or []:
            if isinstance(row, dict):
                order_key = _receipt_order_key(doc, row)
                if order_key:
                    order_keys.add(order_key)
    order_cache = _load_orders_batch(session, order_keys)

    total = 0
    on_time = 0
    overdue = 0
    without_order = 0
    debug_rows = []

    for doc in receipts:
        fact_dt = _parse_dt(doc.get("Date"))
        if not fact_dt:
            continue
        for row in doc.get(TABULAR_FIELD) or []:
            if not isinstance(row, dict):
                continue
            if _is_empty_guid(row.get("Номенклатура_Key")):
                continue
            if _to_float(row.get("Количество")) <= 0:
                continue

            order_key = _receipt_order_key(doc, row)
            order = order_cache.get(order_key) if order_key else None
            plan_dt = _planned_date_from_order(order, row)
            if not order or not plan_dt:
                without_order += 1
                continue

            total += 1
            if fact_dt.date() <= plan_dt.date():
                on_time += 1
            else:
                overdue += 1
            if len(debug_rows) < 50:
                debug_rows.append({
                    "receipt": doc.get("Number"),
                    "receipt_date": doc.get("Date"),
                    "order_key": order_key,
                    "plan_date": plan_dt.isoformat(),
                    "nomenclature_key": row.get("Номенклатура_Key"),
                    "qty": row.get("Количество"),
                    "on_time": fact_dt.date() <= plan_dt.date(),
                })

    pct = _kpi_pct(total, on_time)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month].lower(),
        "plan": total,
        "fact": on_time,
        "kpi_pct": pct,
        "has_data": total > 0 or without_order > 0,
        "values_unit": "поставок",
        "total_deliveries": total,
        "on_time": on_time,
        "overdue": overdue,
        "without_order": without_order,
        "overdue_pct": _kpi_pct(total, overdue),
        "without_order_count": without_order,
        "debug": {
            "receipts_count": len(receipts),
            "orders_linked": len(order_keys),
            "orders_loaded": sum(1 for value in order_cache.values() if value),
            "metric_variant": "B: строки поступления ТМЦ",
            "sample_rows": debug_rows,
        },
    }


def _aggregate(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    by_q: dict[tuple[int, int], dict] = {}
    by_y: dict[int, dict] = {}
    for row in rows:
        y = int(row["year"])
        m = int(row["month"])
        qn = (m - 1) // 3 + 1
        for store, key, extra in (
            (by_q, (y, qn), {"year": y, "quarter": qn, "label": f"Q{qn} {y}"}),
            (by_y, y, {"year": y}),
        ):
            target = store.setdefault(key, {
                **extra,
                "plan": 0,
                "fact": 0,
                "overdue": 0,
                "without_order": 0,
                "has_data": False,
                "values_unit": "поставок",
            })
            target["plan"] += int(row.get("plan") or 0)
            target["fact"] += int(row.get("fact") or 0)
            target["overdue"] += int(row.get("overdue") or 0)
            target["without_order"] += int(row.get("without_order") or 0)
            target["has_data"] = target["has_data"] or bool(row.get("has_data"))

    def finalize(items):
        out = []
        for item in items:
            item["kpi_pct"] = _kpi_pct(item["plan"], item["fact"])
            item["overdue_pct"] = _kpi_pct(item["plan"], item["overdue"])
            out.append(item)
        return out

    return (
        finalize(sorted(by_q.values(), key=lambda r: (r["year"], r["quarter"]))),
        finalize(sorted(by_y.values(), key=lambda r: r["year"])),
    )


def get_logistics_tmc_on_time_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(ref_year, ref_month)
    cached = _load_json(path)
    if cached and cached.get("source") == SOURCE_TAG and cached.get("cache_date") == today.isoformat():
        return cached

    session = requests.Session()
    session.auth = AUTH
    months = [_build_month(session, ref_year, mm) for mm in range(1, ref_month + 1)]
    quarterly_data, yearly_data = _aggregate(months)
    total_plan = sum(int(row.get("plan") or 0) for row in months)
    total_fact = sum(int(row.get("fact") or 0) for row in months)
    total_overdue = sum(int(row.get("overdue") or 0) for row in months)
    total_without_order = sum(int(row.get("without_order") or 0) for row in months)

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months,
        "quarterly_data": quarterly_data,
        "yearly_data": yearly_data,
        "last_full_month_row": dict(months[-1]) if months else None,
        "ytd": {
            "total_plan": total_plan,
            "total_fact": total_fact,
            "kpi_pct": _kpi_pct(total_plan, total_fact),
            "overdue": total_overdue,
            "overdue_pct": _kpi_pct(total_plan, total_overdue),
            "without_order": total_without_order,
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
            "values_unit": "поставок",
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
    }
    _save_json(path, payload)
    return payload


__all__ = ["cache_path", "get_logistics_tmc_on_time_monthly"]
