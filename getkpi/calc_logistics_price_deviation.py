from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

from .calc_budget_limit import AUTH, BASE
from .calc_fot_management import MONTH_RU, _normalize_period
from .cache_manager import CACHE_DIR

SOURCE_TAG = "logistics_price_deviation_v3_weighted_period_colors"
RECEIPT_ENTITY = "Document_ПриобретениеТоваровУслуг"
PRICE_ENTITY = "InformationRegister_ЦеныНоменклатуры_RecordType"
TABULAR_FIELD = "Товары"
PROJECT_PRICE_TYPE_KEY = "25b38f16-6d23-11e7-812d-001e67112509"
EMPTY = "00000000-0000-0000-0000-000000000000"
TARGET_DEVIATION_PCT = 5.0
RUB_KEY = "3fdf75b4-6252-11e7-812d-001e67112509"
EXCLUDED_NOMENCLATURE_TYPES = {"услуга", "работа"}

CURRENCY_CODES = {
    "0a7c6f22-e1b6-11df-963e-001cc4d04388": "USD",
    "d328a18d-7405-11e0-81cd-001583b3d75c": "EUR",
    "095e2c36-45dc-11ec-8756-ac1f6b05524d": "BYN",
    "e2bc7bc0-de2e-11ef-95fc-6cb31113810e": "KZT",
    RUB_KEY: "RUB",
}

EXCHANGE_RATES_TO_RUB = {
    "RUB": 1.0,
    "USD": 90.0,
    "EUR": 98.0,
    "BYN": 28.0,
    "KZT": 0.19,
}


def cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_price_deviation_{year}_{ref_month:02d}.json"


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
    return not value or str(value) == EMPTY


def _guid(value) -> str:
    return "" if _is_empty_guid(value) else str(value)


def _currency_code(value) -> str:
    return CURRENCY_CODES.get(str(value or RUB_KEY), "RUB")


def _to_rub(value: float, currency_key: str | None) -> float:
    code = _currency_code(currency_key)
    return value * EXCHANGE_RATES_TO_RUB.get(code, 1.0)


def _period_start_date(year: int, month: int, scope: str) -> str:
    if scope == "year":
        return f"{year}-01-01"
    if scope == "quarter":
        start_month = ((month - 1) // 3) * 3 + 1
        return f"{year}-{start_month:02d}-01"
    return f"{year}-{month:02d}-01"


def _period_end_date(year: int, month: int) -> str:
    if month == 12:
        return f"{year}-12-31"
    next_month_start = datetime.fromisoformat(f"{year}-{month + 1:02d}-01T00:00:00")
    return (next_month_start.date() - timedelta(days=1)).isoformat()


def _deviation_color(fact_pct: float | None, plan_pct: float = TARGET_DEVIATION_PCT) -> str:
    if fact_pct is None:
        return "unknown"
    if fact_pct < plan_pct:
        return "green"
    if abs(fact_pct - plan_pct) < 1e-9:
        return "yellow"
    return "red"


def _is_excluded_nomenclature_type(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    short_value = normalized.rsplit(".", 1)[-1]
    return short_value in EXCLUDED_NOMENCLATURE_TYPES


def _load_receipts(session: requests.Session, year: int, month: int) -> list[dict]:
    p_start, p_end = _period_bounds(year, month)
    flt = (
        "Posted eq true and DeletionMark eq false"
        f" and Date ge datetime'{p_start}' and Date lt datetime'{p_end}'"
    )
    url = f"{BASE}/{quote(RECEIPT_ENTITY)}?$format=json&$filter={quote(flt, safe='')}"
    return _fetch_all(session, url)


def _load_nomenclature_types(session: requests.Session, nomenclature_keys: set[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    keys = sorted(key for key in nomenclature_keys if key)
    for i in range(0, len(keys), 25):
        chunk = keys[i:i + 25]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in chunk)
        url = (
            f"{BASE}/{quote('Catalog_Номенклатура')}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote('Ref_Key,ТипНоменклатуры', safe=',_')}"
        )
        try:
            rows = _fetch_all(session, url, page=1000)
        except requests.HTTPError:
            return {}
        for row in rows:
            key = _guid(row.get("Ref_Key"))
            if key:
                result[key] = str(row.get("ТипНоменклатуры") or "").strip().lower()
    return result


def _price_identity(row: dict) -> tuple[str, str, str]:
    return (
        _guid(row.get("Номенклатура_Key")),
        _guid(row.get("Характеристика_Key")),
        _guid(row.get("Упаковка_Key")),
    )


def _row_quantity(row: dict) -> float:
    return _to_float(row.get("Количество") or row.get("КоличествоУпаковок"))


def _row_amount_without_vat(row: dict) -> float:
    for field in ("СуммаБезНДС", "Сумма", "СуммаВзаиморасчетов"):
        value = _to_float(row.get(field))
        if value:
            if field == "Сумма" and _to_float(row.get("СуммаНДС")):
                return value - _to_float(row.get("СуммаНДС"))
            return value
    return 0.0


def _actual_unit_price(row: dict) -> float:
    qty = _row_quantity(row)
    amount = _row_amount_without_vat(row)
    if qty > 0 and amount:
        return amount / qty
    return _to_float(row.get("Цена"))


def _load_project_prices(
    session: requests.Session,
    nomenclature_keys: set[str],
    period_end: str,
) -> dict[tuple[str, str, str], list[dict]]:
    result: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    keys = [key for key in sorted(nomenclature_keys) if key]
    chunk_size = 20
    select = "Period,Номенклатура_Key,Характеристика_Key,Упаковка_Key,Валюта_Key,Цена"
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i:i + chunk_size]
        refs = " or ".join(f"Номенклатура_Key eq guid'{key}'" for key in chunk)
        flt = (
            "Active eq true"
            f" and ВидЦены_Key eq guid'{PROJECT_PRICE_TYPE_KEY}'"
            f" and Period lt datetime'{period_end}'"
            f" and ({refs})"
        )
        url = (
            f"{BASE}/{quote(PRICE_ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        for rec in _fetch_all(session, url, page=1000):
            price = _to_float(rec.get("Цена"))
            if price <= 0:
                continue
            result[_price_identity(rec)].append(rec)

    for records in result.values():
        records.sort(key=lambda rec: rec.get("Period") or "")
    return dict(result)


def _lookup_project_price(
    prices: dict[tuple[str, str, str], list[dict]],
    row: dict,
    fact_dt: datetime,
) -> dict | None:
    n_key, h_key, pack_key = _price_identity(row)
    candidates = (
        (n_key, h_key, pack_key),
        (n_key, h_key, ""),
        (n_key, "", pack_key),
        (n_key, "", ""),
    )
    fact_iso = fact_dt.isoformat()
    for key in candidates:
        records = prices.get(key) or []
        for rec in reversed(records):
            period = rec.get("Period")
            if period and str(period).replace("Z", "") <= fact_iso:
                return rec
    return None


def _build_month(session: requests.Session, year: int, month: int) -> dict:
    receipts = _load_receipts(session, year, month)
    _, p_end = _period_bounds(year, month)
    nomenclature_keys: set[str] = set()
    for doc in receipts:
        for row in doc.get(TABULAR_FIELD) or []:
            if isinstance(row, dict):
                key = _guid(row.get("Номенклатура_Key"))
                if key and _row_quantity(row) > 0:
                    nomenclature_keys.add(key)

    nomenclature_types = _load_nomenclature_types(session, nomenclature_keys)
    purchasable_keys = {
        key for key in nomenclature_keys
        if not _is_excluded_nomenclature_type(nomenclature_types.get(key))
    }
    prices = _load_project_prices(session, purchasable_keys, p_end)
    total_rows = 0
    excluded_service_work_rows = 0
    compared_rows = 0
    missing_project_price = 0
    zero_project_price = 0
    currency_converted_rows = 0
    total_project_amount_rub = 0.0
    total_delta_amount_rub = 0.0
    absolute_deviation_sum = 0.0
    relative_deviation_sum = 0.0
    debug_rows = []

    for doc in receipts:
        fact_dt = _parse_dt(doc.get("Date"))
        if not fact_dt:
            continue
        doc_currency = _guid(doc.get("Валюта_Key")) or RUB_KEY
        for row in doc.get(TABULAR_FIELD) or []:
            if not isinstance(row, dict):
                continue
            qty = _row_quantity(row)
            actual_price = _actual_unit_price(row)
            n_key = _guid(row.get("Номенклатура_Key"))
            if not n_key or qty <= 0 or actual_price <= 0:
                continue
            if n_key not in purchasable_keys:
                excluded_service_work_rows += 1
                continue

            total_rows += 1
            project_rec = _lookup_project_price(prices, row, fact_dt)
            if not project_rec:
                missing_project_price += 1
                continue

            project_price = _to_float(project_rec.get("Цена"))
            if project_price <= 0:
                zero_project_price += 1
                continue

            actual_currency = _guid(row.get("Валюта_Key")) or doc_currency
            project_currency = _guid(project_rec.get("Валюта_Key")) or RUB_KEY
            actual_price_rub = _to_rub(actual_price, actual_currency)
            project_price_rub = _to_rub(project_price, project_currency)
            if actual_currency != project_currency:
                currency_converted_rows += 1

            delta = actual_price_rub - project_price_rub
            delta_pct = (actual_price_rub / project_price_rub - 1) * 100
            project_amount_rub = project_price_rub * qty
            total_project_amount_rub += project_amount_rub
            total_delta_amount_rub += delta * qty
            absolute_deviation_sum += delta
            relative_deviation_sum += delta_pct
            compared_rows += 1

            if len(debug_rows) < 50:
                debug_rows.append({
                    "date": doc.get("Date"),
                    "document": doc.get("Number"),
                    "line": row.get("LineNumber"),
                    "nomenclature_key": n_key,
                    "characteristic_key": _guid(row.get("Характеристика_Key")),
                    "quantity": qty,
                    "actual_price": round(actual_price_rub, 4),
                    "project_price": round(project_price_rub, 4),
                    "delta": round(delta, 4),
                    "delta_pct": round(delta_pct, 2),
                    "currency": "RUB",
                    "project_price_period": project_rec.get("Period"),
                    "project_price_missing": False,
                })

    fact_pct = None
    if total_project_amount_rub > 0:
        fact_pct = round(total_delta_amount_rub / total_project_amount_rub * 100, 2)

    project_amount = round(total_project_amount_rub, 2) if total_project_amount_rub > 0 else None
    delta_amount = round(total_delta_amount_rub, 2) if total_project_amount_rub > 0 else None

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month].lower(),
        # Для агрегации нельзя складывать проценты. Поэтому plan/fact — это
        # знаменатель и числитель формулы отклонения, а отображаемый процент
        # лежит отдельно в display_* и kpi_pct.
        "plan": project_amount,
        "fact": delta_amount,
        "kpi_pct": fact_pct,
        "has_data": total_rows > 0,
        "values_unit": "руб.",
        "display_plan": TARGET_DEVIATION_PCT,
        "display_fact": fact_pct,
        "display_unit": "%",
        "color": _deviation_color(fact_pct),
        "aggregation": "weighted_delta_amount_div_project_amount",
        "period_start": _period_start_date(year, month, "month"),
        "period_end": _period_end_date(year, month),
        "total_rows": total_rows,
        "compared_rows": compared_rows,
        "missing_project_price": missing_project_price,
        "zero_project_price": zero_project_price,
        "excluded_service_work_rows": excluded_service_work_rows,
        "currency_converted_rows": currency_converted_rows,
        "avg_absolute_deviation": round(absolute_deviation_sum / compared_rows, 2) if compared_rows else None,
        "avg_relative_deviation": round(relative_deviation_sum / compared_rows, 2) if compared_rows else None,
        "weighted_delta_amount": round(total_delta_amount_rub, 2),
        "project_amount": round(total_project_amount_rub, 2),
        "debug": {
            "receipts_count": len(receipts),
            "nomenclature_count": len(nomenclature_keys),
            "purchasable_nomenclature_count": len(purchasable_keys),
            "price_keys_loaded": len(prices),
            "price_type": "Проектная",
            "price_type_key": PROJECT_PRICE_TYPE_KEY,
            "actual_price_mode": "без НДС: сумма строки / количество",
            "currency_mode": "сравнение в RUB; известные валюты пересчитываются фиксированными курсами",
            "excluded_nomenclature_types": sorted(EXCLUDED_NOMENCLATURE_TYPES),
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
                "plan": None,
                "fact": None,
                "kpi_pct": None,
                "has_data": False,
                "values_unit": "руб.",
                "display_plan": TARGET_DEVIATION_PCT,
                "display_fact": None,
                "display_unit": "%",
                "color": "unknown",
                "aggregation": "weighted_delta_amount_div_project_amount",
                "total_rows": 0,
                "compared_rows": 0,
                "missing_project_price": 0,
                "zero_project_price": 0,
                "excluded_service_work_rows": 0,
                "weighted_delta_amount": 0.0,
                "project_amount": 0.0,
                "max_month": 0,
            })
            target["total_rows"] += int(row.get("total_rows") or 0)
            target["compared_rows"] += int(row.get("compared_rows") or 0)
            target["missing_project_price"] += int(row.get("missing_project_price") or 0)
            target["zero_project_price"] += int(row.get("zero_project_price") or 0)
            target["excluded_service_work_rows"] += int(row.get("excluded_service_work_rows") or 0)
            target["weighted_delta_amount"] += float(row.get("weighted_delta_amount") or 0)
            target["project_amount"] += float(row.get("project_amount") or 0)
            target["max_month"] = max(int(target.get("max_month") or 0), m)
            target["has_data"] = target["has_data"] or bool(row.get("has_data"))

    def finalize(items):
        out = []
        for item in items:
            project_amount = round(item["project_amount"], 2)
            delta_amount = round(item["weighted_delta_amount"], 2)
            item["plan"] = project_amount if item["project_amount"] > 0 else None
            item["fact"] = delta_amount if item["project_amount"] > 0 else None
            if item["project_amount"] > 0:
                item["kpi_pct"] = round(item["weighted_delta_amount"] / item["project_amount"] * 100, 2)
                item["display_fact"] = item["kpi_pct"]
            item["color"] = _deviation_color(item.get("display_fact"))
            item["period_start"] = _period_start_date(
                int(item["year"]),
                int(item.get("quarter", 1)) * 3 if "quarter" in item else 1,
                "quarter" if "quarter" in item else "year",
            )
            item["period_end"] = _period_end_date(int(item["year"]), int(item.get("max_month") or 12))
            item["weighted_delta_amount"] = delta_amount
            item["project_amount"] = project_amount
            item.pop("max_month", None)
            out.append(item)
        return out

    return (
        finalize(sorted(by_q.values(), key=lambda r: (r["year"], r["quarter"]))),
        finalize(sorted(by_y.values(), key=lambda r: r["year"])),
    )


def get_logistics_price_deviation_monthly(year: int | None = None, month: int | None = None) -> dict:
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
    ytd_row = yearly_data[-1] if yearly_data else {}

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
            "total_plan": ytd_row.get("plan"),
            "total_fact": ytd_row.get("fact"),
            "kpi_pct": ytd_row.get("kpi_pct"),
            "display_plan": TARGET_DEVIATION_PCT,
            "display_fact": ytd_row.get("kpi_pct"),
            "display_unit": "%",
            "color": _deviation_color(ytd_row.get("kpi_pct")),
            "total_rows": ytd_row.get("total_rows", 0),
            "compared_rows": ytd_row.get("compared_rows", 0),
            "missing_project_price": ytd_row.get("missing_project_price", 0),
            "zero_project_price": ytd_row.get("zero_project_price", 0),
            "excluded_service_work_rows": ytd_row.get("excluded_service_work_rows", 0),
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
            "values_unit": "руб.",
            "aggregation": "weighted_delta_amount_div_project_amount",
            "period_start": _period_start_date(ref_year, ref_month, "year"),
            "period_end": _period_end_date(ref_year, ref_month),
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


__all__ = ["cache_path", "get_logistics_price_deviation_monthly"]
