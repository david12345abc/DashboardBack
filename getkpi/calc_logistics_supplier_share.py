from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests

from .calc_budget_limit import AUTH, BASE
from .calc_fot_management import MONTH_RU, _normalize_period
from .cache_manager import CACHE_DIR

SOURCE_TAG = "logistics_supplier_share_v7_monthly_grouped_supplier_scores"
ENTITIES = (
    "InformationRegister_ТД_ОценкаПоставщиков_RecordType",
    "InformationRegister_ТД_ОценкаПоставщиков",
    "ТД_ОценкаПоставщиков_RecordType",
    "ТД_ОценкаПоставщиков",
)
QUALIFIED_POINTS_THRESHOLD = 45.0


def cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_supplier_share_{year}_{ref_month:02d}.json"


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


def _period_bounds(year: int, start_month: int, end_month: int) -> tuple[str, str]:
    if end_month == 12:
        return f"{year}-{start_month:02d}-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{start_month:02d}-01T00:00:00", f"{year}-{end_month + 1:02d}-01T00:00:00"


def _fetch_all(session: requests.Session, url: str, page: int = 500) -> list[dict]:
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


def _discover_rating_entities(session: requests.Session) -> list[str]:
    try:
        r = session.get(f"{BASE}?$format=json", timeout=60)
        r.raise_for_status()
        items = r.json().get("value", []) or []
    except (requests.RequestException, ValueError, TypeError):
        return []

    found = []
    for item in items:
        name = str(item.get("name") or item.get("url") or "")
        if not name:
            continue
        if name.endswith("ТД_ОценкаПоставщиков") or name.endswith("ТД_ОценкаПоставщиков_RecordType"):
            found.append(name)
    return found


def _to_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _supplier_key(row: dict) -> str:
    partner = row.get("Партнер_Key") or row.get("Партнер")
    contragent = row.get("Контрагент_Key") or row.get("Контрагент")
    return str(partner or contragent or "")


def _group_key(row: dict) -> tuple[str, str, str, str] | None:
    supplier = _supplier_key(row)
    if not supplier:
        return None
    contragent = str(row.get("Контрагент_Key") or row.get("Контрагент") or "")
    criterion = str(row.get("ВидКритерия") or "")
    criteria_group = str(row.get("ГруппаКритериев") or "")
    return supplier, contragent, criterion, criteria_group


def _load_rating_rows(
    session: requests.Session,
    year: int,
    start_month: int,
    end_month: int,
) -> tuple[list[dict], str | None, str | None]:
    p_start, p_end = _period_bounds(year, start_month, end_month)
    flt = f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
    select = "Period,Партнер_Key,Контрагент_Key,Партнер,Контрагент,ВидКритерия,ГруппаКритериев,ЗначениеКритерия,КоличествоБаллов"
    last_error = None
    candidates = list(dict.fromkeys([*_discover_rating_entities(session), *ENTITIES]))
    for entity in candidates:
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        try:
            return _fetch_all(session, url), entity, None
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            last_error = f"{entity}: HTTP {status}"
            if status in {400, 404}:
                # If select is too strict for this publication, retry without it.
                try:
                    url = f"{BASE}/{quote(entity)}?$format=json&$filter={quote(flt, safe='')}"
                    return _fetch_all(session, url), entity, None
                except requests.HTTPError as retry_exc:
                    status = retry_exc.response.status_code if retry_exc.response is not None else None
                    last_error = f"{entity}: HTTP {status}"
                    continue
            if status in {401, 403}:
                return [], entity, last_error
            continue
        except requests.RequestException as exc:
            last_error = f"{entity}: {exc}"
            continue
    return [], None, last_error or "supplier rating register is unavailable"


def _period_label(start_month: int, end_month: int) -> str:
    if start_month == end_month:
        return MONTH_RU[start_month].lower()
    return f"{MONTH_RU[start_month].lower()}-{MONTH_RU[end_month].lower()}"


def _build_period(
    session: requests.Session,
    year: int,
    start_month: int,
    end_month: int,
    extra: dict | None = None,
) -> dict:
    rows, entity, error = _load_rating_rows(session, year, start_month, end_month)
    extra = extra or {}
    if error and not rows:
        return {
            **extra,
            "year": year,
            "month": end_month,
            "month_name": _period_label(start_month, end_month),
            "plan": None,
            "fact": None,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": "поставщиков",
            "total_suppliers": None,
            "qualified_suppliers": None,
            "threshold_points": QUALIFIED_POINTS_THRESHOLD,
            "rows_count": 0,
            "grouped_rows_count": 0,
            "source_entity": entity,
            "source_error": error,
            "debug": {
                "metric": "unique suppliers from grouped rows where SUM(КоличествоБаллов) > 45",
                "rows_count": 0,
                "grouped_rows_count": 0,
                "supplier_count": 0,
                "source_entity": entity,
                "source_error": error,
                "sample_rows": [],
            },
            "_supplier_scores": {},
            "_qualified_suppliers": [],
            "_all_suppliers": [],
        }

    grouped: dict[tuple[str, str, str, str], dict] = {}
    sample_rows = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _group_key(row)
        if key is None:
            continue
        group = grouped.setdefault(key, {
            "supplier": key[0],
            "contragent": key[1],
            "criterion": key[2],
            "criteria_group": key[3],
            "criterion_value_sum": 0.0,
            "criterion_value_count": 0,
            "points": 0.0,
        })
        criterion_value = row.get("ЗначениеКритерия")
        if criterion_value not in (None, ""):
            group["criterion_value_sum"] += _to_float(criterion_value)
            group["criterion_value_count"] += 1
        group["points"] += _to_float(row.get("КоличествоБаллов"))

        if len(sample_rows) < 50:
            sample_rows.append({
                "period": row.get("Period"),
                "partner": row.get("Партнер_Key") or row.get("Партнер"),
                "contragent": row.get("Контрагент_Key") or row.get("Контрагент"),
                "criterion": row.get("ВидКритерия"),
                "criteria_group": row.get("ГруппаКритериев"),
                "criterion_value": row.get("ЗначениеКритерия"),
                "points": row.get("КоличествоБаллов"),
            })

    grouped_rows = []
    all_suppliers = set()
    supplier_scores: dict[str, float] = {}
    for group in grouped.values():
        supplier = group["supplier"]
        all_suppliers.add(supplier)
        points = round(float(group["points"] or 0), 2)
        supplier_scores[supplier] = round(supplier_scores.get(supplier, 0.0) + points, 2)
        value_count = int(group["criterion_value_count"] or 0)
        grouped_rows.append({
            "supplier": supplier,
            "contragent": group["contragent"],
            "criterion": group["criterion"],
            "criteria_group": group["criteria_group"],
            "criterion_value_avg": (
                round(float(group["criterion_value_sum"]) / value_count, 2)
                if value_count > 0 else None
            ),
            "points": points,
        })

    qualified_suppliers = {
        supplier
        for supplier, points in supplier_scores.items()
        if points > QUALIFIED_POINTS_THRESHOLD
    }
    plan = len(all_suppliers)
    fact = len(qualified_suppliers)
    kpi_pct = round(fact / plan * 100, 1) if plan > 0 else None

    return {
        **extra,
        "year": year,
        "month": end_month,
        "month_name": _period_label(start_month, end_month),
        "plan": plan,
        "fact": fact,
        "kpi_pct": kpi_pct,
        "has_data": bool(rows),
        "values_unit": "поставщиков",
        "total_suppliers": plan,
        "qualified_suppliers": fact,
        "threshold_points": QUALIFIED_POINTS_THRESHOLD,
        "rows_count": len(rows),
        "grouped_rows_count": len(grouped_rows),
        "source_entity": entity,
        "source_error": error,
        "debug": {
            "metric": "unique suppliers where total grouped SUM(КоличествоБаллов) > 45",
            "rows_count": len(rows),
            "grouped_rows_count": len(grouped_rows),
            "supplier_count": plan,
            "source_entity": entity,
            "source_error": error,
            "sample_rows": sample_rows,
            "sample_grouped_rows": grouped_rows[:50],
            "sample_supplier_scores": [
                {"supplier": supplier, "points": points}
                for supplier, points in list(sorted(supplier_scores.items()))[:50]
            ],
        },
        "_supplier_scores": dict(supplier_scores),
        "_qualified_suppliers": sorted(qualified_suppliers),
        "_all_suppliers": sorted(all_suppliers),
    }


def _public_row(row: dict) -> dict:
    return {k: v for k, v in row.items() if not k.startswith("_")}


def _build_quarters(session: requests.Session, year: int, ref_month: int) -> list[dict]:
    result = []
    for quarter in range(1, (ref_month - 1) // 3 + 2):
        start_month = (quarter - 1) * 3 + 1
        end_month = min(start_month + 2, ref_month)
        result.append(_build_period(
            session,
            year,
            start_month,
            end_month,
            {"quarter": quarter, "label": f"Q{quarter} {year}"},
        ))
    return result


def get_logistics_supplier_share_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(ref_year, ref_month)
    cached = _load_json(path)
    if cached and cached.get("source") == SOURCE_TAG and cached.get("cache_date") == today.isoformat():
        return cached

    session = requests.Session()
    session.auth = AUTH
    raw_months = [
        _build_period(session, ref_year, mm, mm)
        for mm in range(1, ref_month + 1)
    ]
    quarterly_data = [_public_row(row) for row in _build_quarters(session, ref_year, ref_month)]
    yearly_data = [_public_row(_build_period(session, ref_year, 1, ref_month))]
    months = [_public_row(row) for row in raw_months]
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
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
            "values_unit": "поставщиков",
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


__all__ = ["cache_path", "get_logistics_supplier_share_monthly"]
