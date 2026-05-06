from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests

from .calc_budget_limit import AUTH, BASE
from .calc_fot_management import MONTH_RU, _normalize_period
from .cache_manager import CACHE_DIR

SOURCE_TAG = "logistics_supplier_share_v3"
ENTITIES = (
    "InformationRegister_ТД_ОценкаПоставщиков_RecordType",
    "InformationRegister_ТД_ОценкаПоставщиков",
)
LOYAL_POINTS_THRESHOLD = 45.0


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


def _period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


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


def _load_rating_rows(session: requests.Session, year: int, month: int) -> tuple[list[dict], str | None, str | None]:
    p_start, p_end = _period_bounds(year, month)
    flt = f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
    select = "Period,Партнер_Key,Контрагент_Key,Партнер,Контрагент,ВидКритерия,ГруппаКритериев,ЗначениеКритерия,КоличествоБаллов"
    last_error = None
    for entity in ENTITIES:
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
            continue
        except requests.RequestException as exc:
            last_error = f"{entity}: {exc}"
            continue
    return [], None, last_error or "supplier rating register is unavailable"


def _build_month(session: requests.Session, year: int, month: int) -> dict:
    rows, entity, error = _load_rating_rows(session, year, month)
    if error and not rows:
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU[month].lower(),
            "plan": None,
            "fact": None,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": "поставщиков",
            "total_suppliers": None,
            "loyal_suppliers": None,
            "threshold_points": LOYAL_POINTS_THRESHOLD,
            "rows_count": 0,
            "source_entity": entity,
            "source_error": error,
            "debug": {
                "metric": "unique suppliers with total КоличествоБаллов > 45",
                "rows_count": 0,
                "supplier_count": 0,
                "source_entity": entity,
                "source_error": error,
                "sample_rows": [],
            },
            "_supplier_scores": {},
            "_supplier_rows": {},
        }

    supplier_scores: dict[str, float] = defaultdict(float)
    supplier_rows: dict[str, int] = defaultdict(int)
    sample_rows = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        supplier = _supplier_key(row)
        if not supplier:
            continue
        supplier_scores[supplier] += _to_float(row.get("КоличествоБаллов"))
        supplier_rows[supplier] += 1
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

    plan = len(supplier_scores)
    fact = sum(1 for score in supplier_scores.values() if score > LOYAL_POINTS_THRESHOLD)
    kpi_pct = round(fact / plan * 100, 1) if plan > 0 else None

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month].lower(),
        "plan": plan,
        "fact": fact,
        "kpi_pct": kpi_pct,
        "has_data": bool(rows),
        "values_unit": "поставщиков",
        "total_suppliers": plan,
        "loyal_suppliers": fact,
        "threshold_points": LOYAL_POINTS_THRESHOLD,
        "rows_count": len(rows),
        "source_entity": entity,
        "source_error": error,
        "debug": {
            "metric": "unique suppliers with total КоличествоБаллов > 45",
            "rows_count": len(rows),
            "supplier_count": plan,
            "source_entity": entity,
            "source_error": error,
            "sample_rows": sample_rows,
        },
        "_supplier_scores": dict(supplier_scores),
        "_supplier_rows": dict(supplier_rows),
    }


def _public_row(row: dict) -> dict:
    return {k: v for k, v in row.items() if not k.startswith("_")}


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
                "kpi_pct": None,
                "has_data": False,
                "values_unit": "поставщиков",
                "_supplier_scores": defaultdict(float),
            })
            for supplier, score in (row.get("_supplier_scores") or {}).items():
                target["_supplier_scores"][supplier] += float(score or 0)
            target["has_data"] = target["has_data"] or bool(row.get("has_data"))

    def finalize(items):
        out = []
        for item in items:
            scores = item.pop("_supplier_scores", {}) or {}
            if not item["has_data"] and not scores:
                item["plan"] = None
                item["fact"] = None
                item["kpi_pct"] = None
            else:
                item["plan"] = len(scores)
                item["fact"] = sum(1 for score in scores.values() if score > LOYAL_POINTS_THRESHOLD)
                item["kpi_pct"] = round(item["fact"] / item["plan"] * 100, 1) if item["plan"] > 0 else None
            item["total_suppliers"] = item["plan"]
            item["loyal_suppliers"] = item["fact"]
            item["threshold_points"] = LOYAL_POINTS_THRESHOLD
            out.append(item)
        return out

    return (
        finalize(sorted(by_q.values(), key=lambda r: (r["year"], r["quarter"]))),
        finalize(sorted(by_y.values(), key=lambda r: r["year"])),
    )


def get_logistics_supplier_share_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(ref_year, ref_month)
    cached = _load_json(path)
    if cached and cached.get("source") == SOURCE_TAG and cached.get("cache_date") == today.isoformat():
        return cached

    session = requests.Session()
    session.auth = AUTH
    raw_months = [_build_month(session, ref_year, mm) for mm in range(1, ref_month + 1)]
    quarterly_data, yearly_data = _aggregate(raw_months)
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
