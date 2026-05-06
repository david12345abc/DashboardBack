from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import requests

from .calc_budget_limit import AUTH, BASE
from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period

ShopKey = Literal["pc1", "pc2"]

SOURCE_TAG = "prod_deputy_output_fact_production_plan_v2"
DOC_ENTITY = "Document_ТД_ПроизводственныйПлан"
TABULAR_FIELD = "ВыполнениеПроизводственногоПлана"

PRODUCTION_DEPT_KEY: dict[ShopKey, str] = {
    "pc1": "3a9ac2d6-214f-11e0-b91c-00248c26ee57",  # ПРОИЗВОДСТВО НПО
    "pc2": "88cbfc9b-83ed-11e6-8121-001e67112509",  # ПРОИЗВОДСТВО АЛМАЗ
}

OUTPUT_PLAN: dict[ShopKey, list[float]] = {
    "pc1": [
        51_850_261, 40_528_324, 112_879_583, 131_788_552,
        147_474_990, 187_746_649, 158_217_075, 133_320_522,
        168_974_477, 111_959_640, 102_591_730, 203_883_850,
    ],
    "pc2": [
        763, 1_537, 4_534, 5_708,
        6_429, 8_735, 10_714, 11_863,
        10_677, 9_334, 4_403, 2_844,
    ],
}

VALUES_UNIT: dict[ShopKey, str] = {
    "pc1": "руб.",
    "pc2": "шт.",
}


def cache_path(shop: ShopKey, year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"prod_deputy_output_{shop}_{year}_{ref_month:02d}.json"


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


def _kpi_pct(plan: float, fact: float) -> float | None:
    return round(fact / plan * 100, 1) if plan > 0 else None


def _to_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


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


def _load_month_docs(session: requests.Session, shop: ShopKey, year: int, month: int) -> list[dict]:
    p_start, p_end = _period_bounds(year, month)
    flt = (
        "Posted eq true and DeletionMark eq false"
        f" and Подразделение_Key eq guid'{PRODUCTION_DEPT_KEY[shop]}'"
        f" and ПериодС ge datetime'{p_start}' and ПериодС lt datetime'{p_end}'"
    )
    sel = "Ref_Key,Number,Date,ПериодС,ПериодПо,Подразделение_Key,ВыполнениеПроизводственногоПлана"
    url = (
        f"{BASE}/{quote(DOC_ENTITY)}?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    return _fetch_all(session, url)


def _fact_from_production_plan(
    session: requests.Session,
    shop: ShopKey,
    year: int,
    month: int,
) -> tuple[float, dict]:
    docs = _load_month_docs(session, shop, year, month)
    fact_field = "ФактРуб" if shop == "pc1" else "ФактШт"
    total = 0.0
    doc_debug = []

    for doc in docs:
        doc_total = 0.0
        for row in doc.get(TABULAR_FIELD) or []:
            doc_total += _to_float(row.get(fact_field))
        total += doc_total
        doc_debug.append({
            "number": doc.get("Number"),
            "date": doc.get("Date"),
            "period_from": doc.get("ПериодС"),
            "period_to": doc.get("ПериодПо"),
            "fact": round(doc_total, 2),
            "rows": len(doc.get(TABULAR_FIELD) or []),
        })

    return round(total, 2), {
        "documents_count": len(docs),
        "fact_field": fact_field,
        "documents": doc_debug,
    }


def _aggregate_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    by_quarter: dict[tuple[int, int], dict] = {}
    by_year: dict[int, dict] = {}
    for row in rows:
        year = int(row.get("year"))
        month = int(row.get("month"))
        quarter = (month - 1) // 3 + 1
        plan = float(row.get("plan") or 0)
        fact = float(row.get("fact") or 0)
        unit = row.get("values_unit")

        q = by_quarter.setdefault(
            (year, quarter),
            {
                "year": year,
                "quarter": quarter,
                "label": f"Q{quarter} {year}",
                "plan": 0.0,
                "fact": 0.0,
                "has_data": False,
                "values_unit": unit,
            },
        )
        q["plan"] += plan
        q["fact"] += fact
        q["has_data"] = q["has_data"] or bool(row.get("has_data"))

        y = by_year.setdefault(
            year,
            {
                "year": year,
                "plan": 0.0,
                "fact": 0.0,
                "has_data": False,
                "values_unit": unit,
            },
        )
        y["plan"] += plan
        y["fact"] += fact
        y["has_data"] = y["has_data"] or bool(row.get("has_data"))

    quarterly = []
    for row in by_quarter.values():
        row["plan"] = round(row["plan"], 2)
        row["fact"] = round(row["fact"], 2)
        row["kpi_pct"] = _kpi_pct(row["plan"], row["fact"])
        quarterly.append(row)

    yearly = []
    for row in by_year.values():
        row["plan"] = round(row["plan"], 2)
        row["fact"] = round(row["fact"], 2)
        row["kpi_pct"] = _kpi_pct(row["plan"], row["fact"])
        yearly.append(row)

    return (
        sorted(quarterly, key=lambda r: (r["year"], r["quarter"])),
        sorted(yearly, key=lambda r: r["year"]),
    )


def get_prod_deputy_output_monthly(
    shop: ShopKey,
    year: int | None = None,
    month: int | None = None,
) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(shop, ref_year, ref_month)

    cached = _load_json(path)
    if (
        cached is not None
        and cached.get("source") == SOURCE_TAG
        and cached.get("cache_date") == today.isoformat()
    ):
        return cached

    unit = VALUES_UNIT[shop]
    session = requests.Session()
    session.auth = AUTH
    rows = []
    debug_by_month: dict[str, dict] = {}
    for mm in range(1, ref_month + 1):
        plan = float(OUTPUT_PLAN[shop][mm - 1])
        fact, fact_debug = _fact_from_production_plan(session, shop, ref_year, mm)
        debug_by_month[f"{ref_year}-{mm:02d}"] = fact_debug
        rows.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan, 2),
            "fact": fact,
            "kpi_pct": _kpi_pct(plan, fact),
            "has_data": plan > 0 or fact > 0,
            "values_unit": unit,
        })

    quarterly_data, yearly_data = _aggregate_rows(rows)
    total_plan = sum(float(row.get("plan") or 0) for row in rows)
    total_fact = sum(float(row.get("fact") or 0) for row in rows)

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "shop": shop,
        "year": ref_year,
        "ref_month": ref_month,
        "months": rows,
        "quarterly_data": quarterly_data,
        "yearly_data": yearly_data,
        "last_full_month_row": dict(rows[-1]) if rows else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if rows else None,
            "total_fact": round(total_fact, 2) if rows else None,
            "kpi_pct": _kpi_pct(total_plan, total_fact),
            "months_with_data": sum(1 for row in rows if row.get("has_data")),
            "months_total": len(rows),
            "values_unit": unit if rows else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
        "debug": {
            "source": DOC_ENTITY,
            "tabular_field": TABULAR_FIELD,
            "production_dept_key": PRODUCTION_DEPT_KEY[shop],
            "months": debug_by_month,
        },
    }
    _save_json(path, payload)
    return payload


__all__ = ["ShopKey", "cache_path", "get_prod_deputy_output_monthly"]
