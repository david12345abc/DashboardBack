"""Плитки отгрузки складов НПО и Алмаз для начальника службы логистики."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Callable

from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period
from .logistics_warehouse_sql import calculate_almaz_month, calculate_npo_month
from sql_connection import SqlConnection

SOURCE_TAG_NPO = "logistics_npo_wh_ship_v1_vp_mp"
SOURCE_TAG_ALMAZ = "logistics_almaz_wh_ship_v1_qty"


def cache_path_npo(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_npo_shipment_{year}_{ref_month:02d}.json"


def cache_path_almaz(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_almaz_shipment_{year}_{ref_month:02d}.json"


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


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    try:
        plan_value = float(plan or 0)
        fact_value = float(fact or 0)
    except (TypeError, ValueError):
        return None
    if plan_value <= 0:
        return None
    return round(fact_value / plan_value * 100, 1)


def _aggregate(months: list[dict], *, unit: str, has_plan: bool) -> tuple[list[dict], list[dict]]:
    by_q: dict[tuple[int, int], dict] = {}
    by_y: dict[int, dict] = {}
    for row in months:
        y = int(row["year"])
        m = int(row["month"])
        qn = (m - 1) // 3 + 1
        for store, key, extra in (
            (by_q, (y, qn), {"year": y, "quarter": qn, "label": f"Q{qn} {y}"}),
            (by_y, y, {"year": y}),
        ):
            target = store.setdefault(
                key,
                {
                    **extra,
                    "plan": 0 if has_plan else None,
                    "fact": 0,
                    "has_data": False,
                    "values_unit": unit,
                },
            )
            if has_plan:
                target["plan"] = round(float(target.get("plan") or 0) + float(row.get("plan") or 0), 2)
            target["fact"] = round(float(target.get("fact") or 0) + float(row.get("fact") or 0), 3)
            target["has_data"] = target["has_data"] or bool(row.get("has_data"))
    items_q = []
    for item in sorted(by_q.values(), key=lambda r: (r["year"], r["quarter"])):
        item["kpi_pct"] = _kpi_pct(item.get("plan"), item.get("fact")) if has_plan else None
        items_q.append(item)
    items_y = []
    for item in sorted(by_y.values(), key=lambda r: r["year"]):
        item["kpi_pct"] = _kpi_pct(item.get("plan"), item.get("fact")) if has_plan else None
        items_y.append(item)
    return items_q, items_y


def _build_payload(
    *,
    year: int | None,
    month: int | None,
    source: str,
    path_fn: Callable[[int, int], Path],
    month_fn: Callable[..., dict[str, Any]],
    unit: str,
    has_plan: bool,
) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = path_fn(ref_year, ref_month)
    cached = _load_json(path)
    if cached and cached.get("source") == source and cached.get("cache_date") == today.isoformat():
        return cached

    sql = SqlConnection()
    months: list[dict] = []
    with sql.connect_ctx() as connection:
        cursor = connection.cursor()
        month_rows = [month_fn(ref_year, mm, cursor=cursor) for mm in range(1, ref_month + 1)]
    for mm, raw in enumerate(month_rows, start=1):
        plan = raw.get("plan")
        fact = raw.get("fact")
        row = {
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": plan,
            "fact": fact,
            "kpi_pct": _kpi_pct(plan, fact) if has_plan else None,
            "has_data": fact is not None,
            "values_unit": unit,
        }
        for extra in ("fg", "repair_out", "repair_in"):
            if extra in raw:
                row[extra] = raw[extra]
        months.append(row)

    quarterly_data, yearly_data = _aggregate(months, unit=unit, has_plan=has_plan)
    total_plan = round(sum(float(row.get("plan") or 0) for row in months), 2) if has_plan else None
    total_fact = round(sum(float(row.get("fact") or 0) for row in months), 3)
    payload = {
        "cache_date": today.isoformat(),
        "source": source,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months,
        "quarterly_data": quarterly_data,
        "yearly_data": yearly_data,
        "last_full_month_row": dict(months[-1]) if months else None,
        "ytd": {
            "total_plan": total_plan,
            "total_fact": total_fact,
            "kpi_pct": _kpi_pct(total_plan, total_fact) if has_plan else None,
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
            "values_unit": unit,
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


def get_logistics_npo_shipment_monthly(year: int | None = None, month: int | None = None) -> dict:
    return _build_payload(
        year=year,
        month=month,
        source=SOURCE_TAG_NPO,
        path_fn=cache_path_npo,
        month_fn=calculate_npo_month,
        unit="руб.",
        has_plan=True,
    )


def get_logistics_almaz_shipment_monthly(year: int | None = None, month: int | None = None) -> dict:
    return _build_payload(
        year=year,
        month=month,
        source=SOURCE_TAG_ALMAZ,
        path_fn=cache_path_almaz,
        month_fn=calculate_almaz_month,
        unit="шт.",
        has_plan=False,
    )
