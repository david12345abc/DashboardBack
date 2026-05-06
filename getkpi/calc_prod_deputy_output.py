from __future__ import annotations

import json
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import requests

from .calc_budget_limit import AUTH, BASE
from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period

ShopKey = Literal["pc1", "pc2"]
OutputPeriod = Literal["month", "week", "total"]

SOURCE_TAG = "prod_deputy_output_fact_production_plan_v4"
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

FACT_FIELD_ALIASES: dict[ShopKey, dict[OutputPeriod, list[str]]] = {
    "pc1": {
        "month": ["ФактРубЗаМесяц", "ФактЗаМесяцРуб", "ФактМесяцРуб", "ЗаМесяцРуб", "МесяцРуб"],
        "week": ["ФактРубЗаНеделю", "ФактЗаНеделюРуб", "ФактНеделяРуб", "ЗаНеделюРуб", "НеделяРуб"],
        "total": ["ФактРубИтого", "ФактИтогоРуб", "ИтогоРуб", "ВсегоРуб"],
    },
    "pc2": {
        "month": ["ФактШтЗаМесяц", "ФактЗаМесяцШт", "ФактМесяцШт", "ЗаМесяцШт", "МесяцШт"],
        "week": ["ФактШтЗаНеделю", "ФактЗаНеделюШт", "ФактНеделяШт", "ЗаНеделюШт", "НеделяШт"],
        "total": ["ФактШтИтого", "ФактИтогоШт", "ИтогоШт", "ВсегоШт"],
    },
}

BASE_FACT_FIELD: dict[ShopKey, str] = {
    "pc1": "ФактРуб",
    "pc2": "ФактШт",
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


def _field_norm(value: str) -> str:
    return "".join(ch for ch in str(value).casefold() if ch.isalnum())


def _row_value_by_alias(row: dict, aliases: list[str], required_tokens: list[str]) -> tuple[float, str | None]:
    for alias in aliases:
        if alias in row:
            return _to_float(row.get(alias)), alias

    normalized = {_field_norm(key): key for key in row.keys()}
    for alias in aliases:
        key = normalized.get(_field_norm(alias))
        if key is not None:
            return _to_float(row.get(key)), key

    tokens = [_field_norm(token) for token in required_tokens if token]
    for norm_key, key in normalized.items():
        if all(token in norm_key for token in tokens):
            return _to_float(row.get(key)), key

    return 0.0, None


def _sum_fact_period(rows: list[dict], shop: ShopKey, period: OutputPeriod) -> tuple[float, dict]:
    aliases = FACT_FIELD_ALIASES[shop][period]
    unit_token = "руб" if shop == "pc1" else "шт"
    period_tokens = {
        "month": ["факт", unit_token, "месяц"],
        "week": ["факт", unit_token, "недел"],
        "total": ["факт", unit_token, "итог"],
    }[period]
    total = 0.0
    fields_used: dict[str, int] = {}
    missing_rows = 0

    for row in rows:
        value, field = _row_value_by_alias(row, aliases, period_tokens)
        total += value
        if field:
            fields_used[field] = fields_used.get(field, 0) + 1
        else:
            missing_rows += 1

    return round(total, 2), {
        "aliases": aliases,
        "tokens": period_tokens,
        "fields_used": fields_used,
        "missing_rows": missing_rows,
    }


def _period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _datetime_bounds(start: date, end_exclusive: date) -> tuple[str, str]:
    return f"{start.isoformat()}T00:00:00", f"{end_exclusive.isoformat()}T00:00:00"


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


def _load_docs_between(
    session: requests.Session,
    shop: ShopKey,
    p_start: str,
    p_end: str,
) -> list[dict]:
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


def _load_month_docs(session: requests.Session, shop: ShopKey, year: int, month: int) -> list[dict]:
    p_start, p_end = _period_bounds(year, month)
    return _load_docs_between(session, shop, p_start, p_end)


def _fact_from_production_plan(
    session: requests.Session,
    shop: ShopKey,
    year: int,
    month: int,
) -> tuple[float, dict]:
    docs = _load_month_docs(session, shop, year, month)
    fact_field = BASE_FACT_FIELD[shop]
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


def _period_facts_from_production_plan(
    session: requests.Session,
    shop: ShopKey,
    year: int,
    month: int,
) -> tuple[dict[OutputPeriod, float], dict]:
    docs = _load_month_docs(session, shop, year, month)
    base_fact_field = BASE_FACT_FIELD[shop]
    period_totals: dict[OutputPeriod, float] = {"month": 0.0, "week": 0.0, "total": 0.0}
    doc_debug = []

    for doc in docs:
        rows = list(doc.get(TABULAR_FIELD) or [])
        month_fact, month_debug = _sum_fact_period(rows, shop, "month")
        week_fact, week_debug = _sum_fact_period(rows, shop, "week")
        total_fact, total_debug = _sum_fact_period(rows, shop, "total")
        legacy_month_fact = round(sum(_to_float(row.get(base_fact_field)) for row in rows), 2)

        # Старые базы отдают только ФактРуб/ФактШт; отдельные колонки периода используем при наличии.
        if not month_debug["fields_used"]:
            month_fact = legacy_month_fact
            month_debug = {**month_debug, "fallback_field": base_fact_field}
        period_totals["month"] += month_fact
        period_totals["week"] += week_fact
        period_totals["total"] += total_fact

        doc_debug.append({
            "number": doc.get("Number"),
            "date": doc.get("Date"),
            "period_from": doc.get("ПериодС"),
            "period_to": doc.get("ПериодПо"),
            "rows": len(rows),
            "legacy_month_fact": legacy_month_fact,
            "period_facts": {
                "month": round(month_fact, 2),
                "week": round(week_fact, 2),
                "total": round(total_fact, 2),
            },
            "period_fields": {
                "month": month_debug,
                "week": week_debug,
                "total": total_debug,
            },
        })

    return (
        {key: round(value, 2) for key, value in period_totals.items()},
        {
            "documents_count": len(docs),
            "base_fact_field": base_fact_field,
            "documents": doc_debug,
        },
    )


def _fact_from_docs_between(
    session: requests.Session,
    shop: ShopKey,
    start: date,
    end_exclusive: date,
) -> tuple[float, dict]:
    p_start, p_end = _datetime_bounds(start, end_exclusive)
    docs = _load_docs_between(session, shop, p_start, p_end)
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
        "period_start": start.isoformat(),
        "period_end": (end_exclusive - timedelta(days=1)).isoformat(),
        "documents": doc_debug,
    }


def _last_week_bounds(ref_year: int, ref_month: int) -> tuple[date, date]:
    ranges = _month_week_ranges(ref_year, ref_month)
    if ranges:
        return ranges[-1]
    month_start = date(ref_year, ref_month, 1)
    return month_start, month_start + timedelta(days=1)


def _daily_plan(shop: ShopKey, day: date) -> float:
    return float(OUTPUT_PLAN[shop][day.month - 1]) / float(monthrange(day.year, day.month)[1])


def _week_plan(shop: ShopKey, start: date, end_exclusive: date) -> float:
    total = 0.0
    cur = start
    while cur < end_exclusive:
        total += _daily_plan(shop, cur)
        cur += timedelta(days=1)
    return round(total, 2)


def _month_week_ranges(year: int, month: int) -> list[tuple[date, date]]:
    month_start = date(year, month, 1)
    month_end_exclusive = date(year, month, monthrange(year, month)[1]) + timedelta(days=1)
    ranges: list[tuple[date, date]] = []
    start = month_start
    while start < month_end_exclusive:
        end = min(start + timedelta(days=7 - start.weekday()), month_end_exclusive)
        ranges.append((start, end))
        start = end
    return ranges


def _weekly_cumulative_points(
    session: requests.Session,
    shop: ShopKey,
    year: int,
    month: int,
    unit: str,
) -> tuple[list[dict], dict]:
    month_start = date(year, month, 1)
    ranges = _month_week_ranges(year, month)
    points: list[dict] = []
    debug: dict[str, dict] = {}

    for idx, (week_start, week_end_exclusive) in enumerate(ranges, start=1):
        cumulative_plan = _week_plan(shop, month_start, week_end_exclusive)
        cumulative_fact, fact_debug = _fact_from_docs_between(session, shop, month_start, week_end_exclusive)
        week_end = week_end_exclusive - timedelta(days=1)
        label = f"{week_start.strftime('%d.%m')}–{week_end.strftime('%d.%m')}"
        points.append({
            "week": idx,
            "label": label,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "plan": cumulative_plan,
            "fact": cumulative_fact,
            "kpi_pct": _kpi_pct(cumulative_plan, cumulative_fact),
            "has_data": cumulative_plan > 0 or cumulative_fact > 0,
            "values_unit": unit,
        })
        debug[label] = fact_debug

    return points, debug


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
        period_facts, fact_debug = _period_facts_from_production_plan(session, shop, ref_year, mm)
        fact = period_facts["month"]
        debug_by_month[f"{ref_year}-{mm:02d}"] = fact_debug
        rows.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan, 2),
            "fact": fact,
            "fact_month": period_facts["month"],
            "fact_week": period_facts["week"],
            "fact_total": period_facts["total"],
            "kpi_pct": _kpi_pct(plan, fact),
            "has_data": plan > 0 or fact > 0,
            "values_unit": unit,
        })

    quarterly_data, yearly_data = _aggregate_rows(rows)
    total_plan = sum(float(row.get("plan") or 0) for row in rows)
    total_fact = sum(float(row.get("fact") or 0) for row in rows)
    week_start, week_end_exclusive = _last_week_bounds(ref_year, ref_month)
    week_plan = _week_plan(shop, week_start, week_end_exclusive)
    selected_month_row = rows[-1] if rows else {}
    week_fact = float(selected_month_row.get("fact_week") or 0)
    weekly_cumulative, weekly_cumulative_debug = _weekly_cumulative_points(
        session,
        shop,
        ref_year,
        ref_month,
        unit,
    )
    week_row = {
        "year": week_start.year,
        "week_start": week_start.isoformat(),
        "week_end": (week_end_exclusive - timedelta(days=1)).isoformat(),
        "label": f"{week_start.strftime('%d.%m')}–{(week_end_exclusive - timedelta(days=1)).strftime('%d.%m.%Y')}",
        "plan": week_plan,
        "fact": week_fact,
        "kpi_pct": _kpi_pct(week_plan, week_fact),
        "has_data": week_plan > 0 or week_fact > 0,
        "values_unit": unit,
    }

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "shop": shop,
        "year": ref_year,
        "ref_month": ref_month,
        "months": rows,
        "last_week_row": week_row,
        "weekly_cumulative": weekly_cumulative,
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
            "weekly_cumulative": weekly_cumulative_debug,
        },
    }
    _save_json(path, payload)
    return payload


def get_prod_deputy_output_period(
    shop: ShopKey,
    period: OutputPeriod,
    year: int | None = None,
    month: int | None = None,
) -> dict:
    data = get_prod_deputy_output_monthly(shop, year=year, month=month)
    period = period if period in {"month", "week", "total"} else "month"
    unit = VALUES_UNIT[shop]

    if period == "week":
        row = dict(data.get("last_week_row") or {})
        return {
            **data,
            "period_type": "week",
            "selected_row": row,
            "last_full_month_row": row,
            "monthly_data": [],
            "quarterly_data": [],
            "yearly_data": [],
            "ytd": {
                "total_plan": row.get("plan"),
                "total_fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "months_with_data": 1 if row.get("has_data") else 0,
                "months_total": 1,
                "values_unit": unit,
            },
            "kpi_period": {
                "type": "last_week",
                "label": row.get("label"),
                "week_start": row.get("week_start"),
                "week_end": row.get("week_end"),
            },
        }

    if period == "total":
        ytd = dict(data.get("ytd") or {})
        selected_month = dict(data.get("last_full_month_row") or {})
        total_plan = ytd.get("total_plan")
        total_fact = selected_month.get("fact_total")
        if total_fact in (None, ""):
            total_fact = ytd.get("total_fact")
        row = {
            "year": data.get("year"),
            "month": data.get("ref_month"),
            "label": f"Итого за {MONTH_RU[data.get('ref_month')].lower()} {data.get('year')}",
            "plan": total_plan,
            "fact": total_fact,
            "kpi_pct": _kpi_pct(float(total_plan or 0), float(total_fact or 0)),
            "has_data": total_plan is not None or total_fact is not None,
            "values_unit": unit,
        }
        return {
            **data,
            "period_type": "total",
            "selected_row": row,
            "last_full_month_row": row,
            "ytd": {
                **ytd,
                "total_plan": row.get("plan"),
                "total_fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "values_unit": unit,
            },
            "kpi_period": {
                "type": "ytd",
                "year": data.get("year"),
                "month": data.get("ref_month"),
            },
        }

    row = dict(data.get("last_full_month_row") or {})
    return {
        **data,
        "period_type": "month",
        "selected_row": row,
        "ytd": {
            "total_plan": row.get("plan"),
            "total_fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
            "months_with_data": 1 if row.get("has_data") else 0,
            "months_total": 1,
            "values_unit": unit,
        },
    }


__all__ = [
    "ShopKey",
    "OutputPeriod",
    "cache_path",
    "get_prod_deputy_output_monthly",
    "get_prod_deputy_output_period",
]
