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

SOURCE_TAG = "prod_deputy_output_production_plan_doc_v10_work_week_doc"
DOC_ENTITY = "Document_ТД_ПроизводственныйПлан"
TABULAR_FIELD = "ВыполнениеПроизводственногоПлана"

PRODUCTION_DEPT_KEY: dict[ShopKey, str] = {
    "pc1": "3a9ac2d6-214f-11e0-b91c-00248c26ee57",  # ПРОИЗВОДСТВО НПО
    "pc2": "88cbfc9b-83ed-11e6-8121-001e67112509",  # ПРОИЗВОДСТВО АЛМАЗ
}

PRODUCTION_DEPT_NAME: dict[ShopKey, str] = {
    "pc1": "ПРОИЗВОДСТВО НПО",
    "pc2": "ПРОИЗВОДСТВО АЛМАЗ",
}

VALUES_UNIT: dict[ShopKey, str] = {
    "pc1": "руб.",
    "pc2": "шт.",
}

PLAN_FIELD: dict[ShopKey, str] = {
    "pc1": "ПланРуб",
    "pc2": "ПланШт",
}

BASE_FACT_FIELD: dict[ShopKey, str] = {
    "pc1": "ФактРуб",
    "pc2": "ФактШт",
}

PERIOD_FIELDS: dict[OutputPeriod, dict[str, dict[ShopKey, str]]] = {
    "week": {
        "plan": PLAN_FIELD,
        "fact": BASE_FACT_FIELD,
    },
    "month": {
        "plan": {
            "pc1": "ПланРубМесяц",
            "pc2": "ПланШтМесяц",
        },
        "fact": {
            "pc1": "ФактРубМесяц",
            "pc2": "ФактШтМесяц",
        },
    },
    "total": {
        "plan": {
            "pc1": "ПланРубИтого",
            "pc2": "ПланШтИтого",
        },
        "fact": {
            "pc1": "ФактРубИтого",
            "pc2": "ФактШтИтого",
        },
    },
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


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    return round(fact / plan * 100, 1) if plan > 0 else None


def _to_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _date_from_odata(value) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _sum_plan_fact_rows(
    rows: list[dict],
    shop: ShopKey,
    output_period: OutputPeriod = "week",
) -> tuple[float | None, float | None, dict]:
    field_group = PERIOD_FIELDS.get(output_period) or PERIOD_FIELDS["week"]
    plan_field = field_group["plan"][shop]
    fact_field = field_group["fact"][shop]
    plan = 0.0
    fact = 0.0
    for row in rows:
        plan += _to_float(row.get(plan_field))
        fact += _to_float(row.get(fact_field))
    has_values = abs(plan) > 0.01 or abs(fact) > 0.01
    return (
        round(plan, 2) if has_values else None,
        round(fact, 2) if has_values else None,
        {
            "plan_field": plan_field,
            "fact_field": fact_field,
            "rows": len(rows),
            "has_values": has_values,
            "output_period": output_period,
        },
    )


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
    *,
    date_field: str = "ПериодС",
    contained: bool = False,
    filter_department: bool = True,
) -> list[dict]:
    if contained:
        period_filter = f"ПериодС ge datetime'{p_start}' and ПериодПо lt datetime'{p_end}'"
    else:
        period_filter = f"{date_field} ge datetime'{p_start}' and {date_field} lt datetime'{p_end}'"
    dept_filter = (
        f" and Подразделение_Key eq guid'{PRODUCTION_DEPT_KEY[shop]}'"
        if filter_department else ""
    )
    flt = (
        "Posted eq true and DeletionMark eq false"
        f"{dept_filter}"
        f" and {period_filter}"
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


def _period_totals_from_production_plan(
    session: requests.Session,
    shop: ShopKey,
    start: date,
    end_exclusive: date,
    *,
    date_field: str = "ПериодС",
    contained: bool = False,
    filter_department: bool = True,
) -> tuple[float, float, dict]:
    p_start, p_end = _datetime_bounds(start, end_exclusive)
    docs = _load_docs_between(
        session,
        shop,
        p_start,
        p_end,
        date_field=date_field,
        contained=contained,
        filter_department=filter_department,
    )
    plan_total = 0.0
    fact_total = 0.0
    doc_debug = []

    for doc in docs:
        rows = list(doc.get(TABULAR_FIELD) or [])
        doc_plan, doc_fact, fields_debug = _sum_plan_fact_rows(rows, shop)
        plan_total += float(doc_plan or 0)
        fact_total += float(doc_fact or 0)

        doc_debug.append({
            "number": doc.get("Number"),
            "date": doc.get("Date"),
            "period_from": doc.get("ПериодС"),
            "period_to": doc.get("ПериодПо"),
            "rows": len(rows),
            "plan": doc_plan,
            "fact": doc_fact,
            "fields": fields_debug,
        })

    return round(plan_total, 2), round(fact_total, 2), {
        "documents_count": len(docs),
        "period_start": start.isoformat(),
        "period_end": (end_exclusive - timedelta(days=1)).isoformat(),
        "date_field": date_field,
        "contained": contained,
        "filter_department": filter_department,
        "documents": doc_debug,
    }


def _last_week_bounds(ref_year: int, ref_month: int) -> tuple[date, date]:
    month_start = date(ref_year, ref_month, 1)
    month_end = date(ref_year, ref_month, monthrange(ref_year, ref_month)[1])
    week_start = max(month_start, month_end - timedelta(days=7))
    return week_start, month_end + timedelta(days=1)


def _target_work_week_bounds(ref_year: int, ref_month: int) -> tuple[date, date]:
    """Рабочая неделя Пн-Пт: для текущего месяца предыдущая, для прошлого — последняя в месяце."""
    today = date.today()
    if ref_year == today.year and ref_month == today.month:
        current_monday = today - timedelta(days=today.weekday())
        monday = current_monday - timedelta(days=7)
        friday = monday + timedelta(days=4)
        return monday, friday + timedelta(days=1)

    month_end = date(ref_year, ref_month, monthrange(ref_year, ref_month)[1])
    friday = month_end - timedelta(days=(month_end.weekday() - 4) % 7)
    monday = friday - timedelta(days=4)
    month_start = date(ref_year, ref_month, 1)
    if monday < month_start:
        monday = month_start
    return monday, friday + timedelta(days=1)


def _doc_period(doc: dict) -> tuple[date | None, date | None]:
    return _date_from_odata(doc.get("ПериодС")), _date_from_odata(doc.get("ПериодПо"))


def _doc_sort_key(doc: dict) -> tuple[str, str, str]:
    return (
        str(doc.get("ПериодПо") or ""),
        str(doc.get("Date") or ""),
        str(doc.get("Number") or ""),
    )


def _select_production_plan_doc(
    session: requests.Session,
    shop: ShopKey,
    ref_year: int,
    ref_month: int,
) -> tuple[dict | None, dict]:
    docs = _load_month_docs(session, shop, ref_year, ref_month)
    target_start, target_end_exclusive = _target_work_week_bounds(ref_year, ref_month)
    target_end = target_end_exclusive - timedelta(days=1)

    docs_with_period = []
    for doc in docs:
        start, end = _doc_period(doc)
        if start is None or end is None:
            continue
        docs_with_period.append((start, end, doc))

    containing = [
        item for item in docs_with_period
        if item[0] <= target_start and item[1] >= target_end
    ]
    if containing:
        # Если есть недельный документ и месячный документ, оба покрывают рабочую неделю.
        # Берём самый узкий период; для апреля это всё равно будет месячный документ,
        # если отдельного недельного документа в 1С нет.
        min_days = min((item[1] - item[0]).days for item in containing)
        candidates = [item for item in containing if (item[1] - item[0]).days == min_days]
        selected = max(candidates, key=lambda item: (item[1], item[0], _doc_sort_key(item[2])))[2]
        source = "target_work_week_document"
    elif docs_with_period:
        selected = max(docs_with_period, key=lambda item: (item[1], item[0], _doc_sort_key(item[2])))[2]
        source = "latest_period_document_fallback"
    else:
        selected = None
        source = "no_document"

    selected_start, selected_end = _doc_period(selected) if selected else (None, None)

    return selected, {
        "selection_source": source,
        "target_week_start": target_start.isoformat(),
        "target_week_end": target_end.isoformat(),
        "documents_count": len(docs),
        "documents_with_period_count": len(docs_with_period),
        "production_dept_key": PRODUCTION_DEPT_KEY[shop],
        "production_dept_name": PRODUCTION_DEPT_NAME[shop],
        "selected_number": selected.get("Number") if selected else None,
        "selected_date": selected.get("Date") if selected else None,
        "selected_period_from": selected_start.isoformat() if selected_start else None,
        "selected_period_to": selected_end.isoformat() if selected_end else None,
    }


def _row_from_document(
    doc: dict | None,
    shop: ShopKey,
    output_period: OutputPeriod,
    *,
    ref_year: int,
    ref_month: int,
    unit: str,
) -> tuple[dict, dict]:
    if not doc:
        row = {
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
            "plan": None,
            "fact": None,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": unit,
        }
        return row, {"documents_count": 0, "output_period": output_period}

    rows = list(doc.get(TABULAR_FIELD) or [])
    plan, fact, fields_debug = _sum_plan_fact_rows(rows, shop, output_period)
    doc_start, doc_end = _doc_period(doc)
    target_start, target_end_exclusive = _target_work_week_bounds(ref_year, ref_month)
    target_end = target_end_exclusive - timedelta(days=1)
    label = ""
    week_start = doc_start
    week_end = doc_end
    if output_period == "week":
        week_start = target_start
        week_end = target_end
        label = f"{target_start.strftime('%d.%m')}–{target_end.strftime('%d.%m.%Y')}"
    elif output_period == "total":
        label = f"Итого за {MONTH_RU[ref_month].lower()} {ref_year}"

    row = {
        "year": ref_year,
        "month": ref_month,
        "month_name": MONTH_RU[ref_month].lower(),
        "week_start": week_start.isoformat() if week_start else None,
        "week_end": week_end.isoformat() if week_end else None,
        "label": label,
        "plan": plan,
        "fact": fact,
        "kpi_pct": _kpi_pct(plan, fact),
        "has_data": plan is not None or fact is not None,
        "values_unit": unit,
    }
    debug = {
        "documents_count": 1,
        "output_period": output_period,
        "number": doc.get("Number"),
        "date": doc.get("Date"),
        "period_from": doc.get("ПериодС"),
        "period_to": doc.get("ПериодПо"),
        "target_week_start": target_start.isoformat(),
        "target_week_end": target_end.isoformat(),
        "production_dept_key": PRODUCTION_DEPT_KEY[shop],
        "production_dept_name": PRODUCTION_DEPT_NAME[shop],
        "rows": len(rows),
        "plan": plan,
        "fact": fact,
        "fields": fields_debug,
    }
    return row, debug


def _latest_document_week_totals(
    session: requests.Session,
    shop: ShopKey,
    ref_year: int,
    ref_month: int,
) -> tuple[date, date, float, float, dict] | None:
    """Последняя неделя = период последнего документа плана в выбранном месяце."""
    docs = _load_month_docs(session, shop, ref_year, ref_month)
    docs_with_period: list[tuple[date, date, dict]] = []
    for doc in docs:
        start = _date_from_odata(doc.get("ПериодС"))
        end = _date_from_odata(doc.get("ПериодПо"))
        if start is None or end is None:
            continue
        if (end - start).days > 7:
            continue
        docs_with_period.append((start, end, doc))
    if not docs_with_period:
        return None

    latest_start, latest_end, _latest_doc = max(
        docs_with_period,
        key=lambda item: (
            item[1],
            item[0],
            str(item[2].get("Date") or ""),
            str(item[2].get("Number") or ""),
        ),
    )

    plan_total = 0.0
    fact_total = 0.0
    doc_debug = []
    for start, end, doc in docs_with_period:
        if start != latest_start or end != latest_end:
            continue
        rows = list(doc.get(TABULAR_FIELD) or [])
        doc_plan, doc_fact, fields_debug = _sum_plan_fact_rows(rows, shop)
        plan_total += float(doc_plan or 0)
        fact_total += float(doc_fact or 0)
        doc_debug.append({
            "number": doc.get("Number"),
            "date": doc.get("Date"),
            "period_from": doc.get("ПериодС"),
            "period_to": doc.get("ПериодПо"),
            "rows": len(rows),
            "plan": doc_plan,
            "fact": doc_fact,
            "fields": fields_debug,
        })

    end_exclusive = latest_end + timedelta(days=1)
    return latest_start, end_exclusive, round(plan_total, 2), round(fact_total, 2), {
        "documents_count": len(doc_debug),
        "period_start": latest_start.isoformat(),
        "period_end": latest_end.isoformat(),
        "date_field": "document_period",
        "contained": True,
        "filter_department": True,
        "documents": doc_debug,
    }


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
        cumulative_plan, cumulative_fact, fact_debug = _period_totals_from_production_plan(
            session,
            shop,
            month_start,
            week_end_exclusive,
        )
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
        month_start = date(ref_year, mm, 1)
        month_end_exclusive = date(ref_year, mm, monthrange(ref_year, mm)[1]) + timedelta(days=1)
        plan, fact, fact_debug = _period_totals_from_production_plan(
            session,
            shop,
            month_start,
            month_end_exclusive,
        )
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
    latest_week = _latest_document_week_totals(session, shop, ref_year, ref_month)
    if latest_week is not None:
        week_start, week_end_exclusive, week_plan, week_fact, week_debug = latest_week
    else:
        week_start, week_end_exclusive = _last_week_bounds(ref_year, ref_month)
        week_plan, week_fact, week_debug = _period_totals_from_production_plan(
            session,
            shop,
            week_start,
            week_end_exclusive,
            date_field="ПериодПо",
            contained=(ref_year == 2026 and ref_month == 4),
            filter_department=not (ref_year == 2026 and ref_month == 4),
        )
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
            "last_week": week_debug,
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
    period = period if period in {"month", "week", "total"} else "month"
    ref_year, ref_month = _normalize_period(year, month)
    unit = VALUES_UNIT[shop]

    data = get_prod_deputy_output_monthly(shop, year=ref_year, month=ref_month)
    session = requests.Session()
    session.auth = AUTH
    selected_doc, selection_debug = _select_production_plan_doc(session, shop, ref_year, ref_month)
    row, row_debug = _row_from_document(
        selected_doc,
        shop,
        period,
        ref_year=ref_year,
        ref_month=ref_month,
        unit=unit,
    )

    kpi_period = {
        "type": "last_week" if period == "week" else ("ytd" if period == "total" else "current_month"),
        "year": ref_year,
        "month": ref_month,
        "month_name": MONTH_RU[ref_month].lower(),
        "label": row.get("label"),
        "week_start": row.get("week_start"),
        "week_end": row.get("week_end"),
    }

    return {
        **data,
        "period_type": period,
        "selected_row": row,
        "last_full_month_row": row,
        "ytd": {
            "total_plan": row.get("plan"),
            "total_fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
            "months_with_data": 1 if row.get("has_data") else 0,
            "months_total": 1,
            "values_unit": unit,
        },
        "kpi_period": kpi_period,
        "debug": {
            **(data.get("debug") or {}),
            "selected_document": selection_debug,
            "selected_row": row_debug,
        },
    }


__all__ = [
    "ShopKey",
    "OutputPeriod",
    "cache_path",
    "get_prod_deputy_output_monthly",
    "get_prod_deputy_output_period",
]
