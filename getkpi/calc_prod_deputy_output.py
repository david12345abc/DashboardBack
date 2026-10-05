# -*- coding: utf-8 -*-
"""PD-M1.1.* / PD-M1.2.* — выполнение производственного плана.

Источник: OData Document_ТД_ПроизводственныйПлан, табличная часть
ВыполнениеПроизводственногоПлана. Неделя, месяц и итого — разные колонки
одной строки, не одна серия.
"""
from __future__ import annotations

import json
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import requests

from comdir.common import to_1c_dt

from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period
from .calc_plan import AUTH, BASE
from .odata_http import request_with_retry

ShopKey = Literal["pc1", "pc2"]
OutputPeriod = Literal["month", "week", "total"]

SOURCE_TAG = "prod_deputy_output_production_plan_odata_v15"
DOC_ENTITY = "Document_ТД_ПроизводственныйПлан"
TABULAR_FIELD = "ВыполнениеПроизводственногоПлана"

FLD_PERIOD_FROM = "ПериодС"
FLD_PERIOD_TO = "ПериодПо"

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

PERIOD_BASE_FIELDS: dict[OutputPeriod, dict[str, str]] = {
    "week": {
        "plan_qty": "ПланШт",
        "fact_qty": "ФактШт",
        "plan_rub": "ПланРуб",
        "fact_rub": "ФактРуб",
    },
    "month": {
        "plan_qty": "ПланШтМесяц",
        "fact_qty": "ФактШтМесяц",
        "plan_rub": "ПланРубМесяц",
        "fact_rub": "ФактРубМесяц",
    },
    "total": {
        "plan_qty": "ПланШтИтого",
        "fact_qty": "ФактШтИтого",
        "plan_rub": "ПланРубИтого",
        "fact_rub": "ФактРубИтого",
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


def _from_1c_dt(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        y = value.year - 2000 if value.year >= 4000 else value.year
        return date(y, value.month, value.day)
    if isinstance(value, date):
        y = value.year - 2000 if value.year >= 4000 else value.year
        return date(y, value.month, value.day)
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _iso_date(value) -> str | None:
    d = _from_1c_dt(value)
    return d.isoformat() if d else None


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


PRODUCT_NAME_FIELDS = (
    "НаименованиеГруппыПродукции",
    "НаименованиеГруппы",
    "ГруппаПродукции",
    "ГруппаПродукции_Key",
    "Номенклатура",
    "Номенклатура_Key",
    "КонтрагентДляРеализации",
    "КонтрагентДляРеализации_Key",
    "LineNumber",
)


def _product_name(row: dict, index: int) -> str:
    for field in PRODUCT_NAME_FIELDS:
        value = row.get(field)
        if value not in (None, ""):
            return str(value).strip()
    return f"Строка {index}"


def _add_amount(target: dict[str, float], name: str, value: float) -> None:
    target[name] = round(float(target.get(name) or 0) + float(value or 0), 2)


def _instrument_breakdown(
    rows: list[dict],
    shop: ShopKey,
    output_period: OutputPeriod,
) -> tuple[list[dict], dict[str, float], dict[str, float]]:
    field_group = PERIOD_FIELDS.get(output_period) or PERIOD_FIELDS["week"]
    base_fields = PERIOD_BASE_FIELDS.get(output_period) or PERIOD_BASE_FIELDS["week"]
    display_plan_field = field_group["plan"][shop]
    display_fact_field = field_group["fact"][shop]
    plan_by_product: dict[str, float] = {}
    fact_by_product: dict[str, float] = {}
    detail_rows: list[dict] = []

    for idx, row in enumerate(rows, start=1):
        name = _product_name(row, idx)
        plan_qty = _to_float(row.get(base_fields["plan_qty"]))
        fact_qty = _to_float(row.get(base_fields["fact_qty"]))
        plan_rub = _to_float(row.get(base_fields["plan_rub"]))
        fact_rub = _to_float(row.get(base_fields["fact_rub"]))
        display_plan = _to_float(row.get(display_plan_field))
        display_fact = _to_float(row.get(display_fact_field))
        _add_amount(plan_by_product, name, display_plan)
        _add_amount(fact_by_product, name, display_fact)
        detail_rows.append({
            "name": name,
            "plan": round(display_plan, 2),
            "fact": round(display_fact, 2),
            "plan_qty": round(plan_qty, 2),
            "fact_qty": round(fact_qty, 2),
            "plan_rub": round(plan_rub, 2),
            "fact_rub": round(fact_rub, 2),
            "values_unit": VALUES_UNIT[shop],
        })

    return detail_rows, plan_by_product, fact_by_product


def _period_dt_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    p0 = to_1c_dt(date(year, month, 1))
    if month == 12:
        p_next = to_1c_dt(date(year + 1, 1, 1))
    else:
        p_next = to_1c_dt(date(year, month + 1, 1))
    return p0, p_next


def _as_real_datetime(value: datetime) -> datetime:
    if value.year >= 4000:
        return value.replace(year=value.year - 2000)
    return value


def _odata_datetime_literal(value: datetime) -> str:
    real = _as_real_datetime(value)
    return f"datetime'{real.strftime('%Y-%m-%dT%H:%M:%S')}'"


def _parse_odata_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).replace("Z", "")[:19]
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


_PRODUCT_NAMES: dict[str, str] | None = None


def _product_name_map() -> dict[str, str]:
    """Catalog_ТД_ВидПродукцииДляМП: в строке плана ГруппаПродукции приходит как guid."""
    global _PRODUCT_NAMES
    if _PRODUCT_NAMES is not None:
        return _PRODUCT_NAMES
    url = (
        f"{BASE}/{quote('Catalog_ТД_ВидПродукцииДляМП')}"
        f"?$format=json&$top=500&$select={quote('Ref_Key,Description', safe=',')}"
    )
    session = requests.Session()
    session.auth = AUTH
    names: dict[str, str] = {}
    for _page in range(10):
        response = request_with_retry(session, url, timeout=60, label="prod_plan_products")
        if response is None or not response.ok:
            status = None if response is None else response.status_code
            body = "" if response is None else response.text[:300]
            raise RuntimeError(f"OData Catalog_ТД_ВидПродукцииДляМП HTTP {status}: {body}")
        payload = response.json()
        for row in payload.get("value") or []:
            key = str(row.get("Ref_Key") or "").strip().lower()
            title = str(row.get("Description") or "").strip()
            if key and title:
                names[key] = title
        url = str(payload.get("odata.nextLink") or "")
        if not url:
            break
    _PRODUCT_NAMES = names
    return names


def _odata_values(filter_expr: str) -> list[dict]:
    select = (
        "Ref_Key,Number,Date,Posted,DeletionMark,ПериодС,ПериодПо,"
        "Подразделение_Key,ВыполнениеПроизводственногоПлана"
    )
    url = (
        f"{BASE}/{quote(DOC_ENTITY)}?$format=json&$top=100"
        f"&$select={quote(select, safe=',')}"
        f"&$filter={quote(filter_expr)}"
    )
    session = requests.Session()
    session.auth = AUTH
    rows: list[dict] = []
    for _page in range(20):
        response = request_with_retry(session, url, timeout=120, label="prod_plan")
        if response is None or not response.ok:
            status = None if response is None else response.status_code
            body = "" if response is None else response.text[:300]
            raise RuntimeError(f"OData {DOC_ENTITY} HTTP {status}: {body}")
        payload = response.json()
        rows.extend(payload.get("value") or [])
        url = str(payload.get("odata.nextLink") or "")
        if not url:
            break
    return rows


def _load_docs_between(
    cur,
    shop: ShopKey,
    p_start: datetime,
    p_end: datetime,
    *,
    date_field: str = FLD_PERIOD_FROM,
    contained: bool = False,
    filter_department: bool = True,
) -> list[dict]:
    """Проведённые документы с табличной частью из OData."""
    del cur
    start = _odata_datetime_literal(p_start)
    end = _odata_datetime_literal(p_end)
    field = "ПериодПо" if date_field in {FLD_PERIOD_TO, "ПериодПо"} else "ПериодС"
    parts = ["Posted eq true", "DeletionMark eq false"]
    if filter_department:
        parts.append(f"Подразделение_Key eq guid'{PRODUCTION_DEPT_KEY[shop]}'")
    if contained:
        parts.append(f"ПериодС ge {start}")
        parts.append(f"ПериодПо lt {end}")
    else:
        parts.append(f"{field} ge {start}")
        parts.append(f"{field} lt {end}")

    product_names = _product_name_map()
    docs: list[dict] = []
    for raw in _odata_values(" and ".join(parts)):
        period_from = _parse_odata_datetime(raw.get("ПериодС"))
        period_to = _parse_odata_datetime(raw.get("ПериодПо"))
        doc_date = _parse_odata_datetime(raw.get("Date"))
        lines = []
        for line in raw.get(TABULAR_FIELD) or []:
            if not isinstance(line, dict):
                continue
            raw_name = str(line.get("ГруппаПродукции") or "").strip()
            title = product_names.get(raw_name.lower())
            if title:
                line["ГруппаПродукции"] = title
            lines.append(line)
        docs.append({
            "Ref_Key": raw.get("Ref_Key"),
            "Number": str(raw.get("Number") or "").strip(),
            "Date": _iso_date(doc_date),
            "ПериодС": _iso_date(period_from),
            "ПериодПо": _iso_date(period_to),
            "Подразделение_Key": raw.get("Подразделение_Key") or PRODUCTION_DEPT_KEY[shop],
            TABULAR_FIELD: lines,
            "_period_from_dt": period_from or datetime.min,
            "_period_to_dt": period_to or datetime.min,
            "_date_dt": doc_date or datetime.min,
        })
    return docs


def _load_month_docs(cur, shop: ShopKey, year: int, month: int) -> list[dict]:
    p0, p_next = _period_dt_bounds(year, month)
    return _load_docs_between(cur, shop, p0, p_next)


def _doc_period(doc: dict) -> tuple[date | None, date | None]:
    return _from_1c_dt(doc.get("_period_from_dt") or doc.get("ПериодС")), _from_1c_dt(
        doc.get("_period_to_dt") or doc.get("ПериодПо")
    )


def _doc_sort_key(doc: dict) -> tuple:
    return (
        doc.get("_period_to_dt") or datetime.min,
        doc.get("_date_dt") or datetime.min,
        str(doc.get("Number") or ""),
    )


def _select_production_plan_doc(
    cur,
    shop: ShopKey,
    ref_year: int,
    ref_month: int,
) -> tuple[dict | None, dict]:
    docs = _load_month_docs(cur, shop, ref_year, ref_month)
    selected = max(docs, key=_doc_sort_key) if docs else None
    source = "latest_document_in_selected_month" if selected else "no_document"
    selected_start, selected_end = _doc_period(selected) if selected else (None, None)
    return selected, {
        "selection_source": source,
        "documents_count": len(docs),
        "requested_year": ref_year,
        "requested_month": ref_month,
        "effective_year": ref_year if selected else None,
        "effective_month": ref_month if selected else None,
        "production_dept_key": PRODUCTION_DEPT_KEY[shop],
        "production_dept_name": PRODUCTION_DEPT_NAME[shop],
        "selected_number": selected.get("Number") if selected else None,
        "selected_date": selected.get("Date") if selected else None,
        "selected_period_from": selected_start.isoformat() if selected_start else None,
        "selected_period_to": selected_end.isoformat() if selected_end else None,
        "source_entity": DOC_ENTITY,
    }


def _select_production_plan_doc_with_fallback(
    cur,
    shop: ShopKey,
    ref_year: int,
    ref_month: int,
    *,
    lookback_months: int = 12,
) -> tuple[dict | None, dict, int, int]:
    """Документ за месяц; если нет — последний найденный за предыдущие месяцы.

    Нужно для незавершённого месяца (август), когда недельные планы ещё не проведены,
    а на плитке «на текущий момент» должен оставаться последний доступный факт.
    """
    y, m = ref_year, ref_month
    for step in range(lookback_months + 1):
        selected, debug = _select_production_plan_doc(cur, shop, y, m)
        if selected is not None:
            if step > 0:
                debug = {
                    **debug,
                    "selection_source": "fallback_latest_prior_month",
                    "requested_year": ref_year,
                    "requested_month": ref_month,
                    "effective_year": y,
                    "effective_month": m,
                    "fallback_steps": step,
                }
            return selected, debug, y, m
        if m == 1:
            y, m = y - 1, 12
        else:
            m -= 1
    empty_debug = {
        "selection_source": "no_document",
        "documents_count": 0,
        "requested_year": ref_year,
        "requested_month": ref_month,
        "effective_year": None,
        "effective_month": None,
        "production_dept_key": PRODUCTION_DEPT_KEY[shop],
        "production_dept_name": PRODUCTION_DEPT_NAME[shop],
        "source_entity": DOC_ENTITY,
    }
    return None, empty_debug, ref_year, ref_month


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
    detail_rows, plan_by_product, fact_by_product = _instrument_breakdown(rows, shop, output_period)
    doc_start, doc_end = _doc_period(doc)
    target_start, target_end_exclusive = _target_work_week_bounds(ref_year, ref_month)
    target_end = target_end_exclusive - timedelta(days=1)
    label = ""
    week_start = doc_start
    week_end = doc_end
    if output_period == "week":
        # «Текущая неделя» на форме — период документа, не расчётный Пн–Пт.
        if not (week_start and week_end):
            week_start = target_start
            week_end = target_end
        if week_start and week_end:
            label = f"{week_start.strftime('%d.%m')}–{week_end.strftime('%d.%m.%Y')}"
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
        "plan_by_dept": plan_by_product,
        "fact_by_dept": fact_by_product,
        "production_plan_rows": detail_rows,
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
        "source_entity": DOC_ENTITY,
    }
    return row, debug


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


def _period_totals_from_production_plan(
    cur,
    shop: ShopKey,
    start: date,
    end_exclusive: date,
    *,
    date_field: str = FLD_PERIOD_FROM,
    contained: bool = False,
    filter_department: bool = True,
) -> tuple[float, float, dict]:
    p_start = to_1c_dt(start)
    p_end = to_1c_dt(end_exclusive)
    docs = _load_docs_between(
        cur,
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
    cur,
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
            cur,
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


_PERIOD_SNAPSHOT_KEYS = (
    "plan",
    "fact",
    "kpi_pct",
    "has_data",
    "label",
    "week_start",
    "week_end",
    "plan_by_dept",
    "fact_by_dept",
    "production_plan_rows",
    "values_unit",
)


def _period_snapshot(row: dict | None) -> dict:
    if not isinstance(row, dict):
        return {}
    return {key: row.get(key) for key in _PERIOD_SNAPSHOT_KEYS if key in row}


def _same_year_month(left: dict, right: dict) -> bool:
    try:
        return int(left.get("year") or 0) == int(right.get("year") or -1) and int(left.get("month") or 0) == int(
            right.get("month") or -1
        )
    except (TypeError, ValueError):
        return False


def as_period(data: dict | None, period: str) -> dict:
    """Копия месячного кэша, где plan/fact — колонка недели, месяца или итого.

    Кэш на диске хранит колонку «текущий месяц» в plan/fact и все три колонки
    в ``periods``. Плитки недели и итого не должны читать месячную серию.
    """
    period = period if period in {"month", "week", "total"} else "month"
    if not isinstance(data, dict):
        return {"months": [], "monthly_data": [], "last_full_month_row": None, "period_type": period}

    ref_year = int(data.get("year") or 0)
    ref_month = int(data.get("ref_month") or 0)
    fallback = None
    if period == "week":
        fallback = data.get("last_week_row")
    elif period == "total":
        fallback = data.get("last_total_row")
    elif period == "month":
        fallback = data.get("last_full_month_row")

    months: list[dict] = []
    for src in data.get("months") or []:
        if not isinstance(src, dict):
            continue
        row = dict(src)
        periods = row.get("periods") if isinstance(row.get("periods"), dict) else {}
        snap = periods.get(period) if period != "month" else None
        if isinstance(snap, dict) and snap:
            row.update(snap)
        elif (
            period != "month"
            and isinstance(fallback, dict)
            and _same_year_month(row, fallback)
        ):
            row.update(_period_snapshot(fallback))
        row.pop("periods", None)
        months.append(row)

    ref = next(
        (
            row
            for row in months
            if int(row.get("year") or 0) == ref_year and int(row.get("month") or 0) == ref_month
        ),
        None,
    )
    if ref is None and isinstance(fallback, dict) and fallback.get("has_data") and _same_year_month(
        fallback, {"year": ref_year, "month": ref_month}
    ):
        ref = dict(fallback)
        ref.update(_period_snapshot(fallback))
    elif ref is None:
        ref = {
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU.get(ref_month, str(ref_month)).lower() if ref_month else "",
            "plan": None,
            "fact": None,
            "kpi_pct": None,
            "has_data": False,
        }

    out = dict(data)
    out["months"] = months
    out["monthly_data"] = months
    out["last_full_month_row"] = dict(ref) if isinstance(ref, dict) else None
    out["selected_row"] = out["last_full_month_row"]
    out["period_type"] = period
    if isinstance(ref, dict):
        out["ytd"] = {
            "total_plan": ref.get("plan"),
            "total_fact": ref.get("fact"),
            "kpi_pct": ref.get("kpi_pct"),
            "months_with_data": 1 if ref.get("has_data") else 0,
            "months_total": 1,
            "values_unit": ref.get("values_unit") or (data.get("ytd") or {}).get("values_unit"),
        }
        month_name = str(ref.get("month_name") or (MONTH_RU.get(ref_month, "") if ref_month else ""))
        year_label = ref.get("year") or ref_year
        if ref.get("has_data") and period == "week" and ref.get("label"):
            period_label = str(ref.get("label"))
            period_type = "last_week"
        elif ref.get("has_data") and period == "total":
            period_label = f"Итого за {month_name} {year_label}".strip()
            period_type = "document_total"
        else:
            pretty = f"{month_name[:1].upper()}{month_name[1:]} {year_label}".strip() if month_name else ""
            period_label = pretty
            period_type = "current_month"
        out["kpi_period"] = {
            "type": period_type,
            "year": ref.get("year") or ref_year,
            "month": ref.get("month") or ref_month,
            "month_name": month_name,
            "requested_year": ref_year,
            "requested_month": ref_month,
            "label": period_label,
            "week_start": ref.get("week_start"),
            "week_end": ref.get("week_end"),
        }
    return out


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
    rows = []
    debug_by_month: dict[str, dict] = {}
    try:
        cur = None
        for mm in range(1, ref_month + 1):
            selected_doc, selection_debug = _select_production_plan_doc(cur, shop, ref_year, mm)
            row, row_debug = _row_from_document(
                selected_doc,
                shop,
                "month",
                ref_year=ref_year,
                ref_month=mm,
                unit=unit,
            )
            week_variant, _week_variant_debug = _row_from_document(
                selected_doc,
                shop,
                "week",
                ref_year=ref_year,
                ref_month=mm,
                unit=unit,
            )
            total_variant, _total_variant_debug = _row_from_document(
                selected_doc,
                shop,
                "total",
                ref_year=ref_year,
                ref_month=mm,
                unit=unit,
            )
            row["periods"] = {
                "month": _period_snapshot(row),
                "week": _period_snapshot(week_variant),
                "total": _period_snapshot(total_variant),
            }
            debug_by_month[f"{ref_year}-{mm:02d}"] = {
                "selected_document": selection_debug,
                "selected_row": row_debug,
                "week": {"plan": week_variant.get("plan"), "fact": week_variant.get("fact")},
                "total": {"plan": total_variant.get("plan"), "fact": total_variant.get("fact")},
            }
            rows.append(row)

        quarterly_data, yearly_data = _aggregate_rows(rows)
        total_plan = sum(float(row.get("plan") or 0) for row in rows)
        total_fact = sum(float(row.get("fact") or 0) for row in rows)

        selected_doc, selection_debug, eff_y, eff_m = _select_production_plan_doc_with_fallback(
            cur, shop, ref_year, ref_month
        )
        week_row, week_debug = _row_from_document(
            selected_doc,
            shop,
            "week",
            ref_year=eff_y,
            ref_month=eff_m,
            unit=unit,
        )
        # Накопление по неделям — за месяц, где реально есть документ.
        weekly_cumulative, weekly_cumulative_debug = _weekly_cumulative_points(
            cur,
            shop,
            eff_y,
            eff_m,
            unit,
        )
        month_doc, month_sel_debug, month_y, month_m = _select_production_plan_doc_with_fallback(
            cur, shop, ref_year, ref_month
        )
        month_tile_row, month_tile_debug = _row_from_document(
            month_doc,
            shop,
            "month",
            ref_year=month_y,
            ref_month=month_m,
            unit=unit,
        )
        total_row, total_debug = _row_from_document(
            month_doc,
            shop,
            "total",
            ref_year=month_y,
            ref_month=month_m,
            unit=unit,
        )
    except Exception:
        if cached is not None:
            return cached
        raise

    last_data_row = next((r for r in reversed(rows) if r.get("has_data")), None)
    display_month_row = month_tile_row if month_tile_row.get("has_data") else (
        dict(last_data_row) if last_data_row else (dict(rows[-1]) if rows else None)
    )
    kpi_year = int((display_month_row or {}).get("year") or ref_year)
    kpi_month = int((display_month_row or {}).get("month") or ref_month)

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "shop": shop,
        "year": ref_year,
        "ref_month": ref_month,
        "months": rows,
        "last_week_row": week_row,
        "last_total_row": total_row,
        "weekly_cumulative": weekly_cumulative,
        "quarterly_data": quarterly_data,
        "yearly_data": yearly_data,
        "last_full_month_row": display_month_row,
        "ytd": {
            "total_plan": round(total_plan, 2) if rows else None,
            "total_fact": round(total_fact, 2) if rows else None,
            "kpi_pct": _kpi_pct(total_plan, total_fact),
            "months_with_data": sum(1 for row in rows if row.get("has_data")),
            "months_total": len(rows),
            "values_unit": unit if rows else None,
        },
        "kpi_period": {
            "type": "last_available_month" if (kpi_year, kpi_month) != (ref_year, ref_month) else "current_month",
            "year": kpi_year,
            "month": kpi_month,
            "month_name": MONTH_RU[kpi_month].lower(),
            "requested_year": ref_year,
            "requested_month": ref_month,
        },
        "debug": {
            "source": DOC_ENTITY,
            "tabular_field": TABULAR_FIELD,
            "production_dept_key": PRODUCTION_DEPT_KEY[shop],
            "months": debug_by_month,
            "last_week": {
                "selected_document": selection_debug,
                "selected_row": week_debug,
            },
            "display_month": {
                "selected_document": month_sel_debug,
                "selected_row": month_tile_debug,
            },
            "last_total": {
                "selected_document": month_sel_debug,
                "selected_row": total_debug,
            },
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
    data = get_prod_deputy_output_monthly(shop, year=ref_year, month=ref_month)
    return as_period(data, period)


__all__ = [
    "ShopKey",
    "OutputPeriod",
    "VALUES_UNIT",
    "cache_path",
    "as_period",
    "get_prod_deputy_output_monthly",
    "get_prod_deputy_output_period",
]
