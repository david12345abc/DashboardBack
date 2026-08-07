# -*- coding: utf-8 -*-
"""PD-M1.1.* — выполнение/факт производственного плана из MSSQL (erp_pm).

Источник: Document.ТД_ПроизводственныйПлан
  header  _Document185292
  tabular _Document185292_VT185297 (ВыполнениеПроизводственногоПлана)
"""
from __future__ import annotations

import json
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from comdir.common import connect, to_1c_dt, uuid_to_1c_bytes

from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period

ShopKey = Literal["pc1", "pc2"]
OutputPeriod = Literal["month", "week", "total"]

SOURCE_TAG = "prod_deputy_output_production_plan_mssql_v13_fallback"
DOC_ENTITY = "Document_ТД_ПроизводственныйПлан"
TABULAR_FIELD = "ВыполнениеПроизводственногоПлана"

DOC_TABLE = "_Document185292"
VT_TABLE = "_Document185292_VT185297"
PRODUCT_CATALOG = "_Reference184258"
FLD_PERIOD_FROM = "_Fld185293"  # ПериодС
FLD_PERIOD_TO = "_Fld185294"  # ПериодПо
FLD_DEPT = "_Fld185296RRef"  # Подразделение

# VT numeric fields ↔ logical OData names (kept for PERIOD_FIELDS / breakdown).
VT_FIELD_MAP: dict[str, str] = {
    "ПланШт": "_Fld185300",
    "ПланРуб": "_Fld185301",
    "ФактШт": "_Fld185302",
    "ФактРуб": "_Fld185303",
    "ПланШтМесяц": "_Fld185323",
    "ПланРубМесяц": "_Fld185324",
    "ФактШтМесяц": "_Fld185325",
    "ФактРубМесяц": "_Fld185326",
    "ПланШтИтого": "_Fld185327",
    "ПланРубИтого": "_Fld185328",
    "ФактШтИтого": "_Fld185329",
    "ФактРубИтого": "_Fld185330",
}

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


def _dept_bin(shop: ShopKey) -> bytes:
    return uuid_to_1c_bytes(PRODUCTION_DEPT_KEY[shop])


def _period_dt_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    p0 = to_1c_dt(date(year, month, 1))
    if month == 12:
        p_next = to_1c_dt(date(year + 1, 1, 1))
    else:
        p_next = to_1c_dt(date(year, month + 1, 1))
    return p0, p_next


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
    """Загрузить документы с табличной частью; ключи — как в OData для совместимости."""
    if contained:
        period_sql = f"d.[{FLD_PERIOD_FROM}] >= ? AND d.[{FLD_PERIOD_TO}] < ?"
        period_params: list[Any] = [p_start, p_end]
    else:
        period_sql = f"d.[{date_field}] >= ? AND d.[{date_field}] < ?"
        period_params = [p_start, p_end]

    dept_sql = ""
    params: list[Any] = list(period_params)
    if filter_department:
        dept_sql = f" AND d.[{FLD_DEPT}] = ?"
        params.append(_dept_bin(shop))

    cur.execute(
        f"""
        SELECT d._IDRRef, d._Number, d._Date_Time,
               d.[{FLD_PERIOD_FROM}], d.[{FLD_PERIOD_TO}], d.[{FLD_DEPT}]
        FROM [{DOC_TABLE}] d WITH (NOLOCK)
        WHERE d._Posted = 0x01
          AND d._Marked = 0x00
          AND {period_sql}
          {dept_sql}
        ORDER BY d.[{FLD_PERIOD_TO}] DESC, d._Date_Time DESC, d._Number DESC
        """,
        *params,
    )
    headers = cur.fetchall()
    if not headers:
        return []

    refs = [r[0] for r in headers]
    # Load VT lines for all selected docs
    placeholders = ",".join("?" for _ in refs)
    vt_cols = ", ".join(f"v.[{col}] AS [{name}]" for name, col in VT_FIELD_MAP.items())
    cur.execute(
        f"""
        SELECT v.[{DOC_TABLE}_IDRRef] AS doc_ref,
               v._LineNo185298 AS LineNumber,
               ISNULL(p._Description, N'') AS ГруппаПродукции,
               v._Fld185304 AS Комментарий,
               {vt_cols}
        FROM [{VT_TABLE}] v WITH (NOLOCK)
        LEFT JOIN [{PRODUCT_CATALOG}] p WITH (NOLOCK)
          ON p._IDRRef = v._Fld185299_RRRef
        WHERE v.[{DOC_TABLE}_IDRRef] IN ({placeholders})
        ORDER BY v.[{DOC_TABLE}_IDRRef], v._LineNo185298
        """,
        *refs,
    )
    vt_rows = cur.fetchall()
    vt_colnames = [d[0] for d in cur.description]
    by_ref: dict[bytes, list[dict]] = {r: [] for r in refs}
    for row in vt_rows:
        item = dict(zip(vt_colnames, row))
        doc_ref = item.pop("doc_ref")
        by_ref.setdefault(doc_ref, []).append(item)

    docs: list[dict] = []
    for ref, number, dt, p_from, p_to, dept in headers:
        docs.append({
            "Ref_Key": ref.hex() if isinstance(ref, (bytes, bytearray)) else str(ref),
            "Number": number,
            "Date": _iso_date(dt),
            "ПериодС": _iso_date(p_from),
            "ПериодПо": _iso_date(p_to),
            "Подразделение_Key": PRODUCTION_DEPT_KEY[shop],
            TABULAR_FIELD: by_ref.get(ref, []),
            "_ref": ref,
            "_period_from_dt": p_from,
            "_period_to_dt": p_to,
            "_date_dt": dt,
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
        "sql_table": DOC_TABLE,
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
        "sql_table": DOC_TABLE,
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
        "sql_table": DOC_TABLE,
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
    cn = connect()
    try:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
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
            debug_by_month[f"{ref_year}-{mm:02d}"] = {
                "selected_document": selection_debug,
                "selected_row": row_debug,
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
    except Exception:
        if cached is not None:
            return cached
        raise
    finally:
        cn.close()

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
            "sql_table": DOC_TABLE,
            "sql_vt": VT_TABLE,
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
    cn = connect()
    try:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        selected_doc, selection_debug, eff_y, eff_m = _select_production_plan_doc_with_fallback(
            cur, shop, ref_year, ref_month
        )
        row, row_debug = _row_from_document(
            selected_doc,
            shop,
            period,
            ref_year=eff_y,
            ref_month=eff_m,
            unit=unit,
        )
    except Exception:
        selected_doc = None
        selection_debug = {"selection_source": "cached_fallback_after_sql_error"}
        eff_y, eff_m = ref_year, ref_month
        if period == "week":
            row = dict(data.get("last_week_row") or data.get("last_full_month_row") or {})
        elif period == "total":
            row = dict(data.get("last_full_month_row") or {})
            if not row.get("has_data"):
                row = dict((data.get("yearly_data") or [{}])[-1] or {})
            row.setdefault("month", (row.get("month") or ref_month))
            row.setdefault("month_name", MONTH_RU[int(row.get("month") or ref_month)].lower())
            row["label"] = f"Итого за {row.get('month_name')} {row.get('year') or ref_year}"
        else:
            row = dict(data.get("last_full_month_row") or {})
        row_debug = {"source": "cached_fallback_after_sql_error"}
        if row.get("year") and row.get("month"):
            eff_y, eff_m = int(row["year"]), int(row["month"])
    finally:
        cn.close()

    used_fallback = (eff_y, eff_m) != (ref_year, ref_month)
    period_type = (
        "last_week" if period == "week" else ("ytd" if period == "total" else "current_month")
    )
    if used_fallback:
        period_type = "last_available_month"
    kpi_period = {
        "type": period_type,
        "year": eff_y,
        "month": eff_m,
        "month_name": MONTH_RU[eff_m].lower(),
        "requested_year": ref_year,
        "requested_month": ref_month,
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
    "VALUES_UNIT",
    "cache_path",
    "get_prod_deputy_output_monthly",
    "get_prod_deputy_output_period",
]
