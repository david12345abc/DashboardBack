from __future__ import annotations

import re
from datetime import date

from . import (
    cache_manager,
    calc_logistics_budget,
    calc_logistics_client_dz,
    calc_logistics_fot,
    calc_logistics_price_deviation,
    calc_logistics_supplier_share,
    calc_logistics_tmc_on_time,
)

from .kpi_periods import last_full_month

LOGISTICS_KPI_IDS = {
    "LOG-M1",
    "LOG-M2",
    "LOG-M3.B",
    "LOG-M3.F",
    "LOG-M5",
    "LOG-Q1",
    "LOG-Q2",
}
LOGISTICS_BUDGET_FOT_SPLIT_IDS = {"LOG-M3.B", "LOG-M3.F"}
LOGISTICS_CLIENT_DZ_IDS = {"LOG-M5"}
LOG_Q1_NAME = "Доля квалифицированных поставщиков"
LOG_Q1_FORMULA = "Поставщики с суммой баллов оценки > 45 / Все поставщики из оценки периода × 100%"

MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def is_logistics_head_department(dept: str | None) -> bool:
    normalized = re.sub(r"\s+", " ", (dept or "").strip().lower())
    return normalized == "начальник службы логистики"


def is_logistics_kpi(kpi_id: str | None) -> bool:
    return str(kpi_id or "") in LOGISTICS_KPI_IDS


def kpi_definition_fallback(department: str) -> list[dict] | None:
    if not is_logistics_head_department(department):
        return None
    try:
        from .management.commands.import_logistics_kpi import LOGISTICS_KPI_DEFINITIONS
    except Exception:
        return None
    return [{**dict(item), "department": department} for item in LOGISTICS_KPI_DEFINITIONS]


def normalize_kpi_definitions(department: str, rows: list[dict]) -> list[dict]:
    if not is_logistics_head_department(department):
        return rows

    ids = {str(row.get("kpi_id") or "") for row in rows}
    if "LOG-M3" in ids or not LOGISTICS_KPI_IDS.issubset(ids):
        fallback = kpi_definition_fallback(department)
        if fallback:
            return fallback

    normalized_rows = [row for row in rows if str(row.get("kpi_id") or "") != "LOG-M3"]
    for row in normalized_rows:
        if str(row.get("kpi_id") or "") == "LOG-Q1":
            row["name"] = LOG_Q1_NAME
            row["frequency"] = "ежемесячно"
            row["formula"] = LOG_Q1_FORMULA
            row["source"] = "1С ERP / Регистр сведений ТД_ОценкаПоставщиков"
            row["monthly_target"] = row.get("monthly_target") or row.get("quarterly_target") or "≥80%"
            row["quarterly_target"] = None
    return normalized_rows


def rag_price_deviation(fact_pct: float | None) -> str:
    if fact_pct is None:
        return "unknown"
    if fact_pct < 5:
        return "green"
    if abs(fact_pct - 5.0) < 1e-9:
        return "yellow"
    return "red"


def _rag_higher_better(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _rag_limit_pct(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct <= 100:
        return "green"
    if pct <= 110:
        return "yellow"
    return "red"


def _rag_turnover_pct(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct < 90:
        return "green"
    if pct <= 100:
        return "yellow"
    return "red"


def _rag_fot_limit_pct(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct > 100:
        return "red"
    if pct >= 90:
        return "yellow"
    return "green"


def _rag_dz_overdue_pct(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct < 5:
        return "green"
    if pct <= 15:
        return "yellow"
    return "red"


def _plan_fact_color(entry: dict) -> tuple[float | None, str] | None:
    row = entry.get("last_full_month_row") or {}
    plan = row.get("plan")
    fact = row.get("fact")
    if plan is None or fact is None:
        ytd = entry.get("ytd") or {}
        plan = ytd.get("total_plan")
        fact = ytd.get("total_fact")
    try:
        plan_value = float(plan)
        fact_value = float(fact)
    except (TypeError, ValueError):
        return None
    pct = round(fact_value / plan_value * 100, 1) if plan_value else None
    if fact_value < plan_value:
        return pct, "green"
    if abs(fact_value - plan_value) < 1e-9:
        return pct, "yellow"
    return pct, "red"


def tile_color(kpi_id: str, entry: dict) -> tuple[float | None, str] | None:
    if kpi_id == "LOG-M2":
        ref_row = entry.get("last_full_month_row") or {}
        pct = ref_row.get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        return pct, str(ref_row.get("color") or rag_price_deviation(pct))

    if kpi_id in LOGISTICS_BUDGET_FOT_SPLIT_IDS:
        color = _plan_fact_color(entry)
        if color is not None:
            return color
        ref_row = entry.get("ytd") or entry.get("last_full_month_row") or {}
        pct = ref_row.get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        return pct, _rag_limit_pct(pct)

    if kpi_id in LOGISTICS_CLIENT_DZ_IDS:
        ref_row = entry.get("last_full_month_row") or {}
        pct = ref_row.get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        return pct, _rag_dz_overdue_pct(pct)

    if kpi_id == "LOG-Q2":
        color = _plan_fact_color(entry)
        if color is not None:
            return color[0], _rag_turnover_pct(color[0])

    if kpi_id == "LOG-Q1":
        ref_row = entry.get("last_full_month_row") or {}
        pct = ref_row.get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        return pct, _rag_higher_better(pct)

    return None


def _ref_period(year: int | None, month: int | None) -> tuple[int, int]:
    if year and month:
        return int(year), int(month)
    today = date.today()
    return today.year, today.month


def _fot_tile_period(ref_y: int, ref_m: int) -> tuple[int, int]:
    """LOG-M3.F: в незакрытом текущем месяце показываем ФОТ за предыдущий."""
    today = date.today()
    if ref_y == today.year and ref_m == today.month:
        return last_full_month(today)
    return ref_y, ref_m


def _month_row_from_months(months: list[dict], ref_y: int, ref_m: int) -> dict | None:
    return next(
        (
            row for row in months
            if int(row.get("year") or 0) == int(ref_y)
            and int(row.get("month") or 0) == int(ref_m)
        ),
        None,
    )


def _plan_fact_period_label(ref_y: int, ref_m: int) -> str:
    return f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}"


def build_kpi_entry(kpi_id: str, entry: dict, *, year: int | None = None, month: int | None = None) -> dict | None:
    if kpi_id == "LOG-M3.B":
        ref_y, ref_m = _ref_period(year, month)
        data = cache_manager.locked_call(
            f"log_m3_budget_{ref_y}_{ref_m}",
            calc_logistics_budget.get_logistics_budget_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry["data_granularity"] = "monthly"
        entry["monthly_data"] = data.get("months") or []
        entry["quarterly_data"] = data.get("quarterly_data") or []
        entry["yearly_data"] = data.get("yearly_data") or []
        entry["last_full_month_row"] = data.get("last_full_month_row")
        entry["ytd"] = data.get("ytd") or {}
        entry["kpi_period"] = data.get("kpi_period")
        return entry

    if kpi_id == "LOG-M3.F":
        ref_y, ref_m = _ref_period(year, month)
        data = cache_manager.locked_call(
            f"log_m3_fot_{ref_y}_{ref_m}",
            calc_logistics_fot.get_logistics_fot_monthly,
            year=ref_y,
            month=ref_m,
        )
        months = data.get("months") or []
        fot_y, fot_m = _fot_tile_period(ref_y, ref_m)
        display_row = _month_row_from_months(months, fot_y, fot_m)
        entry["data_granularity"] = "monthly"
        entry["monthly_data"] = months
        entry["quarterly_data"] = data.get("quarterly_data") or []
        entry["yearly_data"] = data.get("yearly_data") or []
        entry["last_full_month_row"] = display_row
        entry["ytd"] = {
            "total_plan": display_row.get("plan") if display_row else None,
            "total_fact": display_row.get("fact") if display_row else None,
            "kpi_pct": display_row.get("kpi_pct") if display_row else None,
            "months_with_data": 1 if display_row and display_row.get("has_data") else 0,
            "months_total": len(months),
            "values_unit": "руб.",
        }
        entry["kpi_period"] = {
            "type": "last_full_month",
            "year": fot_y,
            "month": fot_m,
            "month_name": MONTH_NAMES_RU[fot_m],
        }
        return entry

    if kpi_id == "LOG-M1":
        ref_y, ref_m = _ref_period(year, month)
        data = cache_manager.locked_call(
            f"log_m1_tmc_on_time_{ref_y}_{ref_m}",
            calc_logistics_tmc_on_time.get_logistics_tmc_on_time_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry["data_granularity"] = "monthly"
        entry["monthly_data"] = data.get("months") or []
        entry["quarterly_data"] = data.get("quarterly_data") or []
        entry["yearly_data"] = data.get("yearly_data") or []
        entry["last_full_month_row"] = data.get("last_full_month_row")
        entry["ytd"] = data.get("ytd") or {}
        entry["kpi_period"] = data.get("kpi_period")
        return entry

    if kpi_id == "LOG-M2":
        ref_y, ref_m = _ref_period(year, month)
        data = cache_manager.locked_call(
            f"log_m2_price_deviation_{ref_y}_{ref_m}",
            calc_logistics_price_deviation.get_logistics_price_deviation_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry["data_granularity"] = "monthly"
        entry["monthly_data"] = data.get("months") or []
        entry["quarterly_data"] = data.get("quarterly_data") or []
        entry["yearly_data"] = data.get("yearly_data") or []
        entry["last_full_month_row"] = data.get("last_full_month_row")
        entry["ytd"] = data.get("ytd") or {}
        entry["kpi_period"] = data.get("kpi_period")
        return entry

    if kpi_id == "LOG-Q1":
        ref_y, ref_m = _ref_period(year, month)
        data = cache_manager.locked_call(
            f"log_q1_supplier_share_{ref_y}_{ref_m}",
            calc_logistics_supplier_share.get_logistics_supplier_share_monthly,
            year=ref_y,
            month=ref_m,
        )
        month_rows = data.get("months") or []
        selected_month_row = next(
            (
                row for row in month_rows
                if int(row.get("year") or 0) == int(ref_y)
                and int(row.get("month") or 0) == int(ref_m)
            ),
            month_rows[-1] if month_rows else None,
        )
        entry["data_granularity"] = "monthly"
        entry["monthly_data"] = month_rows
        entry["quarterly_data"] = data.get("quarterly_data") or []
        entry["yearly_data"] = data.get("yearly_data") or []
        entry["last_full_month_row"] = selected_month_row
        entry["ytd"] = data.get("ytd") or {}
        entry["kpi_period"] = {
            "type": "last_full_month",
            "year": int(ref_y),
            "month": int(ref_m),
        }
        return entry

    if kpi_id in LOGISTICS_CLIENT_DZ_IDS:
        data = cache_manager.locked_call(
            f"log_client_dz_{date.today().isoformat()}",
            calc_logistics_client_dz.calculate,
        )
        plan_value = data.get("total_dz")
        fact_value = data.get("total_overdue")
        row = {
            "year": date.today().year,
            "month": date.today().month,
            "month_name": MONTH_NAMES_RU[date.today().month],
            "fact": fact_value,
            "plan": plan_value,
            "kpi_pct": data.get("overdue_pct"),
            "has_data": True,
            "values_unit": "руб.",
            "total_dz": data.get("total_dz"),
            "total_overdue": data.get("total_overdue"),
            "na_datu": data.get("na_datu"),
            "window": data.get("window") or {},
        }
        entry["data_granularity"] = "snapshot"
        entry["monthly_data"] = [row]
        entry["quarterly_data"] = []
        entry["yearly_data"] = []
        entry["last_full_month_row"] = row
        entry["ytd"] = {
            "total_fact": fact_value,
            "total_plan": plan_value,
            "kpi_pct": data.get("overdue_pct"),
            "values_unit": "руб.",
            "total_dz": data.get("total_dz"),
            "total_overdue": data.get("total_overdue"),
        }
        entry["kpi_period"] = {
            "type": "snapshot",
            "as_of_date": data.get("na_datu"),
            "window": data.get("window") or {},
        }
        return entry

    return None


def _monthly_points_from_entry(entry: dict) -> list[dict]:
    points: list[dict] = []
    for row in entry.get("monthly_data") or []:
        if not isinstance(row, dict):
            continue
        points.append({
            "month": row.get("month"),
            "month_name": row.get("month_name"),
            "year": row.get("year"),
            "plan": row.get("plan"),
            "fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
            "has_data": row.get("has_data"),
        })
    return points


def build_charts(tiles_meta: list[dict], entries_by_id: dict[str, dict], ref_y: int, ref_m: int) -> dict:
    by_id = {k["kpi_id"]: k for k in tiles_meta}
    display_names = {
        "LOG-M1": "Обеспечение ТМЦ в срок",
        "LOG-M3.B": "Бюджет",
        "LOG-M3.F": "ФОТ",
    }
    series: list[dict] = []

    for kid in ("LOG-M1", "LOG-M3.B", "LOG-M3.F"):
        entry = entries_by_id.get(kid) or {}
        points = [
            point for point in _monthly_points_from_entry(entry)
            if int(point.get("year") or 0) < ref_y
            or (int(point.get("year") or 0) == ref_y and int(point.get("month") or 0) <= ref_m)
        ]
        if not points:
            continue
        if not any((p.get("plan") is not None or p.get("fact") is not None) for p in points):
            continue
        meta = by_id.get(kid, {})
        series.append({
            "kpi_id": kid,
            "name": display_names.get(kid, meta.get("name", kid)),
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": "План/факт по месяцам",
            "points": points,
        })

    if not series:
        return {}

    return {
        "LOG-C1": {
            "kpi_id": "LOG-C1",
            "name": "Логистика: ФОТ, бюджет и ТМЦ в срок",
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "Линейный тренд по месяцам",
            "series": series,
        }
    }


def apply_tile_overrides(kpi: dict, tile: dict) -> None:
    kpi_id = kpi.get("kpi_id")
    if kpi_id == "LOG-M2":
        tile["pct_lower_is_better"] = True
        tile["unit"] = "руб."
        tile["units"] = "руб."
    elif kpi_id in LOGISTICS_BUDGET_FOT_SPLIT_IDS:
        tile["pct_lower_is_better"] = True
        tile["unit"] = "руб."
    elif kpi_id in LOGISTICS_CLIENT_DZ_IDS:
        tile["pct_lower_is_better"] = True
        tile["unit"] = "руб."
        tile["units"] = "руб."
    elif kpi_id == "LOG-Q1":
        tile["name"] = LOG_Q1_NAME
        tile["unit"] = "поставщиков"
        tile["units"] = "поставщиков"


def apply_tile_value_overrides(kpi: dict, tile: dict, entry: dict) -> None:
    if kpi.get("kpi_id") == "LOG-M2":
        row = entry.get("last_full_month_row") or {}
        tile["plan"] = row.get("plan")
        tile["fact"] = row.get("fact")
        tile["kpi_pct"] = row.get("kpi_pct")
        tile["percent"] = row.get("kpi_pct")
        tile["kpi_pct_is_deviation"] = True
        tile["unit"] = "руб."
        tile["units"] = "руб."
        tile["has_data"] = bool(row.get("has_data"))
        return

    if kpi.get("kpi_id") in LOGISTICS_CLIENT_DZ_IDS:
        row = entry.get("last_full_month_row") or {}
        pct = row.get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        color = _rag_dz_overdue_pct(pct)
        tile["plan"] = row.get("plan")
        tile["fact"] = row.get("fact")
        tile["kpi_pct"] = pct
        tile["percent"] = pct
        tile["color"] = color
        tile["status_color"] = color
        tile["pct_lower_is_better"] = True
        tile["rag_direction"] = "lower_better"
        tile["unit"] = "руб."
        tile["units"] = "руб."
        tile["has_data"] = bool(row.get("has_data"))
        tile["total_dz"] = row.get("total_dz")
        tile["total_overdue"] = row.get("total_overdue")
        tile["plan_fact_period_label"] = row.get("na_datu") or ""
        return

    if kpi.get("kpi_id") not in LOGISTICS_BUDGET_FOT_SPLIT_IDS:
        return

    if kpi.get("kpi_id") == "LOG-M3.F":
        row = entry.get("last_full_month_row") or {}
        tile["plan"] = row.get("plan")
        tile["fact"] = row.get("fact")
        tile["kpi_pct"] = row.get("kpi_pct")
        tile["has_data"] = bool(row.get("has_data") or row.get("plan") is not None)
        kpi_period = entry.get("kpi_period") or {}
        period_y = kpi_period.get("year")
        period_m = kpi_period.get("month")
        if period_y and period_m:
            tile["plan_fact_period_label"] = _plan_fact_period_label(int(period_y), int(period_m))
        return

    ytd = entry.get("ytd") or {}
    tile["plan"] = ytd.get("total_plan")
    tile["fact"] = ytd.get("total_fact")
    tile["has_data"] = ytd.get("total_plan") is not None
    year = (entry.get("kpi_period") or {}).get("year")
    if year:
        tile["plan_fact_period_label"] = f"{year} год"
