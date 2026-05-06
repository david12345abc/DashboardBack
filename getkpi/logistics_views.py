from __future__ import annotations

import re
from datetime import date

from . import (
    cache_manager,
    calc_logistics_price_deviation,
    calc_logistics_supplier_share,
    calc_logistics_tmc_on_time,
)

LOGISTICS_KPI_IDS = {"LOG-M1", "LOG-M2", "LOG-Q1"}


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


def rag_price_deviation(fact_pct: float | None) -> str:
    if fact_pct is None:
        return "unknown"
    if fact_pct <= 5:
        return "green"
    if fact_pct <= 10:
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


def tile_color(kpi_id: str, entry: dict) -> tuple[float | None, str] | None:
    if kpi_id == "LOG-M2":
        ref_row = entry.get("last_full_month_row") or {}
        pct = ref_row.get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        return pct, rag_price_deviation(pct)

    if kpi_id == "LOG-Q1":
        qrows = entry.get("quarterly_data") or []
        ref_row = qrows[-1] if qrows else {}
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


def build_kpi_entry(kpi_id: str, entry: dict, *, year: int | None = None, month: int | None = None) -> dict | None:
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
        q_year, q_num = int(ref_y), (int(ref_m) - 1) // 3 + 1
        qrows = data.get("quarterly_data") or []
        selected_qrow = next(
            (
                row for row in qrows
                if int(row.get("year") or 0) == q_year and int(row.get("quarter") or 0) == q_num
            ),
            qrows[-1] if qrows else None,
        )
        entry["data_granularity"] = "quarterly"
        entry["quarterly_data"] = [selected_qrow] if selected_qrow else []
        entry["yearly_data"] = data.get("yearly_data") or []
        entry["ytd"] = data.get("ytd") or {}
        entry["kpi_period"] = {"type": "selected_quarter", "year": q_year, "quarter": q_num}
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
        "LOG-M1": "Поставки ТМЦ в срок",
        "LOG-M2": "Отклонение цены",
    }
    series: list[dict] = []

    for kid in ("LOG-M1", "LOG-M2"):
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
            "name": "Логистика: помесячная динамика KPI",
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
    elif kpi_id == "LOG-Q1":
        tile["unit"] = "поставщиков"
        tile["units"] = "поставщиков"
