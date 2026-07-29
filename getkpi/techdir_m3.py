"""KPI TD-M3: бюджет техдирекции — SQL-бэкап ``getkpi.td_m3``, кэш раз в день."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from qualdir.sql_tile_cache import get_ytd_via_cache

from . import td_m3

CACHE_FILE_PREFIX = "techdir_m3_ytd"
CACHE_SOURCE_TAG = "techdir_m3_ytd_sql_v1"
CACHE_VERSION = 8

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _normalize_month_names(payload: dict[str, Any]) -> dict[str, Any]:
    def _fix_row(row: dict[str, Any] | None) -> None:
        if not isinstance(row, dict):
            return
        m = row.get("month")
        if isinstance(m, int) and m in MONTH_NAMES:
            row["month_name"] = MONTH_NAMES[m]

    for row in payload.get("monthly_data") or []:
        _fix_row(row if isinstance(row, dict) else None)
    _fix_row(payload.get("last_full_month_row"))
    period = payload.get("kpi_period")
    if isinstance(period, dict):
        m = period.get("month")
        if isinstance(m, int) and m in MONTH_NAMES:
            period["month_name"] = MONTH_NAMES[m]
    # Плитка показывает опорный месяц, а не сумму YTD.
    ref = payload.get("last_full_month_row") or {}
    ytd = payload.get("ytd")
    if isinstance(ytd, dict) and ref:
        ytd["total_plan"] = ref.get("plan")
        ytd["total_fact"] = ref.get("fact")
        ytd["kpi_pct"] = ref.get("kpi_pct")
    return payload


def _build_td_m3_charts(monthly_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    points = [
        {
            "month": row.get("month"),
            "month_name": row.get("month_name"),
            "year": row.get("year"),
            "plan": row.get("plan"),
            "fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
        }
        for row in monthly_rows
    ]
    categories = [row.get("month_name") for row in monthly_rows]
    plan_values = [row.get("plan") for row in monthly_rows]
    fact_values = [row.get("fact") for row in monthly_rows]
    return {
        "TD-M3-C1": {
            "kpi_id": "TD-M3-C1",
            "name": "Тренд 12 месяцев: бюджет затрат ТД",
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "Линейный тренд",
            "formula": "План и факт берутся из помесячной плитки TD-M3.",
            "series": [{
                "kpi_id": "TD-M3",
                "name": "TD-M3",
                "chart_type": "line_plan_fact_monthly",
                "chart_type_label": "План/факт по месяцам",
                "points": points,
            }],
        },
        "TD-M3-C2": {
            "kpi_id": "TD-M3-C2",
            "name": "План/факт по месяцам: бюджет затрат ТД",
            "periodicity": "ежемесячно",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "Столбцы",
            "formula": "План и факт берутся из помесячной плитки TD-M3.",
            "series": [{
                "kpi_id": "TD-M3",
                "name": "TD-M3",
                "chart_type": "column_plan_fact_monthly",
                "chart_type_label": "Столбцы",
                "categories": categories,
                "plan": plan_values,
                "fact": fact_values,
                "points": points,
            }],
        },
        "TD-M3-C3": {
            "kpi_id": "TD-M3-C3",
            "name": "Структура плана/факта TD-M3",
            "periodicity": "ежемесячно",
            "chart_type": "heatmap_rag",
            "chart_type_label": "Heatmap / структура",
            "formula": "Тепловая карта по помесячным значениям TD-M3.",
            "series": [{
                "kpi_id": "TD-M3",
                "name": "TD-M3",
                "chart_type": "heatmap_rag",
                "chart_type_label": "Heatmap / структура",
                "points": points,
            }],
        },
    }


def _build_payload(year: int, month: int) -> dict[str, Any]:
    payload = _normalize_month_names(td_m3.build_td_m3_payload(year=year, month=month))
    payload["Графики"] = _build_td_m3_charts(payload.get("monthly_data") or [])
    return payload


def _cache_path(year: int, month: int) -> Path:
    return ytd_json_cache.cache_path(CACHE_FILE_PREFIX, year, month)


def get_td_m3_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_FILE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="techdir_m3_sql",
        compute_fn=_build_payload,
        kpi_id="TD-M3",
    )
