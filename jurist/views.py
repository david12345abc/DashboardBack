"""Склейка KPI дашборда «Юрисконсультант» для ``getkpi.views``."""
from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path
from typing import Any

from devdir.rd_monthly_period import normalize_rd_tile_period
from jurist.companies import DEPARTMENT, TILE_KPI_IDS, company_for_kpi
from jurist.contracts import cache_path, goals_rows, load_company_totals

logger = logging.getLogger(__name__)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def is_jurist_department(dept: str | None) -> bool:
    normalized = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", (dept or "").strip()).lower().replace("ё", "е"),
    )
    return normalized in {DEPARTMENT.casefold(), "jurist"}


def is_jurist_tile_kpi_id(kpi_id: str) -> bool:
    return str(kpi_id or "").strip().upper() in TILE_KPI_IDS


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    if not is_jurist_tile_kpi_id(kpi_id):
        return []
    return [cache_path(ref_y, ref_m)]


def clear_jurist_tile_rag(tile: dict[str, Any]) -> None:
    """На плитке одно число, без процента выполнения и цвета."""
    if not is_jurist_tile_kpi_id(str(tile.get("kpi_id") or "")):
        return
    tile.pop("kpi_pct", None)
    tile["color"] = None
    goals = tile.get("goals")
    if not goals:
        row = tile.get("last_full_month_row") or {}
        goals = row.get("goals") if isinstance(row, dict) else None
    if goals:
        tile["goals"] = goals
        tile["goals_title"] = "Цель договора"


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    spec = company_for_kpi(kpi_id)
    if spec is None:
        return False
    company, metric = spec
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    value: int | float | None
    try:
        totals = load_company_totals(year=ref_y, month=ref_m).get(company["code"]) or {}
        raw = totals.get(metric)
        if raw is None:
            value = None
        elif metric == "count":
            value = int(raw)
        else:
            value = round(float(raw), 2)
    except Exception:
        logger.exception("jurist: не удалось посчитать %s", kpi_id)
        value = None
        totals = {}

    goals = goals_rows(totals.get("by_goal"), metric=metric)
    row = {
        "year": ref_y,
        "month": ref_m,
        "month_name": MONTH_NAMES[ref_m],
        "plan": None,
        "fact": value,
        "kpi_pct": None,
        "has_data": value is not None,
        "goals": goals,
    }
    entry["data_granularity"] = "snapshot"
    entry["monthly_data"] = [row]
    entry["last_full_month_row"] = row
    entry["goals"] = goals
    entry["goals_title"] = "Цель договора"
    entry["ytd"] = {
        "total_plan": None,
        "total_fact": value,
        "kpi_pct": None,
        "months_with_data": 1 if value is not None else 0,
        "months_total": 1,
    }
    entry["kpi_period"] = {
        "type": "current_month",
        "year": ref_y,
        "month": ref_m,
        "month_name": MONTH_NAMES[ref_m],
    }
    if totals:
        entry["debug"] = {
            "odata_name": company["odata_name"],
            "org_guid": company["org_guid"],
            "metric": metric,
            "by_status": totals.get("by_status"),
            "rule": (
                "Catalog_ДоговорыКонтрагентов, DeletionMark=false, "
                "ДатаНачалаДействия в выбранном месяце"
            ),
        }
    return True


def merge_contracts_table(tablitsy: dict, *, year: int, month: int) -> None:
    """Помесячная таблица договоров. Клик по плитке фильтрует её по компании."""
    from jurist.contracts import contract_table_rows

    ref_m = max(1, min(12, int(month)))
    ref_y = int(year)
    try:
        rows = contract_table_rows(year=ref_y, month=ref_m)
    except Exception:
        logger.exception("jurist: не удалось собрать таблицу договоров")
        rows = []
    tablitsy["YR-T-CONTRACTS"] = {
        "name": f"Договоры за {MONTH_NAMES[ref_m]} {ref_y}",
        "periodicity": "ежемесячно",
        "description": "Договоры контрагентов за выбранный месяц, без удалённых и без исключённых целей",
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "columns": [
            "Организация",
            "Номер",
            "Наименование",
            "Контрагент",
            "Цель договора",
            "Статус",
            "Действует с",
            "Сумма",
        ],
        "rows": rows,
    }
