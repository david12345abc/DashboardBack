"""Склейка KPI дашборда службы управления персоналом для общего ``getkpi.views``."""
from __future__ import annotations

from typing import Any

from sup.hrd_m1 import get_hrd_m1_ytd
from sup.hrd_m4 import get_hrd_m4_ytd
from sup.hrd_q4 import get_hrd_q4_ytd

SUP_KPI_IDS: frozenset[str] = frozenset({"HRD-M1", "HRD-M4", "HRD-Q4"})
SUP_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = SUP_KPI_IDS

_PAYLOAD_BUILDERS = {
    "HRD-M1": get_hrd_m1_ytd,
    "HRD-M4": get_hrd_m4_ytd,
    "HRD-Q4": get_hrd_q4_ytd,
}


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    builder = _PAYLOAD_BUILDERS.get(str(kpi_id or "").strip().upper())
    if builder is None:
        return False
    payload = builder(year=year, month=month)
    if payload is None:
        return False
    entry["data_granularity"] = payload.get("data_granularity", "monthly")
    entry["monthly_data"] = payload.get("monthly_data") or []
    entry["last_full_month_row"] = payload.get("last_full_month_row")
    entry["ytd"] = payload.get("ytd") or {}
    entry["kpi_period"] = payload.get("kpi_period")
    if payload.get("tables") is not None:
        entry["tables"] = payload["tables"]
    if payload.get("reference_analytics") is not None:
        entry["reference_analytics"] = payload["reference_analytics"]
    if payload.get("debug") is not None:
        entry["debug"] = payload["debug"]
    return True


def merge_sup_tables_into_universal_payload(
    tablitsy: dict[str, Any],
    entries_by_id: dict[str, dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> None:
    hrd_m1 = entries_by_id.get("HRD-M1") or {}
    tables = hrd_m1.get("tables") or {}
    if not isinstance(tables, dict):
        return
    for key, table in tables.items():
        if isinstance(table, dict):
            tablitsy[key] = table
