"""Факт ФОТ 1С-M4 из SQL-бэкапа (``c1_m4_core``)."""

from __future__ import annotations

from typing import Any

from .c1_m4_core import calc_c1_m4_fot_month


def compute_c1_m4_fot_fact_monthly(year: int, month: int) -> dict[str, Any]:
    snap = calc_c1_m4_fot_month(year, month)
    return {
        "year": snap["year"],
        "month": snap["month"],
        "month_name": snap["month_name"],
        "total_fact": snap.get("fact"),
        "groups": snap.get("groups") or {},
        "article_totals": snap.get("article_totals") or {},
        "debug": snap.get("debug") or {},
    }
