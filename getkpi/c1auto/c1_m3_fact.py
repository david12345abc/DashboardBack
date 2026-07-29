"""Факт бюджета 1С-M3 из SQL-бэкапа (``c1_m3_core``)."""

from __future__ import annotations

from typing import Any

from .c1_m3_core import compute_c1_m3_fact_monthly as _compute_sql


def compute_c1_m3_fact_monthly(year: int, month: int) -> dict[str, Any]:
    return _compute_sql(year, month)
