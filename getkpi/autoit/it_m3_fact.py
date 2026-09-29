"""Факт бюджета ИТ-M3: списания безналичных ДС по группе «СА» без статей 1С."""

from __future__ import annotations

from typing import Any

from .it_m3_core import compute_it_m3_fact_monthly as _compute_it_m3_fact_monthly

IT_M3_TD_CFO_LABEL = "Служба автоматизации"
IT_M3_DEPARTMENT_LABEL = "Отдел информационных технологий"


def compute_it_m3_fact_monthly(year: int, month: int) -> dict[str, Any]:
    return _compute_it_m3_fact_monthly(year, month)
