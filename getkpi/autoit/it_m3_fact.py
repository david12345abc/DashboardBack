"""Факт бюджета ИТ-M3: ТД_ЦФО «Служба автоматизации», подразделение «Отдел информационных технологий»."""

from __future__ import annotations

from typing import Any

from getkpi.budget_request_fact import compute_budget_request_fact_monthly, norms_from_labels

IT_M3_TD_CFO_LABEL = "Служба автоматизации"
IT_M3_DEPARTMENT_LABEL = "Отдел информационных технологий"

IT_M3_TD_CFO_NORMS = norms_from_labels(
    IT_M3_TD_CFO_LABEL,
    "служба автоматизации",
)
IT_M3_DEPARTMENT_NORMS = norms_from_labels(
    IT_M3_DEPARTMENT_LABEL,
    "отдел информационных технологий",
    "отдел ит",
    "оит",
)


def compute_it_m3_fact_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_budget_request_fact_monthly(
        year,
        month,
        kpi_id="IT-M3-FACT",
        component="getkpi.autoit.it_m3_fact",
        td_cfo_label=IT_M3_TD_CFO_LABEL,
        td_cfo_norms=IT_M3_TD_CFO_NORMS,
        department_label=IT_M3_DEPARTMENT_LABEL,
        department_norms=IT_M3_DEPARTMENT_NORMS,
    )
