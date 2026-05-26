"""Факт бюджета 1С-M3: ТД_ЦФО «Служба автоматизации», подразделение «Отдел сопровождения 1С»."""

from __future__ import annotations

from typing import Any

from getkpi.budget_request_fact import compute_budget_request_fact_monthly, norms_from_labels

C1_M3_TD_CFO_LABEL = "Служба автоматизации"
C1_M3_DEPARTMENT_LABEL = "Отдел сопровождения 1С"

C1_M3_TD_CFO_NORMS = norms_from_labels(
    C1_M3_TD_CFO_LABEL,
    "служба автоматизации",
)
C1_M3_DEPARTMENT_NORMS = norms_from_labels(
    C1_M3_DEPARTMENT_LABEL,
    "отдел сопровождения 1с",
    "отдел сопровождения 1c",
)


def compute_c1_m3_fact_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_budget_request_fact_monthly(
        year,
        month,
        kpi_id="1C-M3-FACT",
        component="getkpi.c1auto.c1_m3_fact",
        td_cfo_label=C1_M3_TD_CFO_LABEL,
        td_cfo_norms=C1_M3_TD_CFO_NORMS,
        department_label=C1_M3_DEPARTMENT_LABEL,
        department_norms=C1_M3_DEPARTMENT_NORMS,
    )
