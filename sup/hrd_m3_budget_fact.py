"""Факт бюджета HRD-M3: заявки ДС с ЦФО «Директор НПО» и подразделением «Служба управления персоналом»."""

from __future__ import annotations

from typing import Any

from getkpi.budget_request_fact import compute_budget_request_fact_monthly, norms_from_labels

HRD_M3_TD_CFO_LABEL = "Директор НПО"

HRD_M3_TD_CFO_NORMS = norms_from_labels(
    HRD_M3_TD_CFO_LABEL,
    "директор нпо",
)

HRD_M3_DEPARTMENT_LABEL = "Служба управления персоналом"

HRD_M3_DEPARTMENT_NORMS = norms_from_labels(
    HRD_M3_DEPARTMENT_LABEL,
    "служба управления персоналом",
)


def compute_hrd_m3_budget_fact_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_budget_request_fact_monthly(
        year,
        month,
        kpi_id="HRD-M3-FACT",
        component="sup.hrd_m3_budget_fact",
        td_cfo_label=HRD_M3_TD_CFO_LABEL,
        td_cfo_norms=HRD_M3_TD_CFO_NORMS,
        department_label=HRD_M3_DEPARTMENT_LABEL,
        department_norms=HRD_M3_DEPARTMENT_NORMS,
    )
