"""SLA 1С-M1: исполнитель «Отдел сопровождения 1С»."""

from __future__ import annotations

from typing import Any

from getkpi.support_sla_data import compute_support_sla_monthly

EXECUTOR_DEPARTMENT_LABEL = "Отдел сопровождения 1С"
EXECUTOR_DEPARTMENT_ALIASES: tuple[str, ...] = (
    "отдел сопровождения 1с",
    "отдел сопровождения 1c",
    "сопровождения 1с",
)


def compute_c1_m1_sla_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_support_sla_monthly(
        year,
        month,
        kpi_id="1C-M1-SLA",
        component="getkpi.c1auto.c1_m1_sla_data",
        log_tag="1C-M1 SLA",
        executor_department_label=EXECUTOR_DEPARTMENT_LABEL,
        executor_department_aliases=EXECUTOR_DEPARTMENT_ALIASES,
        plan_rule="Date в месяце + ПодразделениеИсполнитель = Отдел сопровождения 1С",
    )
