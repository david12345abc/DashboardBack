"""SLA ИТ-M1: исполнитель «Отдел информационных технологий»."""

from __future__ import annotations

from typing import Any

from getkpi.support_sla_data import compute_support_sla_monthly

EXECUTOR_DEPARTMENT_LABEL = "Отдел информационных технологий"
EXECUTOR_DEPARTMENT_ALIASES: tuple[str, ...] = (
    "отдел информационных технологий",
    "отдел ит",
    "оит",
)


def compute_it_m1_sla_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_support_sla_monthly(
        year,
        month,
        kpi_id="IT-M1-SLA",
        component="getkpi.autoit.it_m1_sla_data",
        log_tag="IT-M1 SLA",
        executor_department_label=EXECUTOR_DEPARTMENT_LABEL,
        executor_department_aliases=EXECUTOR_DEPARTMENT_ALIASES,
        plan_rule="Date в месяце + ПодразделениеИсполнитель = ОИТ",
    )
