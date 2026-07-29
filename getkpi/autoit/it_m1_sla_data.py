"""SLA ИТ-M1: исполнитель «Отдел информационных технологий» (SQL, статус = факт)."""

from __future__ import annotations

from typing import Any

from .it_m1_core import compute_month

EXECUTOR_DEPARTMENT_LABEL = "Отдел информационных технологий"
EXECUTOR_DEPARTMENT_ALIASES: tuple[str, ...] = (
    "отдел информационных технологий",
    "отдел ит",
    "оит",
)


def compute_it_m1_sla_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_month(year, month)
