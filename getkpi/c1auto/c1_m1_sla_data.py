"""SLA 1С-M1: исполнитель «Отдел сопровождения 1С» (SQL ``_Document76754X1``)."""

from __future__ import annotations

from typing import Any

from .c1_m1_core import compute_month


def compute_c1_m1_sla_monthly(year: int, month: int) -> dict[str, Any]:
    return compute_month(year, month)
