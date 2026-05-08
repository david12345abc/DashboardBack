"""
Факт ФОТ контура директора по качеству (QD-M4).

Роли **qualdir** только: сборка ответа QD вызывает ядро
``getkpi.fot_quality_seven_depts.compute_seven_depts_register26_fot_monthly``,
здесь — обёртка с меткой ``kpi_id`` для качества.

Правила расчёта — в docstring ``getkpi.fot_quality_seven_depts``.
"""

from __future__ import annotations

from typing import Any

from getkpi.fot_quality_seven_depts import (
    SEVEN_DEPTS_FOT_SPEC as QD_FOT_SPEC,
    SEVEN_DEPTS_GROUP_ORDER as QD_FOT_GROUP_ORDER,
    compute_seven_depts_register26_fot_monthly,
)


def compute_qd_m4_fact_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт ФОТ QD-M4 за календарный месяц (руб.)."""
    out = compute_seven_depts_register26_fot_monthly(year, month)
    dbg = out.get("debug")
    if isinstance(dbg, dict):
        dbg["kpi_id"] = "QD-M4-FACT"
    return out
