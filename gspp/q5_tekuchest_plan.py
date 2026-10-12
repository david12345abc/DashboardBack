"""Помесячный план текучести (%) для KPI ГСП-Q5.

Документ 1С «Текучесть персонала» № 000000072, подразделение ГСПП, вид «План».
"""

from __future__ import annotations

GSPP_Q5_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 0.8,
    2: 1.7,
    3: 2.5,
    4: 3.3,
    5: 4.2,
    6: 5.0,
    7: 5.8,
    8: 6.7,
    9: 7.5,
    10: 8.3,
    11: 9.2,
    12: 10.0,
}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in GSPP_Q5_PLAN_BY_MONTH_2026:
        return float(GSPP_Q5_PLAN_BY_MONTH_2026[month])
    return None
