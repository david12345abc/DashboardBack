"""Помесячный план текучести (%) для KPI ГСП-Q5.

Значения восстановлены из рабочего кэша дашборда (top2 Document_ТД_ТекучестьПерсонала).
"""

from __future__ import annotations

GSPP_Q5_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 1.2,
    2: 2.5,
    3: 3.8,
    4: 5.0,
    5: 6.3,
    6: 7.5,
    7: 8.7,
    8: 10.0,
    9: 11.3,
    10: 12.5,
    11: 13.8,
    12: 15.0,
}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in GSPP_Q5_PLAN_BY_MONTH_2026:
        return float(GSPP_Q5_PLAN_BY_MONTH_2026[month])
    return None
