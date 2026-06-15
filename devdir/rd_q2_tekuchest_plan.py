"""Помесячный план текучести (%) для KPI RD-Q2 — утверждённая таблица на 2026."""

from __future__ import annotations

RD_Q2_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 1.3,
    2: 2.5,
    3: 3.8,
    4: 5.0,
    5: 6.3,
    6: 7.5,
    7: 8.8,
    8: 10.0,
    9: 11.3,
    10: 12.5,
    11: 13.8,
    12: 15.0,
}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in RD_Q2_PLAN_BY_MONTH_2026:
        return RD_Q2_PLAN_BY_MONTH_2026[month]
    return None
