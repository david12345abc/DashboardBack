"""Помесячный план текучести (%) для KPI TD-Q2.

Значения 1–7 и 8–9, 12 — прежняя лестница top2.
Октябрь и ноябрь 2026 — шапка отчёта 1С по контуру техдиректора: 4.1 и 4.51.
"""

from __future__ import annotations

TD_Q2_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 1.4,
    2: 2.8,
    3: 4.3,
    4: 5.7,
    5: 7.1,
    6: 8.5,
    7: 10.0,
    8: 11.4,
    9: 12.9,
    10: 4.1,
    11: 4.51,
    12: 17.1,
}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in TD_Q2_PLAN_BY_MONTH_2026:
        return float(TD_Q2_PLAN_BY_MONTH_2026[month])
    return None
