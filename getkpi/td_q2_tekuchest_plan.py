"""Помесячный план текучести (%) для KPI TD-Q2.

Значения 1–7 восстановлены из techdir_tekuchet_* (top2 Document_ТД_ТекучестьПерсонала).
8–12 — продолжение лестницы (~+1.4 п.п./мес.).
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
    10: 14.3,
    11: 15.7,
    12: 17.1,
}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in TD_Q2_PLAN_BY_MONTH_2026:
        return float(TD_Q2_PLAN_BY_MONTH_2026[month])
    return None
