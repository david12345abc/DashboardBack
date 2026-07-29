"""Помесячный план текучести (%) для KPI QD-Q2.

Значения 1–7 восстановлены из qualdir_tekuchet_* (top2 Document_ТД_ТекучестьПерсонала).
8–12 — продолжение той же лестницы (+0.7 п.п./мес.).
"""

from __future__ import annotations

QD_Q2_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 0.7,
    2: 1.3,
    3: 2.0,
    4: 2.7,
    5: 3.3,
    6: 4.0,
    7: 4.7,
    8: 5.3,
    9: 6.0,
    10: 6.7,
    11: 7.3,
    12: 8.0,
}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in QD_Q2_PLAN_BY_MONTH_2026:
        return float(QD_Q2_PLAN_BY_MONTH_2026[month])
    return None
