"""Помесячный план текучести (%) для KPI 1С-Q5.

В Document_ТД_ТекучестьПерсонала (top2) для отдела сопровождения 1С план был 0.0
по всем месяцам 2026 — сохраняем это значение, чтобы на плитке не было «—».
"""

from __future__ import annotations

C1_Q5_PLAN_BY_MONTH_2026: dict[int, float] = {m: 0.0 for m in range(1, 13)}


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in C1_Q5_PLAN_BY_MONTH_2026:
        return float(C1_Q5_PLAN_BY_MONTH_2026[month])
    return None
