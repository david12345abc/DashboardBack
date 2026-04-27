"""Периоды для расчёта KPI: последний полный месяц и последний полный календарный квартал."""
from __future__ import annotations

from datetime import date


def last_full_month(d: date | None = None) -> tuple[int, int]:
    """(год, месяц) последнего полностью завершённого месяца."""
    d = d or date.today()
    if d.month == 1:
        return d.year - 1, 12
    return d.year, d.month - 1


def last_full_quarter(d: date | None = None) -> tuple[int, int]:
    """(год, квартал 1–4) последнего полностью завершённого календарного квартала."""
    d = d or date.today()
    cq = (d.month - 1) // 3 + 1
    if cq == 1:
        return d.year - 1, 4
    return d.year, cq - 1


def current_calendar_quarter(d: date | None = None) -> tuple[int, int]:
    """(год, квартал 1–4) календарного квартала, в который попадает дата (текущий квартал для «сегодня»)."""
    d = d or date.today()
    cq = (d.month - 1) // 3 + 1
    return d.year, cq


def quarter_month_tuples(year: int, quarter: int) -> list[tuple[int, int]]:
    """Три месяца календарного квартала: (год, месяц)."""
    start_m = 3 * (quarter - 1) + 1
    return [(year, start_m), (year, start_m + 1), (year, start_m + 2)]


def _month_le(y1: int, m1: int, y2: int, m2: int) -> bool:
    return (y1, m1) <= (y2, m2)


def month_range_inclusive(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """Все (год, месяц) от start до end включительно по календарю."""
    sy, sm = start
    ey, em = end
    out: list[tuple[int, int]] = []
    y, m = sy, sm
    while _month_le(y, m, ey, em):
        out.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def vp_months_for_api(today: date | None = None) -> tuple[list[tuple[int, int]], tuple[int, int], tuple[int, int]]:
    """
    Месяцы ВП для ответа API: объединение
    - в текущем календарном году: с января по последний полный месяц;
    - если последний полный месяц в прошлом году (например январь): только этот месяц;
    - всех месяцев последнего полного квартала (для квартального KPI по ВП).
    Возвращает: (отсортированные уникальные (год, месяц), last_full_month, last_full_quarter).
    """
    today = today or date.today()
    ref_y, ref_m = last_full_month(today)
    lq_y, lq_q = last_full_quarter(today)
    q_months = set(quarter_month_tuples(lq_y, lq_q))
    if ref_y == today.year:
        monthly_span = set(month_range_inclusive((ref_y, 1), (ref_y, ref_m)))
    else:
        monthly_span = {(ref_y, ref_m)}
    merged = sorted(q_months | monthly_span)
    return merged, (ref_y, ref_m), (lq_y, lq_q)
