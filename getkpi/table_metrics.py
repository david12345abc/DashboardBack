"""Общие метрики для строк таблиц KPI."""


def deviation_pct(plan, fact) -> float | None:
    """
    Отклонение: (факт − план) / план × 100.
    Возвращает None, если план или факт отсутствуют или план = 0.
    """
    if plan is None or fact is None:
        return None
    try:
        p = float(plan)
        f = float(fact)
    except (TypeError, ValueError):
        return None
    if p == 0:
        return None
    return round((f - p) / p * 100, 2)
