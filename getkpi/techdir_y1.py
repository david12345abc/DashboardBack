from __future__ import annotations

from datetime import date


def get_td_y1_ytd() -> dict:
    today = date.today()

    return {
        "data_granularity": "yearly",
        "yearly_data": [],
        "kpi_period": {
            "type": "placeholder",
            "year": today.year,
            "snapshot_date": today.isoformat(),
        },
        "ytd": {
            "total_plan": None,
            "total_fact": None,
            "kpi_pct": None,
            "years_with_data": 0,
            "years_total": 0,
        },
        "debug": {
            "status": "placeholder",
            "kpi_id": "TD-Y1",
            "message": (
                "Для TD-Y1 пока не утверждены источник данных и формула расчёта. "
                "Возвращается совместимая заглушка без синтетических значений."
            ),
        },
    }
