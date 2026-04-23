from __future__ import annotations

from datetime import date

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def get_td_m2_ytd() -> dict:
    """
    Заглушка для TD-M2:
    «Отсутствие критичных нарушений по ИБ/ПБ/экологии».

    Пока нет утверждённой методики и подтверждённого источника данных,
    поэтому модуль возвращает пустой payload совместимого формата.
    Это позволяет не подмешивать синтетические значения в плитку.
    """
    today = date.today()
    month_name = MONTH_NAMES[today.month]

    return {
        "data_granularity": "monthly",
        "monthly_data": [],
        "last_full_month_row": None,
        "kpi_period": {
            "type": "placeholder",
            "year": today.year,
            "month": today.month,
            "month_name": month_name,
            "snapshot_date": today.isoformat(),
        },
        "ytd": {
            "total_plan": None,
            "total_fact": None,
            "kpi_pct": None,
            "months_with_data": 0,
            "months_total": 0,
        },
        "debug": {
            "status": "placeholder",
            "kpi_id": "TD-M2",
            "message": (
                "Для TD-M2 пока не утверждены источник данных и формула расчёта. "
                "Здесь должна появиться логика по критичным нарушениям ИБ/ПБ/экологии."
            ),
            "todo": [
                "Согласовать источник данных для ИБ/ПБ/экологии",
                "Определить единицу учёта критичного нарушения",
                "Определить период агрегации (месяц / накопительно)",
                "Определить правила расчёта plan/fact/kpi_pct",
            ],
            "candidate_sources": [
                "Журнал инцидентов ИБ",
                "Журнал проверок ПБ",
                "Реестр экологических нарушений",
            ],
        },
    }
