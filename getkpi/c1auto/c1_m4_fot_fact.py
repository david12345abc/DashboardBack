"""Факт ФОТ отдела сопровождения 1С (1С-M4) из регистра бухучёта, сч. 26."""

from __future__ import annotations

import logging
from typing import Any

import requests

from getkpi.fot_techdir_fact import (
    AUTH,
    FOT_SPEC_ARTICLES,
    MONTH_RU,
    calc_techdir_spec_reg_fact,
    load_fot_structure_map_for_spec,
)

logger = logging.getLogger(__name__)

C1AUTO_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    (
        "Отдел сопровождения 1С",
        (
            "отдел сопровождения 1с",
            "отдел сопровождения 1c",
            "сопровождения 1с",
        ),
    ),
]
C1AUTO_FOT_GROUP = C1AUTO_FOT_SPEC[0][0]


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def compute_c1_m4_fot_fact_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт ФОТ контура 1С за календарный месяц (руб.)."""
    try:
        p_start, p_end = _month_period_bounds(year, month)
        session = requests.Session()
        session.auth = AUTH
        name_to_key, labels = load_fot_structure_map_for_spec(session, C1AUTO_FOT_SPEC)
        totals, article_totals = calc_techdir_spec_reg_fact(session, p_start, p_end, name_to_key)
        row = totals.get(C1AUTO_FOT_GROUP, {})
        salary = float(row.get("fact_salary", 0) or 0)
        insurance = float(row.get("fact_insurance", 0) or 0)
        total_fact = salary + insurance
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": round(total_fact, 2),
            "groups": {
                C1AUTO_FOT_GROUP: {
                    "fact_salary": round(salary, 2),
                    "fact_insurance": round(insurance, 2),
                    "fact_total": round(total_fact, 2),
                },
            },
            "article_totals": dict(article_totals),
            "debug": {
                "status": "ok",
                "kpi_id": "1C-M4-FACT",
                "component": "getkpi.c1auto.c1_m4_fot_fact",
                "cost_articles": list(FOT_SPEC_ARTICLES),
                "department": C1AUTO_FOT_GROUP,
                "structure_labels": labels,
            },
        }
    except Exception as exc:
        logger.exception("1С-M4: ошибка расчёта факта ФОТ за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": None,
            "groups": {},
            "article_totals": {},
            "debug": {
                "status": "error",
                "kpi_id": "1C-M4-FACT",
                "error": str(exc),
            },
        }
