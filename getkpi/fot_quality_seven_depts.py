"""
Факт ФОТ по семи подразделениям качества из регистра бухгалтерии (общее ядро).

Используется для QD-M4 (qualdir): ``AccountingRegister_Хозрасчетный`` /
``RecordsWithExtDimensions``, Дт сч. 26,
две статьи из ``fot_techdir_fact.FOT_SPEC_ARTICLES``, сумма по семи п/п.

Оболочка плитки — ``qualdir.qd_m4_fact``. Факт RD-M*/devdir по развитию —
``getkpi.devdir.service_development_fot``.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from .fot_techdir_fact import (
    AUTH,
    FOT_SPEC_ARTICLES,
    MONTH_RU,
    calc_techdir_spec_reg_fact,
    load_fot_structure_map_for_spec,
)

logger = logging.getLogger(__name__)

# Порядок суммирования — как в ТЗ (семь подразделений контура качества).
SEVEN_DEPTS_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    (
        "Зам. технического директора по качеству",
        (
            "зам. технического директора по качеству",
            "зам технического директора по качеству",
            "заместитель тех. директора по качеству",
            "заместитель технического директора по качеству",
        ),
    ),
    ("Специалист по процессному управлению", ("специалист по процессному управлению",)),
    ("ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ", ("заместитель директора по качеству",)),
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    ("Лаборатория неразрушающего контроля", ("лаборатория неразрушающего контроля", "лнк")),
    (
        "Отдел управления несоответствиями",
        (
            "отдел управления несоответствиями",
            "отдел управления несоотвествиями",
        ),
    ),
]

SEVEN_DEPTS_GROUP_ORDER = [t[0] for t in SEVEN_DEPTS_FOT_SPEC]


def month_period_bounds_hoz(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def compute_seven_depts_register26_fot_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт ФОТ по семи п/п за календарный месяц (руб.), без привязки к роли KPI."""
    try:
        p_start, p_end = month_period_bounds_hoz(year, month)
        session = requests.Session()
        session.auth = AUTH
        name_to_key, _labels = load_fot_structure_map_for_spec(session, SEVEN_DEPTS_FOT_SPEC)
        totals, article_totals = calc_techdir_spec_reg_fact(
            session, p_start, p_end, name_to_key
        )
        total_fact = 0.0
        groups_out: dict[str, dict[str, float]] = {}
        for n in SEVEN_DEPTS_GROUP_ORDER:
            row = totals.get(n, {})
            s = float(row.get("fact_salary", 0) or 0)
            ins = float(row.get("fact_insurance", 0) or 0)
            t = s + ins
            total_fact += t
            groups_out[n] = {
                "fact_salary": s,
                "fact_insurance": ins,
                "fact_total": t,
            }
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "groups": groups_out,
            "total_fact": round(total_fact, 2),
            "article_totals": dict(article_totals),
            "debug": {
                "status": "ok",
                "component": "getkpi.fot_quality_seven_depts.compute_seven_depts_register26_fot_monthly",
                "register": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
                "movement": "дебет счёта 26, поле Сумма (сторно инвертируется)",
                "cost_articles": list(FOT_SPEC_ARTICLES),
                "departments": list(SEVEN_DEPTS_GROUP_ORDER),
            },
        }
    except Exception as exc:
        logger.exception("fot_quality_seven_depts: ошибка расчёта за %d-%02d", year, month)
        groups_out = {
            n: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
            for n in SEVEN_DEPTS_GROUP_ORDER
        }
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "groups": groups_out,
            "total_fact": 0.0,
            "article_totals": {},
            "debug": {
                "status": "error",
                "component": "getkpi.fot_quality_seven_depts.compute_seven_depts_register26_fot_monthly",
                "error": str(exc),
            },
        }
