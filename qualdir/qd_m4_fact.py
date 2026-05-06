"""
Факт ФОТ контура директора по качеству (QD-M4).

Источник и правила (согласовано с ТЗ):
  • только регистр бухгалтерии ``AccountingRegister_Хозрасчетный``,
    срез ``RecordsWithExtDimensions`` (не регистры накопления);
  • счёт 26 (дебетовый оборот по выгрузке, поле суммы движения, учёт ``Сторно``);
  • измерения: подразделение (``ПодразделениеDr_Key``) + статья затрат
    (``ExtDimensionDr1`` при типе статей расходов);
  • статьи только две из п. 4.2 — те же, что в ``fot_techdir_fact.FOT_SPEC_ARTICLES``;
  • семь подразделений — перечень п. «Подразделения»; ФОТ контура = сумма ФОТ по ним,
    ФОТ по подразделению = сумма двух статей за период.

Нормализация наименований — ``normalize_name`` в getkpi.fot_techdir_fact.
"""

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

# Порядок суммирования — как в ТЗ (7 подразделений).
QD_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
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

QD_FOT_GROUP_ORDER = [t[0] for t in QD_FOT_SPEC]


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def compute_qd_m4_fact_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт ФОТ QD-M4 за календарный месяц (руб.)."""
    try:
        p_start, p_end = _month_period_bounds(year, month)
        session = requests.Session()
        session.auth = AUTH
        name_to_key, _labels = load_fot_structure_map_for_spec(session, QD_FOT_SPEC)
        totals, article_totals = calc_techdir_spec_reg_fact(
            session, p_start, p_end, name_to_key
        )
        total_fact = 0.0
        groups_out: dict[str, dict[str, float]] = {}
        for n in QD_FOT_GROUP_ORDER:
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
                "kpi_id": "QD-M4-FACT",
                "register": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
                "movement": "дебет счёта 26, поле Сумма (сторно инвертируется)",
                "cost_articles": list(FOT_SPEC_ARTICLES),
                "departments": list(QD_FOT_GROUP_ORDER),
            },
        }
    except Exception as exc:
        logger.exception("QD-M4 fact: ошибка расчёта за %d-%02d", year, month)
        groups_out = {
            n: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
            for n in QD_FOT_GROUP_ORDER
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
                "kpi_id": "QD-M4-FACT",
                "error": str(exc),
            },
        }
