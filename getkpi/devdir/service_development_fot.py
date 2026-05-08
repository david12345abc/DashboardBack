"""Факт ФОТ контура директора по развитию: 5 п/п, счёт 26, две статьи НПО АУП (ТЗ).

Считается здесь же: регистр ``AccountingRegister_Хозрасчетный`` / ``RecordsWithExtDimensions``.
Используется плитками RD-M2-1 и RD-M4 (``service_development_fot_fact``).
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from ..fot_techdir_fact import (
    AUTH,
    FOT_SPEC_ARTICLES,
    MONTH_RU,
    calc_techdir_spec_reg_fact,
    load_fot_structure_map_for_spec,
)

logger = logging.getLogger(__name__)

# Порядок суммирования — п. «Подразделения» (директор по развитию).
_DEVDIR_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    ("ДИРЕКТОР ПО РАЗВИТИЮ", ("директор по развитию",)),
    ("Служба развития", ("служба развития",)),
    (
        "Сектор по развитию новых продуктов",
        (
            "сектор по развитию новых продуктов",
            "сектор развития новых продуктов",
        ),
    ),
    (
        "Сектор по внедрению искусственного интеллекта",
        (
            "сектор по внедрению искусственного интеллекта",
            "сектор по внедрению ии",
            "сектор внедрения искусственного интеллекта",
        ),
    ),
    (
        "Сектор обучения и развития",
        ("сектор обучения и развития",),
    ),
]
_DEVDIR_GROUP_ORDER = [t[0] for t in _DEVDIR_FOT_SPEC]

SERVICE_DEVELOPMENT_DEPARTMENT = "контур директора по развитию (5 п/п)"
SERVICE_DEVELOPMENT_DEPARTMENTS = list(_DEVDIR_GROUP_ORDER)


def _month_period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _compute_service_development_fot_payload(year: int, month: int) -> dict[str, Any]:
    try:
        p_start, p_end = _month_period_bounds(year, month)
        session = requests.Session()
        session.auth = AUTH
        name_to_key, _labels = load_fot_structure_map_for_spec(session, _DEVDIR_FOT_SPEC)
        totals, article_totals = calc_techdir_spec_reg_fact(
            session, p_start, p_end, name_to_key
        )
        total_fact = 0.0
        groups_out: dict[str, dict[str, float]] = {}
        for n in _DEVDIR_GROUP_ORDER:
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
                "component": "getkpi.devdir.service_development_fot._compute_service_development_fot_payload",
                "register": "AccountingRegister_Хозрасчетный/RecordsWithExtDimensions",
                "movement": "дебет счёта 26, поле Сумма (сторно инвертируется)",
                "cost_articles": list(FOT_SPEC_ARTICLES),
                "departments": list(_DEVDIR_GROUP_ORDER),
            },
        }
    except Exception as exc:
        logger.exception("ФОТ devdir (5 п/п): ошибка расчёта за %d-%02d", year, month)
        groups_out = {
            n: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
            for n in _DEVDIR_GROUP_ORDER
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
                "component": "getkpi.devdir.service_development_fot",
                "error": str(exc),
            },
        }


def service_development_fot_fact(year: int, month: int) -> float | None:
    payload = _compute_service_development_fot_payload(year, month)
    dbg = payload.get("debug") or {}
    if dbg.get("status") == "error":
        logger.warning(
            "ФОТ devdir (5 п/п): ошибка за %s-%02d: %s",
            year,
            month,
            dbg.get("error"),
        )
        return None
    tf = payload.get("total_fact")
    if tf is None:
        return None
    return float(tf)


def service_development_fot_detail(year: int, month: int) -> dict[str, Any]:
    return _compute_service_development_fot_payload(year, month)
