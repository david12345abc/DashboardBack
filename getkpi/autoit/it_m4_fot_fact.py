"""Факт ФОТ отдела информационных технологий (ИТ-M4) из MSSQL, сч. 26."""

from __future__ import annotations

import logging
from typing import Any

from getkpi.fot_techdir_fact import MONTH_RU

from .it_m4_core import AUTOIT_GROUP_ORDER, calc_it_m4_fot_month

logger = logging.getLogger(__name__)

AUTOIT_FOT_GROUP = AUTOIT_GROUP_ORDER[0]


def compute_it_m4_fot_fact_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт ФОТ контура ИТ за календарный месяц (руб.)."""
    try:
        payload = calc_it_m4_fot_month(year, month)
        return {
            "year": year,
            "month": month,
            "month_name": payload.get("month_name") or MONTH_RU.get(month, str(month)),
            "total_fact": payload.get("total_fact"),
            "groups": payload.get("groups") or {},
            "article_totals": payload.get("article_totals") or {},
            "debug": payload.get("debug") or {},
        }
    except Exception as exc:
        logger.exception("ИТ-M4: ошибка расчёта факта ФОТ за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "total_fact": None,
            "groups": {},
            "article_totals": {},
            "debug": {
                "status": "error",
                "kpi_id": "IT-M4-FACT",
                "error": str(exc),
            },
        }
