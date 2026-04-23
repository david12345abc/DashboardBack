from __future__ import annotations

import logging
from datetime import date

from .cache_manager import locked_call
from .kpi_periods import last_full_month
from . import techdir_fot_fact, techdir_fot_plan

logger = logging.getLogger(__name__)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if fact == 0:
        return 100.0 if plan >= 0 else None
    return round(plan / fact * 100, 2)


def get_td_m3_ytd() -> dict | None:
    def _runner() -> dict | None:
        try:
            ref_y, ref_m = last_full_month(date.today())
            plan_payload = techdir_fot_plan.get_td_fot_plan_monthly(ref_y, ref_m)
            fact_payload = techdir_fot_fact.get_td_fot_fact_monthly(ref_y, ref_m)

            plan = plan_payload.get("total_plan")
            fact = fact_payload.get("total_fact")
            has_data = plan is not None and fact is not None
            kpi_pct = _kpi_pct(plan, fact) if has_data else None

            row = {
                "month": ref_m,
                "year": ref_y,
                "month_name": MONTH_NAMES[ref_m],
                "plan": plan,
                "fact": fact,
                "kpi_pct": kpi_pct,
                "has_data": has_data,
                **({"values_unit": "руб."} if has_data else {}),
            }

            return {
                "data_granularity": "monthly",
                "monthly_data": [row],
                "last_full_month_row": dict(row) if has_data else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {
                    "total_plan": plan,
                    "total_fact": fact,
                    "kpi_pct": kpi_pct,
                    "months_with_data": 1 if has_data else 0,
                    "months_total": 1 if has_data else 0,
                    **({"values_unit": "руб."} if has_data else {}),
                },
                "debug": {
                    "status": "ok" if has_data else "no_data",
                    "kpi_id": "TD-M3",
                    "plan_source": "techdir_fot_plan.py",
                    "fact_source": "techdir_fot_fact.py",
                },
            }
        except Exception:
            logger.exception("Ошибка при расчёте TD-M3 по ФОТ техдирекции")
            return None

    return locked_call("techdir_td_m3", _runner)
