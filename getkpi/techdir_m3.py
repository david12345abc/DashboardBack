from __future__ import annotations

import logging
from datetime import date

from .cache_manager import locked_call
from . import calc_budget_techdir_m3

logger = logging.getLogger(__name__)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _kpi_td_m3(plan: float | None, fact: float | None) -> float | None:
    """MIN(100; План/Факт·100) по методике TD-M3."""
    if plan is None or fact is None:
        return None
    if fact == 0:
        return 100.0 if plan <= 0 else None
    return round(min(100.0, plan / fact * 100), 2)


def _month_pairs_from_january() -> tuple[list[tuple[int, int]], tuple[int, int]]:
    today = date.today()
    return [(today.year, mm) for mm in range(1, today.month + 1)], (today.year, today.month)


def get_td_m3_ytd() -> dict | None:
    """TD-M3: бюджет затрат блока техдирекции в пределах лимита (план/факт из оборотов бюджетов)."""

    def _runner() -> dict | None:
        try:
            pairs, (ref_y, ref_m) = _month_pairs_from_january()
            monthly_rows: list[dict] = []
            ref_row: dict | None = None

            for y, m in pairs:
                payload = calc_budget_techdir_m3.get_td_m3_costs_monthly(y, m)
                plan = payload.get("total_plan")
                fact = payload.get("total_fact")
                has_data = bool(payload.get("has_data")) and plan is not None and fact is not None
                kpi_pct = _kpi_td_m3(plan, fact) if has_data else None

                row = {
                    "month": m,
                    "year": y,
                    "month_name": MONTH_NAMES[m],
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": kpi_pct,
                    "has_data": has_data,
                    **({"values_unit": "руб."} if has_data else {}),
                }
                monthly_rows.append(row)
                if (y, m) == (ref_y, ref_m):
                    ref_row = row

            return {
                "data_granularity": "monthly",
                "monthly_data": monthly_rows,
                "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {
                    "total_plan": ref_row.get("plan") if ref_row else None,
                    "total_fact": ref_row.get("fact") if ref_row else None,
                    "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                    "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
                    "months_total": len(monthly_rows),
                    **({"values_unit": "руб."} if ref_row and ref_row.get("has_data") else {}),
                },
                "debug": {
                    "status": "ok" if any(row.get("has_data") for row in monthly_rows) else "no_data",
                    "kpi_id": "TD-M3",
                    "plan_source": "calc_budget_techdir_m3.py",
                    "fact_source": "calc_budget_techdir_m3.py",
                    "register": "AccumulationRegister_ОборотыБюджетов_RecordType",
                },
            }
        except Exception:
            logger.exception("Ошибка при расчёте TD-M3 (бюджет затрат техдирекции)")
            return None

    return locked_call("techdir_td_m3", _runner)
