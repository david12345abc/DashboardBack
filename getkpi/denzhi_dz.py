"""
KD-M2 «Деньги и просроченная ДЗ» (коммерческий директор).

План: 578 500 000 ₽/мес (578,5 млн; одинаковый для каждого месяца).
Факт: жёстко задан по месяцам (см. FACT_DATA).
KPI = факт / план × 100.
Цвет плитки: см. komdir_dashboard._rag_m2_debt (<100 зелёный, 100–110 жёлтый, >110 красный).
"""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_month, vp_months_for_api

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

PLAN_PER_MONTH = 578_500_000

FACT_DATA: dict[tuple[int, int], float] = {
    (2026, 1): 549_732_479,
    (2026, 2): 398_450_029,
    (2026, 3): 333_853_886,
}


def _kpi_pct(fact: float, plan: float) -> float:
    if plan == 0:
        return 0.0
    return round(fact / plan * 100, 2)


def get_kd_m2_ytd() -> dict:
    today = date.today()
    month_tuples, (ref_y, ref_m), _ = vp_months_for_api(today)

    months_out: list[dict] = []
    ref_row: dict | None = None

    for y, m in month_tuples:
        fact = FACT_DATA.get((y, m))
        has_data = fact is not None
        kpi = _kpi_pct(fact, PLAN_PER_MONTH) if has_data else None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": PLAN_PER_MONTH,
            "fact": fact,
            "kpi_pct": kpi,
            "has_data": has_data,
        }
        months_out.append(row)
        if y == ref_y and m == ref_m:
            ref_row = row

    last_full_month_row = dict(ref_row) if ref_row and ref_row.get("has_data") else None

    if ref_row and ref_row.get("has_data"):
        ytd_pct = ref_row["kpi_pct"]
        total_plan = ref_row["plan"]
        total_fact = ref_row["fact"]
        n_kpi = 1
    else:
        ytd_pct = None
        total_plan = PLAN_PER_MONTH
        total_fact = 0
        n_kpi = 0

    return {
        "year": ref_y,
        "months": months_out,
        "last_full_month_row": last_full_month_row,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": total_plan,
            "total_fact": total_fact,
            "kpi_pct": ytd_pct,
            "months_with_data": n_kpi,
            "months_total": 1,
        },
    }
