"""
Квартальные плитки коммерческого директора: KD-M3, KD-Q1, KD-Q2.

KD-M3: 0,5 × MIN(1; План затрат / Факт затрат) + 0,5 × MIN(1; План ФОТ / Факт ФОТ) × 100%
KD-Q1: 0,6 × KPI(ВП квартал) + 0,25 × KPI(ДЗ+ТОП‑5) + 0,15 × KPI(издержки) — части без данных = 100%
KD-Q2: целевой порог текучести ≤5% (квартал), KPI = min(100, 5/fact×100) при fact > 0

Итоговый KPI по квартальным плиткам — только за последний полный календарный квартал.
"""
import random
from datetime import date

from .kpi_periods import last_full_month, last_full_quarter, quarter_month_tuples

_MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _vp_month_map(vp_months: list[dict]) -> dict[tuple[int, int], dict]:
    """Ключ (год, месяц); year в строке опционален (старые ответы — текущий год)."""
    today = date.today()
    default_y = today.year
    out: dict[tuple[int, int], dict] = {}
    for x in vp_months:
        m = x['month']
        y = x.get('year', default_y)
        out[(y, m)] = x
    return out


def quarterly_m3() -> dict:
    """KD-M3 — только последний полный квартал; факт/план пока синтетика."""
    today = date.today()
    lq_y, lq_q = last_full_quarter(today)
    random.seed(hash((lq_y, 'KD-M3', lq_q)))

    fact_z = round(random.uniform(0.85, 1.15) * 1_000_000, 2)
    fact_fot = round(random.uniform(0.88, 1.12) * 500_000, 2)
    plan_z = round(fact_z * random.uniform(0.92, 1.08), 2)
    plan_fot = round(fact_fot * random.uniform(0.92, 1.08), 2)
    term1 = min(1.0, plan_z / fact_z) if fact_z else 0.0
    term2 = min(1.0, plan_fot / fact_fot) if fact_fot else 0.0
    kpi = round((0.5 * term1 + 0.5 * term2) * 100, 1)

    quarter_row = {
        'quarter': lq_q,
        'year': lq_y,
        'label': f'Q{lq_q} {lq_y}',
        'plan_zatraty': plan_z,
        'fact_zatraty': fact_z,
        'plan_fot': plan_fot,
        'fact_fot': fact_fot,
        'kpi_pct': kpi,
    }

    return {
        'year': lq_y,
        'quarterly_data': [quarter_row],
        'kpi_period': {
            'type': 'last_full_quarter',
            'year': lq_y,
            'quarter': lq_q,
        },
        'ytd': {
            'kpi_pct': kpi,
            'quarters_with_data': 1,
            'quarters_total': 1,
        },
    }


def quarterly_q1(vp_months: list[dict]) -> dict:
    """KD-Q1: ВП за последний полный квартал из реальных месяцев valovaya_pribyl."""
    today = date.today()
    lq_y, lq_q = last_full_quarter(today)
    by_m = _vp_month_map(vp_months)

    qmonths = quarter_month_tuples(lq_y, lq_q)
    pf = pp = 0.0
    has_vp = False
    for y, m in qmonths:
        row = by_m.get((y, m))
        if row and row.get('has_data') and row.get('fact') is not None:
            pf += float(row['fact'])
            pp += float(row.get('plan') or 0)
            has_vp = True

    k_vp = round(pf / pp * 100, 1) if has_vp and pp > 0 else 100.0
    k_dz = 100.0
    k_cost = 100.0
    kpi = round(0.6 * k_vp + 0.25 * k_dz + 0.15 * k_cost, 1)

    quarter_row = {
        'quarter': lq_q,
        'year': lq_y,
        'label': f'Q{lq_q} {lq_y}',
        'vp_fact': round(pf, 2) if has_vp else None,
        'vp_plan': round(pp, 2) if has_vp else None,
        'kpi_vp_pct': k_vp,
        'kpi_dz_portfolio_pct': k_dz,
        'kpi_izderzhki_pct': k_cost,
        'kpi_pct': kpi,
    }

    return {
        'year': lq_y,
        'quarterly_data': [quarter_row],
        'kpi_period': {
            'type': 'last_full_quarter',
            'year': lq_y,
            'quarter': lq_q,
        },
        'ytd': {
            'kpi_pct': kpi,
            'quarters_with_data': 1 if has_vp else 0,
            'quarters_total': 1,
        },
    }


def turnover_last_full_month_row() -> dict:
    """Одна строка текучести за последний полный месяц (для таблицы; плитка KD-Q2 — по-прежнему квартал)."""
    today = date.today()
    ry, rm = last_full_month(today)
    random.seed(hash((ry, rm, "KD-Q2-month-table")))

    fact = round(random.uniform(2.0, 8.0), 2)
    target = 5.0
    if fact <= target:
        kpi = 100.0
    else:
        kpi = round(min(100.0, target / fact * 100), 1)

    mn = _MONTH_RU[rm]
    return {
        "month": rm,
        "year": ry,
        "month_name": mn,
        "label": f"{mn} {ry}",
        "plan_max_turnover_pct": target,
        "fact_turnover_pct": fact,
        "kpi_pct": kpi,
        "kpi_period": {
            "type": "last_full_month",
            "year": ry,
            "month": rm,
            "month_name": mn,
        },
    }


def quarterly_q2() -> dict:
    """KD-Q2 — текучесть % за последний полный квартал; цель ≤5%."""
    today = date.today()
    lq_y, lq_q = last_full_quarter(today)
    random.seed(hash((lq_y, 'KD-Q2', lq_q)))

    fact = round(random.uniform(2.0, 8.0), 2)
    target = 5.0
    if fact <= target:
        kpi = 100.0
    else:
        kpi = round(min(100.0, target / fact * 100), 1)

    quarter_row = {
        'quarter': lq_q,
        'year': lq_y,
        'label': f'Q{lq_q} {lq_y}',
        'plan_max_turnover_pct': target,
        'fact_turnover_pct': fact,
        'kpi_pct': kpi,
    }

    return {
        'year': lq_y,
        'quarterly_data': [quarter_row],
        'kpi_period': {
            'type': 'last_full_quarter',
            'year': lq_y,
            'quarter': lq_q,
        },
        'ytd': {
            'kpi_pct': kpi,
            'quarters_with_data': 1,
            'quarters_total': 1,
        },
    }
