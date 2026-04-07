"""
Квартальные плитки коммерческого директора: KD-M3, KD-Q1, KD-Q2.

KD-M3: 0,5 × MIN(1; План затрат / Факт затрат) + 0,5 × MIN(1; План ФОТ / Факт ФОТ) × 100%
KD-Q1: 0,6 × KPI(ВП квартал) + 0,25 × KPI(ДЗ+ТОП‑5) + 0,15 × KPI(издержки) — части без данных = 100%
KD-Q2: целевой порог текучести ≤5% (квартал), KPI = min(100, 5/fact×100) при fact > 0
"""
import random
from datetime import date


def _current_quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def _quarter_months(q: int) -> list[int]:
    return [3 * (q - 1) + 1, 3 * (q - 1) + 2, 3 * q]


def _vp_month_map(vp_months: list[dict]) -> dict[int, dict]:
    return {x['month']: x for x in vp_months}


def quarterly_m3(year: int | None = None) -> dict:
    """KD-M3 — поквартально; факт/план пока синтетика (план = факт → KPI 100%)."""
    today = date.today()
    year = year or today.year
    cq = _current_quarter(today.month)
    random.seed(hash((year, 'KD-M3', today.toordinal())))

    quarters = []
    sum_kpi = 0.0

    for q in range(1, cq + 1):
        fact_z = round(random.uniform(0.85, 1.15) * 1_000_000, 2)
        fact_fot = round(random.uniform(0.88, 1.12) * 500_000, 2)
        plan_z = round(fact_z * random.uniform(0.92, 1.08), 2)
        plan_fot = round(fact_fot * random.uniform(0.92, 1.08), 2)
        term1 = min(1.0, plan_z / fact_z) if fact_z else 0.0
        term2 = min(1.0, plan_fot / fact_fot) if fact_fot else 0.0
        kpi = round((0.5 * term1 + 0.5 * term2) * 100, 1)

        quarters.append({
            'quarter': q,
            'year': year,
            'label': f'Q{q} {year}',
            'plan_zatraty': plan_z,
            'fact_zatraty': fact_z,
            'plan_fot': plan_fot,
            'fact_fot': fact_fot,
            'kpi_pct': kpi,
        })
        sum_kpi += kpi

    return {
        'year': year,
        'quarterly_data': quarters,
        'ytd': {
            'kpi_pct': round(sum_kpi / len(quarters), 1) if quarters else None,
            'quarters_with_data': len(quarters),
            'quarters_total': cq,
        },
    }


def quarterly_q1(vp_ytd_months: list[dict], year: int | None = None) -> dict:
    """KD-Q1 с KPI ВП по кварталам из реальных месяцев valovaya_pribyl."""
    today = date.today()
    year = year or today.year
    cq = _current_quarter(today.month)
    by_m = _vp_month_map(vp_ytd_months)
    random.seed(hash((year, 'KD-Q1', today.toordinal())))

    quarters = []
    sum_kpi = 0.0

    for q in range(1, cq + 1):
        qmonths = _quarter_months(q)
        pf = pp = 0.0
        has_vp = False
        for m in qmonths:
            if m > today.month:
                break
            row = by_m.get(m)
            if row and row.get('has_data') and row.get('fact') is not None:
                pf += float(row['fact'])
                pp += float(row.get('plan') or 0)
                has_vp = True

        k_vp = round(pf / pp * 100, 1) if has_vp and pp > 0 else 100.0
        k_dz = 100.0
        k_cost = 100.0
        kpi = round(0.6 * k_vp + 0.25 * k_dz + 0.15 * k_cost, 1)

        quarters.append({
            'quarter': q,
            'year': year,
            'label': f'Q{q} {year}',
            'vp_fact': round(pf, 2) if has_vp else None,
            'vp_plan': round(pp, 2) if has_vp else None,
            'kpi_vp_pct': k_vp,
            'kpi_dz_portfolio_pct': k_dz,
            'kpi_izderzhki_pct': k_cost,
            'kpi_pct': kpi,
        })
        sum_kpi += kpi

    return {
        'year': year,
        'quarterly_data': quarters,
        'ytd': {
            'kpi_pct': round(sum_kpi / len(quarters), 1) if quarters else None,
            'quarters_with_data': len(quarters),
            'quarters_total': cq,
        },
    }


def quarterly_q2(year: int | None = None) -> dict:
    """KD-Q2 — текучесть %; цель ≤5%."""
    today = date.today()
    year = year or today.year
    cq = _current_quarter(today.month)
    random.seed(hash((year, 'KD-Q2', today.toordinal())))

    quarters = []
    sum_kpi = 0.0

    for q in range(1, cq + 1):
        fact = round(random.uniform(2.0, 8.0), 2)
        target = 5.0
        if fact <= target:
            kpi = 100.0
        else:
            kpi = round(min(100.0, target / fact * 100), 1)

        quarters.append({
            'quarter': q,
            'year': year,
            'label': f'Q{q} {year}',
            'plan_max_turnover_pct': target,
            'fact_turnover_pct': fact,
            'kpi_pct': kpi,
        })
        sum_kpi += kpi

    return {
        'year': year,
        'quarterly_data': quarters,
        'ytd': {
            'kpi_pct': round(sum_kpi / len(quarters), 1) if quarters else None,
            'quarters_with_data': len(quarters),
            'quarters_total': cq,
        },
    }
