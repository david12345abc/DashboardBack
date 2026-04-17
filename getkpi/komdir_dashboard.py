"""
Сборка ответа get_kpi для «Коммерческий директор» и дочерних отделов:
11 ежемесячных KPI-плиток (KD-M11 только комдир), 3 графика, таблица претензий.

Плитки:
  KD-M1  Деньги (План/Факт)
  KD-M2  Отгрузки (План/Факт)
  KD-M3  Договоры (План/Факт)
  KD-M4  Дебиторская задолженность (Факт на дату)
  KD-M5  Просроченная Дебиторская Задолженность (Факт/лимит)
  KD-M6  Валовая прибыль (План/Факт)
  KD-M7  Расходы (Факт/лимит)
  KD-M8  ФОТ (Факт/лимит)
  KD-M9  Скидка / МЦР (Факт/норма)
  KD-M10 ТКП в SLA (Факт/норма)
  KD-M11 Текучесть персонала (План/Факт) — комдир: сумма всех отделов; дети: свои

Графики:
  KD-C1  Линейный: по месяцам Деньги, Отгрузки, Договоры (факт)
  KD-C2  Круговые: 5 диаграмм (Дилеры, Страны, Газпром, Холдинги, БМИ)
  KD-C3  Столбчатый: KPI за месяц (Деньги, Отгрузки, Договоры, Валовая прибыль)

Таблица:
  Претензии (Catalog_Претензии) за выбранный месяц — логика export_claims.py
"""
from __future__ import annotations

import calendar
import random
from datetime import date, datetime
from pathlib import Path

from . import cache_manager, calc_debitorka, calc_dengi_fact, calc_dogovory_fact, calc_dz_limits, calc_fot, calc_kp_price, calc_otgruzki_fact, calc_plan, calc_rashody, calc_tekuchest, calc_tkp_sla, valovaya_pribyl
from .commercial_tiles import DEPT_GUID_TO_DZ_NAME
from .kpi_periods import last_full_month

MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

LOWER_IS_BETTER_IDS = frozenset({'KD-M4', 'KD-M5', 'KD-M7', 'KD-M8', 'KD-M9', 'KD-M11'})
HIGHER_IS_BETTER_IDS = frozenset({'KD-M1', 'KD-M2', 'KD-M3', 'KD-M6', 'KD-M10'})

PIE_CHART_CATEGORIES = [
    'Развитие дилеров',
    'Развитие стран',
    'Закладки Газпром',
    'Развитие холдингов',
    'Закладки БМИ',
]


def _rag_higher_better(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _rag_lower_better(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct < 100:
        return "green"
    if pct <= 110:
        return "yellow"
    return "red"


def _tile_rag(kpi_id: str, pct: float | None) -> str:
    if kpi_id in LOWER_IS_BETTER_IDS:
        return _rag_lower_better(pct)
    return _rag_higher_better(pct)


def _thresholds_block(kpi: dict) -> dict:
    return {
        "green": kpi.get("green_threshold"),
        "yellow": kpi.get("yellow_threshold"),
        "red": kpi.get("red_threshold"),
    }


def _period_label(kpi: dict) -> str:
    f = kpi.get("frequency") or ""
    fl = f.lower()
    if "квартал" in fl:
        return "ежеквартально"
    if "месяц" in fl or "ежемесячно" in fl:
        return "ежемесячно"
    if "год" in fl:
        return "ежегодно"
    return f


def _get_monthly_pairs() -> tuple[list[tuple[int, int]], int, int]:
    """Пары (год, месяц) с января по последний полный месяц — в одной логике с ВП, ФОТ, ДЗ."""
    today = date.today()
    ref_y, ref_m = last_full_month(today)
    if ref_y == today.year:
        pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]
    else:
        pairs = [(ref_y, ref_m)]
    return pairs, ref_y, ref_m


def _series_through_month(today: date, ref_y: int, ref_m: int) -> int:
    """Последний месяц в рядах графиков/кэшей: не раньше последнего полного и не позже текущего календарного."""
    if ref_y < today.year:
        return ref_m
    if ref_y > today.year:
        return ref_m
    return min(12, max(ref_m, today.month))


def _prorate_if_current(plan: float | None, year: int, month: int) -> float | None:
    """Прорейтить план для неполного (текущего) месяца.
    plan_prorated = plan / дней_в_месяце * дней_прошло.
    """
    if plan is None:
        return None
    today = date.today()
    if year == today.year and month == today.month:
        total_days = calendar.monthrange(year, month)[1]
        return round(plan * today.day / total_days, 2)
    return plan


def _generate_tile_monthly_data(kpi_id: str, plan: float,
                                pairs: list[tuple[int, int]]) -> list[dict]:
    """Генерирует помесячные точки для плитки (синтетика)."""
    result = []
    for y, m in pairs:
        random.seed(hash((kpi_id, y, m)))
        if kpi_id in LOWER_IS_BETTER_IDS:
            fact = round(random.uniform(plan * 0.75, plan * 1.15), 2)
        else:
            fact = round(random.uniform(plan * 0.80, plan * 1.20), 2)
        pct = round(fact / plan * 100, 1) if plan else None
        result.append({
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES_RU[m],
            "plan": plan,
            "fact": round(fact, 2),
            "kpi_pct": pct,
            "has_data": False,
        })
    return result


def _build_plan_fact_tile(raw_months: list[dict], plans_by_month: dict[int, float],
                          ref_y: int, ref_m: int) -> dict:
    """Общая логика сборки плитки план/факт для KD-M1/M2/M3."""
    months = []
    ref_row = None
    for row in raw_months:
        m = row.get('month')
        y = row.get('year', ref_y)
        fact = row.get('fact')
        plan = _prorate_if_current(plans_by_month.get(m) or 0, y, m)
        pct = round(fact / plan * 100, 1) if plan and fact is not None else None
        mrow = {
            'month': m,
            'year': y,
            'month_name': MONTH_NAMES_RU.get(m, ''),
            'plan': plan,
            'fact': fact,
            'kpi_pct': pct,
            'has_data': fact is not None,
        }
        months.append(mrow)
        if y == ref_y and m == ref_m:
            ref_row = mrow

    with_data = [r for r in months if r.get('kpi_pct') is not None]
    fallback_plan = _prorate_if_current(plans_by_month.get(ref_m) or 0, ref_y, ref_m)
    return {
        'monthly_data': months,
        'last_full_month_row': dict(ref_row) if ref_row else None,
        'ytd': {
            'total_plan': ref_row['plan'] if ref_row else fallback_plan,
            'total_fact': ref_row['fact'] if ref_row else 0,
            'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
            'months_with_data': len(with_data),
            'months_total': len(months),
        },
        'kpi_period': {
            'type': 'last_full_month',
            'year': ref_y,
            'month': ref_m,
            'month_name': MONTH_NAMES_RU[ref_m],
        },
    }


def _vp_row_for_period(vp: dict, ref_y: int, ref_m: int) -> dict | None:
    """Строка ВП за выбранный в API (ref_y, ref_m) из months_calendar / months."""
    cal = vp.get('months_calendar') or vp.get('months') or []
    for row in cal:
        if row.get('year') == ref_y and row.get('month') == ref_m:
            return row
    return None


def _get_tile_data(kpi_id: str, pairs: list[tuple[int, int]],
                   ref_y: int, ref_m: int, series_m: int,
                   dz_payload: dict | None = None,
                   dept_guid: str | None = None,
                   plans_payload: dict | None = None) -> dict:
    """Получить данные для одной плитки.
    ref_y/ref_m — последний полный месяц (план/факт на плитке, KPI %).
    series_m — последний месяц в загрузке фактов (включает текущий неполный для графиков).

    KD-M1/M2/M3 — факт из calc-модулей + план из plans_payload,
    KD-M4/KD-M5 — из calc_debitorka,
    KD-M6 — из valovaya_pribyl,
    остальные — синтетика.
    dept_guid — GUID подразделения для фильтрации (None = агрегат).
    plans_payload — результат calc_plan.get_plans_monthly().
    """
    plans_months = (plans_payload or {}).get('months', [])

    if kpi_id == 'KD-M1':
        dengi = cache_manager.locked_call(
            f'dengi_{ref_y}_{series_m}',
            calc_dengi_fact.get_dengi_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        plans_by_month = {r['month']: (r.get('dengi') or 0) for r in plans_months}
        return _build_plan_fact_tile(dengi.get('months', []), plans_by_month,
                                     ref_y, ref_m)

    if kpi_id == 'KD-M2':
        otg = cache_manager.locked_call(
            f'otgruzki_{ref_y}_{series_m}',
            calc_otgruzki_fact.get_otgruzki_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        plans_by_month = {r['month']: (r.get('otgruzki') or 0) for r in plans_months}
        return _build_plan_fact_tile(otg.get('months', []), plans_by_month,
                                     ref_y, ref_m)

    if kpi_id == 'KD-M3':
        dog = cache_manager.locked_call(
            f'dogovory_{ref_y}_{series_m}',
            calc_dogovory_fact.get_dogovory_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        plans_by_month = {r['month']: (r.get('dogovory') or 0) for r in plans_months}
        return _build_plan_fact_tile(dog.get('months', []), plans_by_month,
                                     ref_y, ref_m)

    if kpi_id == 'KD-M6':
        vp = cache_manager.locked_call(
            f'vp_{dept_guid}' if dept_guid else 'vp',
            valovaya_pribyl.get_vp_ytd,
            dept_guid=dept_guid,
        )
        cal = vp.get('months_calendar') or vp.get('months') or []
        # last_full_month_row в get_vp_ytd привязан к «сегодня», а не к ?month=&year= —
        # без замены плитка показывала один и тот же факт при любом выбранном месяце.
        lm = _vp_row_for_period(vp, ref_y, ref_m) or vp.get('last_full_month_row')
        pct = lm.get('kpi_pct') if lm else None
        ytd = {
            'total_plan': lm.get('plan') if lm else None,
            'total_fact': lm.get('fact') if lm else None,
            'kpi_pct': pct,
            'months_with_data': 1 if lm and lm.get('fact') is not None else 0,
            'months_total': 1,
        }
        return {
            'monthly_data': cal,
            'last_full_month_row': lm,
            'ytd': ytd,
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    if kpi_id in {'KD-M4', 'KD-M5'}:
        if dz_payload is None:
            dz_payload = calc_debitorka.get_komdir_dz_monthly(year=ref_y, month=series_m)
        raw_months = dz_payload.get('months', [])

        if kpi_id == 'KD-M5':
            plan = calc_dz_limits.get_dept_overdue_limit(dept_guid)
        else:
            plan = 100_000_000.0

        months = []
        ref_row = None
        for row in raw_months:
            fact = row.get('dz_fact') if kpi_id == 'KD-M4' else row.get('overdue_fact')
            pct = round(fact / plan * 100, 1) if plan and fact is not None else None

            mrow = {
                'month': row.get('month'),
                'year': row.get('year'),
                'month_name': MONTH_NAMES_RU.get(row.get('month'), ''),
                'plan': plan,
                'fact': fact,
                'kpi_pct': pct,
                'has_data': fact is not None,
            }
            months.append(mrow)
            if row.get('year') == ref_y and row.get('month') == ref_m:
                ref_row = mrow

        with_data = [r for r in months if r.get('kpi_pct') is not None]
        return {
            'monthly_data': months,
            'last_full_month_row': dict(ref_row) if ref_row else None,
            'ytd': {
                'total_plan': ref_row['plan'] if ref_row else plan,
                'total_fact': ref_row['fact'] if ref_row else 0,
                'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
                'months_with_data': len(with_data),
                'months_total': len(months),
            },
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    if kpi_id == 'KD-M8':
        fot = cache_manager.locked_call(
            f'fot_{ref_y}_{series_m}',
            calc_fot.get_fot_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = fot.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            y = row.get('year', ref_y)
            plan = _prorate_if_current(row.get('plan'), y, m)
            fact = row.get('fact')
            pct = round(fact / plan * 100, 1) if plan and fact is not None else None
            mrow = {
                'month': m,
                'year': y,
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': plan,
                'fact': fact,
                'kpi_pct': pct,
                'has_data': fact is not None and fact != 0,
            }
            months.append(mrow)
            if row.get('year') == ref_y and m == ref_m:
                ref_row = mrow

        with_data = [r for r in months if r.get('kpi_pct') is not None]
        return {
            'monthly_data': months,
            'last_full_month_row': dict(ref_row) if ref_row else None,
            'ytd': {
                'total_plan': ref_row['plan'] if ref_row else 0,
                'total_fact': ref_row['fact'] if ref_row else 0,
                'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
                'months_with_data': len(with_data),
                'months_total': len(months),
            },
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    if kpi_id == 'KD-M11':
        tek = cache_manager.locked_call(
            f'tekuchest_{ref_y}_{series_m}',
            calc_tekuchest.get_tekuchest_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = tek.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            y = row.get('year', ref_y)
            p = _prorate_if_current(row.get('plan'), y, m)
            f = row.get('fact')
            pct = round(f / p * 100, 1) if p and f is not None else None
            mrow = {
                'month': m,
                'year': y,
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': p,
                'fact': f,
                'kpi_pct': pct,
                'has_data': f is not None and f != 0 or p is not None and p != 0,
            }
            months.append(mrow)
            if row.get('year') == ref_y and m == ref_m:
                ref_row = mrow

        with_data = [r for r in months if r.get('kpi_pct') is not None]
        return {
            'monthly_data': months,
            'last_full_month_row': dict(ref_row) if ref_row else None,
            'ytd': {
                'total_plan': ref_row['plan'] if ref_row else 0,
                'total_fact': ref_row['fact'] if ref_row else 0,
                'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
                'months_with_data': len(with_data),
                'months_total': len(months),
            },
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    if kpi_id == 'KD-M7':
        rash = cache_manager.locked_call(
            f'rashody_{ref_y}_{series_m}',
            calc_rashody.get_rashody_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = rash.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            y = row.get('year', ref_y)
            plan = _prorate_if_current(row.get('plan'), y, m)
            fact = row.get('fact', 0)
            pct = round(fact / plan * 100, 1) if plan and fact is not None else None
            mrow = {
                'month': m,
                'year': y,
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': plan,
                'fact': fact,
                'kpi_pct': pct,
                'has_data': fact != 0,
            }
            months.append(mrow)
            if row.get('year') == ref_y and m == ref_m:
                ref_row = mrow

        with_data = [r for r in months if r.get('kpi_pct') is not None]
        return {
            'monthly_data': months,
            'last_full_month_row': dict(ref_row) if ref_row else None,
            'ytd': {
                'total_plan': ref_row['plan'] if ref_row else 0,
                'total_fact': ref_row['fact'] if ref_row else 0,
                'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
                'months_with_data': len(with_data),
                'months_total': len(months),
            },
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    if kpi_id == 'KD-M9':
        kp = cache_manager.locked_call(
            f'kp_price_{ref_y}_{series_m}',
            calc_kp_price.get_kp_price_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = kp.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            fact = row.get('fact', 0)
            calc = row.get('calc', 0)
            pct = round(fact / calc * 100, 1) if calc else None
            mrow = {
                'month': m,
                'year': row.get('year'),
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': calc,
                'fact': fact,
                'kpi_pct': pct,
                'has_data': fact != 0 or calc != 0,
            }
            months.append(mrow)
            if row.get('year') == ref_y and m == ref_m:
                ref_row = mrow

        with_data = [r for r in months if r.get('kpi_pct') is not None]
        return {
            'monthly_data': months,
            'last_full_month_row': dict(ref_row) if ref_row else None,
            'ytd': {
                'total_plan': ref_row['plan'] if ref_row else 0,
                'total_fact': ref_row['fact'] if ref_row else 0,
                'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
                'months_with_data': len(with_data),
                'months_total': len(months),
            },
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    if kpi_id == 'KD-M10':
        sla = cache_manager.locked_call(
            f'tkp_sla_{ref_y}_{series_m}',
            calc_tkp_sla.get_tkp_sla_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = sla.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            plan_val = row.get('plan', 0)
            fact_val = row.get('fact', 0)
            pct = row.get('pct')
            mrow = {
                'month': m,
                'year': row.get('year'),
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': plan_val,
                'fact': fact_val,
                'kpi_pct': pct,
                'has_data': plan_val > 0 or fact_val > 0,
            }
            months.append(mrow)
            if row.get('year') == ref_y and m == ref_m:
                ref_row = mrow

        with_data = [r for r in months if r.get('kpi_pct') is not None]
        return {
            'monthly_data': months,
            'last_full_month_row': dict(ref_row) if ref_row else None,
            'ytd': {
                'total_plan': ref_row['plan'] if ref_row else 0,
                'total_fact': ref_row['fact'] if ref_row else 0,
                'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
                'months_with_data': len(with_data),
                'months_total': len(months),
            },
            'kpi_period': {
                'type': 'last_full_month',
                'year': ref_y,
                'month': ref_m,
                'month_name': MONTH_NAMES_RU[ref_m],
            },
        }

    plan = 100_000_000.0
    months = _generate_tile_monthly_data(kpi_id, plan, pairs)
    ref_row = None
    for r in months:
        if r['year'] == ref_y and r['month'] == ref_m:
            ref_row = r
            break

    with_data = [r for r in months if r.get('kpi_pct') is not None]
    if with_data:
        avg_kpi = round(sum(r['kpi_pct'] for r in with_data) / len(with_data), 1)
    else:
        avg_kpi = None

    return {
        'monthly_data': months,
        'last_full_month_row': dict(ref_row) if ref_row else None,
        'ytd': {
            'total_plan': ref_row['plan'] if ref_row else plan,
            'total_fact': ref_row['fact'] if ref_row else 0,
            'kpi_pct': ref_row['kpi_pct'] if ref_row else None,
            'months_with_data': len(with_data),
            'months_total': len(months),
        },
        'kpi_period': {
            'type': 'last_full_month',
            'year': ref_y,
            'month': ref_m,
            'month_name': MONTH_NAMES_RU[ref_m],
        },
    }


def _build_line_chart(by_id: dict, tiles_data: dict) -> dict:
    """KD-C1: линейный график — Деньги, Отгрузки, Договоры (план + факт по месяцам)."""
    meta = by_id.get('KD-C1', {})
    series = []
    for kid in ['KD-M1', 'KD-M2', 'KD-M3']:
        kpi_meta = by_id.get(kid, {})
        td = tiles_data.get(kid, {})
        monthly = td.get('monthly_data') or []
        points = []
        for row in monthly:
            points.append({
                "month": row.get("month"),
                "month_name": row.get("month_name"),
                "year": row.get("year"),
                "plan": row.get("plan"),
                "fact": row.get("fact"),
            })
        series.append({
            "kpi_id": kid,
            "name": kpi_meta.get("name", kid),
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": f"План/Факт по месяцам: {kpi_meta.get('name', kid)}",
            "points": points,
        })

    return {
        "kpi_id": "KD-C1",
        "name": meta.get("name", "Динамика: Деньги, Отгрузки, Договоры"),
        "periodicity": "ежемесячно",
        "chart_type": meta.get("chart_type", "multi_line_plan_fact_monthly"),
        "chart_type_label": "Линейный тренд по месяцам (план/факт)",
        "series": series,
    }


def _build_pie_charts(ref_y: int, ref_m: int) -> dict:
    """KD-C2: круговые диаграммы — 5 направлений."""
    random.seed(hash(('KD-C2-pies', ref_y, ref_m)))
    pie_data = []
    for cat in PIE_CHART_CATEGORIES:
        value = round(random.uniform(5, 35), 1)
        pie_data.append({"name": cat, "value": value})

    total = sum(d['value'] for d in pie_data)
    for d in pie_data:
        d['pct'] = round(d['value'] / total * 100, 1) if total > 0 else 0

    return {
        "kpi_id": "KD-C2",
        "name": "Круговые диаграммы по направлениям",
        "periodicity": "ежемесячно",
        "chart_type": "donut_multiple",
        "chart_type_label": "Круговые диаграммы",
        "pie_data": pie_data,
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES_RU[ref_m]},
    }


def _build_bar_chart(by_id: dict, tiles_data: dict,
                     ref_y: int, ref_m: int) -> dict:
    """KD-C3: столбчатый — KPI за месяц для Деньги, Отгрузки, Договоры, Валовая прибыль."""
    kids = ['KD-M1', 'KD-M2', 'KD-M3', 'KD-M6']
    categories = []
    plan_values = []
    fact_values = []
    points = []

    for kid in kids:
        meta = by_id.get(kid, {})
        td = tiles_data.get(kid, {})
        lm = td.get('last_full_month_row')
        name = meta.get('name', kid)
        categories.append(name)
        plan_val = lm.get('plan') if lm else None
        fact_val = lm.get('fact') if lm else None
        plan_values.append(plan_val)
        fact_values.append(fact_val)
        points.append({
            "kpi_id": kid,
            "name": name,
            "month": ref_m,
            "year": ref_y,
            "plan": plan_val,
            "fact": fact_val,
            "kpi_pct": lm.get('kpi_pct') if lm else None,
        })

    return {
        "kpi_id": "KD-C3",
        "name": "KPI за месяц: Деньги, Отгрузки, Договоры, Валовая прибыль",
        "periodicity": "ежемесячно",
        "chart_type": "column_plan_fact_monthly",
        "chart_type_label": "Столбцы: план/факт за месяц",
        "series": [{
            "kpi_id": "KD-C3",
            "name": "План/факт за месяц",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "Столбцы",
            "categories": categories,
            "plan": plan_values,
            "fact": fact_values,
            "points": points,
        }],
    }


def _build_claims_table(ref_y: int, ref_m: int,
                        dept_guid: str | None = None) -> dict:
    """Таблица претензий за выбранный месяц.
    dept_guid=None  → коммерческий директор, все претензии;
    dept_guid='...' → дочернее подразделение, только его претензии.
    """
    from .komdir_claims import fetch_claims_for_month

    rows = cache_manager.locked_call(
        f'claims_{ref_y}_{ref_m}',
        fetch_claims_for_month, ref_y, ref_m,
    )

    if dept_guid:
        rows = [r for r in rows if r.get("order_dept_key") == dept_guid]

    return {
        "KD-T-CLAIMS": {
            "name": f"Претензии за {MONTH_NAMES_RU[ref_m]} {ref_y}",
            "periodicity": "ежемесячно",
            "description": "Претензии из 1С (Catalog_Претензии) за выбранный месяц",
            "period": {
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_NAMES_RU[ref_m],
            },
            "rows": rows,
        },
    }


_CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'

TILE_NAMES_RU = {
    'KD-M1': 'Деньги',
    'KD-M2': 'Отгрузки',
    'KD-M3': 'Договоры',
    'KD-M4': 'Дебиторская задолженность',
    'KD-M5': 'Просроченная ДЗ',
    'KD-M6': 'Валовая прибыль',
    'KD-M7': 'Расходы',
    'KD-M8': 'ФОТ',
    'KD-M9': 'Скидка / МЦР',
    'KD-M10': 'ТКП в SLA',
    'KD-M11': 'Текучесть персонала',
}


def _tile_cache_files(kpi_id: str, ref_y: int, ref_m: int) -> list[str]:
    """Кандидаты кэш-файлов для плитки."""
    today = date.today()
    if ref_y == today.year and ref_m == today.month:
        snap = today.isoformat()
    else:
        snap = f"{ref_y}-{ref_m:02d}-{calendar.monthrange(ref_y, ref_m)[1]:02d}"

    return {
        'KD-M1': [f'dengi_monthly_{ref_y}_{ref_m:02d}.json',
                   f'dengi_{ref_y}_{ref_m:02d}.json'],
        'KD-M2': [f'otgruzki_monthly_{ref_y}_{ref_m:02d}.json',
                   f'otgruzki_{ref_y}_{ref_m:02d}.json'],
        'KD-M3': [f'dogovory_monthly_{ref_y}_{ref_m:02d}.json',
                   f'dogovory_{ref_y}_{ref_m:02d}.json'],
        'KD-M4': [f'debitorka_monthly_{ref_y}_{ref_m:02d}.json',
                   f'debitorka_{snap}.json'],
        'KD-M5': [f'debitorka_monthly_{ref_y}_{ref_m:02d}.json',
                   f'debitorka_{snap}.json',
                   'dz_limits_latest.json'],
        'KD-M6': ['vp_result_cache.json'],
        'KD-M7': [f'rashody_{ref_y}_{ref_m:02d}.json'],
        'KD-M8': [f'fot_{ref_y}_{ref_m:02d}.json'],
        'KD-M9': [f'kp_price_{ref_y}_{ref_m:02d}.json'],
        'KD-M10': [f'tkp_sla_{ref_y}_{ref_m:02d}.json'],
        'KD-M11': [f'tekuchest_{ref_y}_{ref_m:02d}.json'],
    }.get(kpi_id, [])


def _tile_cache_updated_at(kpi_id: str, ref_y: int, ref_m: int) -> str | None:
    """ISO-timestamp последнего обновления кэша для плитки (по mtime файла)."""
    latest_mtime: float | None = None
    for fname in _tile_cache_files(kpi_id, ref_y, ref_m):
        p = _CACHE_DIR / fname
        if p.exists():
            mt = p.stat().st_mtime
            if latest_mtime is None or mt > latest_mtime:
                latest_mtime = mt
    if latest_mtime is None:
        return None
    return datetime.fromtimestamp(latest_mtime).isoformat(timespec='seconds')


def get_tiles_cache_status(ref_y: int | None = None,
                           ref_m: int | None = None) -> dict:
    """Статус кэшей по всем плиткам коммерческого директора."""
    today = date.today()
    if ref_y is None:
        ref_y = today.year
    if ref_m is None:
        ref_m = today.month

    tile_ids = [
        'KD-M1', 'KD-M2', 'KD-M3', 'KD-M4', 'KD-M5',
        'KD-M6', 'KD-M7', 'KD-M8', 'KD-M9', 'KD-M10', 'KD-M11',
    ]

    items = []
    for kid in tile_ids:
        files_info = []
        for fname in _tile_cache_files(kid, ref_y, ref_m):
            p = _CACHE_DIR / fname
            if p.exists():
                st = p.stat()
                files_info.append({
                    'file': fname,
                    'updated_at': datetime.fromtimestamp(st.st_mtime).isoformat(timespec='seconds'),
                    'size_kb': round(st.st_size / 1024, 1),
                })

        items.append({
            'kpi_id': kid,
            'name': TILE_NAMES_RU.get(kid, kid),
            'cache_updated_at': _tile_cache_updated_at(kid, ref_y, ref_m),
            'cache_exists': bool(files_info),
            'cache_files': files_info,
        })

    return {
        'period': {
            'year': ref_y,
            'month': ref_m,
            'month_name': MONTH_NAMES_RU.get(ref_m, ''),
        },
        'tiles': items,
    }


def _build_overdue_table(ref_y: int, ref_m: int,
                         dept_guid: str | None = None) -> dict:
    """Таблица детализации просроченной ДЗ по контрагентам.

    Сумма строк совпадает с KD-M5 (просроченная ДЗ).
    dept_guid=None → все отделы, dept_guid='...' → один отдел.
    """
    detail = cache_manager.locked_call(
        f'overdue_detail_{ref_y}_{ref_m}',
        calc_debitorka.get_overdue_detail,
        year=ref_y, month=ref_m, dept_guid=dept_guid,
    )

    rows = []
    for r in detail.get("rows", []):
        rows.append({
            "counterparty": r["partner_name"],
            "amount": r["amount"],
            "days_overdue": r["days_overdue"],
            "reason": None,
        })

    return {
        "name": f"Просроченная дебиторская задолженность на {detail.get('na_datu', '')}",
        "periodicity": "ежемесячно",
        "description": "Детализация просроченной ДЗ по контрагентам",
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES_RU[ref_m],
        },
        "total_overdue": detail.get("total_overdue", 0),
        "columns": ["Контрагент", "Сумма", "Дн. просрочки", "Причина"],
        "rows": rows,
    }


def build_komdir_payload(kpi_list: list[dict],
                         month: int | None = None,
                         year: int | None = None,
                         dept_guid: str | None = None) -> dict:
    """Полный payload для ответа API коммерческого директора.
    dept_guid — GUID подразделения для фильтрации (None = агрегат всех отделов).
    """
    by_id = {k["kpi_id"]: k for k in kpi_list}

    today = date.today()
    if month and year:
        ref_y, ref_m = year, month
        pairs = [(year, mm) for mm in range(1, month + 1)]
        series_m = month
    else:
        pairs_lm, ref_y, ref_m = _get_monthly_pairs()
        series_m = _series_through_month(today, ref_y, ref_m)
        if ref_y == today.year:
            pairs = [(ref_y, mm) for mm in range(1, series_m + 1)]
        else:
            pairs = pairs_lm

    tile_ids = [
        kid for kid in [
            'KD-M1', 'KD-M2', 'KD-M3', 'KD-M4', 'KD-M5',
            'KD-M6', 'KD-M7', 'KD-M8', 'KD-M9', 'KD-M10', 'KD-M11',
        ]
        if kid in by_id
    ]

    dz_dept_name = DEPT_GUID_TO_DZ_NAME.get(dept_guid) if dept_guid else None
    dz_payload = cache_manager.locked_call(
        f'debitorka_{ref_y}_{series_m}',
        calc_debitorka.get_komdir_dz_monthly,
        year=ref_y, month=series_m, dept_name=dz_dept_name,
    )
    plans_payload = cache_manager.locked_call(
        f'plans_{ref_y}_{series_m}',
        calc_plan.get_plans_monthly,
        year=ref_y, month=series_m, dept_guid=dept_guid,
    )

    tiles_data: dict[str, dict] = {}
    for kid in tile_ids:
        tiles_data[kid] = _get_tile_data(
            kid, pairs, ref_y, ref_m, series_m,
            dz_payload=dz_payload,
            dept_guid=dept_guid,
            plans_payload=plans_payload,
        )

    plitki_items = []
    numeric_for_avg: list[float] = []

    for kid in tile_ids:
        meta = by_id.get(kid)
        if not meta:
            continue
        td = tiles_data[kid]
        pct = td['ytd'].get('kpi_pct')
        if pct is not None:
            pct = float(pct)
        color = _tile_rag(kid, pct)
        if pct is not None:
            numeric_for_avg.append(pct)

        lm = td.get('last_full_month_row')
        tile_item = {
            "kpi_id": kid,
            "name": meta["name"],
            "kpi_pct": pct,
            "color": color,
            "period": _period_label(meta),
            "thresholds": _thresholds_block(meta),
            "formula": meta.get("formula"),
            "unit": meta.get("unit"),
            "source": meta.get("source"),
            "frequency": meta.get("frequency"),
            "plan": lm.get("plan") if lm else None,
            "fact": lm.get("fact") if lm else None,
            "has_data": lm.get("has_data", True) if lm else False,
            "plan_fact_period_label": f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}",
            "cache_updated_at": _tile_cache_updated_at(kid, ref_y, series_m),
        }
        plitki_items.append(tile_item)

    avg_pct = round(sum(numeric_for_avg) / len(numeric_for_avg), 1) if numeric_for_avg else None
    plitki_items.append({
        "kpi_id": "KD-AVG",
        "name": "Среднее по плиткам KPI",
        "kpi_pct": avg_pct,
        "color": _rag_higher_better(avg_pct),
        "period": "агрегат",
        "thresholds": {"green": "≥100%", "yellow": "90–99,9%", "red": "<90%"},
        "formula": "Среднее арифметическое kpi_pct всех плиток",
        "unit": "%",
        "source": "Расчётный показатель",
        "frequency": "агрегат",
    })

    grafiki = {
        "KD-C1": _build_line_chart(by_id, tiles_data),
        "KD-C2": _build_pie_charts(ref_y, series_m),
        "KD-C3": _build_bar_chart(by_id, tiles_data, ref_y, ref_m),
    }

    tablitsy: dict = {}

    try:
        tablitsy.update(_build_claims_table(ref_y, series_m, dept_guid=dept_guid))
    except Exception:
        pass

    try:
        tablitsy["KD-T-OVERDUE"] = _build_overdue_table(
            ref_y, series_m, dept_guid=dept_guid,
        )
    except Exception:
        pass

    if not tablitsy:
        tablitsy = {
            "месяц": [
                {
                    "kpi_id": kid,
                    "name": by_id[kid]["name"] if kid in by_id else kid,
                    "plan": td.get('last_full_month_row', {}).get('plan') if (td := tiles_data.get(kid)) else None,
                    "fact": td.get('last_full_month_row', {}).get('fact') if (td := tiles_data.get(kid)) else None,
                    "kpi_pct": tiles_data.get(kid, {}).get('ytd', {}).get('kpi_pct'),
                    "color": _tile_rag(kid, tiles_data.get(kid, {}).get('ytd', {}).get('kpi_pct')),
                    "formula": by_id.get(kid, {}).get("formula"),
                }
                for kid in tile_ids if kid in by_id
            ],
        }

    return {
        "month": series_m,
        "year": ref_y,
        "kpi_ref_month": ref_m,
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": grafiki,
        "Таблицы": tablitsy,
    }
