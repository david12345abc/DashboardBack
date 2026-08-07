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
import json
import logging
import random
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests

from comdir import (
    get_cena_ytd,
    get_debitorka_ytd,
    get_dengi_ytd,
    get_dogovory_ytd,
    get_fot_ytd,
    get_otgruzki_ytd,
    get_rashody_ytd,
    get_tkp_sla_ytd,
    get_vp_ytd,
)
from comdir.ytd import cache_stamp_paths as comdir_cache_stamp_paths

from . import (
    cache_manager,
    calc_debitorka,
    calc_dz_limits,
    calc_odp_ufgh_shipments,
    calc_plan,
    calc_tekuchest,
    kpi_reconciliation,
    odp_excel_breakdown,
)
from .commercial_department_aliases import DEALER_SALES_DEPT, KEY_CLIENTS_DEPT, normalize_commercial_dept_guid
from .commercial_tiles import DEPT_GUID_TO_DZ_NAME
from .kpi_periods import last_full_month

logger = logging.getLogger(__name__)

MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

LOWER_IS_BETTER_IDS = frozenset({'KD-M4', 'KD-M5', 'KD-M7', 'KD-M8', 'KD-M9', 'KD-M11'})
HIGHER_IS_BETTER_IDS = frozenset({'KD-M1', 'KD-M2', 'KD-M3', 'KD-M6', 'KD-M10'})

# Единицы измерения в ответе build_komdir_payload (плитки коммерческого директора / дочерних отделов).
KOMDIR_TILE_UNITS: dict[str, str] = {
    'KD-M9': 'руб.',  # цена фактическая / цена расчётная
    'KD-M10': 'шт',   # ТКП в SLA
}
KOMDIR_PAYLOAD_CACHE_VERSION = 5

ODP_UFG_H_TILE_META = {
    "kpi_id": "UFG-H",
    "name": "Отгрузки UFG-H",
    "goal": "Контролировать план/факт отгрузок по номенклатуре UFG-H",
    "frequency": "Ежемесячно",
    "formula": "Факт отгрузок UFG-H / План отгрузок UFG-H × 100%",
    "unit": "руб.",
    "source": "1С / продажи по номенклатурам UFG-H; заказы клиента",
    "green_threshold": "≥100%",
    "yellow_threshold": "90–99,9%",
    "red_threshold": "<90%",
}

PIE_CHART_CATEGORIES = [
    'Развитие дилеров',
    'Развитие стран',
    'Закладки Газпром',
    'Развитие холдингов',
    'Закладки БМИ',
]

COMMERCIAL_PROPOSAL_ENTITY = "Document_КоммерческоеПредложениеКлиенту"
REALIZATION_ENTITY = "Document_РеализацияТоваровУслуг"
KEY_CLIENTS_CHART_CACHE_VERSION = 1


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


def _rag_turnover_pct(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct < 90:
        return "green"
    if pct <= 100:
        return "yellow"
    return "red"


def _tile_rag(kpi_id: str, pct: float | None) -> str:
    if kpi_id == "KD-M11":
        return _rag_turnover_pct(pct)
    if kpi_id in LOWER_IS_BETTER_IDS:
        return _rag_lower_better(pct)
    return _rag_higher_better(pct)


def _plan_fact_higher_better_rag(plan, fact, pct: float | None) -> str:
    """KD-M1/M2/M3: факт выше плана — всегда зелёный."""
    try:
        plan_value = float(plan)
        fact_value = float(fact)
    except (TypeError, ValueError):
        return _rag_higher_better(pct)
    if fact_value > plan_value:
        return 'green'
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
    """Пары (год, месяц) с января по ТЕКУЩИЙ календарный месяц.

    По умолчанию (когда в запросе не передан month/year) возвращаем плитки и графики
    за текущий календарный месяц — даже если он ещё не завершён. Это исключает
    ситуацию, когда на странице выбран апрель, а данные приходят за март.
    """
    today = date.today()
    ref_y, ref_m = today.year, today.month
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]
    return pairs, ref_y, ref_m


def _fot_tile_period(ref_y: int, ref_m: int) -> tuple[int, int]:
    """KD-M8: в незакрытом текущем месяце показываем ФОТ за предыдущий."""
    today = date.today()
    if ref_y == today.year and ref_m == today.month:
        return last_full_month(today)
    return ref_y, ref_m


def _plan_fact_period_label(ref_y: int, ref_m: int) -> str:
    return f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}"


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


def _plan_values(plan_raw: float | None, year: int, month: int) -> tuple[float | None, float | None]:
    """(план на текущий момент, полный план за месяц)."""
    if plan_raw is None:
        return None, None
    full_plan = float(plan_raw)
    return _prorate_if_current(full_plan, year, month), full_plan


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
                          expected_by_month: dict[int, float] | None,
                          ref_y: int, ref_m: int) -> dict:
    """Общая логика сборки плитки план/факт для KD-M1/M2/M3."""
    months = []
    ref_row = None
    expected_by_month = expected_by_month or {}
    for row in raw_months:
        m = row.get('month')
        y = row.get('year', ref_y)
        fact = row.get('fact')
        plan, plan_full = _plan_values(plans_by_month.get(m) or 0, y, m)
        expected_plan = expected_by_month.get(m) or 0
        pct = round(fact / plan * 100, 1) if plan and fact is not None else None
        mrow = {
            'month': m,
            'year': y,
            'month_name': MONTH_NAMES_RU.get(m, ''),
            'plan': plan,
            'plan_full': plan_full,
            'fact': fact,
            'expected_plan': expected_plan,
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
            'total_expected_plan': ref_row['expected_plan'] if ref_row else (expected_by_month.get(ref_m) or 0),
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

    KD-M1/M2/M3/M6/M7/M8/M9/M10 — SQL-эталоны comdir (get_*_ytd).
    KD-M4/KD-M5 — из comdir SQL (РасчетыСКлиентамиПоСрокам / _AccumRg107662).
    KD-M11 — calc_tekuchest.
    dept_guid — GUID подразделения для фильтрации (None = агрегат).
    plans_payload — устаревший OData-планы; для M1–M3 план берётся из comdir.
    """
    dept_lock_suffix = f"_{_payload_cache_dept_part(dept_guid)}" if dept_guid else ""

    if kpi_id == 'KD-M1':
        dengi = cache_manager.locked_call(
            f'comdir_dengi_{ref_y}_{series_m}{dept_lock_suffix}',
            get_dengi_ytd,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw = dengi.get('months') or []
        plans_by_month = {r['month']: (r.get('plan') or 0) for r in raw}
        expected_by_month = {r['month']: (r.get('expected') or 0) for r in raw}
        return _build_plan_fact_tile(raw, plans_by_month, expected_by_month, ref_y, ref_m)

    if kpi_id == 'KD-M2':
        otg = cache_manager.locked_call(
            f'comdir_otgruzki_{ref_y}_{series_m}{dept_lock_suffix}',
            get_otgruzki_ytd,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw = otg.get('months') or []
        plans_by_month = {r['month']: (r.get('plan') or 0) for r in raw}
        expected_by_month = {r['month']: (r.get('expected') or 0) for r in raw}
        return _build_plan_fact_tile(raw, plans_by_month, expected_by_month, ref_y, ref_m)

    if kpi_id == 'KD-M3':
        dog = cache_manager.locked_call(
            f'comdir_dogovory_{ref_y}_{series_m}{dept_lock_suffix}',
            get_dogovory_ytd,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw = dog.get('months') or []
        plans_by_month = {r['month']: (r.get('plan') or 0) for r in raw}
        expected_by_month = {r['month']: (r.get('expected') or 0) for r in raw}
        return _build_plan_fact_tile(raw, plans_by_month, expected_by_month, ref_y, ref_m)

    if kpi_id == 'KD-M6':
        vp = cache_manager.locked_call(
            f'comdir_vp_{ref_y}_{series_m}{dept_lock_suffix}',
            get_vp_ytd,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        cal = vp.get('months_calendar') or vp.get('months') or []
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
            dz_payload = cache_manager.locked_call(
                f'comdir_debitorka_{ref_y}_{series_m}'
                + (f'_{dept_guid}' if dept_guid else ''),
                get_debitorka_ytd,
                year=ref_y, month=series_m, dept_guid=dept_guid,
            )
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
        fot_y, fot_m = _fot_tile_period(ref_y, ref_m)
        fot = cache_manager.locked_call(
            f'comdir_fot_{ref_y}_{series_m}{dept_lock_suffix}',
            get_fot_ytd,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = fot.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            y = row.get('year', ref_y)
            plan, plan_full = _plan_values(row.get('plan'), y, m)
            fact = row.get('fact')
            pct = round(fact / plan * 100, 1) if plan and fact is not None else None
            mrow = {
                'month': m,
                'year': y,
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': plan,
                'plan_full': plan_full,
                'fact': fact,
                'kpi_pct': pct,
                'has_data': fact is not None and fact != 0,
            }
            months.append(mrow)
            if row.get('year') == fot_y and m == fot_m:
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
                'year': fot_y,
                'month': fot_m,
                'month_name': MONTH_NAMES_RU[fot_m],
            },
        }

    if kpi_id == 'KD-M11':
        tek = cache_manager.locked_call(
            f'tekuchest_{ref_y}_{series_m}{dept_lock_suffix}',
            calc_tekuchest.get_tekuchest_monthly,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = tek.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            y = row.get('year', ref_y)
            p, plan_full = _plan_values(row.get('plan'), y, m)
            f = row.get('fact')
            pct = round(f / p * 100, 1) if p and f is not None else None
            mrow = {
                'month': m,
                'year': y,
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': p,
                'plan_full': plan_full,
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
            f'comdir_rashody_{ref_y}_{series_m}{dept_lock_suffix}',
            get_rashody_ytd,
            year=ref_y, month=series_m, dept_guid=dept_guid,
        )
        raw_months = rash.get('months', [])
        months = []
        ref_row = None
        for row in raw_months:
            m = row.get('month')
            y = row.get('year', ref_y)
            plan, plan_full = _plan_values(row.get('plan'), y, m)
            fact = row.get('fact', 0)
            pct = round(fact / plan * 100, 1) if plan and fact is not None else None
            mrow = {
                'month': m,
                'year': y,
                'month_name': MONTH_NAMES_RU.get(m, ''),
                'plan': plan,
                'plan_full': plan_full,
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
            f'comdir_cena_{ref_y}_{series_m}{dept_lock_suffix}',
            get_cena_ytd,
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
            f'comdir_tkp_sla_{ref_y}_{series_m}{dept_lock_suffix}',
            get_tkp_sla_ytd,
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
                "expected_plan": row.get("expected_plan"),
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


def _month_bounds(ref_y: int, ref_m: int) -> tuple[str, str]:
    start = f"{ref_y}-{ref_m:02d}-01T00:00:00"
    if ref_m == 12:
        end = f"{ref_y + 1}-01-01T00:00:00"
    else:
        end = f"{ref_y}-{ref_m + 1:02d}-01T00:00:00"
    return start, end


def _fetch_odata_all(session: requests.Session, entity: str, query: str, *, page: int = 500) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if query else ""
        url = f"{calc_plan.BASE}/{quote(entity)}?$format=json{sep}{query}&$top={page}&$skip={skip}"
        r = session.get(url, timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _batch_fetch_users_departments(session: requests.Session, user_keys: set[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    keys = sorted(k for k in user_keys if k)
    for i in range(0, len(keys), 25):
        batch = keys[i:i + 25]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        query = (
            f"$filter={quote(flt, safe='')}"
            f"&$select={quote('Ref_Key,Подразделение_Key', safe=',_')}"
        )
        for row in _fetch_odata_all(session, "Catalog_Пользователи", query):
            key = str(row.get("Ref_Key") or "")
            if key:
                result[key] = normalize_commercial_dept_guid(row.get("Подразделение_Key") or "")
    return result


def _key_clients_charts_cache_path(ref_y: int, ref_m: int) -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR / f"key_clients_charts_{ref_y}_{ref_m:02d}.json"


def _load_key_clients_charts_cache(ref_y: int, ref_m: int) -> dict | None:
    path = _key_clients_charts_cache_path(ref_y, ref_m)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError, TypeError):
        return None
    if (
        data.get("cache_date") == date.today().isoformat()
        and data.get("cache_version") == KEY_CLIENTS_CHART_CACHE_VERSION
    ):
        return data
    return None


def _save_key_clients_charts_cache(ref_y: int, ref_m: int, payload: dict) -> None:
    path = _key_clients_charts_cache_path(ref_y, ref_m)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                **payload,
                "cache_date": date.today().isoformat(),
                "cache_version": KEY_CLIENTS_CHART_CACHE_VERSION,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def _proposal_customer_key(row: dict) -> str:
    return str(
        row.get("КонтрагентЗаказчик_Key")
        or row.get("КонтрагентКонечныйЗаказчик_Key")
        or row.get("Контрагент_Key")
        or ""
    )


def _load_key_clients_proposals(session: requests.Session, ref_y: int, ref_m: int) -> tuple[list[dict], dict]:
    start, end = _month_bounds(ref_y, ref_m)
    flt = (
        f"Date ge datetime'{start}' and Date lt datetime'{end}' "
        f"and Posted eq true and DeletionMark eq false"
    )
    select = (
        "Ref_Key,Date,Number,Менеджер_Key,ТД_ПроектнаяЗакладка,"
        "КонтрагентКонечныйЗаказчик_Key,Контрагент_Key"
    )
    query = (
        f"$filter={quote(flt, safe='')}"
        f"&$select={quote(select, safe=',_')}"
    )
    docs = _fetch_odata_all(session, COMMERCIAL_PROPOSAL_ENTITY, query)
    manager_keys = {
        row.get("Менеджер_Key")
        for row in docs
        if row.get("Менеджер_Key") and row.get("Менеджер_Key") != calc_plan.EMPTY
    }
    manager_depts = _batch_fetch_users_departments(session, set(manager_keys))
    filtered = [
        row for row in docs
        if manager_depts.get(str(row.get("Менеджер_Key") or "")) == KEY_CLIENTS_DEPT
    ]
    return filtered, {
        "period_start": start,
        "period_end": end,
        "docs_loaded": len(docs),
        "docs_for_key_clients": len(filtered),
        "managers_loaded": len(manager_depts),
    }


def _customers_with_shipments_last_two_years(
    session: requests.Session,
    customer_keys: set[str],
    ref_y: int,
    ref_m: int,
) -> tuple[set[str], dict]:
    if not customer_keys:
        return set(), {"shipments_loaded": 0, "period_start": None, "period_end": None}
    period_start = date(ref_y - 2, ref_m, 1).isoformat() + "T00:00:00"
    _month_start, period_end = _month_bounds(ref_y, ref_m)
    shipped: set[str] = set()
    loaded = 0
    keys = sorted(k for k in customer_keys if k and k != calc_plan.EMPTY)
    select = "Ref_Key,Date,Number,Контрагент_Key,Партнер_Key,Подразделение_Key"
    for i in range(0, len(keys), 25):
        batch = keys[i:i + 25]
        customer_filter = " or ".join(f"Контрагент_Key eq guid'{key}'" for key in batch)
        flt = (
            f"Date ge datetime'{period_start}' and Date lt datetime'{period_end}' "
            f"and Posted eq true and DeletionMark eq false "
            f"and ({customer_filter})"
        )
        query = (
            f"$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        rows = _fetch_odata_all(session, REALIZATION_ENTITY, query)
        loaded += len(rows)
        for row in rows:
            customer = str(row.get("Контрагент_Key") or "")
            if customer:
                shipped.add(customer)
    return shipped, {
        "shipments_loaded": loaded,
        "period_start": period_start,
        "period_end": period_end,
        "customers_checked": len(keys),
    }


def _build_key_clients_pie_charts(ref_y: int, ref_m: int) -> dict:
    cached = _load_key_clients_charts_cache(ref_y, ref_m)
    if cached is not None:
        return cached

    session = requests.Session()
    session.auth = calc_plan.AUTH
    proposals, proposal_debug = _load_key_clients_proposals(session, ref_y, ref_m)

    bookmarks = [
        row for row in proposals
        if bool(row.get("ТД_ПроектнаяЗакладка"))
    ]
    customers = {
        customer
        for row in proposals
        if (customer := _proposal_customer_key(row)) and customer != calc_plan.EMPTY
    }
    shipped_customers, shipment_debug = _customers_with_shipments_last_two_years(
        session,
        customers,
        ref_y,
        ref_m,
    )
    new_customers = customers - shipped_customers
    new_count = len(new_customers)
    total_customers = len(customers)
    new_pct = round(new_count / total_customers * 100, 1) if total_customers else 0.0

    pie_data = [
        {
            "name": "Закладки в проекты",
            "value": len(bookmarks),
            "pct": 100.0 if bookmarks else 0.0,
            "has_data": True,
            "unit": "шт.",
        },
        {
            "name": "Развитие холдингов (шт.)",
            "value": new_count,
            "pct": 100.0 if new_count else 0.0,
            "has_data": True,
            "unit": "шт.",
        },
        {
            "name": "Развитие холдингов (%)",
            "value": new_pct,
            "pct": new_pct,
            "has_data": True,
            "unit": "%",
        },
    ]
    payload = {
        "kpi_id": "KD-C2",
        "name": "Ключевые клиенты: развитие и проектные закладки",
        "periodicity": "ежемесячно",
        "chart_type": "donut_multiple",
        "chart_type_label": "Круговые диаграммы",
        "pie_data": pie_data,
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES_RU[ref_m]},
        "debug": {
            "proposals": proposal_debug,
            "shipments": shipment_debug,
            "customers_total": total_customers,
            "customers_without_shipments_2y": new_count,
            "bookmarks": len(bookmarks),
        },
    }
    _save_key_clients_charts_cache(ref_y, ref_m, payload)
    return payload


def _build_pie_charts(
    ref_y: int,
    ref_m: int,
    active_dealers_report: dict | None = None,
    dept_guid: str | None = None,
) -> dict:
    """KD-C2: круговые диаграммы — 5 направлений."""
    if dept_guid == KEY_CLIENTS_DEPT:
        return _build_key_clients_pie_charts(ref_y, ref_m)

    random.seed(hash(('KD-C2-pies', ref_y, ref_m)))
    pie_data = []

    dealer_fact: float | None = None
    dealer_has_data = False
    if active_dealers_report and active_dealers_report.get("has_data"):
        dealer_fact = float(active_dealers_report.get("pie_value", 0))
        dealer_has_data = True

    for cat in PIE_CHART_CATEGORIES:
        if cat == "Развитие дилеров" and dealer_fact is not None:
            value = round(dealer_fact, 1)
            pie_data.append({
                "name": cat,
                "value": value,
                "has_data": dealer_has_data,
                "unit": "шт",
            })
        else:
            value = round(random.uniform(5, 35), 1)
            pie_data.append({"name": cat, "value": value, "has_data": False})

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
    """KD-C3: столбчатый — KPI за месяц для Деньги, Отгрузки, Договоры."""
    kids = ['KD-M1', 'KD-M2', 'KD-M3']
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
        expected_val = lm.get('expected_plan') if lm else None
        plan_values.append(plan_val)
        fact_values.append(fact_val)
        points.append({
            "kpi_id": kid,
            "name": name,
            "month": ref_m,
            "year": ref_y,
            "plan": plan_val,
            "fact": fact_val,
            "expected_plan": expected_val,
            "kpi_pct": lm.get('kpi_pct') if lm else None,
        })

    return {
        "kpi_id": "KD-C3",
        "name": "KPI за месяц: Деньги, Отгрузки, Договоры",
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


def _build_odp_ufg_h_tile(ref_y: int, ref_m: int) -> dict | None:
    data = cache_manager.locked_call(
        f"odp_ufg_h_shipments_{ref_y}_{ref_m}",
        calc_odp_ufgh_shipments.get_ufg_h_shipments_monthly,
        year=ref_y,
        month=ref_m,
        dept_guid=DEALER_SALES_DEPT,
    )
    if data is None:
        return None

    lm = data.get("last_full_month_row") or {}
    pct = lm.get("kpi_pct")
    if pct is not None:
        pct = float(pct)

    return {
        "kpi_id": ODP_UFG_H_TILE_META["kpi_id"],
        "name": ODP_UFG_H_TILE_META["name"],
        "goal": ODP_UFG_H_TILE_META.get("goal"),
        "kpi_pct": pct,
        "color": _tile_rag("UFG-H", pct),
        "period": "ежемесячно",
        "thresholds": _thresholds_block(ODP_UFG_H_TILE_META),
        "formula": ODP_UFG_H_TILE_META.get("formula"),
        "unit": ODP_UFG_H_TILE_META.get("unit"),
        "source": ODP_UFG_H_TILE_META.get("source"),
        "frequency": ODP_UFG_H_TILE_META.get("frequency"),
        "plan": lm.get("plan"),
        "fact": lm.get("fact"),
        "has_data": lm.get("has_data", True) if lm else False,
        "plan_fact_period_label": f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}",
        "cache_updated_at": None,
        "monthly_data": data.get("monthly_data") or data.get("months") or [],
    }


def _build_claims_table(ref_y: int, ref_m: int,
                        dept_guid: str | None = None) -> dict:
    """Таблица претензий за выбранный месяц.
    dept_guid=None  → коммерческий директор, все претензии;
    dept_guid='...' → дочернее подразделение, только его претензии.
    """
    from .komdir_claims import VED_DEPT_KEY, fetch_claims_for_month, _is_uztransgaz_partner

    rows = cache_manager.locked_call(
        f'claims_all_{ref_y}_{ref_m}' if dept_guid is None or dept_guid == VED_DEPT_KEY else f'claims_{ref_y}_{ref_m}',
        fetch_claims_for_month, ref_y, ref_m,
        include_all=dept_guid is None or dept_guid == VED_DEPT_KEY,
    )

    if dept_guid:
        rows = [
            r for r in rows
            if (
                r.get("order_dept_key") == dept_guid
                or r.get("normalized_order_dept_key") == dept_guid
                or (dept_guid == VED_DEPT_KEY and _is_uztransgaz_partner(r.get("partner", "")))
            )
        ]

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


def _build_lawsuits_table(ref_y: int, ref_m: int,
                          dept_guid: str | None = None) -> dict:
    """Таблица судов на конец выбранного месяца.

    Источник — Document_ТД_ПретензииСудебныеСпорыИсковаяРабота (через komdir_lawsuits).
    В таблице — активные (не закрытые) дела с Date <= конец месяца.

    dept_guid=None  → коммерческий директор: видит ВСЕ суды компании;
    dept_guid='...' → оставляем только суды конкретного отдела (по инициатору).
    """
    from .komdir_lawsuits import (
        fetch_lawsuits_for_month,
        normalize_lawsuits_rows,
        _cache_path as lawsuits_cache_path,
    )

    cache_key = f'lawsuits_all_{ref_y}_{ref_m}'
    cache_manager.register_cache_path(
        cache_key, lawsuits_cache_path(ref_y, ref_m, include_all=True),
    )
    rows = normalize_lawsuits_rows(
        cache_manager.locked_call(
            cache_key,
            fetch_lawsuits_for_month, ref_y, ref_m,
            include_all=True,
        )
    )

    if dept_guid:
        dept_lower = str(dept_guid).lower()
        rows = [
            r for r in rows
            if str(r.get("initiator_dept_key") or "").lower() == dept_lower
        ]

    return {
        "KD-T-LAWSUITS": {
            "name": f"Суды на {MONTH_NAMES_RU[ref_m]} {ref_y}",
            "periodicity": "ежемесячно",
            "description": (
                "Активные судебные споры и исковая работа из 1С "
                "(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) "
                "на конец выбранного месяца (статус ≠ Закрыта)"
            ),
            "period": {
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_NAMES_RU[ref_m],
            },
            "columns": [
                "Тип документа", "Контрагент", "Предмет спора",
                "Роль ГК в споре", "Юр. лицо", "Подразделение",
                "Дата SLA", "Краткое описание ситуации",
                "Сумма требований, руб.",
            ],
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

    comdir_names = [p.name for p in comdir_cache_stamp_paths(kpi_id, ref_y, ref_m)]
    if comdir_names:
        return comdir_names

    return {
        'KD-M4': [f'debitorka_monthly_{ref_y}_{ref_m:02d}.json',
                   f'debitorka_{snap}.json'],
        'KD-M5': [f'debitorka_monthly_{ref_y}_{ref_m:02d}.json',
                   f'debitorka_{snap}.json',
                   'dz_limits_latest.json'],
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


def _department_cell_from_overdue_row(row: dict) -> str:
    """Единое значение колонки «Подразделение» (в т.ч. ликвидированные с пометкой)."""
    liquidated = (
        (row.get("liquidated_dept_name") or "").strip()
        or (row.get("Ликвидированное подразделение") or "").strip()
    )
    dept_name = (row.get("dept_name") or "").strip()
    return (
        (row.get("department") or "").strip()
        or (row.get("Подразделение") or "").strip()
        or liquidated
        or dept_name
    )


def _normalize_overdue_table(table: dict | None) -> dict | None:
    """Миграция старого формата KD-T-OVERDUE: колонка и поле «Подразделение»."""
    if not isinstance(table, dict):
        return table
    columns = list(table.get("columns") or [])
    table["columns"] = [
        "Подразделение" if str(col).strip() == "Ликвидированное подразделение" else col
        for col in columns
    ] or [
        "№ Заказа клиента", "Контрагент", "Дн. просрочки",
        "Подразделение", "Причина", "Действие", "Сумма",
    ]
    rows = []
    for r in table.get("rows") or []:
        if not isinstance(r, dict):
            continue
        department = _department_cell_from_overdue_row(r)
        next_row = dict(r)
        next_row["department"] = department
        next_row["Подразделение"] = department
        next_row.pop("Ликвидированное подразделение", None)
        rows.append(next_row)
    table["rows"] = rows
    return table


def _normalize_komdir_payload_tables(payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return payload
    tables = payload.get("Таблицы")
    if isinstance(tables, dict) and isinstance(tables.get("KD-T-OVERDUE"), dict):
        tables["KD-T-OVERDUE"] = _normalize_overdue_table(tables["KD-T-OVERDUE"])
    return payload


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
        liquidated = (r.get("liquidated_dept_name") or "").strip()
        dept_name = (r.get("dept_name") or "").strip()
        department = _department_cell_from_overdue_row(r)
        rows.append({
            "counterparty": r.get("partner_name") or r.get("counterparty") or "",
            "partner_name": r.get("partner_name") or "",
            "order_num": r.get("order_num") or "",
            "order_date": r.get("order_date") or "",
            "order_key": r.get("order_key") or "",
            "amount": r.get("amount"),
            "dz_total": r.get("dz_total"),
            "days_overdue": r.get("days_overdue"),
            "installments_count": r.get("installments_count"),
            "installments": r.get("installments") or [],
            "reason": r.get("reason") or "",
            "action": r.get("action") or "",
            "dept_key": r.get("dept_key") or "",
            "dept_name": dept_name,
            "source_dept_key": r.get("source_dept_key") or "",
            "liquidated_dept_name": liquidated,
            "department": department,
            "Подразделение": department,
        })

    table = {
        "name": f"Просроченная дебиторская задолженность на {detail.get('na_datu', '')}",
        "periodicity": "ежемесячно",
        "description": "Детализация просроченной ДЗ по заказам клиентов",
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES_RU[ref_m],
        },
        "total_overdue": detail.get("total_overdue", 0),
        "columns": [
            "№ Заказа клиента", "Контрагент", "Дн. просрочки",
            "Подразделение", "Причина", "Действие", "Сумма",
        ],
        "rows": rows,
    }
    if detail.get("cache_refresh_status"):
        table["cache_refresh_status"] = detail.get("cache_refresh_status")
    return table


def _payload_cache_dept_part(dept_guid: str | None) -> str:
    return str(dept_guid or 'all').strip().lower().replace('-', '')


def _payload_cache_path(ref_y: int, ref_m: int, dept_guid: str | None) -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR / f"komdir_payload_{_payload_cache_dept_part(dept_guid)}_{ref_y}_{ref_m:02d}.json"


def _save_payload_cache(ref_y: int, ref_m: int, dept_guid: str | None, payload: dict) -> None:
    try:
        with _payload_cache_path(ref_y, ref_m, dept_guid).open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "cache_version": KOMDIR_PAYLOAD_CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "payload": payload,
                },
                f,
                ensure_ascii=False,
            )
    except OSError:
        logger.exception("komdir payload cache: failed to save")


def _unwrap_payload_cache(raw: dict) -> dict:
    if isinstance(raw.get("payload"), dict):
        payload = dict(raw["payload"])
        _normalize_komdir_payload_tables(payload)
        if raw.get("cache_version") == KOMDIR_PAYLOAD_CACHE_VERSION:
            payload["cache_refresh_status"] = "running"
            for tile in (payload.get("Плитки") or {}).get("items") or []:
                if isinstance(tile, dict):
                    tile["cache_refresh_status"] = "running"
            for table in (payload.get("Таблицы") or {}).values():
                if isinstance(table, dict):
                    table["cache_refresh_status"] = "running"
        return payload
    if isinstance(raw, dict) and ("Плитки" in raw or "Таблицы" in raw):
        _normalize_komdir_payload_tables(raw)
    return raw


def _payload_with_active_refresh_status(payload: dict, ref_y: int, ref_m: int, payload_cache_path: Path) -> dict:
    """Вернуть snapshot со статусом running, если сейчас обновляются его кэши."""
    if not isinstance(payload, dict):
        return payload
    payload_refreshing = cache_manager.is_cache_path_refreshing(payload_cache_path)
    any_refreshing = payload_refreshing
    next_payload = dict(payload)
    for tile in (next_payload.get("Плитки") or {}).get("items") or []:
        if not isinstance(tile, dict):
            continue
        kid = str(tile.get("kpi_id") or "").strip()
        tile_refreshing = payload_refreshing or cache_manager.is_any_cache_path_refreshing(
            [_CACHE_DIR / fname for fname in _tile_cache_files(kid, ref_y, ref_m)]
        )
        if tile_refreshing:
            tile["cache_refresh_status"] = "running"
            any_refreshing = True
    if payload_refreshing:
        for table in (next_payload.get("Таблицы") or {}).values():
            if isinstance(table, dict):
                table["cache_refresh_status"] = "running"
    if any_refreshing:
        next_payload["cache_refresh_status"] = "running"
    return next_payload


def _load_fresh_payload_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if (
        raw.get("cache_version") == KOMDIR_PAYLOAD_CACHE_VERSION
        and raw.get("cache_date") == date.today().isoformat()
        and isinstance(raw.get("payload"), dict)
    ):
        return _normalize_komdir_payload_tables(raw["payload"])
    return None


def _komdir_payload_period(month: int | None, year: int | None) -> tuple[int, int, int]:
    today = date.today()
    if month and year:
        return int(year), int(month), int(month)
    _pairs, ref_y, ref_m = _get_monthly_pairs()
    return ref_y, ref_m, _series_through_month(today, ref_y, ref_m)


def build_komdir_payload(kpi_list: list[dict],
                         month: int | None = None,
                         year: int | None = None,
                         dept_guid: str | None = None) -> dict:
    ref_y, ref_m, _series_m = _komdir_payload_period(month, year)
    cache_key = f"komdir_payload_{_payload_cache_dept_part(dept_guid)}_{ref_y}_{ref_m:02d}"
    payload_cache_path = _payload_cache_path(ref_y, ref_m, dept_guid)
    cache_manager.register_cache_path(cache_key, payload_cache_path)
    if not cache_manager.is_force_compute_context():
        cached_payload = _load_fresh_payload_cache(payload_cache_path)
        if cached_payload is not None:
            return _payload_with_active_refresh_status(cached_payload, ref_y, ref_m, payload_cache_path)
    raw = cache_manager.locked_call(
        cache_key,
        _build_komdir_payload_fresh,
        kpi_list,
        month=month,
        year=year,
        dept_guid=dept_guid,
        cache_path=payload_cache_path,
    )
    return _unwrap_payload_cache(raw)


def _plans_payload_from_tile(tile: dict, kpi_id: str) -> dict:
    plan_key_by_kpi = {
        "KD-M1": ("dengi", "dengi_expected"),
        "KD-M2": ("otgruzki", "otgruzki_expected"),
        "KD-M3": ("dogovory", "dogovory_expected"),
    }
    keys = plan_key_by_kpi.get(kpi_id)
    if not keys:
        return {"months": []}
    plan_key, expected_key = keys
    months = []
    for row in tile.get("monthly_data") or []:
        if not isinstance(row, dict):
            continue
        months.append({
            "year": row.get("year"),
            "month": row.get("month"),
            plan_key: row.get("plan_full", row.get("plan")),
            expected_key: row.get("expected_plan"),
        })
    return {"months": months}


def _patch_payload_tile(payload: dict, kpi_id: str, tile_data: dict, ref_y: int, ref_m: int) -> None:
    items = (payload.get("Плитки") or {}).get("items") or []
    for tile in items:
        if not isinstance(tile, dict) or tile.get("kpi_id") != kpi_id:
            continue
        lm = tile_data.get("last_full_month_row") or {}
        ytd = tile_data.get("ytd") or {}
        pct = ytd.get("kpi_pct")
        tile["kpi_pct"] = pct
        if kpi_id in {"KD-M1", "KD-M2", "KD-M3"}:
            tile["color"] = _plan_fact_higher_better_rag(lm.get("plan"), lm.get("fact"), pct)
        else:
            tile["color"] = _tile_rag(kpi_id, float(pct) if pct is not None else None)
        if "monthly_data" in tile_data:
            tile["monthly_data"] = tile_data.get("monthly_data") or []
        if lm:
            for key in ("plan", "fact", "expected_plan", "has_data"):
                if key in lm:
                    tile[key] = lm.get(key)
            kpi_period = tile_data.get("kpi_period") or {}
            period_y = kpi_period.get("year", ref_y)
            period_m = kpi_period.get("month", ref_m)
            tile["plan_fact_period_label"] = _plan_fact_period_label(period_y, period_m)
        tile["cache_updated_at"] = datetime.now().isoformat(timespec="seconds")
        tile.pop("cache_refresh_status", None)
        break


def refresh_komdir_tile_cache(kpi_id: str,
                              *,
                              month: int | None = None,
                              year: int | None = None,
                              dept_guid: str | None = None) -> bool:
    """Точечно пересчитать кэш одной коммерческой плитки и обновить snapshot payload."""
    kid = str(kpi_id or "").strip().upper()
    ref_y, ref_m, series_m = _komdir_payload_period(month, year)
    payload_cache_path = _payload_cache_path(ref_y, ref_m, dept_guid)
    raw = None
    if payload_cache_path.exists():
        try:
            with payload_cache_path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            raw = None
    if not isinstance(raw, dict) or not isinstance(raw.get("payload"), dict):
        return False

    payload = dict(raw["payload"])
    existing_tile = None
    for tile in (payload.get("Плитки") or {}).get("items") or []:
        if isinstance(tile, dict) and tile.get("kpi_id") == kid:
            existing_tile = tile
            break
    if existing_tile is None:
        return False

    plans_payload = _plans_payload_from_tile(existing_tile, kid)
    if kid in {"KD-M1", "KD-M2", "KD-M3", "KD-M8", "KD-M11"}:
        tile_data = _get_tile_data(
            kid,
            [(ref_y, m) for m in range(1, series_m + 1)],
            ref_y,
            ref_m,
            series_m,
            dept_guid=dept_guid,
            plans_payload=plans_payload,
        )
    else:
        return False

    _patch_payload_tile(payload, kid, tile_data, ref_y, ref_m)
    _save_payload_cache(ref_y, ref_m, dept_guid, payload)
    return True


def _build_komdir_payload_fresh(kpi_list: list[dict],
                                month: int | None = None,
                                year: int | None = None,
                                dept_guid: str | None = None,
                                cache_path: Path | None = None) -> dict:
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

    dz_lock = f'comdir_debitorka_{ref_y}_{series_m}'
    if dept_guid:
        dz_lock = f'{dz_lock}_{dept_guid}'
    dz_payload = cache_manager.locked_call(
        dz_lock,
        get_debitorka_ytd,
        year=ref_y, month=series_m, dept_guid=dept_guid,
    )
    tiles_data: dict[str, dict] = {}
    for kid in tile_ids:
        tiles_data[kid] = _get_tile_data(
            kid, pairs, ref_y, ref_m, series_m,
            dz_payload=dz_payload,
            dept_guid=dept_guid,
            plans_payload=None,
        )

    plitki_items = []

    for kid in tile_ids:
        meta = by_id.get(kid)
        if not meta:
            continue
        td = tiles_data[kid]
        pct = td['ytd'].get('kpi_pct')
        if pct is not None:
            pct = float(pct)
        lm = td.get('last_full_month_row')
        kpi_period = td.get('kpi_period') or {}
        period_y = kpi_period.get('year', ref_y)
        period_m = kpi_period.get('month', ref_m)
        if kid in {'KD-M1', 'KD-M2', 'KD-M3'} and lm:
            color = _plan_fact_higher_better_rag(lm.get('plan'), lm.get('fact'), pct)
        else:
            color = _tile_rag(kid, pct)
        monthly_data = td.get("monthly_data") or []
        if kid in LOWER_IS_BETTER_IDS or kid in HIGHER_IS_BETTER_IDS:
            monthly_data = [
                {
                    **row,
                    "color": _tile_rag(kid, row.get("kpi_pct")),
                }
                if isinstance(row, dict) and row.get("kpi_pct") is not None
                else row
                for row in monthly_data
            ]
        tile_item = {
            "kpi_id": kid,
            "name": meta["name"],
            "goal": meta.get("goal"),
            "kpi_pct": pct,
            "color": color,
            "period": _period_label(meta),
            "thresholds": _thresholds_block(meta),
            "formula": meta.get("formula"),
            "unit": KOMDIR_TILE_UNITS.get(kid) or meta.get("unit"),
            "source": meta.get("source"),
            "description": meta.get("description"),
            "frequency": meta.get("frequency"),
            "plan": lm.get("plan") if lm else None,
            "fact": lm.get("fact") if lm else None,
            "expected_plan": lm.get("expected_plan") if lm else None,
            "has_data": (
                (lm.get("fact") is not None if lm else False)
                if kid in {"KD-M1", "KD-M2", "KD-M3"}
                else (lm.get("has_data", True) if lm else False)
            ),
            "plan_fact_period_label": _plan_fact_period_label(period_y, period_m),
            "cache_updated_at": _tile_cache_updated_at(kid, ref_y, series_m),
            "monthly_data": monthly_data,
        }
        if kid in LOWER_IS_BETTER_IDS:
            tile_item["pct_lower_is_better"] = True
        elif kid in HIGHER_IS_BETTER_IDS:
            tile_item["pct_higher_is_better"] = True
        plitki_items.append(tile_item)

    if dept_guid == DEALER_SALES_DEPT:
        odp_ufg_h_tile = _build_odp_ufg_h_tile(ref_y, ref_m)
        if odp_ufg_h_tile is not None:
            plitki_items.append(odp_ufg_h_tile)

    active_dealers_report: dict | None = None
    try:
        from . import calc_komdir_active_dealers

        active_dealers_report = cache_manager.locked_call(
            f"active_dealers_{date.today().isoformat()}",
            calc_komdir_active_dealers.compute_active_dealers_report,
            date.today(),
        )
    except Exception:
        logger.exception("Действующие дилеры: не удалось получить отчёт (кэш/1С)")

    grafiki = {
        "KD-C1": _build_line_chart(by_id, tiles_data),
        "KD-C2": _build_pie_charts(ref_y, series_m, active_dealers_report, dept_guid=dept_guid),
        "KD-C3": _build_bar_chart(by_id, tiles_data, ref_y, ref_m),
    }
    try:
        from . import calc_ks_razvitie
        ks_plans = cache_manager.locked_call(
            f"ks_razvitie_{ref_y}",
            calc_ks_razvitie.get_ks_razvitie_plans,
            year=ref_y,
        )
        ks_by_dept = ks_plans.get("by_dept") or {}
        ks_dept_inds = ks_plans.get("dept_indicators") or {}
        ks_by_dept_guid = ks_plans.get("by_dept_guid") or {}
        all_charts = ks_plans.get("charts") or []

        if dept_guid:
            dept_slice = ks_by_dept_guid.get(dept_guid.lower())
            if dept_slice:
                # Конкретное дочернее подразделение коммерческого блока
                indicators = dept_slice.get("indicators") or []
                months_map = dept_slice.get("months") or {}
                dept_name = dept_slice.get("dept_name") or ""
                by_dept_out = {dept_name: dept_slice} if dept_name else {}
                dept_charts = dept_slice.get("charts") or []
            else:
                # Подразделение передано, но документов нет
                indicators = []
                months_map = {}
                by_dept_out = {}
                dept_charts = []
        else:
            # Коммерческий директор — все документы всех подразделений.
            indicators = ks_plans.get("indicators") or []
            months_map = ks_plans.get("months") or {}
            by_dept_out = ks_by_dept
            dept_charts = all_charts

        grafiki["KS-RAZVITIE"] = {
            "kpi_id": "KS-RAZVITIE",
            "name": "КС развитие — планы по месяцам",
            "periodicity": "ежемесячно",
            "chart_type": "donut_multiple_monthly",
            "chart_type_label": "Круговые диаграммы по месяцам (КС развитие)",
            "period": {
                "year": ref_y,
                "month": ref_m,
                "month_name": MONTH_NAMES_RU.get(ref_m, ""),
            },
            "indicators": indicators,
            "indicator_units": ks_plans.get("indicator_units") or {},
            "months": months_map,
            "by_dept": by_dept_out,
            "by_dept_guid": ks_by_dept_guid,
            "dept_indicators": ks_dept_inds,
            # Плоский список «отдел × показатель» — по одному элементу на диаграмму.
            # Каждая диаграмма содержит массив 12 месячных точек {plan, fact}.
            "charts": dept_charts,
        }
    except Exception:
        logger.exception("KS-RAZVITIE: failed to load plans")

    tablitsy: dict = {}

    if active_dealers_report:
        ad_rep = active_dealers_report
        dd = ad_rep.get("dealer_detection") or {}
        tablitsy["KD-T-ACTIVE-DEALERS"] = {
            "kpi_id": "KD-T-ACTIVE-DEALERS",
            "name": "Действующие дилеры (12 мес.)",
            "periodicity": "скользящий год",
            "description": (
                "Уникальные дилеры с оплатой и/или отгрузкой по заказам клиента "
                "за последние 12 месяцев от даты формирования."
            ),
            "report_as_of": ad_rep.get("as_of"),
            "period_from": ad_rep.get("period_from"),
            "period_to": ad_rep.get("period_to"),
            "dealer_rule_ru": dd.get("description_ru"),
            "dealer_rule_method": dd.get("method"),
            "active_dealers_count": ad_rep.get("active_dealers_count", 0),
            "columns": [
                "Дилер",
                "Код",
                "Идентификатор (Ref_Key)",
                "Последняя оплата",
                "Сумма оплат",
                "Последняя отгрузка",
                "Сумма отгрузок",
                "Основание",
            ],
            "rows": [
                {
                    # Поля для универсального грида (api.js tableRowHasDisplayableData)
                    "name": r.get("dealer_name"),
                    "code": r.get("dealer_code"),
                    "partner": r.get("dealer_name"),
                    "fact": round(
                        float(r.get("payments_sum") or 0) + float(r.get("shipments_sum") or 0),
                        2,
                    ),
                    "comment": (
                        f"Основание: {r.get('basis')}; "
                        f"оплаты {r.get('payments_sum')} (последняя {r.get('last_payment_date')}); "
                        f"отгрузки {r.get('shipments_sum')} (последняя {r.get('last_shipment_date')})"
                    ),
                    "Дилер": r.get("dealer_name"),
                    "Код": r.get("dealer_code"),
                    "Идентификатор (Ref_Key)": r.get("dealer_ref"),
                    "Последняя оплата": r.get("last_payment_date"),
                    "Сумма оплат": r.get("payments_sum"),
                    "Последняя отгрузка": r.get("last_shipment_date"),
                    "Сумма отгрузок": r.get("shipments_sum"),
                    "Основание": r.get("basis"),
                }
                for r in (ad_rep.get("rows") or [])
            ],
            "segments_used": dd.get("segments"),
            "warnings": [dd["warning"]] if dd.get("warning") else [],
        }

    try:
        tablitsy.update(_build_claims_table(ref_y, series_m, dept_guid=dept_guid))
    except Exception:
        pass

    try:
        tablitsy.update(_build_lawsuits_table(ref_y, series_m, dept_guid=dept_guid))
    except Exception:
        pass

    try:
        tablitsy["KD-T-OVERDUE"] = _build_overdue_table(
            ref_y, series_m, dept_guid=dept_guid,
        )
    except Exception:
        pass

    if dept_guid and str(dept_guid).lower() == DEALER_SALES_DEPT.lower():
        try:
            odp_ref = odp_excel_breakdown.load_odp_reference_table(ref_y, ref_m)
            if odp_ref:

                def _tile_fact(kpi: str) -> float | None:
                    td = tiles_data.get(kpi) or {}
                    lm = td.get("last_full_month_row") or {}
                    v = lm.get("fact")
                    return float(v) if v is not None else None

                odp_ref = kpi_reconciliation.attach_dashboard_vs_reference(
                    odp_ref,
                    _tile_fact("KD-M3"),
                    _tile_fact("KD-M1"),
                    _tile_fact("KD-M2"),
                )
                tablitsy[odp_ref["kpi_id"]] = odp_ref
        except Exception:
            logger.exception("ODP-T-REFERENCE: не удалось загрузить выгрузку из temp")

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

    payload = {
        "month": series_m,
        "year": ref_y,
        "kpi_ref_month": ref_m,
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": grafiki,
        "Таблицы": tablitsy,
    }
    _save_payload_cache(ref_y, ref_m, dept_guid, payload)
    return payload
