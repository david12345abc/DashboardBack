"""
Сборка ответа get_kpi для «Коммерческий директор» и дочерних отделов:
10 ежемесячных KPI-плиток, 3 графика, таблица претензий.

Плитки:
  KD-M1  Деньги (План/Факт)
  KD-M2  Отгрузки (План/Факт)
  KD-M3  Договоры (План/Факт)
  KD-M4  ДЗ Факт на дату (Факт на дату)
  KD-M5  Просроч. ДЗ (Факт/лимит)
  KD-M6  Валовая прибыль (План/Факт)
  KD-M7  Расходы (Факт/лимит)
  KD-M8  ФОТ (Факт/лимит)
  KD-M9  Скидка / МЦР (Факт/норма)
  KD-M10 ТКП в SLA (Факт/норма)

Графики:
  KD-C1  Линейный: по месяцам Деньги, Отгрузки, Договоры (факт)
  KD-C2  Круговые: 5 диаграмм (Дилеры, Страны, Газпром, Холдинги, БМИ)
  KD-C3  Столбчатый: KPI за месяц (Деньги, Отгрузки, Договоры, Валовая прибыль)

Таблица:
  Претензии (Catalog_Претензии) за выбранный месяц — логика export_claims.py
"""
from __future__ import annotations

import random
from datetime import date

from . import valovaya_pribyl
from .kpi_periods import last_full_month

MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

LOWER_IS_BETTER_IDS = frozenset({'KD-M4', 'KD-M5', 'KD-M7', 'KD-M8', 'KD-M9'})
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
    """Возвращает (пары месяцев, ref_year, ref_month)."""
    today = date.today()
    ref_y, ref_m = last_full_month(today)
    if ref_y == today.year:
        pairs = [(today.year, mm) for mm in range(1, ref_m + 1)]
    else:
        pairs = [(ref_y, ref_m)]
    return pairs, ref_y, ref_m


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
            "has_data": True,
        })
    return result


def _get_tile_data(kpi_id: str, pairs: list[tuple[int, int]],
                   ref_y: int, ref_m: int) -> dict:
    """Получить данные для одной плитки.
    Для KD-M6 (Валовая прибыль) — из valovaya_pribyl, остальные — синтетика.
    """
    if kpi_id == 'KD-M6':
        vp = valovaya_pribyl.get_vp_ytd()
        return {
            'monthly_data': vp.get('months_calendar') or vp['months'],
            'last_full_month_row': vp.get('last_full_month_row'),
            'ytd': vp['ytd'],
            'kpi_period': vp.get('kpi_period'),
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
    """KD-C1: линейный график — Деньги, Отгрузки, Договоры (только факт по месяцам)."""
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
                "fact": row.get("fact"),
            })
        series.append({
            "kpi_id": kid,
            "name": kpi_meta.get("name", kid),
            "chart_type": "line_fact_monthly",
            "chart_type_label": f"Факт по месяцам: {kpi_meta.get('name', kid)}",
            "points": points,
        })

    return {
        "kpi_id": "KD-C1",
        "name": meta.get("name", "Динамика: Деньги, Отгрузки, Договоры"),
        "periodicity": "ежемесячно",
        "chart_type": meta.get("chart_type", "multi_line_fact_monthly"),
        "chart_type_label": "Линейный тренд по месяцам (факт)",
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


def _build_claims_table(ref_y: int, ref_m: int) -> dict:
    """Таблица претензий за выбранный месяц — логика export_claims.py, но в JSON."""
    from .komdir_claims import fetch_claims_for_month

    rows = fetch_claims_for_month(ref_y, ref_m)
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


def build_komdir_payload(kpi_list: list[dict],
                         month: int | None = None,
                         year: int | None = None) -> dict:
    """Полный payload для ответа API коммерческого директора."""
    by_id = {k["kpi_id"]: k for k in kpi_list}

    if month and year:
        ref_y, ref_m = year, month
        pairs = [(year, mm) for mm in range(1, month + 1)]
    else:
        pairs, ref_y, ref_m = _get_monthly_pairs()

    tile_ids = [
        'KD-M1', 'KD-M2', 'KD-M3', 'KD-M4', 'KD-M5',
        'KD-M6', 'KD-M7', 'KD-M8', 'KD-M9', 'KD-M10',
    ]

    tiles_data: dict[str, dict] = {}
    for kid in tile_ids:
        tiles_data[kid] = _get_tile_data(kid, pairs, ref_y, ref_m)

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
        "KD-C2": _build_pie_charts(ref_y, ref_m),
        "KD-C3": _build_bar_chart(by_id, tiles_data, ref_y, ref_m),
    }

    try:
        tablitsy = _build_claims_table(ref_y, ref_m)
    except Exception:
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
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": grafiki,
        "Таблицы": tablitsy,
    }
