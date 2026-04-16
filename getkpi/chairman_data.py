"""
Дашборд «Председатель совета директоров».

Полный payload: Плитки (11 + AVG), Графики (3), Таблицы.
Формат ответа идентичен komdir_dashboard.build_komdir_payload.
"""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_month

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

LOWER_IS_BETTER = frozenset({"FND-T7", "FND-T8", "FND-T10"})

# ═══════════════════════════════════════════════════════════════
#  Захардкоженные данные по месяцам
# ═══════════════════════════════════════════════════════════════

# FND-T1  Выручка / план-факт  (план + факт)
_T1_PLAN = {1: 201_300_000, 2: 201_300_000, 3: 201_300_000}
_T1_FACT = {1: 185_420_000, 2: 210_750_000, 3: 198_600_000}

# FND-T2  Чистая прибыль / план-факт  (план + факт)
_T2_PLAN = {1: 27_800_000, 2: 27_800_000, 3: 27_800_000}
_T2_FACT = {1: 22_150_000, 2: 31_420_000, 3: 26_980_000}

# FND-T3  Текущая ликвидность  (только факт)
_T3_FACT = {1: 1.85, 2: 1.72, 3: 1.91}

# FND-T4  Своевременная отгрузка  (среднее по всем месяцам)
_T4_FACT = {1: 94.2, 2: 96.8, 3: 93.5}

# FND-T5  Качество: рекламации и САРА  (reclamations + capa_overdue)
_T5_DATA: dict[int, tuple[int, int]] = {1: (3, 1), 2: (2, 0), 3: (4, 2)}

# FND-T6  Портфель проектов  (синтетика)
_T6_FACT = {1: 100.5, 2: 99.8, 3: 100.1}

# FND-T7  Дебиторская задолженность  (план + факт)
_T7_PLAN = {1: 120_000_000, 2: 120_000_000, 3: 120_000_000}
_T7_FACT = {1: 98_500_000, 2: 115_200_000, 3: 132_400_000}

# FND-T8  Критичные дефициты  (только факт)
_T8_FACT = {1: 1, 2: 0, 3: 2}

# FND-T9  Выпуск / план-факт  (план + факт)
_T9_PLAN = {1: 150_000_000, 2: 150_000_000, 3: 150_000_000}
_T9_FACT = {1: 142_300_000, 2: 155_800_000, 3: 148_900_000}

# FND-T10  Красные поручения  (только факт)
_T10_FACT = {1: 3, 2: 1, 3: 2}


# ═══════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ═══════════════════════════════════════════════════════════════

def _kpi_pct(fact, plan) -> float | None:
    if plan is None or plan == 0 or fact is None:
        return None
    return round(fact / plan * 100, 2)


def _rag(kpi_id: str, pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if kpi_id in LOWER_IS_BETTER:
        if pct < 100:
            return "green"
        if pct <= 110:
            return "yellow"
        return "red"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _rag_higher_better(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _month_pairs(ref_y: int, ref_m: int) -> list[int]:
    today = date.today()
    if ref_y == today.year:
        return list(range(1, ref_m + 1))
    return [ref_m]


def _period_label(kpi: dict) -> str:
    f = (kpi.get("frequency") or "").lower()
    if "квартал" in f:
        return "ежеквартально"
    if "месяц" in f or "ежемесячно" in f:
        return "ежемесячно"
    if "год" in f:
        return "ежегодно"
    return kpi.get("frequency") or ""


def _thresholds(kpi: dict) -> dict:
    return {
        "green": kpi.get("green_threshold"),
        "yellow": kpi.get("yellow_threshold"),
        "red": kpi.get("red_threshold"),
    }


# ═══════════════════════════════════════════════════════════════
#  Генераторы помесячных данных по KPI
# ═══════════════════════════════════════════════════════════════

def _months_plan_fact(plan_dict, fact_dict, months):
    rows = []
    for m in months:
        plan = plan_dict.get(m)
        fact = fact_dict.get(m)
        has = plan is not None and fact is not None
        rows.append({
            "month": m, "year": 2026, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has else None,
            "has_data": has,
        })
    return rows


def _months_fact_only(fact_dict, months):
    rows = []
    for m in months:
        fact = fact_dict.get(m)
        has = fact is not None
        rows.append({
            "month": m, "year": 2026, "month_name": MONTH_NAMES[m],
            "plan": None, "fact": fact,
            "kpi_pct": None,
            "has_data": has,
        })
    return rows


def _get_tile_data(kpi_id: str, months: list[int], ref_y: int, ref_m: int) -> dict:
    """Вернуть monthly_data + ytd + kpi_period для одного KPI."""

    if kpi_id == "FND-T1":
        rows = _months_plan_fact(_T1_PLAN, _T1_FACT, months)
    elif kpi_id == "FND-T2":
        rows = _months_plan_fact(_T2_PLAN, _T2_FACT, months)
    elif kpi_id == "FND-T3":
        rows = _months_fact_only(_T3_FACT, months)
    elif kpi_id == "FND-T4":
        vals = [_T4_FACT[m] for m in months if m in _T4_FACT]
        avg = round(sum(vals) / len(vals), 2) if vals else None
        rows = [{
            "month": ref_m, "year": ref_y, "month_name": MONTH_NAMES[ref_m],
            "plan": None, "fact": avg,
            "kpi_pct": avg, "has_data": avg is not None,
            "aggregation": "avg", "source_months": len(vals),
        }]
    elif kpi_id == "FND-T5":
        rows = []
        for m in months:
            pair = _T5_DATA.get(m)
            has = pair is not None
            rec = pair[0] if has else None
            capa = pair[1] if has else None
            total = (rec or 0) + (capa or 0) if has else None
            rows.append({
                "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
                "plan": None, "fact": total,
                "reclamations": rec, "capa_overdue": capa,
                "kpi_pct": None, "has_data": has,
            })
    elif kpi_id == "FND-T6":
        rows = _months_fact_only(_T6_FACT, months)
    elif kpi_id == "FND-T7":
        rows = _months_plan_fact(_T7_PLAN, _T7_FACT, months)
    elif kpi_id == "FND-T8":
        rows = _months_fact_only(_T8_FACT, months)
    elif kpi_id == "FND-T9":
        rows = _months_plan_fact(_T9_PLAN, _T9_FACT, months)
    elif kpi_id == "FND-T10":
        rows = _months_fact_only(_T10_FACT, months)
    else:
        rows = []

    ref_row = None
    for r in rows:
        if r["year"] == ref_y and r["month"] == ref_m:
            ref_row = r
            break

    with_data = [r for r in rows if r.get("has_data")]
    plans = [r["plan"] for r in with_data if r.get("plan") is not None]
    facts = [r["fact"] for r in with_data if r.get("fact") is not None]
    kpis = [r["kpi_pct"] for r in with_data if r.get("kpi_pct") is not None]
    ytd_pct = round(sum(kpis) / len(kpis), 2) if kpis else None

    return {
        "monthly_data": rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "ytd": {
            "total_plan": sum(plans) if plans else None,
            "total_fact": sum(facts) if facts else None,
            "kpi_pct": ytd_pct,
            "months_with_data": len(with_data),
            "months_total": len(rows),
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
    }


# ═══════════════════════════════════════════════════════════════
#  Графики
# ═══════════════════════════════════════════════════════════════

def _build_chart_c1(by_id: dict, tiles_data: dict) -> dict:
    """FND-C1: Тренд 12 мес — Выручка, EBITDA (чистая прибыль), Выпуск."""
    meta = by_id.get("FND-C1", {})
    series = []
    for kid in ["FND-T1", "FND-T2", "FND-T9"]:
        kpi_meta = by_id.get(kid, {})
        td = tiles_data.get(kid, {})
        monthly = td.get("monthly_data") or []
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
        "kpi_id": "FND-C1",
        "name": meta.get("name", "Тренд 12 месяцев: выручка / EBITDA / валовая маржа"),
        "periodicity": "ежемесячно",
        "chart_type": meta.get("chart_type", "combo_bar_line_monthly"),
        "chart_type_label": "Линейный тренд по месяцам (план/факт)",
        "series": series,
    }


def _build_chart_c2(by_id: dict, tiles_data: dict, ref_y: int, ref_m: int) -> dict:
    """FND-C2: Waterfall EBITDA — разложение отклонения на драйверы."""
    meta = by_id.get("FND-C2", {})
    t2 = tiles_data.get("FND-T2", {})
    lm = t2.get("last_full_month_row") or {}
    plan_val = lm.get("plan", 27_800_000)
    fact_val = lm.get("fact", 26_980_000)
    delta = fact_val - plan_val

    drivers = [
        {"name": "План EBITDA", "value": plan_val, "type": "total"},
        {"name": "Выручка", "value": round(delta * 0.45), "type": "driver"},
        {"name": "Маржа", "value": round(delta * 0.25), "type": "driver"},
        {"name": "Скидки", "value": round(delta * -0.10), "type": "driver"},
        {"name": "Постоянные расходы", "value": round(delta * 0.30), "type": "driver"},
        {"name": "Разовые эффекты", "value": round(delta * 0.10), "type": "driver"},
        {"name": "Факт EBITDA", "value": fact_val, "type": "total"},
    ]
    return {
        "kpi_id": "FND-C2",
        "name": meta.get("name", "Waterfall EBITDA: план → факт"),
        "periodicity": "ежемесячно",
        "chart_type": "waterfall",
        "chart_type_label": "Waterfall EBITDA",
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
        "drivers": drivers,
    }


def _build_chart_c3(by_id: dict, tiles_data: dict, ref_y: int, ref_m: int) -> dict:
    """FND-C3: Heatmap по директорам/контурам."""
    meta = by_id.get("FND-C3", {})
    directors = [
        "Коммерческий директор",
        "Финансовый директор",
        "Технический директор",
        "Директор по производству",
        "Директор по качеству",
        "Директор по персоналу",
    ]
    kpi_columns = ["Выручка", "Прибыль", "Отгрузка", "Качество", "Кадры"]
    heatmap_rows = []
    statuses = ["green", "yellow", "red", "green", "yellow"]
    for i, d in enumerate(directors):
        cells = []
        for j, col in enumerate(kpi_columns):
            idx = (i + j) % len(statuses)
            cells.append({"kpi": col, "status": statuses[idx]})
        worst = "red" if any(c["status"] == "red" for c in cells) else \
                "yellow" if any(c["status"] == "yellow" for c in cells) else "green"
        heatmap_rows.append({"director": d, "overall_status": worst, "cells": cells})

    return {
        "kpi_id": "FND-C3",
        "name": meta.get("name", "Heatmap по директорам / контурам"),
        "periodicity": "ежемесячно",
        "chart_type": "heatmap_rag",
        "chart_type_label": "Heatmap RAG",
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
        "kpi_columns": kpi_columns,
        "rows": heatmap_rows,
    }


# ═══════════════════════════════════════════════════════════════
#  Главная сборка payload
# ═══════════════════════════════════════════════════════════════

TILE_IDS = [
    "FND-T1", "FND-T2", "FND-T3", "FND-T4", "FND-T5",
    "FND-T6", "FND-T7", "FND-T8", "FND-T9", "FND-T10",
]


def build_chairman_payload(
    kpi_list: list[dict],
    month: int | None = None,
    year: int | None = None,
) -> dict:
    by_id = {k["kpi_id"]: k for k in kpi_list}

    if month and year:
        ref_y, ref_m = year, month
    else:
        ref_y, ref_m = last_full_month(date.today())

    months = _month_pairs(ref_y, ref_m)

    tile_ids = [kid for kid in TILE_IDS if kid in by_id]
    tiles_data: dict[str, dict] = {}
    for kid in tile_ids:
        tiles_data[kid] = _get_tile_data(kid, months, ref_y, ref_m)

    plitki_items: list[dict] = []
    numeric_for_avg: list[float] = []

    for kid in tile_ids:
        meta = by_id.get(kid)
        if not meta:
            continue
        td = tiles_data[kid]
        pct = td["ytd"].get("kpi_pct")
        if pct is not None:
            pct = float(pct)
        color = _rag(kid, pct)
        if pct is not None:
            numeric_for_avg.append(pct)

        lm = td.get("last_full_month_row")
        plitki_items.append({
            "kpi_id": kid,
            "name": meta["name"],
            "kpi_pct": pct,
            "color": color,
            "period": _period_label(meta),
            "thresholds": _thresholds(meta),
            "formula": meta.get("formula"),
            "unit": meta.get("unit"),
            "source": meta.get("source"),
            "frequency": meta.get("frequency"),
            "plan": lm.get("plan") if lm else None,
            "fact": lm.get("fact") if lm else None,
            "has_data": lm.get("has_data", True) if lm else False,
            "plan_fact_period_label": f"{MONTH_NAMES[ref_m].capitalize()} {ref_y}",
            "monthly_data": td.get("monthly_data"),
        })

    avg_pct = round(sum(numeric_for_avg) / len(numeric_for_avg), 1) if numeric_for_avg else None
    plitki_items.append({
        "kpi_id": "FND-AVG",
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
        "FND-C1": _build_chart_c1(by_id, tiles_data),
        "FND-C2": _build_chart_c2(by_id, tiles_data, ref_y, ref_m),
        "FND-C3": _build_chart_c3(by_id, tiles_data, ref_y, ref_m),
    }

    return {
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": grafiki,
        "Таблицы": {},
    }


def is_chairman_department(dept: str) -> bool:
    d = dept.strip().lower()
    return "председатель" in d and "совет" in d and "директор" in d
