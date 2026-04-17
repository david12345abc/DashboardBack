"""
Дашборд «Председатель совета директоров».

Полный payload: Плитки (11 + AVG), Графики (3), Таблицы.
Формат ответа идентичен komdir_dashboard.build_komdir_payload.
"""
from __future__ import annotations

from datetime import date

from . import (
    cache_manager,
    calc_otgruzki_fact,
    calc_plan,
    calc_shipment_share_bmi_gazprom,
    calc_tenders_bmi,
)
from .kpi_periods import last_full_month
from .komdir_dashboard import (
    MONTH_NAMES_RU,
    _build_line_chart,
    _get_monthly_pairs,
    _get_tile_data as _komdir_get_tile_data,
    _series_through_month,
    _tile_rag,
)

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

# Блок «Председатель / коммерция» (плитки MRK-*)
CHAIRMAN_BLOCK_COMMERCE = "commerce"
CHAIRMAN_BLOCK_MY_DASHBOARD = "my_dashboard"

# Реестр значений query-параметра `for` (порядок — для UI; дополняйте новыми dict).
CHAIRMAN_FOR_BLOCKS: tuple[dict[str, str | tuple[str, ...]], ...] = (
    {
        "id": CHAIRMAN_BLOCK_MY_DASHBOARD,
        "label": "",
        "aliases": (
            "my_dashboard",
            "mydashboard",
            "dashboard",
            "мой_дашборд",
            "мойдашборд",
        ),
    },
    {
        "id": CHAIRMAN_BLOCK_COMMERCE,
        "label": "Коммерческий блок",
        "aliases": ("commerce", "коммерция", "commercial"),
    },
)

_ALIAS_TO_FOR_ID: dict[str, str] = {}
for _blk in CHAIRMAN_FOR_BLOCKS:
    bid = str(_blk["id"])
    _ALIAS_TO_FOR_ID[bid.lower()] = bid
    for _al in _blk["aliases"]:
        _ALIAS_TO_FOR_ID[str(_al).lower()] = bid


# Виртуальный блок ПСД → реальное подразделение, чью «ветку структуры» ПСД видит.
# my_dashboard не маппится (смотрит свою собственную ветку, сам ПСД).
CHAIRMAN_FOR_TARGET_DEPT: dict[str, str] = {
    CHAIRMAN_BLOCK_COMMERCE: "коммерческий директор",
}


def chairman_for_target_department(for_raw: str | None) -> str | None:
    """
    Подразделение, ПО которому ПСД «виртуально» смотрит при заданном for.
    Возвращает None, если for пустой или соответствует my_dashboard (своя ветка).
    """
    if for_raw is None or not str(for_raw).strip():
        return None
    block = normalize_chairman_for_param(for_raw)
    return CHAIRMAN_FOR_TARGET_DEPT.get(block)


def get_chairman_for_catalog() -> dict:
    """
    Каталог параметра for для председателя: id, подпись, алиасы, словарь id→label.
    Новые блоки — только дописать в CHAIRMAN_FOR_BLOCKS.
    """
    items = [
        {
            "id": b["id"],
            "label": b["label"],
            "aliases": list(b["aliases"]),
        }
        for b in CHAIRMAN_FOR_BLOCKS
    ]
    labels = {str(b["id"]): b["label"] for b in CHAIRMAN_FOR_BLOCKS}
    return {"items": items, "labels": labels}


COMMERCE_TILE_IDS = [f"MRK-{i:02d}" for i in range(1, 11)]

# Демо-факт на опорный месяц (значения с макета дашборда).
# ВАЖНО: MRK-01/02/03 считаются по данным КомДира, MRK-04 — из _mrk04_shipment_growth_yoy,
# MRK-06 — из calc_shipment_share_bmi_gazprom, MRK-09 — из calc_tenders_bmi.
# Числа ниже остаются только для плиток, у которых ещё нет реальных калькуляторов
# (MRK-05, MRK-07, MRK-08, MRK-10) — fallback-ветка в конце цикла.
_COMMERCE_FACT: dict[str, float | int] = {
    "MRK-05": -42_000_000,
    "MRK-07": 94.7,
    "MRK-08": 98.6,
    "MRK-10": 4,
}

_COMMERCE_RAG: dict[str, str] = {
    "MRK-01": "green", "MRK-02": "green", "MRK-03": "green",
    "MRK-04": "yellow", "MRK-05": "red", "MRK-06": "red",
    "MRK-07": "yellow", "MRK-08": "green", "MRK-09": "red",
    "MRK-10": "red",
}

_COMMERCE_PCT_IDS = frozenset({"MRK-04", "MRK-06", "MRK-07", "MRK-08", "MRK-09"})


def normalize_chairman_for_param(raw: str | None) -> str:
    """Нормализация query-параметра for для председателя (алиасы из CHAIRMAN_FOR_BLOCKS)."""
    if raw is None or not str(raw).strip():
        return CHAIRMAN_BLOCK_MY_DASHBOARD
    s = str(raw).strip().lower().replace(" ", "_").replace("-", "_")
    return _ALIAS_TO_FOR_ID.get(s, CHAIRMAN_BLOCK_MY_DASHBOARD)


def _komdir_commerce_context(
    month: int | None,
    year: int | None,
) -> tuple[int, int, list[tuple[int, int]], int]:
    """Те же ref_y, ref_m, pairs, series_m, что в komdir_dashboard.build_komdir_payload (агрегат комдира)."""
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
    return ref_y, ref_m, pairs, series_m


def _to_int_or_none(x) -> int | None:
    """Приводит рублёвую сумму к целому числу (rub, без масштабирования и «млн»)."""
    if x is None:
        return None
    try:
        return int(round(float(x)))
    except (TypeError, ValueError):
        return None


def _ytd_sum_plan_fact(monthly_data: list[dict]) -> dict[str, float | None]:
    """Накопительно: сумма плана и факта по строкам monthly_data (янв.–опорный месяц)."""
    plans = [r.get("plan") for r in monthly_data if r.get("plan") is not None]
    facts = [r.get("fact") for r in monthly_data if r.get("fact") is not None]
    sp = float(sum(plans)) if plans else None
    sf = float(sum(facts)) if facts else None
    kpi_pct = round(sf / sp * 100, 1) if sp and sf is not None else None
    return {"total_plan": sp, "total_fact": sf, "kpi_pct": kpi_pct}


def _mrk04_shipment_growth_yoy(ref_m: int) -> tuple[float | None, dict]:
    """
    Рост отгрузок 2026 к 2025: отношение факт/план, где
      план = сумма отгрузок за весь 2025,
      факт = сумма отгрузок за весь 2026 (все доступные месяцы до последнего полного).
    Возвращает одну цифру kpi_pct = факт / план * 100 (%).
    Источники и dept_guid=None — те же, что у плитки KD-M2 коммерческого директора.
    """
    today = date.today()
    m_2026 = 12 if today.year > 2026 else max(1, min(12, int(ref_m) if ref_m else today.month))
    o25 = cache_manager.locked_call(
        "otgruzki_2025_12",
        calc_otgruzki_fact.get_otgruzki_monthly,
        year=2025,
        month=12,
        dept_guid=None,
    )
    o26 = cache_manager.locked_call(
        f"otgruzki_2026_{m_2026}",
        calc_otgruzki_fact.get_otgruzki_monthly,
        year=2026,
        month=m_2026,
        dept_guid=None,
    )
    s25 = sum((r.get("fact") or 0) for r in (o25.get("months") or []))
    s26 = sum((r.get("fact") or 0) for r in (o26.get("months") or []))
    detail = {
        "months_2025": 12,
        "months_2026": m_2026,
        "sum_shipments_rub_2025": s25,
        "sum_shipments_rub_2026": s26,
        "label": "Весь 2026 к всему 2025",
    }
    if s25 <= 0:
        return None, detail
    pct = round(s26 / s25 * 100, 1)
    return pct, detail


def _mrk04_rag(growth_pct: float | None) -> str:
    """RAG для факт/план·100 (%): ≥100 — зелёный, 90–99.9 — жёлтый, <90 — красный."""
    if growth_pct is None:
        return "unknown"
    if growth_pct >= 100:
        return "green"
    if growth_pct >= 90:
        return "yellow"
    return "red"


def _mrk09_tenders_bmi(ref_y: int, ref_m: int) -> dict:
    """
    Данные плитки MRK-09 «% выигранных тендеров БМИ».
    Окно всегда «с 01.01 ref_y по сегодня» для текущего года и «весь год» для прошлых —
    независимо от выбранного опорного месяца ref_m (это не помесячная плитка).
    """
    today = date.today()
    if ref_y == today.year:
        cache_key = f"tenders_bmi_{ref_y}_{today.isoformat()}"
        return cache_manager.locked_call(
            cache_key,
            calc_tenders_bmi.get_tenders_bmi,
            year=ref_y,
            month=today.month,
        )
    return cache_manager.locked_call(
        f"tenders_bmi_{ref_y}_12",
        calc_tenders_bmi.get_tenders_bmi,
        year=ref_y,
        month=12,
    )


def _mrk09_rag(pct: float | None) -> str:
    """RAG для MRK-09 по порогам из kpi-справочника: ≥25 — зелёный, 15–24.9 — жёлтый, <15 — красный."""
    if pct is None:
        return "unknown"
    if pct >= 25:
        return "green"
    if pct >= 15:
        return "yellow"
    return "red"


def _mrk06_share_bmi_gazprom(ref_y: int, ref_m: int) -> dict:
    """
    Данные плитки MRK-06 «Доля Газпром + БМИ в отгрузке».
    Окно «с 01.01 ref_y по сегодня» для текущего года, «весь год» — для прошлых
    (плитка не помесячная, поэтому не ограничиваем по ref_m).
    """
    today = date.today()
    if ref_y == today.year:
        return cache_manager.locked_call(
            f"share_bmi_gp_{ref_y}_{today.isoformat()}",
            calc_shipment_share_bmi_gazprom.get_shipment_share_bmi_gazprom,
            year=ref_y,
            month=today.month,
        )
    return cache_manager.locked_call(
        f"share_bmi_gp_{ref_y}_12",
        calc_shipment_share_bmi_gazprom.get_shipment_share_bmi_gazprom,
        year=ref_y,
        month=12,
    )


def _mrk06_rag(pct: float | None) -> str:
    """RAG для MRK-06 (меньше = лучше): ≤70 — зелёный, 70.1–75 — жёлтый, >75 — красный."""
    if pct is None:
        return "unknown"
    if pct <= 70:
        return "green"
    if pct <= 75:
        return "yellow"
    return "red"


def _get_commerce_tile_data(kpi_id: str, months: list[int], ref_y: int, ref_m: int) -> dict:
    fact = _COMMERCE_FACT.get(kpi_id)
    rows = []
    for m in months:
        rows.append({
            "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
            "plan": None, "fact": fact,
            "kpi_pct": fact if kpi_id in _COMMERCE_PCT_IDS else None,
            "has_data": fact is not None,
        })
    ref_row = None
    for r in rows:
        if r["year"] == ref_y and r["month"] == ref_m:
            ref_row = r
            break
    with_data = [r for r in rows if r.get("has_data")]
    facts = [r["fact"] for r in with_data if r.get("fact") is not None]
    return {
        "monthly_data": rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "ytd": {
            "total_plan": None,
            "total_fact": sum(facts) if facts else None,
            "kpi_pct": None,
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


def build_chairman_commerce_payload(
    kpi_list: list[dict],
    month: int | None = None,
    year: int | None = None,
) -> dict:
    """
    Блок «Председатель / коммерция»: MRK-01…03 из тех же данных, что KD-M2/M3/M1 у коммерческого директора;
    на плитках — накопительная сумма план/факт с начала года по опорный месяц; в monthly_data — помесячно для графиков.
    MRK-04 — рост отгрузок (сумма Jan..m 2026 к Jan..m 2025). MRK-05…10 — заглушки.
    """
    by_id = {k["kpi_id"]: k for k in kpi_list}

    ref_y, ref_m, pairs, series_m = _komdir_commerce_context(month, year)
    plans_payload = cache_manager.locked_call(
        f"plans_{ref_y}_{series_m}",
        calc_plan.get_plans_monthly,
        year=ref_y,
        month=series_m,
        dept_guid=None,
    )

    td_m1 = _komdir_get_tile_data(
        "KD-M1", pairs, ref_y, ref_m, series_m,
        dz_payload=None, dept_guid=None, plans_payload=plans_payload,
    )
    td_m2 = _komdir_get_tile_data(
        "KD-M2", pairs, ref_y, ref_m, series_m,
        dz_payload=None, dept_guid=None, plans_payload=plans_payload,
    )
    td_m3 = _komdir_get_tile_data(
        "KD-M3", pairs, ref_y, ref_m, series_m,
        dz_payload=None, dept_guid=None, plans_payload=plans_payload,
    )

    komdir_for_chart = {"KD-M1": td_m1, "KD-M2": td_m2, "KD-M3": td_m3}
    ytd_m1 = _ytd_sum_plan_fact(td_m1.get("monthly_data") or [])
    ytd_m2 = _ytd_sum_plan_fact(td_m2.get("monthly_data") or [])
    ytd_m3 = _ytd_sum_plan_fact(td_m3.get("monthly_data") or [])

    mrk_from_komdir: dict[str, tuple[str, dict, dict]] = {
        "MRK-01": ("KD-M2", td_m2, ytd_m2),
        "MRK-02": ("KD-M3", td_m3, ytd_m3),
        "MRK-03": ("KD-M1", td_m1, ytd_m1),
    }

    ytd_label = (
        f"Накопительно с начала {ref_y} г. "
        f"(янв.–{MONTH_NAMES_RU[ref_m]})"
    )

    plitki_items: list[dict] = []
    months_stub = _month_pairs(ref_y, ref_m)
    tile_ids = [kid for kid in COMMERCE_TILE_IDS if kid in by_id]

    for kid in tile_ids:
        meta = by_id.get(kid)
        if not meta:
            continue

        if kid in mrk_from_komdir:
            kd_id, _td, ytd_agg = mrk_from_komdir[kid]
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": ytd_agg.get("kpi_pct"),
                "color": _tile_rag(kd_id, ytd_agg.get("kpi_pct")),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": _to_int_or_none(ytd_agg.get("total_plan")),
                "fact": _to_int_or_none(ytd_agg.get("total_fact")),
                "has_data": ytd_agg.get("total_fact") is not None,
                "plan_fact_period_label": ytd_label,
                "monthly_data": _td.get("monthly_data"),
            })
            continue

        if kid == "MRK-04":
            growth_pct, growth_detail = _mrk04_shipment_growth_yoy(ref_m)
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": growth_pct,
                "color": _mrk04_rag(growth_pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": _to_int_or_none(growth_detail.get("sum_shipments_rub_2025")),
                "fact": _to_int_or_none(growth_detail.get("sum_shipments_rub_2026")),
                "has_data": growth_pct is not None,
                "plan_fact_period_label": growth_detail.get("label", ""),
                "monthly_data": [],
                "yoy_detail": growth_detail,
            })
            continue

        if kid == "MRK-06":
            share = _mrk06_share_bmi_gazprom(ref_y, ref_m)
            pct = share.get("pct_pair")
            total = share.get("total") or 0
            pair = share.get("pair") or 0
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": pct,
                "color": _mrk06_rag(pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": _to_int_or_none(total),
                "fact": _to_int_or_none(pair),
                "has_data": total > 0,
                "plan_fact_period_label": f"{ref_y} г. (янв.–{MONTH_NAMES_RU[ref_m]})",
                "monthly_data": [],
                "share_detail": {
                    "by_dept": share.get("by_dept") or {},
                    "bmi": share.get("bmi"),
                    "gp": share.get("gp"),
                    "pct_bmi": share.get("pct_bmi"),
                    "pct_gp": share.get("pct_gp"),
                    "period_start": share.get("period_start"),
                    "period_end": share.get("period_end"),
                    "counted_rows": share.get("counted_rows"),
                },
            })
            continue

        if kid == "MRK-09":
            tenders = _mrk09_tenders_bmi(ref_y, ref_m)
            plan_n = int(tenders.get("plan") or 0)
            fact_n = int(tenders.get("fact") or 0)
            pct = tenders.get("pct")
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": pct,
                "color": _mrk09_rag(pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": plan_n,
                "fact": fact_n,
                "has_data": plan_n > 0,
                "plan_fact_period_label": (
                    f"{tenders.get('period_start') or f'{ref_y}-01-01'} — "
                    f"{tenders.get('period_end') or ''}"
                ).rstrip(" —"),
                "monthly_data": [],
                "tenders_detail": {
                    "distribution": tenders.get("distribution") or {},
                    "samples": tenders.get("samples") or [],
                    "year": tenders.get("year"),
                    "month": tenders.get("month"),
                    "period_start": tenders.get("period_start"),
                    "period_end": tenders.get("period_end"),
                },
            })
            continue

        td_stub = _get_commerce_tile_data(kid, months_stub, ref_y, ref_m)
        lm = td_stub.get("last_full_month_row")
        unit = (meta.get("unit") or "").lower()
        is_pct = "%" in unit or "процент" in unit
        fact_val = lm.get("fact") if lm else None
        kpi_pct = float(fact_val) if (is_pct and fact_val is not None) else None
        plan_raw = lm.get("plan") if lm else None
        fact_raw = lm.get("fact") if lm else None
        plitki_items.append({
            "kpi_id": kid,
            "name": meta["name"],
            "goal": meta.get("goal"),
            "kpi_pct": kpi_pct,
            "color": _COMMERCE_RAG.get(kid, "unknown"),
            "period": _period_label(meta),
            "thresholds": _thresholds(meta),
            "formula": meta.get("formula"),
            "unit": meta.get("unit"),
            "source": meta.get("source"),
            "frequency": meta.get("frequency"),
            "plan": (plan_raw if is_pct else _to_int_or_none(plan_raw)),
            "fact": (fact_raw if is_pct else _to_int_or_none(fact_raw)),
            "has_data": lm.get("has_data", True) if lm else False,
            "plan_fact_period_label": f"{MONTH_NAMES[ref_m].capitalize()} {ref_y}",
            "monthly_data": td_stub.get("monthly_data"),
        })

    by_id_chart = {
        "KD-C1": {"name": "Динамика: деньги, отгрузки, договоры"},
        "KD-M1": {"name": (by_id.get("MRK-03") or {}).get("name", "Деньги")},
        "KD-M2": {"name": (by_id.get("MRK-01") or {}).get("name", "Отгрузки")},
        "KD-M3": {"name": (by_id.get("MRK-02") or {}).get("name", "Договоры")},
    }
    chart = _build_line_chart(by_id_chart, komdir_for_chart)
    chart["kpi_id"] = "MRK-C1"
    chart["name"] = "План/факт по месяцам (отгрузки, договоры, деньги)"
    # Порядок серий как у плиток MRK-01 → MRK-02 → MRK-03 (в KD-C1 было M1, M2, M3)
    ser = chart.get("series") or []
    if len(ser) == 3:
        chart["series"] = [ser[1], ser[2], ser[0]]

    return {
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": {"MRK-C1": chart},
        "Таблицы": {},
    }


def build_chairman_payload_by_for(
    kpi_list: list[dict],
    month: int | None = None,
    year: int | None = None,
    for_raw: str | None = None,
) -> tuple[dict, str]:
    """
    Сборка payload председателя по блоку просмотра.
    Возвращает (payload, нормализованный for).
    """
    block = normalize_chairman_for_param(for_raw)
    if block == CHAIRMAN_BLOCK_COMMERCE:
        mkpis = [k for k in kpi_list if str(k.get("kpi_id", "")).startswith("MRK-")]
        return build_chairman_commerce_payload(mkpis, month=month, year=year), block
    return build_chairman_payload(kpi_list, month=month, year=year), block


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
