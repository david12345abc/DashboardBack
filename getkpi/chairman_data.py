"""
Помесячные данные для дашборда «Председатель совета директоров».

Каждая функция get_fnd_tN() возвращает dict в стандартном формате:
  {months, last_full_month_row, ytd, kpi_period}
"""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_month, vp_months_for_api

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _kpi_pct(fact: float, plan: float) -> float | None:
    if plan is None or plan == 0 or fact is None:
        return None
    return round(fact / plan * 100, 2)


def _make_period(ref_y: int, ref_m: int) -> dict:
    return {
        "type": "last_full_month",
        "year": ref_y,
        "month": ref_m,
        "month_name": MONTH_NAMES[ref_m],
    }


def _build_result(
    months: list[dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    ref_row = None
    for r in months:
        if r["year"] == ref_y and r["month"] == ref_m:
            ref_row = r
            break

    with_data = [r for r in months if r.get("has_data")]
    last_full = dict(ref_row) if ref_row and ref_row.get("has_data") else None

    if with_data:
        plans = [r["plan"] for r in with_data if r.get("plan") is not None]
        facts = [r["fact"] for r in with_data if r.get("fact") is not None]
        kpis = [r["kpi_pct"] for r in with_data if r.get("kpi_pct") is not None]
        ytd_pct = round(sum(kpis) / len(kpis), 2) if kpis else None
    else:
        plans, facts = [], []
        ytd_pct = None

    return {
        "year": ref_y,
        "months": months,
        "last_full_month_row": last_full,
        "kpi_period": _make_period(ref_y, ref_m),
        "ytd": {
            "total_plan": sum(plans) if plans else None,
            "total_fact": sum(facts) if facts else None,
            "kpi_pct": ytd_pct,
            "months_with_data": len(with_data),
            "months_total": len(months),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# FND-T1  Выручка / план-факт  (план + факт за каждый месяц)
# ═══════════════════════════════════════════════════════════════════════════
_T1_PLAN: dict[tuple[int, int], float] = {
    (2026, 1): 201_300_000,
    (2026, 2): 201_300_000,
    (2026, 3): 201_300_000,
}

_T1_FACT: dict[tuple[int, int], float] = {
    (2026, 1): 185_420_000,
    (2026, 2): 210_750_000,
    (2026, 3): 198_600_000,
}


def get_fnd_t1() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        plan = _T1_PLAN.get((y, m))
        fact = _T1_FACT.get((y, m))
        has = plan is not None and fact is not None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has else None,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T2  Чистая прибыль / план-факт  (план + факт за каждый месяц)
# ═══════════════════════════════════════════════════════════════════════════
_T2_PLAN: dict[tuple[int, int], float] = {
    (2026, 1): 27_800_000,
    (2026, 2): 27_800_000,
    (2026, 3): 27_800_000,
}

_T2_FACT: dict[tuple[int, int], float] = {
    (2026, 1): 22_150_000,
    (2026, 2): 31_420_000,
    (2026, 3): 26_980_000,
}


def get_fnd_t2() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        plan = _T2_PLAN.get((y, m))
        fact = _T2_FACT.get((y, m))
        has = plan is not None and fact is not None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has else None,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T3  Текущая ликвидность  (только факт за каждый месяц, без плана)
# ═══════════════════════════════════════════════════════════════════════════
_T3_FACT: dict[tuple[int, int], float] = {
    (2026, 1): 1.85,
    (2026, 2): 1.72,
    (2026, 3): 1.91,
}


def get_fnd_t3() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        fact = _T3_FACT.get((y, m))
        has = fact is not None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": None, "fact": fact,
            "kpi_pct": fact if has else None,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T4  Своевременная отгрузка  (одно число — среднее по всем месяцам)
# ═══════════════════════════════════════════════════════════════════════════
_T4_FACT: dict[tuple[int, int], float] = {
    (2026, 1): 94.2,
    (2026, 2): 96.8,
    (2026, 3): 93.5,
}


def get_fnd_t4() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)

    values: list[float] = []
    for y, m in tuples:
        val = _T4_FACT.get((y, m))
        if val is not None:
            values.append(val)

    avg = round(sum(values) / len(values), 2) if values else None

    row = {
        "month": ref_m, "year": ref_y, "month_name": MONTH_NAMES[ref_m],
        "plan": None, "fact": avg,
        "kpi_pct": avg,
        "has_data": avg is not None,
        "aggregation": "avg",
        "source_months": len(values),
    }

    return _build_result([row], ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T5  Качество: рекламации и САРА  (два числа за каждый месяц)
# ═══════════════════════════════════════════════════════════════════════════
_T5_DATA: dict[tuple[int, int], tuple[int, int]] = {
    (2026, 1): (3, 1),
    (2026, 2): (2, 0),
    (2026, 3): (4, 2),
}


def get_fnd_t5() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        pair = _T5_DATA.get((y, m))
        has = pair is not None
        reclamations = pair[0] if has else None
        capa_overdue = pair[1] if has else None
        total = (reclamations or 0) + (capa_overdue or 0) if has else None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": None,
            "fact": total,
            "reclamations": reclamations,
            "capa_overdue": capa_overdue,
            "kpi_pct": total,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T7  Дебиторская задолженность  (план + факт за каждый месяц)
# ═══════════════════════════════════════════════════════════════════════════
_T7_PLAN: dict[tuple[int, int], float] = {
    (2026, 1): 120_000_000,
    (2026, 2): 120_000_000,
    (2026, 3): 120_000_000,
}

_T7_FACT: dict[tuple[int, int], float] = {
    (2026, 1): 98_500_000,
    (2026, 2): 115_200_000,
    (2026, 3): 132_400_000,
}


def get_fnd_t7() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        plan = _T7_PLAN.get((y, m))
        fact = _T7_FACT.get((y, m))
        has = plan is not None and fact is not None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has else None,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T9  Выпуск / план-факт  (два числа за каждый месяц)
# ═══════════════════════════════════════════════════════════════════════════
_T9_PLAN: dict[tuple[int, int], float] = {
    (2026, 1): 150_000_000,
    (2026, 2): 150_000_000,
    (2026, 3): 150_000_000,
}

_T9_FACT: dict[tuple[int, int], float] = {
    (2026, 1): 142_300_000,
    (2026, 2): 155_800_000,
    (2026, 3): 148_900_000,
}


def get_fnd_t9() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        plan = _T9_PLAN.get((y, m))
        fact = _T9_FACT.get((y, m))
        has = plan is not None and fact is not None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has else None,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# ═══════════════════════════════════════════════════════════════════════════
# FND-T10  Красные поручения  (одно число за каждый месяц)
# ═══════════════════════════════════════════════════════════════════════════
_T10_FACT: dict[tuple[int, int], int] = {
    (2026, 1): 3,
    (2026, 2): 1,
    (2026, 3): 2,
}


def get_fnd_t10() -> dict:
    today = date.today()
    tuples, (ref_y, ref_m), _ = vp_months_for_api(today)
    rows: list[dict] = []
    for y, m in tuples:
        fact = _T10_FACT.get((y, m))
        has = fact is not None
        rows.append({
            "month": m, "year": y, "month_name": MONTH_NAMES[m],
            "plan": None, "fact": fact,
            "kpi_pct": fact if has else None,
            "has_data": has,
        })
    return _build_result(rows, ref_y, ref_m)


# Маппинг kpi_id → функция
FND_HANDLERS: dict[str, callable] = {
    "FND-T1": get_fnd_t1,
    "FND-T2": get_fnd_t2,
    "FND-T3": get_fnd_t3,
    "FND-T4": get_fnd_t4,
    "FND-T5": get_fnd_t5,
    "FND-T7": get_fnd_t7,
    "FND-T9": get_fnd_t9,
    "FND-T10": get_fnd_t10,
}


def is_chairman_kpi(kpi_id: str) -> bool:
    return kpi_id in FND_HANDLERS


def get_chairman_kpi_data(kpi_id: str) -> dict | None:
    handler = FND_HANDLERS.get(kpi_id)
    if handler is None:
        return None
    return handler()
