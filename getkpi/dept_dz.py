"""
Дебиторская задолженность по подразделениям (ОТД-M2-2, КОМ-M2-2 и т.п.).

Расчёт аналогичен KD-M2: KPI = факт / план × 100.
Цвет плитки в API: < 100 % → зелёный, 100–110 % → жёлтый, > 110 % → красный (см. views._rag_dz_lower_better).

Конфигурация — словарь DEPT_DZ_CONFIG:
  ключ = название подразделения (как в kpi_data.json / commercial_tiles.py),
  значение = {plan_per_month, fact_data}.
Когда появятся новые подразделения или месяцы — просто добавить записи.
"""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_month

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

DZ_KPI_IDS = frozenset({
    "ОТД-M2-2",
    "КОМ-M2-2",
    "ОВЭ-M2-2",
    "ОПЭ-M2-2",
})

DEPT_DZ_CONFIG: dict[str, dict] = {
    "Отдел по работе с ПАО «Газпром»": {
        "plan_per_month": 170_700_000,
        "fact_data": {
            (2026, 1): 171_809_977,
            (2026, 2): 132_882_228,
            (2026, 3): 111_798_353,
        },
    },
    "Отдел продаж БМИ": {
        "plan_per_month": 278_500_000,
        "fact_data": {
            (2026, 1): 293_477_993,
            (2026, 2): 125_915_343,
            (2026, 3): 95_294_032,
        },
    },
    # Ключи = kpi_storage_key из commercial_kpi_key (как в kpi_data.json).
    "ОВЭД": {
        "plan_per_month": 65_300_000,
        "fact_data": {
            (2026, 1): 64_849_081,
            (2026, 2): 102_188_156,
            (2026, 3): 91_198_618,
        },
    },
    "Отдел дилерских продаж": {
        "plan_per_month": 25_600_000,
        "fact_data": {
            (2026, 1): 12_099_654,
            (2026, 2): 11_286_834,
            (2026, 3): 4_408_874,
        },
    },
    "ОПЭОиУ": {
        "plan_per_month": 3_800_000,
        "fact_data": {
            (2026, 1): 1_048_772,
            (2026, 2): 1_583_834,
            (2026, 3): 4_075_739,
        },
    },
    "Отдел по работе с ключевыми клиентами": {
        "plan_per_month": 34_600_000,
        "fact_data": {
            (2026, 1): 6_446_999,
            (2026, 2): 24_593_630,
            (2026, 3): 27_078_288,
        },
    },
}


def _kpi_pct(fact: float, plan: float) -> float:
    if plan == 0:
        return 0.0
    return round(fact / plan * 100, 2)


def is_dz_kpi(kpi_id: str) -> bool:
    return kpi_id in DZ_KPI_IDS


def get_dept_dz_ytd(dept_key: str) -> dict | None:
    """Возвращает помесячные данные ДЗ для подразделения или None, если конфига нет."""
    cfg = DEPT_DZ_CONFIG.get(dept_key)
    if cfg is None:
        return None

    plan = cfg["plan_per_month"]
    fact_map = cfg["fact_data"]

    today = date.today()
    ref_y, ref_m = last_full_month(today)
    if ref_y == today.year:
        pairs = [(today.year, mm) for mm in range(1, ref_m + 1)]
    else:
        pairs = [(ref_y, ref_m)]

    months_out: list[dict] = []
    ref_row: dict | None = None

    for y, m in pairs:
        fact = fact_map.get((y, m))
        has_data = fact is not None
        kpi = _kpi_pct(fact, plan) if has_data else None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
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
        total_plan = plan
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
