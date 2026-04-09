"""
Бюджет подразделения в пределах лимита (*-M3-1).

Для ОВЭД показатель по сути **квартальный**, фактические цифры могут быть только за отдельные
месяцы (например, только март). В API:
- в `months` попадают **только месяцы с данными в текущем квартале** (до последнего полного месяца),
  без «пустых» января/февраля — иначе клиенты нередко усредняют ряд и получают ~71% вместо ~7%;
- `ytd.kpi_pct` — **среднее** `kpi_pct` по этим месяцам квартала (один месяц → одно число);
- `ytd.total_plan` / `total_fact` — за **последний полный месяц**, если по нему есть данные.

План и факт в конфиге — **уже в процентах** (например план **1,8%**, факт **25,0%**), не рубли.

kpi_pct по месяцу: план / факт × 100 (для 1,8 и 25 → ≈ 7,2% — отдельный KPI-%, не сумма процентов).
"""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_month

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

# Ключ = kpi_storage_key (как в kpi_data / commercial_tiles).
# plan_by_month / fact_by_month — значения в % (1.8 = 1,8%; 25.0 = 25,0%).
# Опционально kpi_pct_override — явный % KPI на плитке (например при факте 0, когда план/факт не считают).
# Указывайте только те (год, месяц), по которым реально есть план/факт.
DEPT_BUDGET_M3_CONFIG: dict[str, dict] = {
    "ОВЭД": {
        "plan_by_month": {
            (2026, 3): 1.8,  # 1,8%
        },
        "fact_by_month": {
            (2026, 3): 25.0,  # 25,0%
        },
    },
    "Отдел по работе с ПАО «Газпром»": {
        "plan_by_month": {
            (2026, 3): 0.8,  # 0,8%
        },
        "fact_by_month": {
            (2026, 3): 0.0,
        },
        "kpi_pct_override": 100.0,
    },
}


def _kpi_pct(plan: float, fact: float) -> float:
    if fact == 0:
        return 0.0
    return round(plan / fact * 100, 2)


def _quarter_start_month(month: int) -> int:
    return (month - 1) // 3 * 3 + 1


def get_dept_budget_m3_ytd(dept_key: str) -> dict | None:
    cfg = DEPT_BUDGET_M3_CONFIG.get(dept_key)
    if cfg is None:
        return None

    plan_map: dict[tuple[int, int], float] = cfg["plan_by_month"]
    fact_map: dict[tuple[int, int], float] = cfg["fact_by_month"]

    today = date.today()
    ref_y, ref_m = last_full_month(today)
    if ref_y == today.year:
        pairs = [(today.year, mm) for mm in range(1, ref_m + 1)]
    else:
        pairs = [(ref_y, ref_m)]

    months_out: list[dict] = []
    ref_row: dict | None = None

    for y, m in pairs:
        plan = plan_map.get((y, m))
        fact = fact_map.get((y, m))
        has_data = plan is not None and fact is not None
        if has_data:
            ovr = cfg.get("kpi_pct_override")
            kpi = float(ovr) if ovr is not None else _kpi_pct(plan, fact)
        else:
            kpi = None

        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": kpi,
            "has_data": has_data,
            **({"values_unit": "%"} if has_data else {}),
        }
        months_out.append(row)
        if y == ref_y and m == ref_m:
            ref_row = row

    q0 = _quarter_start_month(ref_m)
    q_end = min(ref_m, q0 + 2)

    quarter_with_data = [
        r
        for r in months_out
        if r["year"] == ref_y and q0 <= r["month"] <= q_end and r.get("has_data")
    ]
    quarter_with_data.sort(key=lambda r: r["month"])

    # В ответ только месяцы квартала, по которым есть цифры (без «пустышек»).
    months_public = [dict(r) for r in quarter_with_data]

    if quarter_with_data:
        ytd_pct = round(
            sum(r["kpi_pct"] for r in quarter_with_data) / len(quarter_with_data),
            2,
        )
        n_kpi = len(quarter_with_data)
    else:
        ytd_pct = None
        n_kpi = 0

    if ref_row and ref_row.get("has_data"):
        total_plan = ref_row["plan"]
        total_fact = ref_row["fact"]
    elif quarter_with_data:
        last_q = max(quarter_with_data, key=lambda r: r["month"])
        total_plan = last_q["plan"]
        total_fact = last_q["fact"]
    else:
        total_plan = None
        total_fact = None

    last_full_month_row = dict(ref_row) if ref_row and ref_row.get("has_data") else None

    return {
        "year": ref_y,
        "months": months_public,
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
            "months_total": len(quarter_with_data) if quarter_with_data else 0,
            **({"values_unit": "%"} if total_plan is not None else {}),
        },
    }
