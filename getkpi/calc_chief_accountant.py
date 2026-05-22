"""KPI payload builders for the Chief Accountant dashboard."""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_month, last_full_quarter

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

CHIEF_ACCOUNTANT_TILE_ORDER = (
    "GB-M1",
    "GB-M2",
    "GB-M3.B",
    "GB-M3.F",
    "GB-Q1",
    "GB-Q2",
)

BASE_DEFINITION = {
    "block": "плитка",
    "perspective": "Операции",
    "unit": "%",
    "monthly_target": None,
    "quarterly_target": None,
    "yearly_target": None,
    "green_threshold": "≥100%",
    "yellow_threshold": "90–99,9%",
    "red_threshold": "<90%",
}

KPI_DEFINITIONS = {
    "GB-M1": {
        **BASE_DEFINITION,
        "kpi_id": "GB-M1",
        "name": "Своевременное закрытие бухгалтерского периода",
        "frequency": "Ежемесячно",
        "goal": "Обеспечить закрытие месяца в календарный срок без срыва управленческой отчётности",
        "formula": "Закрытие периода в срок / План закрытия периода × 100%",
        "source": "Главный бухгалтер",
        "weight_pct": 20.0,
    },
    "GB-M2": {
        **BASE_DEFINITION,
        "kpi_id": "GB-M2",
        "name": "Качество бухгалтерских регистров и отсутствие существенных исправлений",
        "frequency": "Ежемесячно",
        "goal": "Снизить риски ошибок учёта и искажения финансового результата",
        "formula": "Регистры без существенных исправлений / Все проверенные регистры × 100%",
        "source": "Главный бухгалтер",
        "weight_pct": 20.0,
    },
    "GB-M3.B": {
        **BASE_DEFINITION,
        "kpi_id": "GB-M3.B",
        "name": "Бюджет бухгалтерии в пределах лимита",
        "frequency": "Ежемесячно",
        "goal": "Контролировать бюджет бухгалтерии в пределах лимита",
        "formula": "Факт затрат / Плановый лимит × 100%",
        "source": "БДР / 1С",
        "green_threshold": "≤100%",
        "yellow_threshold": "100–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
    },
    "GB-M3.F": {
        **BASE_DEFINITION,
        "kpi_id": "GB-M3.F",
        "name": "ФОТ бухгалтерии в пределах лимита",
        "frequency": "Ежемесячно",
        "goal": "Контролировать ФОТ бухгалтерии в пределах лимита",
        "formula": "Факт ФОТ / Плановый лимит ФОТ × 100%",
        "source": "1С / ЗУП / HRIS",
        "green_threshold": "≤100%",
        "yellow_threshold": "100–110%",
        "red_threshold": ">110%",
        "weight_pct": 10.0,
    },
    "GB-Q1": {
        **BASE_DEFINITION,
        "kpi_id": "GB-Q1",
        "name": "Повышение зрелости учётного процесса / устранение повторных ошибок",
        "frequency": "Ежеквартально",
        "goal": "Повысить устойчивость бухгалтерского контура",
        "formula": "Устранённые повторные ошибки / План мероприятий × 100%",
        "source": "Главный бухгалтер",
        "weight_pct": 20.0,
    },
    "GB-Q2": {
        **BASE_DEFINITION,
        "kpi_id": "GB-Q2",
        "name": "Текучесть персонала бухгалтерии",
        "frequency": "Ежеквартально",
        "goal": "Сохранить устойчивость учёта и преемственность закрытия",
        "formula": "Уволено / Штатная численность бухгалтерии × 100%",
        "source": "HR + Главный бухгалтер",
        "green_threshold": "<90%",
        "yellow_threshold": "90–100%",
        "red_threshold": ">100%",
        "weight_pct": 20.0,
    },
}

MONTHLY_FACT_BY_ID = {
    "GB-M1": {1: 100.0, 2: 100.0, 3: 100.0, 4: 98.0, 5: 100.0, 6: 100.0, 7: 100.0, 8: 100.0, 9: 100.0, 10: 100.0, 11: 100.0, 12: 100.0},
    "GB-M2": {1: 97.0, 2: 99.0, 3: 98.0, 4: 100.0, 5: 99.0, 6: 100.0, 7: 98.0, 8: 99.0, 9: 100.0, 10: 99.0, 11: 100.0, 12: 100.0},
    "GB-M3.B": {1: 92.0, 2: 96.0, 3: 101.0, 4: 98.0, 5: 95.0, 6: 99.0, 7: 97.0, 8: 100.0, 9: 98.0, 10: 96.0, 11: 99.0, 12: 100.0},
    "GB-M3.F": {1: 94.0, 2: 97.0, 3: 99.0, 4: 100.0, 5: 98.0, 6: 97.0, 7: 99.0, 8: 100.0, 9: 101.0, 10: 99.0, 11: 100.0, 12: 100.0},
}

QUARTERLY_FACT_BY_ID = {
    "GB-Q1": {1: 95.0, 2: 100.0, 3: 100.0, 4: 100.0},
    "GB-Q2": {1: 4.0, 2: 5.0, 3: 4.5, 4: 4.0},
}


def normalize_kpi_definitions(department: str, rows: list[dict]) -> list[dict]:
    source_by_id = {
        str(row.get("kpi_id") or ""): dict(row)
        for row in rows
        if str(row.get("kpi_id") or "").startswith("GB-")
    }
    source_m3 = source_by_id.get("GB-M3") or {}
    result: list[dict] = []
    for pos, kpi_id in enumerate(CHIEF_ACCOUNTANT_TILE_ORDER):
        fallback = dict(KPI_DEFINITIONS[kpi_id])
        row = dict(source_by_id.get(kpi_id) or fallback)
        if kpi_id in {"GB-M3.B", "GB-M3.F"} and source_m3:
            row = {**source_m3, **fallback, **source_by_id.get(kpi_id, {})}
        row["department"] = department
        row["kpi_id"] = kpi_id
        row["block"] = "плитка"
        row["position"] = pos
        result.append(row)
    return result


def _kpi_pct(fact: float | None, plan: float | None) -> float | None:
    if fact is None or plan is None or plan == 0:
        return None
    return round(float(fact) / float(plan) * 100, 1)


def _months_until(ref_y: int, ref_m: int) -> list[tuple[int, int]]:
    current_year = date.today().year
    if ref_y == current_year:
        return [(ref_y, m) for m in range(1, ref_m + 1)]
    return [(ref_y, ref_m)]


def _monthly_payload(kpi_id: str, year: int | None, month: int | None) -> dict:
    if year and month:
        ref_y, ref_m = int(year), max(1, min(12, int(month)))
    else:
        ref_y, ref_m = last_full_month(date.today())

    plan = 100.0
    fact_by_month = MONTHLY_FACT_BY_ID.get(kpi_id, {})
    rows = []
    for y, m in _months_until(ref_y, ref_m):
        fact = fact_by_month.get(m)
        has_data = fact is not None
        rows.append({
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan if has_data else None,
            "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has_data else None,
            "has_data": has_data,
            **({"values_unit": "%"} if has_data else {}),
        })

    with_data = [row for row in rows if row.get("has_data")]
    last_row = rows[-1] if rows else None
    ytd_pct = None
    total_plan = None
    total_fact = None
    if with_data:
        total_plan = sum(float(row["plan"]) for row in with_data if row.get("plan") is not None)
        total_fact = sum(float(row["fact"]) for row in with_data if row.get("fact") is not None)
        ytd_pct = _kpi_pct(total_fact, total_plan)

    return {
        "data_granularity": "monthly",
        "monthly_data": rows,
        "last_full_month_row": dict(last_row) if last_row else None,
        "ytd": {
            "total_plan": total_plan,
            "total_fact": total_fact,
            "kpi_pct": ytd_pct,
            "months_with_data": len(with_data),
            "months_total": len(rows),
            **({"values_unit": "%"} if total_plan is not None else {}),
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
    }


def _quarterly_payload(kpi_id: str, year: int | None, month: int | None) -> dict:
    if year and month:
        ref_y = int(year)
        ref_q = (max(1, min(12, int(month))) - 1) // 3 + 1
    else:
        ref_y, ref_q = last_full_quarter(date.today())

    plan = 5.0 if kpi_id == "GB-Q2" else 100.0
    fact_by_quarter = QUARTERLY_FACT_BY_ID.get(kpi_id, {})
    rows = []
    for q in range(1, ref_q + 1):
        fact = fact_by_quarter.get(q)
        has_data = fact is not None
        rows.append({
            "quarter": q,
            "year": ref_y,
            "label": f"Q{q} {ref_y}",
            "plan": plan if has_data else None,
            "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has_data else None,
            "has_data": has_data,
            **({"values_unit": "%"} if has_data else {}),
        })

    with_data = [row for row in rows if row.get("has_data")]
    last_row = rows[-1] if rows else None
    total_plan = None
    total_fact = None
    ytd_pct = None
    if with_data:
        total_plan = sum(float(row["plan"]) for row in with_data if row.get("plan") is not None)
        total_fact = sum(float(row["fact"]) for row in with_data if row.get("fact") is not None)
        ytd_pct = _kpi_pct(total_fact, total_plan)

    return {
        "data_granularity": "quarterly",
        "quarterly_data": rows,
        "last_full_quarter_row": dict(last_row) if last_row else None,
        "ytd": {
            "total_plan": total_plan,
            "total_fact": total_fact,
            "kpi_pct": ytd_pct,
            "quarters_with_data": len(with_data),
            "quarters_total": len(rows),
            **({"values_unit": "%"} if total_plan is not None else {}),
        },
        "kpi_period": {
            "type": "last_full_quarter",
            "year": ref_y,
            "quarter": ref_q,
        },
    }


def build_kpi_entry(kpi_id: str, year: int | None = None, month: int | None = None) -> dict | None:
    if kpi_id in MONTHLY_FACT_BY_ID:
        return _monthly_payload(kpi_id, year, month)
    if kpi_id in QUARTERLY_FACT_BY_ID:
        return _quarterly_payload(kpi_id, year, month)
    return None
