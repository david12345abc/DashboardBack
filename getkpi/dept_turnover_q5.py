"""
Текучесть персонала (*-Q5) по подразделениям коммерческого блока.

Два режима конфигурации:
- **Явный квартал** (`fact_turnover_quarter_pct` + `plan_max_turnover_pct`): в графике C2 в полях
  plan/fact уходят эти проценты; `kpi_pct` — отдельно (например среднее за 3 мес. при одном введённом месяце).
- **Помесячно** (`monthly_turnover_pct`): среднее за последний полный квартал только по заполненным месяцам;
  plan — `plan_max_turnover_pct`, fact и kpi — это среднее по месяцам.
"""
from __future__ import annotations

from datetime import date

from .kpi_periods import last_full_quarter, quarter_month_tuples


def is_turnover_q5_kpi(kpi_id: str) -> bool:
    return kpi_id.endswith("-Q5") or kpi_id == "ZKD-Q2"


DEPT_TURNOVER_Q5_CONFIG: dict[str, dict] = {
    # График: план 1,8%, факт 25%; на плитке kpi_pct = 7% (среднее за квартал при данных за 1 мес.).
    "ОВЭД": {
        "plan_max_turnover_pct": 1.8,
        "fact_turnover_quarter_pct": 25.0,
        "kpi_pct": 7.0,
        "months_with_turnover_data": 1,
    },
}


def _avg_for_last_full_quarter(cfg: dict) -> tuple[list[float], int, int, int] | None:
    """Возвращает (значения %, год, квартал, число месяцев с данными) или None."""
    ly, lq = last_full_quarter(date.today())
    month_map: dict[tuple[int, int], float] = cfg.get("monthly_turnover_pct") or {}
    vals: list[float] = []
    for y, m in quarter_month_tuples(ly, lq):
        v = month_map.get((y, m))
        if v is not None:
            vals.append(float(v))
    if not vals:
        return None
    return vals, ly, lq, len(vals)


def build_turnover_q5_entry(dept_key: str) -> dict | None:
    cfg = DEPT_TURNOVER_Q5_CONFIG.get(dept_key)
    if cfg is None:
        return None

    ly, lq = last_full_quarter(date.today())
    plan_cap = float(cfg.get("plan_max_turnover_pct", 5.0))

    fixed_fact = cfg.get("fact_turnover_quarter_pct")
    if fixed_fact is not None:
        fact_q = float(fixed_fact)
        kpi_tile = cfg.get("kpi_pct")
        if kpi_tile is None:
            kpi_tile = round(plan_cap / fact_q * 100, 2) if fact_q else None
        else:
            kpi_tile = float(kpi_tile)
        n_m = int(cfg.get("months_with_turnover_data", 1))
        data_complete = bool(cfg.get("data_complete", n_m >= 3))
    else:
        parsed = _avg_for_last_full_quarter(cfg)
        if parsed is None:
            return None
        vals, ly, lq, n_m = parsed
        fact_q = round(sum(vals) / len(vals), 2)
        kpi_tile = fact_q
        data_complete = n_m >= 3

    row = {
        "quarter": lq,
        "year": ly,
        "label": f"Q{lq} {ly}",
        "plan_max_turnover_pct": plan_cap,
        "fact_turnover_pct": fact_q,
        "kpi_pct": kpi_tile,
        "data_complete": data_complete,
        "months_with_turnover_data": n_m,
    }

    return {
        "data_granularity": "quarterly",
        "quarterly_data": [row],
        "kpi_period": {
            "type": "last_full_quarter",
            "year": ly,
            "quarter": lq,
            "label": f"Q{lq} {ly}",
            "data_complete": data_complete,
        },
        "ytd": {
            "total_plan": plan_cap,
            "total_fact": fact_q,
            "kpi_pct": kpi_tile,
            "quarters_with_data": 1,
            "quarters_total": 1,
        },
    }
