"""
Сборка ответа get_kpi для «Коммерческий директор»: Плитки, Графики, Таблицы.
"""
from __future__ import annotations

import random
from datetime import date

from . import denzhi_dz, komdir_quarterly, valovaya_pribyl
from .kpi_periods import last_full_month

MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

def _rag_higher_better(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _rag_lower_turnover(fact_pct: float | None) -> str:
    """Пороги как в Excel: ≤5% зелёный, 5,1–7% жёлтый, >7% красный."""
    if fact_pct is None:
        return "unknown"
    if fact_pct <= 5:
        return "green"
    if fact_pct <= 7:
        return "yellow"
    return "red"


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


def _chart_type_bundle(meta: dict, *, fallback_type: str, fallback_label: str) -> dict[str, str]:
    """Тип графика из kpi_data (chart_type / chart_type_label) с запасными значениями."""
    ct = meta.get("chart_type") or fallback_type
    ctl = meta.get("chart_type_label")
    if not ctl:
        if meta.get("block") == "график":
            ctl = meta.get("perspective") or fallback_label
        else:
            ctl = fallback_label
    return {"chart_type": ct, "chart_type_label": ctl}


def monthly_m3_chart_series() -> list[dict]:
    """Помесячный план/факт для графика KD-M3 — только завершённые месяцы (до последнего полного)."""
    today = date.today()
    ref_y, ref_m = last_full_month(today)
    if ref_y == today.year:
        pairs = [(today.year, mm) for mm in range(1, ref_m + 1)]
    else:
        pairs = [(ref_y, ref_m)]
    random.seed(hash((ref_y, "KD-M3-monthly-chart", ref_m)))
    out = []
    for y, m in pairs:
        fact_z = round(random.uniform(0.85, 1.15) * 800_000, 2)
        fact_fot = round(random.uniform(0.88, 1.12) * 400_000, 2)
        plan_z = round(fact_z * random.uniform(0.92, 1.08), 2)
        plan_fot = round(fact_fot * random.uniform(0.92, 1.08), 2)
        fact_total = fact_z + fact_fot
        plan_total = plan_z + plan_fot
        t1 = min(1.0, plan_z / fact_z) if fact_z else 0.0
        t2 = min(1.0, plan_fot / fact_fot) if fact_fot else 0.0
        kpi = round((0.5 * t1 + 0.5 * t2) * 100, 1)
        out.append({
            "month": m,
            "month_name": MONTH_NAMES_RU[m],
            "year": y,
            "plan": round(plan_total, 2),
            "fact": round(fact_total, 2),
            "kpi_pct": kpi,
        })
    return out


def _table_b1_rows() -> list[dict]:
    random.seed(date.today().toordinal())
    statuses = ["Зелёный", "Жёлтый", "Красный"]
    actions = [
        "Скорректировать план",
        "Провести анализ причин",
        "Назначить ответственного",
        "Усилить контроль",
    ]
    n = random.randint(3, 5)
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "rank": i,
            "kpi_name": random.choice(["KD-M1", "KD-M2", "KD-Q1"]),
            "status": random.choice(statuses),
            "deviation_pct": round(random.uniform(-15, -1), 1),
            "action": random.choice(actions),
        })
    return rows


def _table_b2_rows() -> list[dict]:
    random.seed(date.today().toordinal() + 1)
    n = random.randint(2, 4)
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "rank": i,
            "risk": f"Риск зона {i}",
            "decision": "Эскалация / согласование",
            "deadline": f"2026-{date.today().month + 1:02d}-15",
            "owner": "Ответственный",
            "status": random.choice(["Открыто", "В работе", "Закрыто"]),
        })
    return rows


def build_komdir_payload(kpi_list: list[dict]) -> dict:
    by_id = {k["kpi_id"]: k for k in kpi_list}

    vp = valovaya_pribyl.get_vp_ytd()
    m2 = denzhi_dz.get_kd_m2_ytd()
    qm3 = komdir_quarterly.quarterly_m3()
    qq1 = komdir_quarterly.quarterly_q1(vp["months"])
    qq2 = komdir_quarterly.quarterly_q2()
    m3_monthly = monthly_m3_chart_series()

    # --- Плитки: KPI % для отображения
    tile_values: list[tuple[str, float | None, str, dict]] = []
    # (kpi_id, kpi_pct, color, kpi_meta)

    y1m = vp["ytd"].get("kpi_pct")
    tile_values.append(("KD-M1", y1m, _rag_higher_better(y1m), by_id["KD-M1"]))

    y2m = m2["ytd"].get("kpi_pct")
    tile_values.append(("KD-M2", y2m, _rag_higher_better(y2m), by_id["KD-M2"]))

    y3m = qm3["ytd"].get("kpi_pct")
    tile_values.append(("KD-M3", y3m, _rag_higher_better(y3m), by_id["KD-M3"]))

    yq1 = qq1["ytd"].get("kpi_pct")
    tile_values.append(("KD-Q1", yq1, _rag_higher_better(yq1), by_id["KD-Q1"]))

    q2_last = qq2["quarterly_data"][-1] if qq2["quarterly_data"] else None
    yq2_kpi = qq2["ytd"].get("kpi_pct")
    q2_turnover = q2_last["fact_turnover_pct"] if q2_last else None
    tile_values.append(
        ("KD-Q2", yq2_kpi, _rag_lower_turnover(q2_turnover), by_id["KD-Q2"]),
    )

    plitki_items = []
    numeric_for_avg: list[float] = []
    for kid, pct, color, meta in tile_values:
        if pct is not None:
            numeric_for_avg.append(pct)
        plitki_items.append({
            "kpi_id": kid,
            "name": meta["name"],
            "kpi_pct": pct,
            "color": color,
            "period": _period_label(meta),
            "thresholds": _thresholds_block(meta),
        })

    avg_pct = round(sum(numeric_for_avg) / len(numeric_for_avg), 1) if numeric_for_avg else None
    plitki_items.append({
        "kpi_id": "KD-AVG",
        "name": "Среднее по плиткам KPI",
        "kpi_pct": avg_pct,
        "color": _rag_higher_better(avg_pct),
        "period": "агрегат",
        "thresholds": {
            "green": "≥100%",
            "yellow": "90–99,9%",
            "red": "<90%",
        },
    })

    # --- График KD-C1 (ежемесячно)
    meta_c1 = by_id["KD-C1"]
    c1_types = _chart_type_bundle(
        meta_c1,
        fallback_type="multi_line_plan_fact_monthly",
        fallback_label="График тренда",
    )
    points_m1 = []
    for row in vp["months"]:
        if not row.get("has_data"):
            continue
        points_m1.append({
            "month": row["month"],
            "month_name": row["month_name"],
            "year": row.get("year", vp["year"]),
            "plan": row.get("plan"),
            "fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
        })

    points_m2 = []
    for row in m2["months"]:
        if not row.get("has_data"):
            continue
        points_m2.append({
            "month": row["month"],
            "month_name": row["month_name"],
            "year": row.get("year", m2["year"]),
            "plan": row.get("plan"),
            "fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
        })

    points_m3 = m3_monthly

    meta_c2 = by_id["KD-C2"]
    c2_types = _chart_type_bundle(
        meta_c2,
        fallback_type="column_plan_fact_waterfall_quarterly",
        fallback_label="План/факт / waterfall",
    )

    ser_m1 = _chart_type_bundle(
        by_id["KD-M1"],
        fallback_type="line_plan_fact_monthly",
        fallback_label="Линейный тренд: план и факт по месяцам",
    )
    ser_m2 = _chart_type_bundle(
        by_id["KD-M2"],
        fallback_type="line_plan_fact_monthly",
        fallback_label="Линейный тренд: план и факт по месяцам",
    )
    ser_m3 = _chart_type_bundle(
        by_id["KD-M3"],
        fallback_type="line_plan_fact_monthly",
        fallback_label="Линейный тренд: план и факт по месяцам",
    )
    ser_q1 = _chart_type_bundle(
        by_id["KD-Q1"],
        fallback_type="column_plan_fact_quarterly",
        fallback_label="Столбцы: план/факт по кварталам (деньги)",
    )
    ser_q2 = _chart_type_bundle(
        by_id["KD-Q2"],
        fallback_type="column_plan_fact_quarterly_percent",
        fallback_label="Столбцы: план/факт по кварталам (%)",
    )

    grafiki = {
        "KD-C1": {
            "kpi_id": "KD-C1",
            "name": meta_c1["name"],
            "periodicity": "ежемесячно",
            **c1_types,
            "series": [
                {
                    "kpi_id": "KD-M1",
                    "name": by_id["KD-M1"]["name"],
                    **ser_m1,
                    "points": points_m1,
                },
                {
                    "kpi_id": "KD-M2",
                    "name": by_id["KD-M2"]["name"],
                    **ser_m2,
                    "points": points_m2,
                },
                {
                    "kpi_id": "KD-M3",
                    "name": by_id["KD-M3"]["name"],
                    **ser_m3,
                    "points": points_m3,
                },
            ],
        },
        "KD-C2": {
            "kpi_id": "KD-C2",
            "name": meta_c2["name"],
            "periodicity": "ежеквартально",
            **c2_types,
            "series": [
                {
                    "kpi_id": "KD-Q1",
                    "name": by_id["KD-Q1"]["name"],
                    **ser_q1,
                    "points": [
                        {
                            "quarter": p["quarter"],
                            "year": p["year"],
                            "label": p["label"],
                            "plan": p.get("vp_plan"),
                            "fact": p.get("vp_fact"),
                            "kpi_pct": p.get("kpi_pct"),
                        }
                        for p in qq1["quarterly_data"]
                    ],
                },
                {
                    "kpi_id": "KD-Q2",
                    "name": by_id["KD-Q2"]["name"],
                    **ser_q2,
                    "points": [
                        {
                            "quarter": p["quarter"],
                            "year": p["year"],
                            "label": p["label"],
                            "plan": p.get("plan_max_turnover_pct"),
                            "fact": p.get("fact_turnover_pct"),
                            "kpi_pct": p.get("kpi_pct"),
                        }
                        for p in qq2["quarterly_data"]
                    ],
                },
            ],
        },
    }

    # --- Таблицы
    tablitsy = {
        "KD-B1": {
            "kpi_id": "KD-B1",
            "name": by_id["KD-B1"]["name"],
            "periodicity": _period_label(by_id["KD-B1"]),
            "rows": _table_b1_rows(),
        },
        "KD-B2": {
            "kpi_id": "KD-B2",
            "name": by_id["KD-B2"]["name"],
            "periodicity": _period_label(by_id["KD-B2"]),
            "rows": _table_b2_rows(),
        },
    }

    return {
        "Плитки": {
            "count": len(plitki_items),
            "items": plitki_items,
        },
        "Графики": grafiki,
        "Таблицы": tablitsy,
    }
