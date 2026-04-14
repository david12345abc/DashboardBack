"""
Графики и таблица для ответов API коммерческих подразделений.

Все коммерческие подразделения теперь используют единый формат:
10 ежемесячных KPI (KD-M1..KD-M10), 3 графика, таблица претензий.

Этот модуль оставлен для обратной совместимости, но основная логика
теперь в komdir_dashboard.py через build_komdir_payload.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from .commercial_tiles import tile_order_for_kpi_key
from .table_metrics import deviation_pct


def _prefix_from_storage_key(kpi_storage_key: str) -> str:
    order = tile_order_for_kpi_key(kpi_storage_key)
    if not order:
        return "COMM"
    return order[0].split("-", 1)[0]


def _freq_lower(kpi: dict) -> str:
    return (kpi.get("frequency") or "").lower()


def _is_monthly_tile(kpi: dict) -> bool:
    f = _freq_lower(kpi)
    return "месяц" in f or "ежемесячн" in f


def _is_quarterly_tile(kpi: dict) -> bool:
    return "квартал" in _freq_lower(kpi)


def _find_meta_by_id(raw: list[dict], kpi_id: str) -> dict | None:
    for x in raw:
        if x.get("kpi_id") == kpi_id:
            return x
    return None


def _chart_type_bundle(meta: dict, *, fallback_type: str, fallback_label: str) -> dict[str, str]:
    ct = meta.get("chart_type") or fallback_type
    ctl = meta.get("chart_type_label")
    if not ctl:
        if meta.get("block") == "график":
            ctl = meta.get("perspective") or fallback_label
        else:
            ctl = fallback_label
    return {"chart_type": ct, "chart_type_label": ctl}


def _series_bundle_tile(meta: dict) -> dict[str, str]:
    return _chart_type_bundle(
        meta,
        fallback_type="line_plan_fact_monthly",
        fallback_label="План и факт по месяцам",
    )


def _series_bundle_quarter_tile(meta: dict) -> dict[str, str]:
    nm = (meta.get("name") or "").lower()
    kid = meta.get("kpi_id") or ""
    if "текучесть" in nm or kid.endswith("-Q5") or kid == "ZKD-Q2":
        return _chart_type_bundle(
            meta,
            fallback_type="column_plan_fact_quarterly_percent",
            fallback_label="План/факт по кварталам (%)",
        )
    return _chart_type_bundle(
        meta,
        fallback_type="column_plan_fact_quarterly",
        fallback_label="План/факт по кварталам",
    )


def _points_from_monthly_entry(entry: dict) -> list[dict]:
    rows = entry.get("monthly_data") or []
    default_year = date_year_from_entry(entry)
    out: list[dict] = []
    for row in rows:
        y = row.get("year", default_year)
        pt = {
            "month": row["month"],
            "month_name": row["month_name"],
            "year": y,
            "plan": row.get("plan"),
            "fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
            "has_data": row.get("has_data", True),
        }
        if row.get("values_unit"):
            pt["values_unit"] = row["values_unit"]
        out.append(pt)
    return out


def date_year_from_entry(entry: dict) -> int:
    kp = entry.get("kpi_period") or {}
    if isinstance(kp.get("year"), int):
        return kp["year"]
    return date.today().year


def _points_from_quarterly_entry(entry: dict) -> list[dict]:
    qd = entry.get("quarterly_data") or []
    points: list[dict] = []
    for p in qd:
        if p.get("fact_turnover_pct") is not None:
            pt = {
                "quarter": p["quarter"],
                "year": p["year"],
                "label": p.get("label"),
                "plan": p.get("plan_max_turnover_pct"),
                "fact": p.get("fact_turnover_pct"),
                "kpi_pct": p.get("kpi_pct"),
            }
            if p.get("data_complete") is not None:
                pt["data_complete"] = p["data_complete"]
            if p.get("months_with_turnover_data") is not None:
                pt["months_with_turnover_data"] = p["months_with_turnover_data"]
            points.append(pt)
        else:
            points.append({
                "quarter": p["quarter"],
                "year": p["year"],
                "label": p.get("label"),
                "plan": p.get("plan"),
                "fact": p.get("fact"),
                "kpi_pct": p.get("kpi_pct"),
            })
    return points


def _table_row_last_month(kpi: dict, entry: dict) -> dict | None:
    kpi_period = entry.get("kpi_period") or {}
    lm = entry.get("last_full_month_row")
    if lm and lm.get("has_data", True):
        pl, fc = lm.get("plan"), lm.get("fact")
        out = {
            "kpi_id": kpi["kpi_id"],
            "name": kpi["name"],
            "period_type": "month",
            "kpi_period": kpi_period,
            "plan": pl,
            "fact": fc,
            "kpi_pct": lm.get("kpi_pct"),
            "deviation_pct": deviation_pct(pl, fc),
        }
        if lm.get("values_unit"):
            out["values_unit"] = lm["values_unit"]
        return out
    md = entry.get("monthly_data") or []
    if not md:
        return None
    if kpi_period.get("type") == "last_full_month":
        ry = kpi_period.get("year")
        rm = kpi_period.get("month")
        for row in md:
            if row.get("year", ry) == ry and row.get("month") == rm:
                pl, fc = row.get("plan"), row.get("fact")
                out = {
                    "kpi_id": kpi["kpi_id"],
                    "name": kpi["name"],
                    "period_type": "month",
                    "kpi_period": kpi_period,
                    "plan": pl,
                    "fact": fc,
                    "kpi_pct": row.get("kpi_pct"),
                    "deviation_pct": deviation_pct(pl, fc),
                }
                if row.get("values_unit"):
                    out["values_unit"] = row["values_unit"]
                return out
    row = md[-1]
    pl, fc = row.get("plan"), row.get("fact")
    out = {
        "kpi_id": kpi["kpi_id"],
        "name": kpi["name"],
        "period_type": "month",
        "kpi_period": kpi_period,
        "plan": pl,
        "fact": fc,
        "kpi_pct": row.get("kpi_pct"),
        "deviation_pct": deviation_pct(pl, fc),
    }
    if row.get("values_unit"):
        out["values_unit"] = row["values_unit"]
    return out


def build_commercial_branch_payload(
    kpi_storage_key: str,
    kpis_meta: list[dict],
    entries: list[dict],
    raw_department_kpis: list[dict],
) -> dict[str, Any]:
    prefix = _prefix_from_storage_key(kpi_storage_key)
    by_id = {e["kpi_id"]: e for e in entries}

    meta_c1 = _find_meta_by_id(raw_department_kpis, f"{prefix}-C1")
    meta_c2 = _find_meta_by_id(raw_department_kpis, f"{prefix}-C2")

    if meta_c1 is None:
        meta_c1 = {
            "kpi_id": f"{prefix}-C1",
            "name": "Ежемесячные показатели (плитки)",
            "block": "график",
            "perspective": "Финансы",
        }
    if meta_c2 is None:
        meta_c2 = {
            "kpi_id": f"{prefix}-C2",
            "name": "Ежеквартальные показатели (плитки)",
            "block": "график",
            "perspective": "Финансы",
        }

    c1_types = _chart_type_bundle(
        meta_c1,
        fallback_type="multi_line_plan_fact_monthly",
        fallback_label="Динамика по месяцам",
    )
    c2_types = _chart_type_bundle(
        meta_c2,
        fallback_type="column_plan_fact_waterfall_quarterly",
        fallback_label="План/факт по кварталам",
    )

    monthly_metas = [k for k in kpis_meta if _is_monthly_tile(k)]
    quarterly_metas = [k for k in kpis_meta if _is_quarterly_tile(k)]

    series_monthly: list[dict] = []
    for k in monthly_metas:
        kid = k["kpi_id"]
        e = by_id.get(kid)
        if not e:
            continue
        sb = _series_bundle_tile(k)
        series_monthly.append({
            "kpi_id": kid,
            "name": k["name"],
            **sb,
            "points": _points_from_monthly_entry(e),
        })

    series_quarterly: list[dict] = []
    for k in quarterly_metas:
        kid = k["kpi_id"]
        e = by_id.get(kid)
        if not e:
            continue
        sb = _series_bundle_quarter_tile(k)
        series_quarterly.append({
            "kpi_id": kid,
            "name": k["name"],
            **sb,
            "points": _points_from_quarterly_entry(e),
        })

    grafiki: dict[str, Any] = {
        meta_c1["kpi_id"]: {
            "kpi_id": meta_c1["kpi_id"],
            "name": meta_c1.get("name") or "Ежемесячно",
            "periodicity": "ежемесячно",
            **c1_types,
            "series": series_monthly,
        },
        meta_c2["kpi_id"]: {
            "kpi_id": meta_c2["kpi_id"],
            "name": meta_c2.get("name") or "Ежеквартально",
            "periodicity": "ежеквартально",
            **c2_types,
            "series": series_quarterly,
        },
    }

    table_key = f"{prefix}-T-LAST-MONTH"
    table_rows: list[dict] = []
    for k in monthly_metas:
        e = by_id.get(k["kpi_id"])
        if not e:
            continue
        row = _table_row_last_month(k, e)
        if row:
            table_rows.append(row)

    tablitsy: dict[str, Any] = {
        table_key: {
            "name": "Показатели за последний полный месяц",
            "periodicity": "ежемесячно",
            "description": (
                "По одной строке на каждый ежемесячный KPI из плиток. "
                "deviation_pct = (факт − план) / план × 100."
            ),
            "rows": table_rows,
        },
    }

    return {
        "Графики": grafiki,
        "Таблицы": tablitsy,
    }
