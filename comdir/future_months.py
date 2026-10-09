"""Будущие месяцы коммерческого директора: ноябрь, декабрь и дальше по году.

Деньги, отгрузки и договоры: факт 0, прогноз = ожидаемо, план из тех же
регистров, что и на текущем месяце. Остальные плитки — их обычный расчёт
ровно за этот месяц.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
_PLAN_FACT_IDS = ("KD-M1", "KD-M2", "KD-M3")
_mem: dict[str, Any] = {"mtime": None, "data": None}


def cache_path(year: int) -> Path:
    return _CACHE_DIR / f"comdir_future_months_{int(year)}.json"


def future_month_numbers(year: int, today: date | None = None) -> list[int]:
    today = today or date.today()
    if int(year) < today.year:
        return []
    start = today.month + 1 if int(year) == today.year else 1
    if start > 12:
        return []
    return list(range(start, 13))


def load(year: int) -> dict | None:
    path = cache_path(year)
    if not path.exists():
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    if _mem.get("mtime") == mtime and isinstance(_mem.get("data"), dict):
        return _mem["data"]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("future months: не прочитался %s", path)
        return None
    if not isinstance(data, dict):
        return None
    _mem["mtime"] = mtime
    _mem["data"] = data
    return data


def rows_for_tile(kpi_id: str, year: int, today: date | None = None) -> list[dict]:
    """Точки строго после сегодня. Прошлый и текущий месяц сюда не входят."""
    data = load(year)
    if not data:
        return []
    tiles = data.get("tiles") or {}
    raw = tiles.get(str(kpi_id)) or []
    if not isinstance(raw, list):
        return []
    today = today or date.today()
    out = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            y = int(row.get("year") or 0)
            m = int(row.get("month") or 0)
        except (TypeError, ValueError):
            continue
        if (y, m) <= (today.year, today.month):
            continue
        out.append(row)
    out.sort(key=lambda row: (int(row.get("year") or 0), int(row.get("month") or 0)))
    return out


def _save(payload: dict, year: int) -> None:
    path = cache_path(year)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    _mem["mtime"] = None
    _mem["data"] = None


def _point(
    year: int,
    month: int,
    *,
    plan,
    fact,
    kpi,
    has_data: bool,
    extra: dict | None = None,
) -> dict:
    from comdir.common import MONTH_RU

    row = {
        "month": month,
        "year": year,
        "month_name": MONTH_RU.get(month, ""),
        "plan": plan,
        "fact": fact,
        "kpi_pct": kpi,
        "has_data": has_data,
        "future_month": True,
    }
    if extra:
        row.update(extra)
    return row


def _pct(num, den) -> float | None:
    try:
        d = float(den)
        n = float(num)
    except (TypeError, ValueError):
        return None
    if not d:
        return None
    return round(n / d * 100, 1)


def _sum_map(values: dict | None) -> float:
    if not values:
        return 0.0
    return round(sum(float(v or 0) for v in values.values()), 2)


def _plan_fact_sql(year: int, month: int) -> dict[str, dict[str, float]]:
    """План и SQL-ожидаемо за месяц. Факт будущего месяца не считаем."""
    from comdir import (
        calc_plan_fact_dengi as dengi_mod,
        calc_plan_fact_dogovory as dog_mod,
        calc_plan_fact_otgruzki as otg_mod,
    )
    from comdir.common import aggregate_by_odata_name, connect_ctx, period_bounds

    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        money_plan = aggregate_by_odata_name(dengi_mod.calc_plan(cur, p0, p_next))
        money_exp = aggregate_by_odata_name(dengi_mod.calc_expected(cur, p0, p_next))
        ship_plan = aggregate_by_odata_name(otg_mod.calc_mp_plan(cur, p0, p_next))
        ship_exp = aggregate_by_odata_name(
            otg_mod.calc_expected(cur, p_next), include_liquidated=False,
        )
        dog_plan = aggregate_by_odata_name(dog_mod.calc_mp_plan(cur, p0, p_next))
        dog_exp = aggregate_by_odata_name(
            dog_mod.calc_expected(cur, p0, p_next, p_asof=p_next)
        )
    return {
        "KD-M1": {"plan": _sum_map(money_plan), "expected": _sum_map(money_exp)},
        "KD-M2": {"plan": _sum_map(ship_plan), "expected": _sum_map(ship_exp)},
        "KD-M3": {"plan": _sum_map(dog_plan), "expected": _sum_map(dog_exp)},
    }


def _overlay_live_expected(year: int, months: list[int], cells: dict[int, dict]) -> None:
    """Ожидаемо денег и отгрузок — тот же OData, что на плитке текущего месяца."""
    from getkpi.calc_plan import get_dengi_expected_by_month, get_otgruzki_expected_by_month

    end = max(months) if months else 0
    if end < 1:
        return
    try:
        money = get_dengi_expected_by_month(year, end)
    except Exception:
        logger.exception("future months: ожидаемо денег не собралось")
        money = {}
    try:
        ship = get_otgruzki_expected_by_month(year, end)
    except Exception:
        logger.exception("future months: ожидаемо отгрузок не собралось")
        ship = {}
    for month in months:
        bucket = cells.setdefault(month, {})
        if money.get(month):
            bucket.setdefault("KD-M1", {})["expected"] = _sum_map(money.get(month))
        if ship.get(month):
            bucket.setdefault("KD-M2", {})["expected"] = _sum_map(ship.get(month))


def _plan_fact_points(year: int, month: int, cell: dict) -> dict[str, dict]:
    out = {}
    for kpi_id in _PLAN_FACT_IDS:
        src = cell.get(kpi_id) or {}
        plan = round(float(src.get("plan") or 0), 2)
        expected = round(float(src.get("expected") or 0), 2)
        out[kpi_id] = _point(
            year,
            month,
            plan=plan,
            fact=0,
            kpi=_pct(expected, plan),
            has_data=True,
            extra={
                "plan_full": plan,
                "forecast": expected,
                "expected_plan": expected,
                "expected_plan_current": expected,
                "expected_plan_full": expected,
            },
        )
    return out


def _other_points(year: int, month: int) -> dict[str, dict]:
    """Остальные плитки — штатный помесячный расчёт, без отдельной формулы."""
    out: dict[str, dict] = {}
    try:
        from comdir.ytd import compute_debitorka_month

        row = compute_debitorka_month(year, month)
        dz = row.get("dz_fact")
        overdue = row.get("overdue_fact")
        dz_plan = 100_000_000.0
        out["KD-M4"] = _point(
            year, month,
            plan=dz_plan,
            fact=dz,
            kpi=_pct(dz, dz_plan),
            has_data=dz is not None,
        )
        try:
            from getkpi.calc_dz_limits import get_dept_overdue_limit

            limit = float(get_dept_overdue_limit(None))
        except Exception:
            logger.exception("future months: лимит просрочки")
            limit = None
        out["KD-M5"] = _point(
            year, month,
            plan=limit,
            fact=overdue,
            kpi=_pct(overdue, limit),
            has_data=overdue is not None,
        )
    except Exception:
        logger.exception("future months: дебиторка %s-%02d", year, month)

    try:
        from comdir.ytd import compute_rashody_month

        row = compute_rashody_month(year, month)
        plan = row.get("plan")
        fact = row.get("fact")
        out["KD-M7"] = _point(
            year, month,
            plan=plan,
            fact=fact,
            kpi=_pct(fact, plan),
            has_data=True,
            extra={"plan_full": plan},
        )
    except Exception:
        logger.exception("future months: расходы %s-%02d", year, month)

    try:
        from comdir.ytd import compute_fot_month

        row = compute_fot_month(year, month)
        plan = row.get("plan")
        fact = row.get("fact")
        out["KD-M8"] = _point(
            year, month,
            plan=plan,
            fact=fact,
            kpi=_pct(fact, plan),
            has_data=True,
            extra={"plan_full": plan},
        )
    except Exception:
        logger.exception("future months: ФОТ %s-%02d", year, month)

    try:
        from comdir.ytd import compute_cena_month

        row = compute_cena_month(year, month)
        plan = row.get("plan")
        fact = row.get("fact")
        out["KD-M9"] = _point(
            year, month,
            plan=plan,
            fact=fact,
            kpi=_pct(fact, plan),
            has_data=True,
        )
    except Exception:
        logger.exception("future months: цена %s-%02d", year, month)

    try:
        from comdir.ytd import compute_tkp_sla_month

        row = compute_tkp_sla_month(year, month)
        plan = row.get("plan")
        fact = row.get("fact")
        pct = row.get("pct")
        if pct is None:
            pct = _pct(fact, plan)
        out["KD-M10"] = _point(
            year, month,
            plan=plan,
            fact=fact,
            kpi=pct,
            has_data=True,
        )
    except Exception:
        logger.exception("future months: ТКП %s-%02d", year, month)

    try:
        from getkpi.calc_tekuchest import get_tekuchest_monthly

        data = get_tekuchest_monthly(year, month)
        row = None
        for item in data.get("months") or []:
            if int(item.get("month") or 0) == month:
                row = item
        if row:
            fact = row.get("fact")
            try:
                fact_value = round(float(fact), 1) if fact is not None else None
            except (TypeError, ValueError):
                fact_value = None
            out["KD-M11"] = _point(
                year, month,
                plan=None,
                fact=fact_value,
                kpi=fact_value,
                has_data=fact_value is not None,
                extra={
                    "staff_units": row.get("staff_units"),
                    "dismissed": row.get("dismissed"),
                    "turnover_rows": row.get("turnover_rows") or [],
                },
            )
    except Exception:
        logger.exception("future months: текучесть %s-%02d", year, month)
    return out


def compute_and_save(year: int | None = None) -> dict:
    today = date.today()
    year = int(year or today.year)
    months = future_month_numbers(year, today)
    tiles: dict[str, list] = {}
    cells: dict[int, dict] = {}
    for month in months:
        try:
            cells[month] = _plan_fact_sql(year, month)
        except Exception:
            logger.exception("future months: план %s-%02d", year, month)
            cells[month] = {}
    _overlay_live_expected(year, months, cells)
    for month in months:
        for kpi_id, row in _plan_fact_points(year, month, cells.get(month) or {}).items():
            tiles.setdefault(kpi_id, []).append(row)
        for kpi_id, row in _other_points(year, month).items():
            tiles.setdefault(kpi_id, []).append(row)
    payload = {
        "cache_date": today.isoformat(),
        "year": year,
        "months": months,
        "tiles": tiles,
    }
    _save(payload, year)
    return payload


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.stdout.reconfigure(encoding="utf-8")
    data = compute_and_save()
    print("saved", cache_path(data["year"]))
    for kpi_id, rows in (data.get("tiles") or {}).items():
        for row in rows:
            print(
                kpi_id,
                row.get("month"),
                "plan", row.get("plan"),
                "fact", row.get("fact"),
                "expected", row.get("expected_plan"),
                "kpi", row.get("kpi_pct"),
            )
