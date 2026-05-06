"""
QD-M4 — ФОТ подразделения в пределах лимита (директор по качеству / qualdir).

План по месяцам задаётся константами (руб.): в исходной таблице по каждому месяцу
три числовые ячейки в колонке — **суммируются** в один плановый показатель.

Факт (п.п. 2.1–2.5, 4.2, 5 ТЗ): регистр бухгалтерии «Хозрасчётный»,
``RecordsWithExtDimensions``, дебетовый оборот счёта 26 по паре
«подразделение + статья затрат»; только две статьи п. 4.2 и семь подразделений
из ТЗ — см. ``qualdir.qd_m4_fact``.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from getkpi.cache_manager import locked_call

from qualdir.qd_m4_fact import compute_qd_m4_fact_monthly

logger = logging.getLogger(__name__)

SOURCE_TAG = "qualdir_qd_m4_v2"

# Плановый ФОТ контура качества, 2026, руб./мес. (сумма трёх строк по столбцу месяца).
QD_M4_PLAN_BY_MONTH_2026: dict[int, int] = {
    1: 511_958 + 507_692 + 61_890,      # январь
    2: 508_161 + 416_331 + 100_169,     # февраль
    3: 525_100 + 964_361 + 112_724,     # март
    4: 649_510 + 1_154_788 + 127_162,   # апрель
    5: 532_549 + 1_143_118 + 132_887,   # май
    6: 570_589 + 1_429_682 + 160_353,   # июнь
    7: 549_005 + 1_406_250 + 158_705,   # июль
    8: 634_292 + 1_049_684 + 196_695,   # август
    9: 537_874 + 1_293_779 + 184_608,   # сентябрь
    10: 525_597 + 1_096_186 + 170_822,  # октябрь
    11: 522_884 + 919_101 + 112_025,    # ноябрь
    12: 574_639 + 1_446_782 + 95_580,   # декабрь
}

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    if ref_year == today.year:
        ref_month = min(ref_month, today.month)
    return ref_year, ref_month


def _tile_month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    if year == 2026 and QD_M4_PLAN_BY_MONTH_2026:
        upper_month = max(max(QD_M4_PLAN_BY_MONTH_2026), ref_month)
    else:
        upper_month = ref_month
    return [(year, mm) for mm in range(1, upper_month + 1)]


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in QD_M4_PLAN_BY_MONTH_2026:
        return float(QD_M4_PLAN_BY_MONTH_2026[month])
    return None


def _fact_qd_contour(year: int, month: int) -> tuple[float | None, dict[str, Any]]:
    """Факт из 1С; при ошибке источника — (None, payload), без исключения наружу."""
    try:
        payload = compute_qd_m4_fact_monthly(year, month)
    except Exception:
        logger.exception("QD-M4: сбой вызова compute_qd_m4_fact_monthly за %d-%02d", year, month)
        return None, {"debug": {"status": "error"}, "total_fact": None, "groups": {}}
    dbg = payload.get("debug") or {}
    if dbg.get("status") == "error":
        return None, payload
    total = payload.get("total_fact")
    if total is None:
        return None, payload
    return float(total), payload


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan == 0:
        return None
    return round(fact / plan * 100, 2)


def get_qd_m4_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """QD-M4: план из констант qualdir, факт — сумма по контуру качества (1С)."""

    def _runner() -> dict[str, Any]:
        try:
            ref_y, ref_m = _normalize_period(year, month)
            pairs = _tile_month_pairs(ref_y, ref_m)
            monthly_rows: list[dict[str, Any]] = []
            ref_row: dict[str, Any] | None = None

            for y, m in pairs:
                plan = _plan_for_month(y, m)
                try:
                    fact, _fp = _fact_qd_contour(y, m)
                except Exception:
                    logger.exception("QD-M4: факт за %d-%02d", y, m)
                    fact = None
                has_data = plan is not None and fact is not None
                kpi_pct = _kpi_pct(plan, fact) if has_data else None
                row: dict[str, Any] = {
                    "month": m,
                    "year": y,
                    "month_name": MONTH_NAMES[m],
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": kpi_pct,
                    "has_data": has_data,
                }
                if plan is not None:
                    row["values_unit"] = "руб."
                monthly_rows.append(row)
                if (y, m) == (ref_y, ref_m):
                    ref_row = row

            return {
                "data_granularity": "monthly",
                "monthly_data": monthly_rows,
                # Плитка и API берут plan/fact из last_full_month_row: план из констант
                # должен быть виден даже при отсутствии факта (1С).
                "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("plan") is not None else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {
                    "total_plan": ref_row.get("plan") if ref_row else None,
                    "total_fact": ref_row.get("fact") if ref_row else None,
                    "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                    "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
                    "months_total": len(monthly_rows),
                    **({"values_unit": "руб."} if ref_row and ref_row.get("has_data") else {}),
                },
                "debug": {
                    "status": "ok" if any(row.get("has_data") for row in monthly_rows) else "no_data",
                    "kpi_id": "QD-M4",
                    "source": SOURCE_TAG,
                    "plan_source": "qualdir.qd_m4.QD_M4_PLAN_BY_MONTH_2026",
                    "fact_source": "qualdir.qd_m4_fact (регистр Хозрасчётный, сч.26, 7 п/п, статьи п.4.2)",
                },
            }
        except Exception as exc:
            logger.exception("Ошибка при расчёте QD-M4 (ФОТ qualdir)")
            ref_y, ref_m = _normalize_period(year, month)
            plan_fallback = _plan_for_month(ref_y, ref_m)
            fallback_row: dict[str, Any] | None = None
            if plan_fallback is not None:
                fallback_row = {
                    "month": ref_m,
                    "year": ref_y,
                    "month_name": MONTH_NAMES[ref_m],
                    "plan": plan_fallback,
                    "fact": None,
                    "kpi_pct": None,
                    "has_data": False,
                    "values_unit": "руб.",
                }
            return {
                "data_granularity": "monthly",
                "monthly_data": [fallback_row] if fallback_row else [],
                "last_full_month_row": dict(fallback_row) if fallback_row else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {
                    "total_plan": fallback_row.get("plan") if fallback_row else None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 1 if fallback_row else 0,
                    **({"values_unit": "руб."} if fallback_row else {}),
                },
                "debug": {
                    "status": "error",
                    "kpi_id": "QD-M4",
                    "source": SOURCE_TAG,
                    "error": str(exc),
                    "plan_fallback_row": bool(fallback_row),
                },
            }

    lock_y, lock_m = year, month
    if lock_y is None or lock_m is None:
        lock_y, lock_m = _normalize_period(None, None)
    return locked_call(f"qualdir_qd_m4_{lock_y}_{lock_m:02d}", _runner)
