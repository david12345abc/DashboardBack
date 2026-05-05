"""
QD-Q2 — текучесть персонала контура директора по качеству.

Та же выборка из Document_ТД_ТекучестьПерсонала, что у TD-Q2 (max план/факт по строкам месяца
на подразделение). Итог по плитке: сумма планов и сумма фактов по четырём подразделениям
ОТК-1, ОТК-2, Лаборатория неразрушающего контроля, Отдел управления несоответствиями.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from getkpi.cache_manager import locked_call
from getkpi.techdir_tekuchet import MONTH_RU, build_turnover_month_payload

# Кэш в каталоге getkpi/dashboard — как у остальных KPI-бэкендов.
_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
SOURCE_TAG = "qualdir_qd_q2_monthly_v3"
CACHE_VERSION = 3

QD_Q2_GROUP_ALIASES: dict[str, list[str]] = {
    "ОТК-1": [
        "отк-1",
        "отк 1",
    ],
    "ОТК-2": [
        "отк-2",
        "отк 2",
    ],
    "Лаборатория неразрушающего контроля": [
        "лаборатория неразрушающего контроля",
        "лнк",
    ],
    "Отдел управления несоответствиями": [
        "отдел управления несоответствиями",
        "отдел управления несоотвествиями",
    ],
}

QD_Q2_GROUP_ORDER = list(QD_Q2_GROUP_ALIASES.keys())


def _qd_q2_kpi_pct(plan: Any, fact: Any) -> float | None:
    """KPI по формуле плитки: факт / план × 100 %."""
    if plan is None or fact is None:
        return None
    try:
        pv = float(plan)
        fv = float(fact)
    except (TypeError, ValueError):
        return None
    if pv <= 0:
        return None
    return round(fv / pv * 100.0, 1)


def turnover_month_cache_path(year: int, month: int) -> Path:
    """Путь к JSON-кэшу помесячного снимка QD-Q2 (для views / cache_manager)."""
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_tekuchet_{year}_{month:02d}.json"


def _load_cache(year: int, month: int) -> dict | None:
    path = turnover_month_cache_path(year, month)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    return data


def _save_cache(year: int, month: int, payload: dict[str, Any]) -> None:
    try:
        with turnover_month_cache_path(year, month).open("w", encoding="utf-8") as f:
            json.dump(
                {
                    **payload,
                    "source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def compute_qd_q2_turnover_month(year: int, month: int) -> dict[str, Any]:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached
    result = build_turnover_month_payload(
        year,
        month,
        group_aliases=QD_Q2_GROUP_ALIASES,
        group_order=QD_Q2_GROUP_ORDER,
        aggregate="sum_all",
    )
    _save_cache(year, month, result)
    return result


def _tile_month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def get_qd_q2_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """QD-Q2: текучесть персонала службы по качеству (помесячно, как TD-Q2)."""

    def _runner() -> dict[str, Any]:
        try:
            nonlocal year, month
            if year is None or month is None:
                year, month = _last_full_month()

            month_rows: list[dict[str, Any]] = []
            for row_year, row_month in _tile_month_pairs(year, month):
                snapshot = compute_qd_q2_turnover_month(row_year, row_month)
                plan = snapshot["total_plan"]
                fact = snapshot["total_fact"]
                has_data = plan is not None and fact is not None
                month_rows.append({
                    "year": row_year,
                    "month": row_month,
                    "month_name": MONTH_RU[row_month].lower(),
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": _qd_q2_kpi_pct(plan, fact),
                    "has_data": has_data,
                    "values_unit": "шт.",
                })

            with_data = [row for row in month_rows if row["has_data"]]
            months_with_data = len(with_data)
            ref_row = next(
                (row for row in month_rows if row["month"] == month and row["year"] == year),
                None,
            )
            if ref_row is None and month_rows:
                ref_row = month_rows[-1]

            return {
                "data_granularity": "monthly",
                "monthly_data": month_rows,
                "last_full_month_row": dict(ref_row) if ref_row else None,
                "ytd": {
                    "total_plan": ref_row.get("plan") if ref_row else None,
                    "total_fact": ref_row.get("fact") if ref_row else None,
                    "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                    "months_with_data": months_with_data,
                    "months_total": len(month_rows),
                    "values_unit": "шт.",
                },
                "kpi_period": {
                    "type": "last_full_month",
                    "year": year,
                    "month": month,
                    "month_name": MONTH_RU[month],
                    "data_complete": ref_row is not None,
                },
                "debug": {
                    "status": "ok",
                    "kpi_id": "QD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала",
                    "groups": QD_Q2_GROUP_ORDER,
                },
            }
        except Exception as exc:
            y, m = year, month
            if y is None or m is None:
                y, m = _last_full_month()
            return {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 0,
                    "values_unit": "шт.",
                },
                "kpi_period": {
                    "type": "last_full_month",
                    "year": y,
                    "month": m,
                    "month_name": MONTH_RU[m],
                    "data_complete": False,
                },
                "debug": {
                    "status": "error",
                    "kpi_id": "QD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала",
                    "error": str(exc),
                },
            }

    lock_y, lock_m = year, month
    if lock_y is None or lock_m is None:
        lock_y, lock_m = _last_full_month()
    return locked_call(f"qualdir_qd_q2_{lock_y}_{lock_m:02d}", _runner)
