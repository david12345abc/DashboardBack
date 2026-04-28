from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from .cache_manager import locked_call
from . import fot_techdir_fact, fot_techdir_plan

logger = logging.getLogger(__name__)
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "techdir_m4_monthly_v1_single_month_cache"

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if fact == 0:
        return 100.0 if plan >= 0 else None
    return round(plan / fact * 100, 2)


def _month_pairs_from_january() -> tuple[list[tuple[int, int]], tuple[int, int]]:
    today = date.today()
    return [(today.year, mm) for mm in range(1, today.month + 1)], (today.year, today.month)


def _normalize_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    if ref_year == today.year:
        ref_month = min(ref_month, today.month)
    return ref_year, ref_month


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"techdir_m4_monthly_{year}_{month:02d}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("year") == date.today().year and data.get("month") == date.today().month:
        return data if data.get("cache_date") == date.today().isoformat() else None
    return data


def _save_json(path: Path, payload: dict) -> None:
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        logger.exception("Не удалось сохранить кэш TD-M4 в %s", path)


def _month_payload(year: int, month: int) -> dict[str, Any]:
    path = _cache_path(year, month)
    cached = _load_json(path)
    if cached is not None:
        return cached

    plan_payload = fot_techdir_plan.get_td_fot_plan_monthly(year, month)
    fact_payload = fot_techdir_fact.get_td_fot_fact_monthly(year, month)
    payload = {
        "source": SOURCE_TAG,
        "cache_date": date.today().isoformat(),
        "year": year,
        "month": month,
        "total_plan": plan_payload.get("total_plan"),
        "total_fact": fact_payload.get("total_fact"),
        "plan_debug": plan_payload.get("debug"),
        "fact_debug": fact_payload.get("debug"),
    }
    _save_json(path, payload)
    return payload


def get_td_m4_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    """TD-M4: ФОТ блока техдирекции в пределах лимита (план/факт из ЗУП и бухоборота)."""

    def _runner() -> dict | None:
        try:
            ref_y, ref_m = _normalize_period(year, month)
            pairs = [(ref_y, ref_m)]
            monthly_rows: list[dict] = []
            ref_row: dict | None = None

            for y, m in pairs:
                payload = _month_payload(y, m)
                plan = payload.get("total_plan")
                fact = payload.get("total_fact")
                has_data = plan is not None and fact is not None
                kpi_pct = _kpi_pct(plan, fact) if has_data else None

                row = {
                    "month": m,
                    "year": y,
                    "month_name": MONTH_NAMES[m],
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": kpi_pct,
                    "has_data": has_data,
                    **({"values_unit": "руб."} if has_data else {}),
                }
                monthly_rows.append(row)
                if (y, m) == (ref_y, ref_m):
                    ref_row = row

            return {
                "data_granularity": "monthly",
                "monthly_data": monthly_rows,
                "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
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
                    "kpi_id": "TD-M4",
                    "plan_source": "fot_techdir_plan.py",
                    "fact_source": "fot_techdir_fact.py",
                },
            }
        except Exception:
            logger.exception("Ошибка при расчёте TD-M4 (ФОТ техдирекции в пределах лимита)")
            return None

    return locked_call("techdir_td_m4", _runner)
