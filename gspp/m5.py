"""
ГСП-M5 — бюджет план/факт проекта TurboProject «номенклатур*».

Целевой проект ищется теми же правилами, что и ГСП-Q4:
название содержит ``номенклатур`` и руководитель Ермаков Илья Николаевич.
План и факт берутся из 1С-части карточки TurboProject:
``data_1c.byudzhet_plan`` и ``data_1c.byudzhet_fakt``.
"""
from __future__ import annotations

import argparse
import json
import logging
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from getkpi.cache_manager import locked_call
from getkpi.devdir import ytd_json_cache
from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from gspp.q4 import _find_target_project, _login, _project_display_name

logger = logging.getLogger(__name__)

GSPP_M5_CACHE_PREFIX = "gspp_m5_ytd"
GSPP_M5_DISK_TAG = "gspp_m5_budget_payload_v1"
GSPP_M5_DISK_VERSION = 1


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if not value:
            return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if num == num else None


def _parse_project_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    for candidate in (raw, raw[:10]):
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.year > 1900:
            return parsed.date()
    if len(raw) >= 10 and raw[2:3] == "." and raw[5:6] == ".":
        try:
            parsed = datetime.strptime(raw[:10], "%d.%m.%Y")
        except ValueError:
            return None
        if parsed.year > 1900:
            return parsed.date()
    return None


def _month_start_end(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _project_date_bounds(details: dict[str, Any]) -> tuple[date | None, date | None]:
    data_1c = details.get("data_1c") or {}
    meta = details.get("project") or {}
    start = (
        _parse_project_date(data_1c.get("data_nachala"))
        or _parse_project_date(data_1c.get("planovaya_data_nachala"))
        or _parse_project_date(meta.get("start_date"))
        or _parse_project_date(meta.get("baseline_start"))
    )
    finish = (
        _parse_project_date(data_1c.get("data_okonchaniya"))
        or _parse_project_date(data_1c.get("planovaya_data_okonchaniya"))
        or _parse_project_date(meta.get("finish_date"))
        or _parse_project_date(meta.get("baseline_finish"))
    )
    return start, finish


def _project_alive_in_month(details: dict[str, Any], year: int, month: int) -> bool:
    period_start, period_end = _month_start_end(year, month)
    start, finish = _project_date_bounds(details)
    if start is not None and start > period_end:
        return False
    if finish is not None and finish < period_start:
        return False
    return True


def _budget_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _empty_month_row(year: int, month: int) -> dict[str, Any]:
    return {
        "month": month,
        "year": year,
        "month_name": MONTH_NAMES[month],
        "plan": None,
        "fact": None,
        "kpi_pct": None,
        "has_data": False,
        "values_unit": "руб.",
    }


def _build_gspp_m5_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    monthly_rows = [_empty_month_row(ref_y, m) for m in range(1, ref_m + 1)]
    debug: dict[str, Any] = {
        "kpi_id": "ГСП-M5",
        "source": "gspp/m5.py (TurboProject)",
        "project_filter": "name contains 'номенклатур', rukovoditel == Ермаков Илья Николаевич",
        "plan_field": "data_1c.byudzhet_plan",
        "fact_field": "data_1c.byudzhet_fakt",
        "status": "no_project",
    }

    try:
        session = requests.Session()
        token = _login(session)
        item, details, err = _find_target_project(session, token)
        if details is None:
            debug["hint"] = err
        else:
            data_1c = details.get("data_1c") or {}
            plan = _safe_float(data_1c.get("byudzhet_plan"))
            fact = _safe_float(data_1c.get("byudzhet_fakt"))
            debug.update({
                "status": "ok",
                "file_id": item.get("id") if item else None,
                "project_name": _project_display_name(details, item or {}),
                "project_code": data_1c.get("nomer_proekta"),
                "project_manager": data_1c.get("rukovoditel"),
                "raw_plan": data_1c.get("byudzhet_plan"),
                "raw_fact": data_1c.get("byudzhet_fakt"),
            })
            monthly_rows = []
            for m in range(1, ref_m + 1):
                if not _project_alive_in_month(details, ref_y, m):
                    monthly_rows.append(_empty_month_row(ref_y, m))
                    continue
                row = {
                    "month": m,
                    "year": ref_y,
                    "month_name": MONTH_NAMES[m],
                    "plan": round(plan, 2) if plan is not None else None,
                    "fact": round(fact, 2) if fact is not None else None,
                    "kpi_pct": _budget_pct(plan, fact),
                    "has_data": plan is not None or fact is not None,
                    "values_unit": "руб.",
                }
                monthly_rows.append(row)
    except Exception as exc:
        logger.exception("ГСП-M5: сбой TurboProject")
        debug["status"] = "error"
        debug["error"] = str(exc)

    ref_row = next(
        (row for row in reversed(monthly_rows) if row.get("month") == ref_m),
        monthly_rows[-1] if monthly_rows else _empty_month_row(ref_y, ref_m),
    )
    with_data = [row for row in monthly_rows if row.get("has_data")]
    ytd: dict[str, Any] = {
        "months_with_data": len(with_data),
        "months_total": len(monthly_rows),
        "values_unit": "руб.",
    }
    if ref_row.get("has_data"):
        ytd.update({
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
        })
    else:
        ytd.update({
            "total_plan": None,
            "total_fact": None,
            "kpi_pct": None,
        })

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": ytd,
        "debug": debug,
    }


def gspp_m5_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_M5_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_m5_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = gspp_m5_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any] | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=GSPP_M5_DISK_TAG,
            version=GSPP_M5_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        payload = _build_gspp_m5_payload(year=ref_y, month=ref_m)
        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                cache_path,
                payload,
                source_tag=GSPP_M5_DISK_TAG,
                version=GSPP_M5_DISK_VERSION,
            )
        return payload

    return locked_call(f"gspp_m5_turbo_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    parser = argparse.ArgumentParser(description="Получить бюджет план/факт ГСП-M5 из TurboProject.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_gspp_m5_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
