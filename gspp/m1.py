"""ГСП-M1 — ТКП в срок по ОЛ, которые были в ГСПП."""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

from getkpi.cache_manager import locked_call
from getkpi.calc_tkp_sla import (
    AUTH,
    BASE,
    DOC_TYPE_SET,
    DOC_TYPES,
    EMPTY,
    EMPTY_DATE,
    LIMIT_BOTTOM,
    MONTH_RU,
    ТОЧКА_АННУЛ,
    ТОЧКА_ТКП,
    _business_days_cal,
    _detect_manager_field,
    _is_empty_date,
    _load_calendar,
    _load_paginated,
    _parse_dt,
)
from getkpi.devdir import ytd_json_cache
from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

GSPP_M1_CACHE_PREFIX = "gspp_m1_ytd"
GSPP_M1_DISK_TAG = "gspp_m1_tkp_payload_v1"
GSPP_M1_DISK_VERSION = 1

# Catalog_ТД_ТочкиЭтапов: "Анализ ОЛ группой ГСПП".
ТОЧКА_АНАЛИЗ_ОЛ_ГСПП = "605622b5-0004-11ec-86a3-ac1f6b05524d"


def _period_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def _pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _load_version_dates(session: requests.Session, p_end: date) -> dict[str, date]:
    p_end_s = f"{p_end.isoformat()}T23:59:59"
    flt = (
        "НомерВерсии eq 1"
        f" and ДатаВерсии gt datetime'{LIMIT_BOTTOM}'"
        f" and ДатаВерсии le datetime'{p_end_s}'"
    )
    url = (
        f"{BASE}/InformationRegister_ВерсииОбъектов"
        f"?$filter={flt}"
        f"&$orderby=ДатаВерсии"
        f"&$select=Объект,Объект_Type,ДатаВерсии"
    )
    rows = _load_paginated(session, url)
    out: dict[str, date] = {}
    for row in rows:
        if row.get("Объект_Type") in DOC_TYPE_SET:
            out[row["Объект"]] = _parse_dt(row["ДатаВерсии"])
    return out


def _load_ol_statuses(
    session: requests.Session,
    version_dates: dict[str, date],
    p_end: date,
) -> dict[str, dict[str, Any]]:
    needed = set(version_dates)
    if not needed:
        return {}
    mgr_field = _detect_manager_field(session, f"Date ge datetime'{LIMIT_BOTTOM}'")
    select_fields = "Ref_Key,Date,Number,Статус"
    if mgr_field:
        select_fields += f",{mgr_field}"
    p_end_s = f"{p_end.isoformat()}T23:59:59"
    flt = f"Date ge datetime'{LIMIT_BOTTOM}' and Date le datetime'{p_end_s}'"
    out: dict[str, dict[str, Any]] = {}
    for doc_type in DOC_TYPES:
        url = (
            f"{BASE}/{doc_type}"
            f"?$filter={flt}"
            f"&$select={select_fields}"
        )
        for row in _load_paginated(session, url):
            key = row.get("Ref_Key")
            if key in needed:
                out[key] = {
                    "status": row.get("Статус", ""),
                    "number": row.get("Number", ""),
                }
    return out


def _load_stage_dates(session: requests.Session, point_key: str) -> dict[str, set[str]]:
    flt = f"ТочкаЭтапа_Key eq guid'{point_key}'"
    url = (
        f"{BASE}/AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
        f"?$filter={flt}"
        f"&$select=Recorder,ДатаЗавершенияПлан,ДатаЗавершенияФакт,НомерСтрокиТЧ"
    )
    out: dict[str, set[str]] = {}
    for row in _load_paginated(session, url):
        rec = row.get("Recorder")
        if not rec:
            continue
        fact = row.get("ДатаЗавершенияФакт")
        if fact and not _is_empty_date(fact):
            out.setdefault(rec, set()).add(fact)
    return out


def _load_gspp_ol_keys(session: requests.Session) -> set[str]:
    flt = f"ТочкаЭтапа_Key eq guid'{ТОЧКА_АНАЛИЗ_ОЛ_ГСПП}'"
    url = (
        f"{BASE}/AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
        f"?$filter={flt}"
        f"&$select=Recorder"
    )
    return {
        row["Recorder"]
        for row in _load_paginated(session, url)
        if row.get("Recorder")
    }


def _load_unfinished_debt_ol_keys(session: requests.Session) -> set[str]:
    flt = (
        f"ДатаЗавершенияПлан ne datetime'{EMPTY_DATE}'"
        f" and ДатаЗавершенияФакт eq datetime'{EMPTY_DATE}'"
    )
    url = (
        f"{BASE}/AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
        f"?$filter={flt}"
        f"&$select=Recorder"
    )
    return {
        row["Recorder"]
        for row in _load_paginated(session, url)
        if row.get("Recorder")
    }


def _compute_gspp_m1_month(session: requests.Session, year: int, month: int) -> dict[str, Any]:
    p_start, p_end = _period_bounds(year, month)
    version_dates = _load_version_dates(session, p_end)
    ol_statuses = _load_ol_statuses(session, version_dates, p_end)
    gspp_ol_keys = _load_gspp_ol_keys(session)
    unfinished_debts = _load_unfinished_debt_ol_keys(session)
    tkp_dates = _load_stage_dates(session, ТОЧКА_ТКП)
    ann_dates = _load_stage_dates(session, ТОЧКА_АННУЛ)

    today = date.today()
    as_of = min(today, p_end) if (year, month) >= (today.year, today.month) else p_end
    items: list[dict[str, Any]] = []
    for ol_key, created in version_dates.items():
        if ol_key not in gspp_ol_keys:
            continue
        status = (ol_statuses.get(ol_key) or {}).get("status", "")
        in_current_period = p_start <= created <= p_end
        fact_dates = ann_dates.get(ol_key, set()) if status == "Аннулирован" else tkp_dates.get(ol_key, set())

        lifecycle: int | None = None
        worked = False
        if in_current_period:
            if fact_dates:
                completed = min(_parse_dt(raw) for raw in fact_dates if not _is_empty_date(raw))
                lifecycle = _business_days_cal(created, completed)
                worked = True
            else:
                lifecycle = _business_days_cal(created, as_of)
        elif created < p_start and ol_key in unfinished_debts and status != "Аннулирован":
            lifecycle = _business_days_cal(created, as_of)

        if lifecycle is None:
            continue
        items.append({
            "ol": ol_key,
            "created": created.isoformat(),
            "lifecycle": lifecycle,
            "worked": worked,
            "current_period": in_current_period,
        })

    plan = len(items)
    fact = sum(1 for item in items if int(item["lifecycle"]) <= 3)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "plan": plan,
        "fact": fact,
        "kpi_pct": _pct(fact, plan),
        "has_data": True,
        "values_unit": "шт.",
        "debug": {
            "status": "ok",
            "source": "gspp.m1",
            "rule": "plan = all OL in GSPP selection; fact = lifecycle <= 3 working days",
            "gspp_stage_point_key": ТОЧКА_АНАЛИЗ_ОЛ_ГСПП,
            "records_count": len(items),
            "worked_count": sum(1 for item in items if item["worked"]),
            "debt_count": sum(1 for item in items if not item["current_period"]),
        },
    }


def _build_gspp_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = requests.Session()
    session.auth = AUTH
    _load_calendar(session, sorted(set(range(2021, ref_y + 2))))

    monthly_rows: list[dict[str, Any]] = []
    month_debug: dict[int, Any] = {}
    for m in range(1, ref_m + 1):
        row = _compute_gspp_m1_month(session, ref_y, m)
        month_debug[m] = row.pop("debug", {})
        monthly_rows.append(row)

    ref_row = monthly_rows[-1] if monthly_rows else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
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
            "months_with_data": len(monthly_rows),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "ГСП-M1",
            "status": "ok",
            "month_debug": month_debug,
        },
    }


def gspp_m1_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_M1_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_m1_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = gspp_m1_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any] | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=GSPP_M1_DISK_TAG,
            version=GSPP_M1_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_gspp_m1_payload(year=ref_y, month=ref_m)
        except Exception as exc:
            logger.exception("ГСП-M1: ошибка расчёта ТКП в срок")
            return {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {"total_plan": None, "total_fact": None, "kpi_pct": None},
                "debug": {"kpi_id": "ГСП-M1", "status": "error", "error": str(exc)},
            }
        ytd_json_cache.save_payload(
            cache_path,
            payload,
            source_tag=GSPP_M1_DISK_TAG,
            version=GSPP_M1_DISK_VERSION,
        )
        return payload

    return locked_call(f"gspp_m1_tkp_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    parser = argparse.ArgumentParser(description="Рассчитать ГСП-M1: ТКП в срок.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_gspp_m1_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
