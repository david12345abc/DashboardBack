"""ГСП-Q5 — текучесть персонала ГСПП по документам 1С.

План: ``Document_ТД_ТекучестьПерсонала`` с ``ВидДокумента = 0``.
Факт: те же документы с ``ВидДокумента = 1``.
Отбор подразделений строго по шапке документа:
ГСПП и четыре сектора ГСПП.
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from getkpi.cache_manager import locked_call
from getkpi.devdir import ytd_json_cache
from getkpi.devdir.calc_tekuchest_dev_service import (
    AUTH,
    BASE,
    EMPTY,
    STRUCTURE_ENTITY,
    TEKUCHEST_ENTITY,
    fetch_all,
)
from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)

GSPP_Q5_CACHE_PREFIX = "gspp_q5_tekuchest"
GSPP_Q5_DISK_TAG = "gspp_q5_tekuchest_payload_v2"
GSPP_Q5_DISK_VERSION = 3

GSPP_TURNOVER_DEPARTMENTS = {
    "ГСПП",
    "Сектор сопровождения продаж",
    "Сектор сопровождения производства и продаж",
    "Сектор доработки и улучшения продукции",
    "Сектор постановки на производство несерийной продукции",
}


def _normalize_label(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _safe_float(value: Any) -> float:
    if isinstance(value, str):
        value = value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        num = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return num if num == num else 0.0


def _doc_type(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("__value") or value.get("Value") or value.get("value")
    raw = str(value if value is not None else "").strip()
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def load_target_department_keys(session: requests.Session) -> tuple[set[str], dict[str, str]]:
    url = (
        f"{BASE}/{quote(STRUCTURE_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote('DeletionMark eq false', safe='')}"
        f"&$select={quote('Ref_Key,Description', safe=',_')}"
    )
    rows = fetch_all(session, url, page=1000)
    wanted = {_normalize_label(name) for name in GSPP_TURNOVER_DEPARTMENTS}
    keys: set[str] = set()
    labels: dict[str, str] = {}
    for row in rows:
        key = str(row.get("Ref_Key") or "").strip()
        label = str(row.get("Description") or "").strip()
        if key and _normalize_label(label) in wanted:
            keys.add(key)
            labels[key] = label
    return keys, labels


def load_turnover_docs(session: requests.Session) -> list[dict[str, Any]]:
    url = (
        f"{BASE}/{quote(TEKUCHEST_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote('DeletionMark eq false', safe='')}"
        f"&$orderby=Ref_Key"
    )
    return fetch_all(session, url, page=500)


def calculate_gspp_turnover_by_month(
    docs: list[dict[str, Any]],
    dept_keys: set[str],
    year: int,
) -> dict[int, dict[str, float]]:
    result: dict[int, dict[str, float]] = defaultdict(lambda: {"plan": 0.0, "fact": 0.0})
    for doc in docs:
        if doc.get("Подразделение_Key", EMPTY) not in dept_keys:
            continue
        kind = _doc_type(doc.get("ВидДокумента"))
        if kind not in {"0", "1"}:
            continue
        rows = doc.get("Текучесть") or []
        if isinstance(rows, dict) and "value" in rows:
            rows = rows["value"]
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            month_value = str(row.get("Месяц") or "")
            if len(month_value) < 7:
                continue
            try:
                row_year = int(month_value[:4])
                month = int(month_value[5:7])
            except ValueError:
                continue
            if row_year != year:
                continue
            if not 1 <= month <= 12:
                continue
            if kind == "0":
                result[month]["plan"] += _safe_float(row.get("План"))
            elif kind == "1":
                result[month]["fact"] += _safe_float(row.get("Факт"))
    return result


def fetch_yearly_monthly_totals(session: requests.Session, year: int) -> tuple[dict[int, dict[str, float]], dict[str, Any]]:
    dept_keys, labels_by_key = load_target_department_keys(session)
    docs = load_turnover_docs(session)
    totals = calculate_gspp_turnover_by_month(docs, dept_keys, year)
    return (
        {m: {"plan": float(totals[m]["plan"]), "fact": float(totals[m]["fact"])} for m in range(1, 13)},
        {
            "target_departments": sorted(GSPP_TURNOVER_DEPARTMENTS),
            "matched_department_keys": labels_by_key,
            "documents_count": len(docs),
        },
    )


def _kpi_pct(plan: float, fact: float) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _build_gspp_q5_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = requests.Session()
    session.auth = AUTH
    by_month, debug = fetch_yearly_monthly_totals(session, ref_y)

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None
    for m in range(1, ref_m + 1):
        plan = round(float(by_month[m]["plan"]), 2)
        fact = round(float(by_month[m]["fact"]), 2)
        row = {
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": _kpi_pct(plan, fact),
            "has_data": plan > 0 or fact > 0,
            "values_unit": "%",
        }
        monthly_rows.append(row)
        if m == ref_m:
            ref_row = row

    assert ref_row is not None
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
        "ytd": {
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "%",
        },
        "debug": {
            "kpi_id": "ГСП-Q5",
            "source": "gspp/q5.py Document_ТД_ТекучестьПерсонала",
            "plan_filter": "ВидДокумента = 0",
            "fact_filter": "ВидДокумента = 1",
            **debug,
        },
    }


def gspp_q5_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_Q5_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_q5_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = gspp_q5_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any] | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=GSPP_Q5_DISK_TAG,
            version=GSPP_Q5_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_gspp_q5_payload(year=ref_y, month=ref_m)
        except Exception:
            logger.exception("ГСП-Q5: ошибка расчёта текучести")
            return None
        ytd_json_cache.save_payload(
            cache_path,
            payload,
            source_tag=GSPP_Q5_DISK_TAG,
            version=GSPP_Q5_DISK_VERSION,
        )
        return payload

    return locked_call(f"gspp_q5_tekuchest_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    parser = argparse.ArgumentParser(description="Рассчитать текучесть ГСП-Q5.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(get_gspp_q5_ytd(year=args.year, month=args.month), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
