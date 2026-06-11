"""
tkp.py — расчет ТКП из 1С по логике запроса из задания.

Скрипт считает:
  - План: количество ОЛ, у которых ДатаЗавершенияПлан попала в месяц
    и точка этапа = ПроверкаОпросногоЛистаДиспетчеромГСПП.
  - Факт: процент ОЛ без срыва срока по формуле
      100 - КоличествоОЛНеВСрок / ВсегоОЛПоступило * 100

Запуск:
  python gspp/tkp.py [ГГГГ-ММ]

Пример:
  python gspp/tkp.py 2026-03
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from getkpi.cache_manager import locked_call
from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
BASE = (
    os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL)
    .strip()
    .rstrip("/")
)
if not BASE.endswith("/odata/standard.odata"):
    BASE = f"{BASE}/odata/standard.odata"

AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)

EMPTY = "00000000-0000-0000-0000-000000000000"
EMPTY_DATE = "0001-01-01T00:00:00"

# Из probe-скриптов для этого участка
CAL_KEY = "d658bace-6313-11e7-812d-001e67112509"  # Пятидневка
STAGE_POINT_PDN = "ПроверкаОпросногоЛистаДиспетчеромГСПП"
REGISTER_ENTITY = "AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
POINTS_ENTITY = "Catalog_ТД_ТочкиЭтапов"
GSPP_M1_CACHE_PREFIX = "gspp_m1_ytd"
GSPP_M1_DISK_TAG = "gspp_m1_tkp_plan_fact_payload_v1"
GSPP_M1_DISK_VERSION = 1

# Типы документов, у которых живёт регистр ТД_МониторингЭтаповОпросныхЛистов
DOC_TYPES = [
    "Document_ТД_КартаЗаказаUFG",
    "Document_ТД_КартаЗаказаCFM",
    "Document_ТД_КартаЗаказаUFGH",
    "Document_ТД_КартаЗаказаTFG",
    "Document_ТД_КартаЗаказаUFL",
    "Document_ТД_КартаЗаказаПлотномер",
]

MONTH_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def parse_period_arg(args: list[str] | None = None) -> tuple[int, int]:
    args = [arg.strip() for arg in (args or []) if arg.strip()]
    if len(args) == 1 and len(args[0]) == 7 and args[0][4] == "-":
        year = int(args[0][:4])
        month = int(args[0][5:7])
        if not 1 <= month <= 12:
            raise ValueError("Месяц должен быть от 1 до 12")
        return year, month

    now = datetime.now()
    return now.year, now.month


def month_bounds(year: int, month: int) -> tuple[date, date]:
    period_start = date(year, month, 1)
    if month == 12:
        period_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        period_end = date(year, month + 1, 1) - timedelta(days=1)
    return period_start, period_end


def _kpi_pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def load_paginated(
    session: requests.Session,
    url: str,
    page_size: int = 5000,
    timeout: int = 120,
) -> list[dict]:
    items: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page_size}&$skip={skip}&$format=json"
        r = session.get(page_url, timeout=timeout)
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
        batch = r.json().get("value", [])
        if not batch:
            break
        items.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return items


def parse_dt(value: str) -> date:
    return datetime.fromisoformat(value.replace("Z", "")).date()


def is_empty_date(value: str | None) -> bool:
    return value is None or value == "" or value.startswith("0001-01-01")


def business_days_calendar(d1: date, d2: date, cumulative: dict[date, int]) -> int:
    """ЖЦ = КолДней(d2) - КолДней(d1) + 247 * (Год(d2) - Год(d1))."""
    if d1 >= d2:
        return 0

    def cal_value(d: date) -> int:
        if d in cumulative:
            return cumulative[d]
        closest = max((key for key in cumulative if key <= d), default=None)
        if closest is None:
            return 0
        return cumulative[closest]

    return cal_value(d2) - cal_value(d1) + 247 * (d2.year - d1.year)


def load_calendar(session: requests.Session, years: list[int]) -> dict[date, int]:
    cumulative: dict[date, int] = {}
    for yr in years:
        flt = f"Календарь_Key eq guid'{CAL_KEY}' and Год eq {yr}"
        url = (
            f"{BASE}/{quote('InformationRegister_КалендарныеГрафики')}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select={quote('ДатаГрафика,КоличествоДнейВГрафикеСНачалаГода', safe='')}"
        )
        for item in load_paginated(session, url, page_size=1000, timeout=60):
            cumulative[date.fromisoformat(item["ДатаГрафика"][:10])] = int(
                item["КоличествоДнейВГрафикеСНачалаГода"]
            )
    return cumulative


def load_version_dates(
    session: requests.Session,
    start_dt: date,
    end_dt: date,
) -> dict[str, date]:
    flt = (
        "НомерВерсии eq 1"
        f" and ДатаВерсии ge datetime'{start_dt.isoformat()}T00:00:00'"
        f" and ДатаВерсии le datetime'{end_dt.isoformat()}T23:59:59'"
    )
    url = (
        f"{BASE}/{quote('InformationRegister_ВерсииОбъектов')}"
        f"?$filter={quote(flt, safe='')}"
        f"&$orderby={quote('ДатаВерсии', safe='')}"
        f"&$select={quote('Объект,Объект_Type,ДатаВерсии', safe='')}"
    )
    type_set = {f"StandardODATA.{doc_type}" for doc_type in DOC_TYPES}
    version_dates: dict[str, date] = {}
    for row in load_paginated(session, url, page_size=5000, timeout=60):
        if row.get("Объект_Type") in type_set:
            version_dates[row["Объект"]] = parse_dt(row["ДатаВерсии"])
    return version_dates


def load_documents(session: requests.Session, end_dt: date) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    flt = (
        f"Date ge datetime'2021-11-01T00:00:00'"
        f" and Date le datetime'{end_dt.isoformat()}T23:59:59'"
    )
    for doc_type in DOC_TYPES:
        url = (
            f"{BASE}/{quote(doc_type)}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select={quote('Ref_Key,Date,Number,Статус', safe='')}"
        )
        for row in load_paginated(session, url, page_size=1000, timeout=60):
            docs[row["Ref_Key"]] = {
                "date": parse_dt(row["Date"]),
                "status": row.get("Статус", ""),
                "number": row.get("Number", ""),
                "doc_type": doc_type,
            }
    return docs


def load_stage_completions(
    session: requests.Session,
    point_key: str,
) -> dict[str, set[str]]:
    flt = (
        f"ТочкаЭтапа_Key eq guid'{point_key}'"
        f" and ДатаЗавершенияФакт ne datetime'{EMPTY_DATE}'"
    )
    url = (
        f"{BASE}/{quote('AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType')}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Recorder,ДатаЗавершенияФакт', safe='')}"
    )
    completions: dict[str, set[str]] = defaultdict(set)
    for row in load_paginated(session, url, page_size=5000, timeout=60):
        completions[row["Recorder"]].add(row["ДатаЗавершенияФакт"])
    return completions


def load_debt_recorders(session: requests.Session) -> set[str]:
    flt = (
        f"ДатаЗавершенияФакт eq datetime'{EMPTY_DATE}'"
        f" and ДатаЗавершенияПлан ne datetime'{EMPTY_DATE}'"
    )
    url = (
        f"{BASE}/{quote('AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType')}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Recorder', safe='')}"
    )
    return {row["Recorder"] for row in load_paginated(session, url, page_size=5000, timeout=60)}


def normalize_text(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join(
        "".join(ch if ch.isalnum() else " " for ch in value).split()
    )


def resolve_stage_point(session: requests.Session, predefined_name: str) -> tuple[str, str]:
    url = (
        f"{BASE}/{quote(POINTS_ENTITY)}"
        f"?$select={quote('Ref_Key,Description,PredefinedDataName', safe='')}"
        f"&$orderby={quote('Description', safe='')}"
    )
    target = normalize_text(predefined_name)
    for row in load_paginated(session, url, page_size=1000, timeout=60):
        desc = row.get("Description", "")
        pdn = row.get("PredefinedDataName", "")
        if normalize_text(pdn) == target or normalize_text(desc) == target:
            return row["Ref_Key"], desc or predefined_name
    raise RuntimeError(f"Не найдена точка этапа: {predefined_name}")


def load_tkp_plan_rows(
    session: requests.Session,
    point_key: str,
    period_start: date,
    period_end: date,
) -> dict[str, dict]:
    flt = (
        f"ДатаЗавершенияПлан ge datetime'{period_start.isoformat()}T00:00:00'"
        f" and ДатаЗавершенияПлан le datetime'{period_end.isoformat()}T23:59:59'"
        f" and ТочкаЭтапа_Key eq guid'{point_key}'"
    )
    url = (
        f"{BASE}/{quote(REGISTER_ENTITY)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Recorder,ДатаЗавершенияПлан,ДатаЗавершенияФакт,ТочкаЭтапа_Key', safe='')}"
    )
    raw_rows = load_paginated(session, url, page_size=5000, timeout=60)

    rows_by_recorder: dict[str, dict] = {}
    for row in raw_rows:
        recorder = row.get("Recorder", "")
        if not recorder:
            continue
        prev = rows_by_recorder.get(recorder)
        if prev is None:
            rows_by_recorder[recorder] = row
            continue

        # Если регистр прислал несколько строк на один ОЛ,
        # оставляем строку с более поздней датой плана.
        prev_plan = prev.get("ДатаЗавершенияПлан", "")
        curr_plan = row.get("ДатаЗавершенияПлан", "")
        if curr_plan > prev_plan:
            rows_by_recorder[recorder] = row

    return rows_by_recorder


def build_month_row(
    session: requests.Session,
    point_key: str,
    year: int,
    month: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    period_start, period_end = month_bounds(year, month)
    rows_by_recorder = load_tkp_plan_rows(session, point_key, period_start, period_end)

    late_count = 0
    unfinished_count = 0
    for row in rows_by_recorder.values():
        plan_raw = row.get("ДатаЗавершенияПлан")
        fact_raw = row.get("ДатаЗавершенияФакт")
        if is_empty_date(plan_raw):
            continue
        if is_empty_date(fact_raw):
            unfinished_count += 1
            continue
        if parse_dt(fact_raw) > parse_dt(plan_raw):
            late_count += 1

    plan = len(rows_by_recorder)
    fact = max(plan - late_count, 0)
    row = {
        "month": month,
        "year": year,
        "month_name": MONTH_NAMES[month],
        "plan": plan,
        "fact": fact,
        "kpi_pct": _kpi_pct(fact, plan),
        "has_data": plan > 0,
        "values_unit": "шт.",
    }
    debug = {
        "status": "ok",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "rows_count": plan,
        "late_count": late_count,
        "unfinished_count": unfinished_count,
        "rule": "plan = count of unique OL with plan completion in month; fact = plan - late OL",
    }
    return row, debug


def build_gspp_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = requests.Session()
    session.auth = AUTH

    point_key, point_name = resolve_stage_point(session, STAGE_POINT_PDN)
    monthly_rows: list[dict[str, Any]] = []
    month_debug: dict[int, dict[str, Any]] = {}
    for m in range(1, ref_m + 1):
        row, debug = build_month_row(session, point_key, ref_y, m)
        monthly_rows.append(row)
        month_debug[m] = debug

    ref_row = monthly_rows[-1] if monthly_rows else {
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "plan": 0,
        "fact": 0,
        "kpi_pct": None,
        "has_data": False,
        "values_unit": "шт.",
    }
    with_data = [row for row in monthly_rows if row.get("has_data")]
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
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
            "months_with_data": len(with_data),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "ГСП-M1",
            "status": "ok",
            "source": "gspp.tkp",
            "stage_point": point_name,
            "stage_point_key": point_key,
            "formula": "kpi_pct = fact / plan * 100",
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
            payload = build_gspp_m1_payload(year=ref_y, month=ref_m)
        except Exception as exc:
            logger.exception("ГСП-M1: ошибка расчёта ТКП")
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
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 0,
                    "values_unit": "шт.",
                },
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
    parser = argparse.ArgumentParser(description="Рассчитать ГСП-M1: ТКП план/факт.")
    parser.add_argument("period", nargs="?", help="Период в формате ГГГГ-ММ")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()

    year = args.year
    month = args.month
    if args.period and (year is None or month is None):
        year, month = parse_period_arg([args.period])

    payload = get_gspp_m1_ytd(year=year, month=month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
