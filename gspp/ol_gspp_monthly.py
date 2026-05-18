"""
Помесячный расчёт ОЛ ГСПП по ТЗ.

План: ВсегоОЛПоступило — количество строк регистра, где
ДатаЗавершенияПлан попала в месяц и точка этапа =
ПроверкаОпросногоЛистаДиспетчеромГСПП.

Факт: ВсегоОЛПоступило - КоличествоОЛНеВСрок, где ОЛ не в срок —
строки с ДатаЗавершенияФакт > ДатаЗавершенияПлан.

Использование:
  python gspp/ol_gspp_monthly.py
  python gspp/ol_gspp_monthly.py 2026
  python gspp/ol_gspp_monthly.py 2026-03
  python gspp/ol_gspp_monthly.py 2026-01 2026-05

Результат сохраняется в текстовый файл рядом со скриптом.
"""

from __future__ import annotations

import functools
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from getkpi.cache_manager import locked_call
from getkpi.devdir import ytd_json_cache
from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
REGISTER_ENTITY = "AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
POINTS_ENTITY = "Catalog_ТД_ТочкиЭтапов"
STAGE_POINT_PDN = "ПроверкаОпросногоЛистаДиспетчеромГСПП"
EMPTY_DATE = "0001-01-01T00:00:00"
SCRIPT_DIR = Path(__file__).resolve().parent
GSPP_M2_CACHE_PREFIX = "gspp_m2_ol_monthly"
GSPP_M2_DISK_TAG = "gspp_m2_ol_monthly_payload_v1"
GSPP_M2_DISK_VERSION = 1


def normalize_odata_base(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    if base.endswith("/odata/standard.odata"):
        return base
    return f"{base}/odata/standard.odata"


BASE = normalize_odata_base(os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL))
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)


def parse_month(value: str) -> tuple[int, int]:
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Месяц должен быть в формате ГГГГ-ММ")

    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def parse_period_args() -> tuple[tuple[int, int], tuple[int, int], str]:
    args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    now = datetime.now()

    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)

    if len(args) == 1 and len(args[0]) == 4:
        year = int(args[0])
        return (year, 1), (year, 12), args[0]

    if len(args) == 1:
        period = parse_month(args[0])
        return period, period, args[0]

    if len(args) == 2:
        start = parse_month(args[0])
        end = parse_month(args[1])
        if start > end:
            raise ValueError("Дата начала должна быть не позже даты окончания")
        return start, end, f"{args[0]}_{args[1]}"

    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ или ГГГГ-ММ ГГГГ-ММ")


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[str]:
    year, month = start
    result: list[str] = []
    while (year, month) <= end:
        result.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return result


def fetch_all(
    session: requests.Session,
    url: str,
    page: int = 5000,
    timeout: int = 120,
) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}&$format=json"
        response = session.get(page_url, timeout=timeout)
        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

        batch = response.json().get("value", [])
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)

    return rows


def normalize_text(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in value).split())


def resolve_stage_point(session: requests.Session, predefined_name: str) -> tuple[str, str]:
    url = (
        f"{BASE}/{quote(POINTS_ENTITY)}"
        f"?$select={quote('Ref_Key,Description,PredefinedDataName', safe=',_')}"
        f"&$orderby={quote('Description', safe='')}"
    )
    target = normalize_text(predefined_name)
    for row in fetch_all(session, url, page=1000, timeout=60):
        description = row.get("Description", "")
        predefined = row.get("PredefinedDataName", "")
        if normalize_text(predefined) == target or normalize_text(description) == target:
            return row["Ref_Key"], description or predefined_name

    raise RuntimeError(f"Не найдена точка этапа: {predefined_name}")


def parse_odata_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", ""))


def is_empty_date(value: str | None) -> bool:
    return not value or value.startswith(EMPTY_DATE)


def is_late(row: dict) -> bool:
    plan_raw = row.get("ДатаЗавершенияПлан")
    fact_raw = row.get("ДатаЗавершенияФакт")
    if is_empty_date(plan_raw) or is_empty_date(fact_raw):
        return False
    return parse_odata_dt(fact_raw) > parse_odata_dt(plan_raw)


def kpi_pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def load_register_rows(
    session: requests.Session,
    point_key: str,
    start_dt: date,
    end_dt: date,
) -> list[dict]:
    flt = (
        f"ДатаЗавершенияПлан ge datetime'{start_dt.isoformat()}T00:00:00'"
        f" and ДатаЗавершенияПлан le datetime'{end_dt.isoformat()}T23:59:59'"
        f" and ТочкаЭтапа_Key eq guid'{point_key}'"
    )
    url = (
        f"{BASE}/{quote(REGISTER_ENTITY)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Recorder,ДатаЗавершенияПлан,ДатаЗавершенияФакт', safe=',_')}"
    )
    return fetch_all(session, url, page=5000, timeout=120)


def build_monthly_report(
    session: requests.Session,
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> tuple[str, list[dict]]:
    point_key, point_name = resolve_stage_point(session, STAGE_POINT_PDN)
    start_dt = month_start(*start_period)
    end_dt = month_end(*end_period)
    rows = load_register_rows(session, point_key, start_dt, end_dt)

    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"plan": 0, "late": 0})
    for row in rows:
        plan_raw = row.get("ДатаЗавершенияПлан")
        if is_empty_date(plan_raw):
            continue

        month_key = plan_raw[:7]
        stats[month_key]["plan"] += 1
        if is_late(row):
            stats[month_key]["late"] += 1

    report_rows: list[dict] = []
    for month_key in iter_months(start_period, end_period):
        plan = stats[month_key]["plan"]
        late = stats[month_key]["late"]
        report_rows.append(
            {
                "month": month_key,
                "plan": plan,
                "fact": plan - late,
                "late": late,
            }
        )

    return point_name, report_rows


def format_report(point_name: str, rows: list[dict]) -> str:
    lines = [
        "ОЛ ГСПП, запущенные в производство без срыва срока",
        f"Точка этапа: {point_name}",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'Не в срок':>10}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 10}",
    ]
    for row in rows:
        lines.append(
            f"{row['month']:<10} "
            f"{row['plan']:>8} "
            f"{row['fact']:>8} "
            f"{row['late']:>10}"
        )

    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 10}",
            f"{'ИТОГО':<10} "
            f"{sum(row['plan'] for row in rows):>8} "
            f"{sum(row['fact'] for row in rows):>8} "
            f"{sum(row['late'] for row in rows):>10}",
            "",
        ]
    )
    return "\n".join(lines)


def save_report(period_slug: str, point_name: str, rows: list[dict]) -> Path:
    output_path = SCRIPT_DIR / f"ol_gspp_monthly_{period_slug}.txt"
    output_path.write_text(format_report(point_name, rows), encoding="utf-8-sig")
    return output_path


def build_gspp_m2_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = requests.Session()
    session.auth = AUTH
    point_name, rows = build_monthly_report(session, (ref_y, 1), (ref_y, ref_m))

    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        month_num = int(str(row["month"])[5:7])
        plan = int(row.get("plan") or 0)
        fact = int(row.get("fact") or 0)
        late = int(row.get("late") or 0)
        monthly_rows.append({
            "month": month_num,
            "year": ref_y,
            "month_name": MONTH_NAMES[month_num],
            "plan": plan,
            "fact": fact,
            "late": late,
            "kpi_pct": kpi_pct(fact, plan),
            "has_data": plan > 0,
            "values_unit": "шт.",
        })

    ref_row = monthly_rows[-1] if monthly_rows else {
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "plan": 0,
        "fact": 0,
        "late": 0,
        "kpi_pct": None,
        "has_data": False,
        "values_unit": "шт.",
    }
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
            "total_late": ref_row.get("late"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "ГСП-M2",
            "status": "ok",
            "source": "gspp.ol_gspp_monthly",
            "stage_point": point_name,
            "rule": "plan = rows with planned completion in month; fact = plan - late rows",
            "rows_by_month": rows,
        },
    }


def gspp_m2_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.cache_path(GSPP_M2_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_m2_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    cache_path = gspp_m2_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict[str, Any] | None:
        cached = ytd_json_cache.load_payload(
            cache_path,
            source_tag=GSPP_M2_DISK_TAG,
            version=GSPP_M2_DISK_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = build_gspp_m2_payload(year=ref_y, month=ref_m)
        except Exception as exc:
            logger.exception("ГСП-M2: ошибка расчёта ОЛ ГСПП")
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
                "debug": {"kpi_id": "ГСП-M2", "status": "error", "error": str(exc)},
            }
        ytd_json_cache.save_payload(
            cache_path,
            payload,
            source_tag=GSPP_M2_DISK_TAG,
            version=GSPP_M2_DISK_VERSION,
        )
        return payload

    return locked_call(f"gspp_m2_ol_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    try:
        start_period, end_period, period_slug = parse_period_args()
        session = requests.Session()
        session.auth = AUTH
        point_name, rows = build_monthly_report(session, start_period, end_period)
        output_path = save_report(period_slug, point_name, rows)
        print(f"Отчёт сохранён: {output_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
