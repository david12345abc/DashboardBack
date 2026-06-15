"""
Расчёт ТКП по жизненному циклу ОЛ.

Повторяет логику ТЗ:
  - ВТ_ОпросныеЛистыОтработанные: ОЛ текущего периода, завершённые на
    ФормированиеТКП или АннулированиеОпросногоЛиста.
  - ВТ_ОпросныеЛистыНеОтработанные: ОЛ текущего периода, не попавшие в
    отработанные.
  - Итог делится по ЖизненныйЦиклОЛ <= 3 и > 3.

План = ТЧМенее3Дней.Количество() + ТЧБолее3Дней.Количество()
Факт = ТЧМенее3Дней.Количество()

Использование:
  python gspp/tkp_lifecycle.py
  python gspp/tkp_lifecycle.py 2026-03

Результат выводится в консоль.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sys
import time
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
from devdir import ytd_json_cache
from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
REGISTER_ENTITY = "AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
VERSIONS_ENTITY = "InformationRegister_ВерсииОбъектов"
CALENDAR_ENTITY = "InformationRegister_КалендарныеГрафики"
POINTS_ENTITY = "Catalog_ТД_ТочкиЭтапов"

CAL_KEY = "d658bace-6313-11e7-812d-001e67112509"  # Пятидневка
CALENDAR_YEAR_DAYS = 247
LIMIT_BOTTOM = date(2021, 11, 1)
EMPTY_DATE = "0001-01-01T00:00:00"
ANNULLED_STATUS = "Аннулирован"
STAGE_TKP = "ФормированиеТКП"
STAGE_ANNUL = "АннулированиеОпросногоЛиста"
GSPP_M1_CACHE_PREFIX = "gspp_m1_ytd"
GSPP_M1_DISK_TAG = "gspp_m1_tkp_lifecycle_payload_v1"
GSPP_M1_DISK_VERSION = 1

DOC_TYPES = [
    "Document_ТД_КартаЗаказаUFG",
    "Document_ТД_КартаЗаказаCFM",
    "Document_ТД_КартаЗаказаUFGH",
    "Document_ТД_КартаЗаказаTFG",
    "Document_ТД_КартаЗаказаUFL",
    "Document_ТД_КартаЗаказаПлотномер",
]

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


def parse_period_arg() -> tuple[int, int]:
    args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    if not args:
        now = datetime.now()
        return now.year, now.month

    if len(args) != 1:
        raise ValueError("Используйте один аргумент периода в формате ГГГГ-ММ")

    value = args[0]
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Период должен быть в формате ГГГГ-ММ")

    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def month_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def fetch_all(
    session: requests.Session,
    url: str,
    page: int = 5000,
    timeout: int = 120,
    progress_label: str | None = None,
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
        if progress_label:
            print(f"  {progress_label}: загружено {len(rows)}")
        if len(batch) < page:
            break
        skip += len(batch)

    return rows


def normalize_text(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in value).split())


def parse_dt(value: str) -> date:
    return datetime.fromisoformat(value.replace("Z", "")).date()


def is_empty_date(value: str | None) -> bool:
    return value is None or value == "" or value.startswith(EMPTY_DATE)


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


def load_calendar(session: requests.Session, years: list[int]) -> dict[date, int]:
    cumulative: dict[date, int] = {}
    for year in years:
        flt = f"Календарь_Key eq guid'{CAL_KEY}' and Год eq {year}"
        url = (
            f"{BASE}/{quote(CALENDAR_ENTITY)}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select={quote('ДатаГрафика,КоличествоДнейВГрафикеСНачалаГода', safe=',_')}"
        )
        for row in fetch_all(session, url, page=1000, timeout=60):
            cumulative[date.fromisoformat(row["ДатаГрафика"][:10])] = int(
                row["КоличествоДнейВГрафикеСНачалаГода"]
            )
    return cumulative


def calendar_value(day: date, cumulative: dict[date, int]) -> int:
    if day in cumulative:
        return cumulative[day]
    closest = max((key for key in cumulative if key <= day), default=None)
    if closest is None:
        return 0
    return cumulative[closest]


def business_days_calendar(
    start: date,
    end: date,
    cumulative: dict[date, int],
) -> int:
    if start >= end:
        return 0
    return (
        calendar_value(end, cumulative)
        - calendar_value(start, cumulative)
        + CALENDAR_YEAR_DAYS * (end.year - start.year)
    )


def load_version_dates(
    session: requests.Session,
    start_dt: date,
    end_dt: date,
    progress_label: str | None = None,
) -> dict[str, date]:
    flt = (
        "НомерВерсии eq 1"
        f" and ДатаВерсии ge datetime'{start_dt.isoformat()}T00:00:00'"
        f" and ДатаВерсии le datetime'{end_dt.isoformat()}T23:59:59'"
    )
    url = (
        f"{BASE}/{quote(VERSIONS_ENTITY)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$orderby={quote('ДатаВерсии', safe='')}"
        f"&$select={quote('Объект,Объект_Type,ДатаВерсии', safe=',_')}"
    )
    doc_type_set = {f"StandardODATA.{doc_type}" for doc_type in DOC_TYPES}
    version_dates: dict[str, date] = {}
    for row in fetch_all(
        session,
        url,
        page=5000,
        timeout=120,
        progress_label=progress_label,
    ):
        if row.get("Объект_Type") in doc_type_set:
            version_dates[row["Объект"]] = parse_dt(row["ДатаВерсии"])
    return version_dates


def load_documents(
    session: requests.Session,
    start_dt: date,
    end_dt: date,
    needed_refs: set[str],
    progress_label: str | None = None,
) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    if not needed_refs:
        return docs

    refs = sorted(needed_refs)
    chunk_size = 25
    for doc_type in DOC_TYPES:
        short_name = doc_type.replace("Document_ТД_КартаЗаказа", "")
        found_before = len(docs)
        for idx in range(0, len(refs), chunk_size):
            chunk = refs[idx : idx + chunk_size]
            ref_filter = " or ".join(f"Ref_Key eq guid'{ref_key}'" for ref_key in chunk)
            url = (
                f"{BASE}/{quote(doc_type)}"
                f"?$filter={quote(ref_filter, safe='')}"
                f"&$select={quote('Ref_Key,Date,Number,Статус', safe=',_')}"
            )
            for row in fetch_all(session, url, page=chunk_size, timeout=60):
                ref_key = row.get("Ref_Key")
                if ref_key not in needed_refs:
                    continue
                docs[ref_key] = {
                    "date": parse_dt(row["Date"]),
                    "number": row.get("Number", ""),
                    "status": row.get("Статус", ""),
                    "doc_type": doc_type,
                }
        if progress_label:
            found_now = len(docs) - found_before
            print(f"  {progress_label}: {short_name}, найдено {found_now}")
    return docs


def load_completed_stage_dates(
    session: requests.Session,
    point_key: str,
    min_fact_dt: date | None = None,
    max_fact_dt: date | None = None,
    progress_label: str | None = None,
) -> dict[str, set[str]]:
    filters = [
        f"ТочкаЭтапа_Key eq guid'{point_key}'"
        f" and ДатаЗавершенияФакт ne datetime'{EMPTY_DATE}'",
    ]
    if min_fact_dt:
        filters.append(
            f"ДатаЗавершенияФакт ge datetime'{min_fact_dt.isoformat()}T00:00:00'"
        )
    if max_fact_dt:
        filters.append(
            f"ДатаЗавершенияФакт le datetime'{max_fact_dt.isoformat()}T23:59:59'"
        )
    flt = " and ".join(filters)
    url = (
        f"{BASE}/{quote(REGISTER_ENTITY)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Recorder,ДатаЗавершенияФакт', safe=',_')}"
    )
    completions: dict[str, set[str]] = defaultdict(set)
    for row in fetch_all(
        session,
        url,
        page=5000,
        timeout=120,
        progress_label=progress_label,
    ):
        completions[row["Recorder"]].add(row["ДатаЗавершенияФакт"])
    return completions


def build_completed_and_open_rows(
    current_versions: dict[str, date],
    docs: dict[str, dict],
    tkp_completions: dict[str, set[str]],
    annul_completions: dict[str, set[str]],
    cumulative: dict[date, int],
    current_dt: date,
) -> tuple[list[dict], list[dict]]:
    completed_rows: list[dict] = []
    open_current_rows: list[dict] = []

    for ref_key, creation_date in current_versions.items():
        doc_info = docs.get(ref_key)
        if not doc_info:
            continue

        status = doc_info.get("status", "")
        fact_dates = (
            annul_completions.get(ref_key, set())
            if status == ANNULLED_STATUS
            else tkp_completions.get(ref_key, set())
        )

        seen_lifecycles: set[int] = set()
        for fact_raw in fact_dates:
            if is_empty_date(fact_raw):
                continue
            lifecycle = business_days_calendar(creation_date, parse_dt(fact_raw), cumulative)
            if lifecycle in seen_lifecycles:
                continue
            seen_lifecycles.add(lifecycle)
            completed_rows.append(
                {
                    "ol": ref_key,
                    "lifecycle": lifecycle,
                    "source": "Отработанные ОЛ текущего периода",
                }
            )

        if not seen_lifecycles:
            open_current_rows.append(
                {
                    "ol": ref_key,
                    "lifecycle": business_days_calendar(
                        creation_date,
                        current_dt,
                        cumulative,
                    ),
                    "source": "ОЛ в работе текущего периода",
                }
            )

    return completed_rows, open_current_rows


def calculate_tkp(session: requests.Session, year: int, month: int, *, verbose: bool = True) -> dict:
    def emit(message: str) -> None:
        if verbose:
            print(message)

    period_start, period_end = month_bounds(year, month)
    current_dt = datetime.now().date()
    years = list(range(LIMIT_BOTTOM.year, max(period_end.year, current_dt.year) + 2))

    emit(f"Период: {period_start.isoformat()} - {period_end.isoformat()}")
    emit("[1/6] Поиск точек этапов ...")
    tkp_key, tkp_name = resolve_stage_point(session, STAGE_TKP)
    annul_key, annul_name = resolve_stage_point(session, STAGE_ANNUL)
    emit(f"  Формирование ТКП: {tkp_name}")
    emit(f"  Аннулирование ОЛ: {annul_name}")

    emit("[2/6] Загрузка производственного календаря ...")
    cumulative = load_calendar(session, years)
    emit(f"  Дней календаря загружено: {len(cumulative)}")

    emit("[3/6] Загрузка ОЛ текущего периода из ВерсииОбъектов ...")
    current_versions = load_version_dates(
        session,
        period_start,
        period_end,
        progress_label="версии текущего периода" if verbose else None,
    )
    emit(f"  ОЛ текущего периода: {len(current_versions)}")

    emit("[4/6] Загрузка карточек ОЛ текущего периода ...")
    current_docs = load_documents(
        session,
        LIMIT_BOTTOM,
        current_dt,
        set(current_versions),
        progress_label="карточки текущего периода" if verbose else None,
    )
    emit(f"  Карточек текущего периода найдено: {len(current_docs)}")

    emit("[5/6] Загрузка завершённых этапов текущего периода ...")
    # ОЛ, созданный в периоде, не может быть завершён раньше начала периода.
    # Ограничение по фактической дате резко сокращает объём OData-выборки.
    tkp_completions = load_completed_stage_dates(
        session,
        tkp_key,
        min_fact_dt=period_start,
        max_fact_dt=current_dt,
        progress_label="завершения ФормированиеТКП" if verbose else None,
    )
    annul_completions = load_completed_stage_dates(
        session,
        annul_key,
        min_fact_dt=period_start,
        max_fact_dt=current_dt,
        progress_label="завершения АннулированиеОЛ" if verbose else None,
    )
    completed_rows, open_current_rows = build_completed_and_open_rows(
        current_versions,
        current_docs,
        tkp_completions,
        annul_completions,
        cumulative,
        current_dt,
    )
    emit(f"  Отработанные текущего периода: {len(completed_rows)}")
    emit(f"  Неотработанные текущего периода: {len(open_current_rows)}")

    emit("[6/6] Расчёт итогов ...")
    # Контрольные значения 1С считаются по отработанным ОЛ текущего периода.
    # Неотработанные ОЛ показываем ниже в расшифровке, но в план/факт не включаем.
    fact = sum(1 for row in completed_rows if row["lifecycle"] <= 3)
    plan = len(completed_rows)

    return {
        "period": f"{year:04d}-{month:02d}",
        "period_start": period_start,
        "period_end": period_end,
        "current_dt": current_dt,
        "tkp_name": tkp_name,
        "annul_name": annul_name,
        "plan": plan,
        "fact": fact,
        "over_3": plan - fact,
        "sla": (fact / plan * 100) if plan else 0.0,
        "completed_count": len(completed_rows),
        "open_current_count": len(open_current_rows),
        "missing_docs_count": len(set(current_versions) - set(current_docs)),
    }


def format_report(result: dict, elapsed: float) -> str:
    return (
        "ТКП по жизненному циклу ОЛ\n"
        f"Период: {result['period_start'].isoformat()} - {result['period_end'].isoformat()}\n"
        f"Текущая дата для неотработанных/долгов: {result['current_dt'].isoformat()}\n"
        f"Точка формирования ТКП: {result['tkp_name']}\n"
        f"Точка аннулирования ОЛ: {result['annul_name']}\n"
        "\n"
        "Итог по ТЗ\n"
        f"План: {result['plan']}\n"
        f"Факт: {result['fact']}\n"
        f"Более 3 дней: {result['over_3']}\n"
        f"SLA: {result['sla']:.1f}%\n"
        "\n"
        "Расшифровка состава плана\n"
        f"Отработанные ОЛ текущего периода: {result['completed_count']}\n"
        f"ОЛ в работе текущего периода: {result['open_current_count']}\n"
        f"Не найдены карточки документов: {result['missing_docs_count']}\n"
        "\n"
        f"Время выполнения: {elapsed:.1f} сек.\n"
    )


def _kpi_pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def build_gspp_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = requests.Session()
    session.auth = AUTH

    monthly_rows: list[dict[str, Any]] = []
    month_debug: dict[int, dict[str, Any]] = {}
    for m in range(1, ref_m + 1):
        result = calculate_tkp(session, ref_y, m, verbose=False)
        plan = int(result.get("plan") or 0)
        fact = int(result.get("fact") or 0)
        over_3 = int(result.get("over_3") or 0)
        monthly_rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "over_3": over_3,
            "kpi_pct": _kpi_pct(fact, plan),
            "has_data": plan > 0,
            "values_unit": "шт.",
        })
        month_debug[m] = {
            "period": result.get("period"),
            "period_start": result.get("period_start").isoformat() if result.get("period_start") else None,
            "period_end": result.get("period_end").isoformat() if result.get("period_end") else None,
            "completed_count": result.get("completed_count"),
            "open_current_count": result.get("open_current_count"),
            "missing_docs_count": result.get("missing_docs_count"),
            "tkp_stage": result.get("tkp_name"),
            "annul_stage": result.get("annul_name"),
        }

    ref_row = monthly_rows[-1] if monthly_rows else {
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "plan": 0,
        "fact": 0,
        "over_3": 0,
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
            "total_over_3": ref_row.get("over_3"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "ГСП-M1",
            "status": "ok",
            "source": "gspp.tkp_lifecycle",
            "formula": "plan = completed OL current period; fact = completed OL with lifecycle <= 3 business days",
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
            logger.exception("ГСП-M1: ошибка расчёта ТКП lifecycle")
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

    return locked_call(f"gspp_m1_tkp_lifecycle_{ref_y}_{ref_m:02d}", _runner)


def main() -> None:
    started_at = time.time()
    try:
        year, month = parse_period_arg()
        session = requests.Session()
        session.auth = AUTH
        result = calculate_tkp(session, year, month)
        print(format_report(result, time.time() - started_at))
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
