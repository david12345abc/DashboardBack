"""Срок и статусы плиток «всего» / «в работе» для форм 03-17, 03-18, 03-19.

Всего
  план — месяц даты документа + 2 рабочих дня;
         все статусы, кроме «Отменена», «НеСогласовано», «Подготовлен»;
  факт — месяц «ДатаУстраненияФакт», статус «Выполнено» (в интерфейсе «Завершено»).

В работе
  статусы согласования и КМ, кроме «Исполнение КМ», делятся по сроку
  «дата документа + 2 рабочих дня + 30 календарных дней»:
  не превышает срок — без просрочки, превышает — просрочка;
  «Исполнение КМ» всегда без просрочки.
  В месяц попадают только формы, у которых дата документа + 2 рабочих дня
  лежит в этом месяце, и текущий статус всё ещё из списка «в работе».
  Просрочка считается на конец месяца, для незакрытого месяца — на сегодня.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable
from urllib.parse import quote

from qualdir.form_status import STATUS_BY_ORDER

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
PLAN_WORKDAYS = 2
OVERDUE_CALENDAR_DAYS = 30
PLAN_LOOKBACK_DAYS = 21

PLAN_EXCLUDED_STATUSES = frozenset(
    {
        "НеСогласовано",
        "Отменена",
        "Подготовлен",
        "Подготовлено",
    }
)
EXECUTED_STATUS = "Выполнено"
EXECUTION_STATUS = "ИсполнениеКМ"
IN_WORK_DEADLINE_STATUSES = frozenset(
    {
        "НаСогласовании",
        "НаСогласованииПотребителя",
        "НаРассмотренииПоставщика",
        "НаПроверкеОформления",
        "ТребуетсяКорректировка",
        "СпорТребуетсяРешение",
        "РазработкаКМ",
        "СогласованиеКМ",
    }
)
IN_WORK_STATUSES = IN_WORK_DEADLINE_STATUSES | {EXECUTION_STATUS}

CAL_KEY = "d658bace-6313-11e7-812d-001e67112509"
_CALENDAR_CACHE: dict[tuple[int, int], Callable[[date], bool]] = {}


def normalize_text(value: str | None) -> str:
    text = (value or "").strip().lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in text).split())


_PLAN_EXCLUDED_NORM = frozenset(normalize_text(name) for name in PLAN_EXCLUDED_STATUSES)
_IN_WORK_NORM = {normalize_text(name): name for name in IN_WORK_STATUSES}
_EXECUTED_NORM = normalize_text(EXECUTED_STATUS)
_EXECUTION_NORM = normalize_text(EXECUTION_STATUS)


def is_plan_status(status_name: str | None) -> bool:
    if not status_name:
        return False
    return normalize_text(status_name) not in _PLAN_EXCLUDED_NORM


def is_executed_status(status_name: str | None) -> bool:
    return normalize_text(status_name) == _EXECUTED_NORM


def canonical_in_work_status(status_name: str | None) -> str | None:
    if not status_name:
        return None
    return _IN_WORK_NORM.get(normalize_text(status_name))


def weekday_is_working(day: date) -> bool:
    return day.weekday() < 5


def add_working_days(
    start: date,
    days: int,
    is_working: Callable[[date], bool] | None = None,
) -> date:
    """Дата через N рабочих дней после start. Сам start не считается."""
    if days <= 0:
        return start
    working = is_working or weekday_is_working
    day = start
    left = days
    while left:
        day += timedelta(days=1)
        if working(day):
            left -= 1
    return day


def deadline_date(created: date, is_working: Callable[[date], bool] | None = None) -> date:
    """Дата создания + 2 рабочих дня + 30 календарных дней."""
    return add_working_days(created, PLAN_WORKDAYS, is_working) + timedelta(days=OVERDUE_CALENDAR_DAYS)


def plan_bucket_date(created: date, is_working: Callable[[date], bool] | None = None) -> date:
    return add_working_days(created, PLAN_WORKDAYS, is_working)


def in_work_on_time(
    status_name: str | None,
    created: date,
    as_of: date,
    is_working: Callable[[date], bool] | None = None,
) -> bool | None:
    """True — без просрочки, False — просрочено, None — статус не из «в работе»."""
    status = canonical_in_work_status(status_name)
    if status is None:
        return None
    if normalize_text(status) == _EXECUTION_NORM:
        return True
    return as_of <= deadline_date(created, is_working)


def kpi_pct(plan: int | None, fact: int | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(fact / plan * 100.0, 1)


def month_key(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def from_sql_datetime(value: Any) -> date | None:
    if value is None:
        return None
    year = int(value.year) - YEAR_OFFSET
    if year < 1900:
        return None
    return date(year, int(value.month), int(value.day))


def to_sql_datetime(value: date):
    from datetime import datetime

    return datetime(value.year + YEAR_OFFSET, value.month, value.day)


def _working_days_from_counts(rows: list[tuple[date, int]]) -> tuple[date, date, set[date]] | None:
    if not rows:
        return None
    by_year: dict[int, list[tuple[date, int]]] = defaultdict(list)
    for day, count in rows:
        by_year[day.year].append((day, int(count)))
    working: set[date] = set()
    span_days: list[date] = []
    for items in by_year.values():
        items.sort(key=lambda item: item[0])
        prev_count = 0
        for day, count in items:
            span_days.append(day)
            if count > prev_count:
                working.add(day)
            prev_count = count
    if not span_days:
        return None
    return min(span_days), max(span_days), working


def _load_calendar_rows(year_from: int, year_to: int) -> list[tuple[date, int]]:
    import requests
    from requests.auth import HTTPBasicAuth

    base = os.getenv("ONEC_BASE_URL", "http://192.168.2.229:81/erp_pm/odata/standard.odata").rstrip("/")
    if not base.endswith("/odata/standard.odata"):
        base = f"{base.rstrip('/')}/odata/standard.odata"
    session = requests.Session()
    session.auth = HTTPBasicAuth(
        os.getenv("ODATA_USER", "odata.user"),
        os.getenv("ODATA_PASSWORD", "npo852456"),
    )
    found: list[tuple[date, int]] = []
    for year in range(year_from, year_to + 1):
        flt = f"Календарь_Key eq guid'{CAL_KEY}' and Год eq {year}"
        url = (
            f"{base}/{quote('InformationRegister_КалендарныеГрафики')}"
            f"?$format=json&$top=400"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote('ДатаГрафика,КоличествоДнейВГрафикеСНачалаГода', safe=',_')}"
        )
        response = session.get(url, timeout=60)
        response.raise_for_status()
        for item in response.json().get("value") or []:
            found.append(
                (
                    date.fromisoformat(str(item["ДатаГрафика"])[:10]),
                    int(item["КоличествоДнейВГрафикеСНачалаГода"]),
                )
            )
    return found


def is_working_for_years(year_from: int, year_to: int) -> Callable[[date], bool]:
    """Пятидневка из календаря 1С. Если календарь недоступен — пн–пт."""
    key = (year_from, year_to)
    cached = _CALENDAR_CACHE.get(key)
    if cached is not None:
        return cached

    checker = weekday_is_working
    try:
        parsed = _working_days_from_counts(_load_calendar_rows(year_from, year_to))
    except Exception as exc:  # noqa: BLE001 — плитка должна посчитаться и без OData
        logger.warning("Календарь форм 03-17/18/19 недоступен, берём пн–пт: %s", exc)
        parsed = None
    if parsed is not None:
        span_start, span_end, working = parsed

        def checker(day: date, _start=span_start, _end=span_end, _working=working) -> bool:
            if _start <= day <= _end:
                return day in _working
            return weekday_is_working(day)

    _CALENDAR_CACHE[key] = checker
    return checker


@dataclass(frozen=True)
class FormSqlSpec:
    doc_table: str
    dept_table: str
    col_date: str
    col_marked: str
    col_status: str
    col_significant: str
    col_dept: str
    col_elimination: str


@dataclass(frozen=True)
class FormHit:
    created: date
    status: str
    elimination: date | None
    dept_name: str | None
    significant: bool
    doc_id: bytes
    plan_month: str | None
    fact_month: str | None


def fetch_hits(
    cur,
    spec: FormSqlSpec,
    status_bins: dict[str, bytes],
    period_start: date,
    period_end: date,
    is_working: Callable[[date], bool] | None = None,
) -> list[FormHit]:
    """Документы, которые могут попасть в план, факт или «в работе» периода."""
    working = is_working or is_working_for_years(period_start.year - 1, period_end.year + 1)
    bin_to_status = {blob: name for name, blob in status_bins.items()}
    in_work_bins = [status_bins[name] for name in IN_WORK_STATUSES if name in status_bins]
    plan_from = period_start - timedelta(days=PLAN_LOOKBACK_DAYS)
    end_exclusive = period_end + timedelta(days=1)
    placeholders = ",".join("?" * len(in_work_bins)) if in_work_bins else None
    status_sql = f"OR doc.[{spec.col_status}] IN ({placeholders})" if placeholders else ""
    cur.execute(
        f"""
        SELECT
            doc._IDRRef,
            doc.[{spec.col_date}],
            doc.[{spec.col_status}],
            doc.[{spec.col_significant}],
            doc.[{spec.col_elimination}],
            dept._Description
        FROM [{spec.doc_table}] doc WITH (NOLOCK)
        LEFT JOIN [{spec.dept_table}] dept WITH (NOLOCK)
            ON dept._IDRRef = doc.[{spec.col_dept}]
        WHERE doc.[{spec.col_marked}] = 0x00
          AND (
                (doc.[{spec.col_date}] >= ? AND doc.[{spec.col_date}] < ?)
             OR (doc.[{spec.col_elimination}] >= ? AND doc.[{spec.col_elimination}] < ?)
             {status_sql}
          )
        """,
        to_sql_datetime(plan_from),
        to_sql_datetime(end_exclusive),
        to_sql_datetime(period_start),
        to_sql_datetime(end_exclusive),
        *in_work_bins,
    )

    hits: list[FormHit] = []
    for idr, created_raw, status_bin, sig_raw, elimination_raw, dept_name in cur.fetchall():
        created = from_sql_datetime(created_raw)
        if created is None or status_bin is None or idr is None:
            continue
        status_name = bin_to_status.get(bytes(status_bin)) or ""
        if not status_name:
            continue
        elimination = from_sql_datetime(elimination_raw)
        plan_month = None
        if is_plan_status(status_name):
            planned_on = plan_bucket_date(created, working)
            if period_start <= planned_on <= period_end:
                plan_month = month_key(planned_on)
        fact_month = None
        if is_executed_status(status_name) and elimination is not None:
            if period_start <= elimination <= period_end:
                fact_month = month_key(elimination)
        dept = (dept_name or "").strip() or None
        significant = bytes(sig_raw) != b"\x00" if sig_raw is not None else False
        hits.append(
            FormHit(
                created=created,
                status=status_name,
                elimination=elimination,
                dept_name=dept,
                significant=significant,
                doc_id=bytes(idr),
                plan_month=plan_month,
                fact_month=fact_month,
            )
        )
    return hits


def aggregate_hits(
    hits: list[FormHit],
    month_keys: list[str],
    *,
    dept_key: Callable[[str | None], str],
    today: date,
    is_working: Callable[[date], bool] | None = None,
) -> dict[str, dict[str, Any]]:
    working = is_working or weekday_is_working
    stats: dict[str, dict[str, Any]] = {
        key: {
            "plan": 0,
            "fact": 0,
            "significant": 0,
            "departments": defaultdict(int),
            "in_work_on_time": 0,
            "in_work_overdue": 0,
            "in_work_departments": defaultdict(int),
        }
        for key in month_keys
    }
    for hit in hits:
        if hit.plan_month in stats:
            bucket = stats[hit.plan_month]
            bucket["plan"] += 1
            if hit.significant:
                bucket["significant"] += 1
            bucket["departments"][dept_key(hit.dept_name)] += 1
        if hit.fact_month in stats:
            stats[hit.fact_month]["fact"] += 1

    for key in month_keys:
        year = int(key[:4])
        month = int(key[5:7])
        month_start = date(year, month, 1)
        if month == 12:
            month_end = date(year, 12, 31)
        else:
            month_end = date(year, month + 1, 1) - timedelta(days=1)
        if month_start > today:
            continue
        as_of = today if month_end > today else month_end
        bucket = stats[key]
        for hit in hits:
            # Месяц плитки — месяц входа в работу, не весь открытый хвост.
            if hit.plan_month != key or hit.created > as_of:
                continue
            on_time = in_work_on_time(hit.status, hit.created, as_of, working)
            if on_time is None:
                continue
            if on_time:
                bucket["in_work_on_time"] += 1
            else:
                bucket["in_work_overdue"] += 1
            bucket["in_work_departments"][dept_key(hit.dept_name)] += 1
    return stats


def name_counts(counts: dict[str, int]) -> list[dict[str, Any]]:
    rows = [{"name": name, "count": int(count)} for name, count in counts.items()]
    rows.sort(key=lambda row: (-row["count"], str(row["name"]).lower()))
    return rows


def as_in_work_payload(source: dict[str, Any], kpi_id: str) -> dict[str, Any]:
    """Плитка «в работе»: план — все в работе, факт — без просрочки."""
    monthly: list[dict[str, Any]] = []
    for row in source.get("monthly_data") or []:
        on_time = int(row.get("in_work_on_time") or 0)
        overdue = int(row.get("in_work_overdue") or 0)
        plan = on_time + overdue
        monthly.append(
            {
                "month": row.get("month"),
                "year": row.get("year"),
                "month_name": row.get("month_name"),
                "plan": plan,
                "fact": on_time,
                "overdue": overdue,
                "kpi_pct": kpi_pct(plan, on_time),
                "has_data": True,
                "values_unit": "шт.",
                "departments": list(row.get("in_work_departments") or []),
            }
        )
    ref = None
    period = source.get("kpi_period") or {}
    ref_month = period.get("month")
    if ref_month is not None:
        ref = next((item for item in monthly if item.get("month") == ref_month), None)
    if ref is None and monthly:
        ref = monthly[-1]
    return {
        "data_granularity": source.get("data_granularity") or "monthly",
        "monthly_data": monthly,
        "last_full_month_row": dict(ref) if ref else None,
        "departments": list((ref or {}).get("departments") or []),
        "kpi_period": dict(period) if period else None,
        "ytd": {
            "total_plan": ref.get("plan") if ref else None,
            "total_fact": ref.get("fact") if ref else None,
            "total_overdue": ref.get("overdue") if ref else None,
            "kpi_pct": ref.get("kpi_pct") if ref else None,
            "months_with_data": sum(1 for item in monthly if item.get("has_data")),
            "months_total": len(monthly),
            **({"values_unit": "шт."} if ref else {}),
        },
        "debug": {
            "kpi_id": kpi_id,
            "status": "ok",
            "source": (source.get("debug") or {}).get("source"),
            "rule": (
                "plan = формы месяца (дата документа + 2 раб. дня) в статусах «в работе»; "
                "fact = из них без просрочки (ещё + 30 календарных дней, "
                "либо статус ИсполнениеКМ); overdue = plan - fact"
            ),
        },
    }


def known_status_names() -> tuple[str, ...]:
    return tuple(STATUS_BY_ORDER.values())
