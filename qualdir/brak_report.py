"""Общая логика отчётов по браку (формы 0318/0319) и API для плитки QD-M5."""

from __future__ import annotations

import argparse
import calendar
import csv
import functools
import os
import re
import shutil
import sys
import textwrap
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

try:
    from getkpi.odata_http import request_with_retry as _odata_request_with_retry
except ImportError:
    _odata_request_with_retry = None

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)

BASE = os.getenv("ONEC_BASE_URL", "http://192.168.2.229:81/erp_pm/odata/standard.odata").rstrip("/")
if not BASE.endswith("/odata/standard.odata"):
    BASE = f"{BASE.rstrip('/')}/odata/standard.odata"
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)
EMPTY = "00000000-0000-0000-0000-000000000000"
KIND_ENTITY = "Catalog_ТД_ВидыНесоответствияПроцессовСТО"

INTERNAL_BRAK_ENTITY = "Document_ТД_Форма0318"
EXTERNAL_BRAK_ENTITY = "Document_ТД_Форма0319"
FORM_0317_ENTITY = "Document_ТД_Форма0317"

INDUSTRIAL_DEPT = "отк-1"
HOUSEHOLD_DEPT = "отк-2"
DIRECTION_LABELS = {
    "industrial": "Промышленное направление (ОТК-1)",
    "household": "Бытовое направление (ОТК-2)",
    "other": "Прочие подразделения",
}
DEPARTMENT_SHORT_NAMES = {
    "industrial": "ОТК-1",
    "household": "ОТК-2",
    "other": "Прочие",
}


@dataclass(frozen=True)
class ReportConfig:
    doc_entity: str
    title: str
    description: str


INTERNAL_BRAK_CONFIG = ReportConfig(
    doc_entity=INTERNAL_BRAK_ENTITY,
    title="Внутренний брак · ТД_Форма0318",
    description="Внутренний брак по документам ТД_Форма0318.",
)

EXTERNAL_BRAK_CONFIG = ReportConfig(
    doc_entity=EXTERNAL_BRAK_ENTITY,
    title="Внешний брак · ТД_Форма0319",
    description="Внешний брак по документам ТД_Форма0319.",
)

FORM_0317_CONFIG = ReportConfig(
    doc_entity=FORM_0317_ENTITY,
    title="ТД_Форма0317",
    description="Документы ТД_Форма0317: подразделение поставщика и виды несоответствий.",
)

# Черновики и отменённые формы — только для опциональной фильтрации (таблицы/CLI).
EXCLUDED_APPROVAL_STATUSES = frozenset({
    "НеСогласовано",
    "Подготовлен",
    "Подготовлено",
    "НаСогласовании",
    "Отменена",
})
EXECUTED_APPROVAL_STATUS = "Выполнено"
# В plan не входят отклонённые, отменённые, ещё не согласованные и подготовленные заявки.
PLAN_EXCLUDED_APPROVAL_STATUSES = frozenset({
    "НеСогласовано",
    "Отменена",
    "НаСогласовании",
    "Подготовлен",
    "Подготовлено",
})


def normalize_text(value: str | None) -> str:
    text = (value or "").strip().lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return " ".join(text.split())


PLAN_EXCLUDED_APPROVAL_STATUSES_NORM = frozenset(
    normalize_text(status) for status in PLAN_EXCLUDED_APPROVAL_STATUSES
)


def _http_get(session: requests.Session, url: str, *, timeout: int = 180) -> requests.Response:
    if _odata_request_with_retry is not None:
        response = _odata_request_with_retry(
            session,
            url,
            timeout=timeout,
            retries=4,
            label="brak_report",
        )
        if response is None:
            raise RuntimeError(f"OData request failed: {url[:120]}")
        return response
    response = session.get(url, timeout=timeout)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
    return response


def fetch_all(session: requests.Session, url: str, page: int = 500) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}&$format=json"
        response = _http_get(session, page_url)
        batch = response.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_month(value: str) -> tuple[date, date]:
    match = re.fullmatch(r"(\d{4})-(\d{1,2})", value.strip())
    if not match:
        raise ValueError("Формат --month: YYYY-MM")
    year = int(match.group(1))
    month = int(match.group(2))
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 1 до 12")
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def month_bounds(year: int, month: int) -> tuple[date, date]:
    month = max(1, min(12, int(month)))
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def resolve_period(args: argparse.Namespace) -> tuple[date, date]:
    if args.month:
        return parse_month(args.month)
    if args.date_from and args.date_to:
        start = parse_iso_date(args.date_from)
        end = parse_iso_date(args.date_to)
        if end < start:
            raise ValueError("--to не может быть раньше --from")
        return start, end
    if args.date_from or args.date_to:
        raise ValueError("Укажите оба параметра --from и --to")
    today = date.today()
    return today.replace(day=1), today


def odata_day_start(day: date) -> str:
    return f"{day.isoformat()}T00:00:00"


def odata_day_end(day: date) -> str:
    return f"{day.isoformat()}T23:59:59"


def build_filter(date_from: date, date_to: date) -> str:
    parts = [
        "DeletionMark eq false",
        f"Date ge datetime'{odata_day_start(date_from)}'",
        f"Date le datetime'{odata_day_end(date_to)}'",
    ]
    return " and ".join(parts)


def is_countable_brak_document(row: dict) -> bool:
    """Форма после согласования (не черновик/отказ/отмена) — для таблиц и CLI."""
    status = (row.get("Статус") or "").strip()
    if not status:
        return False
    return status not in EXCLUDED_APPROVAL_STATUSES


def is_plan_brak_document(row: dict) -> bool:
    """Заявка в plan: документ месяца, кроме черновиков/отменённых/несогласованных."""
    status = (row.get("Статус") or "").strip()
    if not status:
        return False
    return normalize_text(status) not in PLAN_EXCLUDED_APPROVAL_STATUSES_NORM


def is_executed_brak_document(row: dict) -> bool:
    """Исполненная заявка: ``Статус = Выполнено``."""
    return (row.get("Статус") or "").strip() == EXECUTED_APPROVAL_STATUS


def ref_name(value: Any) -> str:
    if isinstance(value, dict):
        return (value.get("Description") or "").strip()
    if isinstance(value, str):
        return value.strip()
    return ""


def load_documents(
    session: requests.Session,
    config: ReportConfig,
    date_from: date,
    date_to: date,
    *,
    extra_select_fields: tuple[str, ...] = (),
    countable_only: bool = False,
) -> list[dict]:
    filter_expr = quote(build_filter(date_from, date_to), safe="")
    expand = quote("ПодразделениеПоставщика", safe=",/")
    select_fields = [
        "Ref_Key",
        "Number",
        "Date",
        "Posted",
        "Статус",
        "НаименованиеИзделия",
        "ПодразделениеПоставщика",
        "Несоответствия",
        *extra_select_fields,
    ]
    select = quote(",".join(select_fields), safe=",_")
    url = (
        f"{BASE}/{quote(config.doc_entity)}"
        f"?$filter={filter_expr}"
        f"&$select={select}"
        f"&$expand={expand}"
        f"&$orderby=Date desc"
    )
    log(f"Загрузка {config.doc_entity} …")
    rows = fetch_all(session, url)
    if countable_only:
        accepted = [row for row in rows if is_countable_brak_document(row)]
        skipped = len(rows) - len(accepted)
        log(f"  Документов: {len(accepted)}" + (f" (исключено по Статус: {skipped})" if skipped else ""))
        return accepted
    accepted = [row for row in rows if is_plan_brak_document(row)]
    skipped = len(rows) - len(accepted)
    log(f"  Документов: {len(accepted)}" + (f" (исключено из plan: {skipped})" if skipped else ""))
    return accepted


def load_kind_names(session: requests.Session, keys: set[str]) -> dict[str, str]:
    valid = sorted(key for key in keys if key and key != EMPTY)
    if not valid:
        return {}
    names: dict[str, str] = {}
    chunk_size = 20
    for idx in range(0, len(valid), chunk_size):
        chunk = valid[idx : idx + chunk_size]
        ref_filter = " or ".join(f"Ref_Key eq guid'{key}'" for key in chunk)
        url = (
            f"{BASE}/{quote(KIND_ENTITY)}"
            f"?$filter={quote(ref_filter, safe='')}"
            f"&$select={quote('Ref_Key,Description', safe=',_')}"
        )
        for row in fetch_all(session, url, page=500):
            key = row.get("Ref_Key")
            if key:
                names[key] = (row.get("Description") or "").strip()
    return names


def classify_direction(supplier_dept: str) -> str:
    norm = normalize_text(supplier_dept)
    if norm == INDUSTRIAL_DEPT or "отк 1" in norm:
        return "industrial"
    if norm == HOUSEHOLD_DEPT or "отк 2" in norm:
        return "household"
    return "other"


def fmt_date(value: Any) -> str:
    text = str(value or "")
    if not text or text.startswith("0001-01-01"):
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%d.%m.%Y")
    except ValueError:
        return text[:10]


def fmt_doc_number(number: str | None) -> str:
    text = (number or "").strip()
    if not text:
        return "—"
    return text if text.startswith("№") else f"№{text}"


def defect_kinds(row: dict, kind_names: dict[str, str]) -> list[str]:
    kinds: list[str] = []
    seen: set[str] = set()
    for item in row.get("Несоответствия") or []:
        key = item.get("ВидНесоответствия_Key") or ""
        name = kind_names.get(key, "").strip()
        if not name:
            name = (item.get("ОписаниеНесоответствия") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        kinds.append(name)
    return kinds


def normalize_documents(rows: list[dict], kind_names: dict[str, str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        supplier = ref_name(row.get("ПодразделениеПоставщика"))
        direction = classify_direction(supplier)
        result.append(
            {
                "number": fmt_doc_number(row.get("Number")),
                "date": fmt_date(row.get("Date")),
                "direction": direction,
                "direction_label": DIRECTION_LABELS[direction],
                "supplier_dept": supplier or "—",
                "product": (row.get("НаименованиеИзделия") or "").strip() or "—",
                "kinds": defect_kinds(row, kind_names),
                "is_significant": row.get("ФормаЯвляетсяЗначимой") is True,
                "status": (row.get("Статус") or "").strip() or "—",
            }
        )
    return result


def count_by_direction(docs: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"industrial": 0, "household": 0, "other": 0}
    for doc in docs:
        direction = doc.get("direction") or "other"
        if direction not in counts:
            direction = "other"
        counts[direction] += 1
    return counts


def count_by_supplier_dept(docs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for doc in docs:
        name = (doc.get("supplier_dept") or "—").strip() or "—"
        counts[name] = counts.get(name, 0) + 1
    return counts


def departments_payload_by_name(counts: dict[str, int]) -> list[dict[str, Any]]:
    rows = [{"name": name, "count": int(count)} for name, count in counts.items()]
    rows.sort(key=lambda row: (-row["count"], row["name"].lower()))
    return rows


def kinds_payload(counts: dict[str, int]) -> list[dict[str, Any]]:
    rows = [{"name": name, "count": int(count)} for name, count in counts.items()]
    rows.sort(key=lambda row: (-row["count"], row["name"].lower()))
    return rows


def count_significant_forms(rows: list[dict]) -> int:
    """Документы с ``ФормаЯвляетсяЗначимой = Истина`` (0317 / 0319 и др.)."""
    return sum(1 for row in rows if row.get("ФормаЯвляетсяЗначимой") is True)


def count_executed_forms(rows: list[dict]) -> int:
    """Исполненные заявки: ``Статус = Выполнено``."""
    return sum(1 for row in rows if is_executed_brak_document(row))


def count_by_kind(rows: list[dict], kind_names: dict[str, str]) -> dict[str, int]:
    """Считает строки ТЧ ``Несоответствия`` по ``ВидНесоответствия``."""
    counts: dict[str, int] = {}
    for row in rows:
        for item in row.get("Несоответствия") or []:
            key = item.get("ВидНесоответствия_Key") or ""
            name = kind_names.get(key, "").strip()
            if not name:
                name = (item.get("ОписаниеНесоответствия") or "").strip()
            if not name:
                name = "—"
            counts[name] = counts.get(name, 0) + 1
    return counts


def departments_payload(counts: dict[str, int]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for direction in ("industrial", "household", "other"):
        count = int(counts.get(direction) or 0)
        if direction == "other" and count == 0:
            continue
        rows.append(
            {
                "direction": direction,
                "name": DEPARTMENT_SHORT_NAMES[direction],
                "direction_label": DIRECTION_LABELS[direction],
                "count": count,
            }
        )
    return rows


def compute_brak_counts(
    date_from: date,
    date_to: date,
    *,
    session: requests.Session | None = None,
    config: ReportConfig | None = None,
    group_by: str = "direction",
) -> dict[str, Any]:
    """Сводка по документам брака за период (QD-M1 / QD-M5).

    group_by:
      - ``direction`` — ОТК-1 / ОТК-2 / Прочие (QD-M5);
      - ``supplier_dept`` — фактическое ``ПодразделениеПоставщика`` (QD-M1).
    """
    cfg = config or INTERNAL_BRAK_CONFIG
    own_session = session is None
    if session is None:
        session = requests.Session()
        session.auth = AUTH

    try:
        raw_docs = load_documents(session, cfg, date_from, date_to)
        kind_keys = {
            item.get("ВидНесоответствия_Key")
            for row in raw_docs
            for item in row.get("Несоответствия") or []
            if item.get("ВидНесоответствия_Key")
        }
        kind_names = load_kind_names(session, kind_keys)
        docs = normalize_documents(raw_docs, kind_names)
        if group_by == "supplier_dept":
            counts = count_by_supplier_dept(docs)
            departments = departments_payload_by_name(counts)
        else:
            counts = count_by_direction(docs)
            departments = departments_payload(counts)
        total = len(docs)
        return {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "total": total,
            "counts": counts,
            "departments": departments,
            "group_by": group_by,
            "has_data": True,
        }
    finally:
        if own_session:
            session.close()


def compute_brak_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
    config: ReportConfig | None = None,
    group_by: str = "direction",
) -> dict[str, Any]:
    date_from, date_to = month_bounds(year, month)
    payload = compute_brak_counts(
        date_from,
        date_to,
        session=session,
        config=config,
        group_by=group_by,
    )
    payload["year"] = year
    payload["month"] = month
    return payload


def compute_internal_brak_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """QD-M5: форма 0318 — plan (все заявки), fact (исполненные), ОТК-1 / ОТК-2."""
    date_from, date_to = month_bounds(year, month)
    own_session = session is None
    if session is None:
        session = requests.Session()
        session.auth = AUTH

    try:
        raw_docs = load_documents(
            session,
            INTERNAL_BRAK_CONFIG,
            date_from,
            date_to,
            extra_select_fields=("ФормаЯвляетсяЗначимой",),
        )
        kind_keys = {
            item.get("ВидНесоответствия_Key")
            for row in raw_docs
            for item in row.get("Несоответствия") or []
            if item.get("ВидНесоответствия_Key")
        }
        kind_names = load_kind_names(session, kind_keys)
        docs = normalize_documents(raw_docs, kind_names)
        counts = count_by_direction(docs)
        departments = departments_payload(counts)
        total = len(docs)
        executed = count_executed_forms(raw_docs)
        significant = count_significant_forms(raw_docs)
        return {
            "year": year,
            "month": month,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "total": total,
            "executed": executed,
            "significant": significant,
            "departments": departments,
            "has_data": True,
        }
    except Exception as exc:
        return {
            "year": year,
            "month": month,
            "total": None,
            "executed": None,
            "significant": None,
            "departments": [],
            "has_data": False,
            "error": str(exc),
        }
    finally:
        if own_session:
            session.close()


def compute_external_brak_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """QD-M1: форма 0319 — plan (все заявки), fact (исполненные), подразделения поставщика."""
    date_from, date_to = month_bounds(year, month)
    own_session = session is None
    if session is None:
        session = requests.Session()
        session.auth = AUTH

    try:
        raw_docs = load_documents(
            session,
            EXTERNAL_BRAK_CONFIG,
            date_from,
            date_to,
            extra_select_fields=("ФормаЯвляетсяЗначимой",),
        )
        kind_keys = {
            item.get("ВидНесоответствия_Key")
            for row in raw_docs
            for item in row.get("Несоответствия") or []
            if item.get("ВидНесоответствия_Key")
        }
        kind_names = load_kind_names(session, kind_keys)
        docs = normalize_documents(raw_docs, kind_names)
        departments = departments_payload_by_name(count_by_supplier_dept(docs))
        total = len(docs)
        executed = count_executed_forms(raw_docs)
        significant = count_significant_forms(raw_docs)
        return {
            "year": year,
            "month": month,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "total": total,
            "executed": executed,
            "significant": significant,
            "departments": departments,
            "has_data": True,
        }
    except Exception as exc:
        return {
            "year": year,
            "month": month,
            "total": None,
            "executed": None,
            "significant": None,
            "departments": [],
            "has_data": False,
            "error": str(exc),
        }
    finally:
        if own_session:
            session.close()


def compute_forma0317_month(
    year: int,
    month: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """QD-M8: документы 0317 — plan (все заявки), fact (исполненные), поставщик, виды несоответствий."""
    date_from, date_to = month_bounds(year, month)
    own_session = session is None
    if session is None:
        session = requests.Session()
        session.auth = AUTH

    try:
        raw_docs = load_documents(
            session,
            FORM_0317_CONFIG,
            date_from,
            date_to,
            extra_select_fields=("ФормаЯвляетсяЗначимой",),
        )
        kind_keys = {
            item.get("ВидНесоответствия_Key")
            for row in raw_docs
            for item in row.get("Несоответствия") or []
            if item.get("ВидНесоответствия_Key")
        }
        kind_names = load_kind_names(session, kind_keys)
        docs = normalize_documents(raw_docs, kind_names)
        departments = departments_payload_by_name(count_by_supplier_dept(docs))
        kinds = kinds_payload(count_by_kind(raw_docs, kind_names))
        total = len(docs)
        executed = count_executed_forms(raw_docs)
        significant = count_significant_forms(raw_docs)
        return {
            "year": year,
            "month": month,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "total": total,
            "executed": executed,
            "significant": significant,
            "departments": departments,
            "kinds": kinds,
            "has_data": True,
        }
    except Exception as exc:
        return {
            "year": year,
            "month": month,
            "total": None,
            "executed": None,
            "significant": None,
            "departments": [],
            "kinds": [],
            "has_data": False,
            "error": str(exc),
        }
    finally:
        if own_session:
            session.close()


# backward compat
compute_internal_brak_counts = compute_brak_counts

BRAK_TABLE_COLUMNS = [
    "Документ",
    "Объект несоответствия",
    "Вид несоответствия",
    "Подразделение",
    "Статус",
    "Значимая форма",
]


def fmt_significant_flag(value: Any) -> str:
    return "да" if value is True else "нет"


def document_table_row(doc: dict[str, Any]) -> dict[str, str]:
    doc_label = doc.get("number") or "—"
    doc_date = doc.get("date") or ""
    if doc_date:
        doc_label = f"{doc_label} от {doc_date}"
    kinds = doc.get("kinds") or []
    return {
        "Документ": doc_label,
        "Объект несоответствия": str(doc.get("product") or "—"),
        "Вид несоответствия": "; ".join(kinds) if kinds else "—",
        "Подразделение": str(doc.get("supplier_dept") or "—"),
        "Статус": str(doc.get("status") or "—"),
        "Значимая форма": fmt_significant_flag(doc.get("is_significant")),
    }


def load_brak_documents(
    year: int,
    month: int,
    *,
    config: ReportConfig,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Нормализованные документы брака за календарный месяц."""
    date_from, date_to = month_bounds(year, month)
    own_session = session is None
    if session is None:
        session = requests.Session()
        session.auth = AUTH

    try:
        raw_docs = load_documents(
            session,
            config,
            date_from,
            date_to,
            extra_select_fields=("ФормаЯвляетсяЗначимой",),
        )
        kind_keys = {
            item.get("ВидНесоответствия_Key")
            for row in raw_docs
            for item in row.get("Несоответствия") or []
            if item.get("ВидНесоответствия_Key")
        }
        kind_names = load_kind_names(session, kind_keys)
        return normalize_documents(raw_docs, kind_names)
    finally:
        if own_session:
            session.close()


def load_brak_table_rows(
    year: int,
    month: int,
    *,
    config: ReportConfig,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    docs = load_brak_documents(year, month, config=config, session=session)
    return [document_table_row(doc) for doc in docs]


def terminal_width(default: int = 100, maximum: int = 110) -> int:
    try:
        return min(maximum, max(72, shutil.get_terminal_size(fallback=(default, 24)).columns))
    except OSError:
        return default


def wrap_block(text: str, width: int, indent: str = "    ") -> list[str]:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return ["—"]
    usable = max(20, width - len(indent))
    return textwrap.wrap(text, width=usable) or ["—"]


def print_tiles(
    docs: list[dict[str, Any]],
    config: ReportConfig,
    date_from: date,
    date_to: date,
) -> None:
    width = terminal_width()
    rule = "═" * (width - 2)
    thin = "─" * (width - 2)
    counts = count_by_direction(docs)
    total = len(docs)

    print()
    print(rule)
    print(f"  {config.title}")
    print(f"  Период: {date_from.strftime('%d.%m.%Y')} — {date_to.strftime('%d.%m.%Y')}")
    print(rule)
    print()
    print(f"  ┌─ Плитки {thin[10:]}")
    print(f"  │ Всего документов                 {total:>6}")
    print(f"  │ {DIRECTION_LABELS['industrial']:<33}{counts['industrial']:>6}")
    print(f"  │ {DIRECTION_LABELS['household']:<33}{counts['household']:>6}")
    if counts["other"]:
        print(f"  │ {DIRECTION_LABELS['other']:<33}{counts['other']:>6}")
    print(f"  └{thin[2:]}")


def print_cards(docs: list[dict[str, Any]]) -> None:
    if not docs:
        print()
        print("  Документы за период не найдены.")
        return

    width = terminal_width()
    thin = "─" * (width - 2)
    label_width = 20
    value_start = 2 + label_width + 2

    grouped: dict[str, list[dict[str, Any]]] = {
        "industrial": [],
        "household": [],
        "other": [],
    }
    for doc in docs:
        grouped[doc["direction"]].append(doc)

    index = 0
    for direction in ("industrial", "household", "other"):
        section = grouped[direction]
        if not section:
            continue
        print()
        print(f"  {DIRECTION_LABELS[direction]} · документов: {len(section)}")
        print(f"  {thin}")

        for doc in section:
            index += 1
            print()
            print(f"  ┌─ {index}/{len(docs)} {thin[6:]}")

            def emit(label: str, lines: list[str]) -> None:
                pad = " " * (label_width - len(label))
                first_prefix = f"  │ {label}{pad}  "
                next_prefix = " " * len(first_prefix)
                for line_no, line in enumerate(lines):
                    prefix = first_prefix if line_no == 0 else next_prefix
                    print(f"{prefix}{line}")

            header = f"{doc['number']} от {doc['date'] or '—'}"
            emit("Документ", wrap_block(header, width, indent=" " * value_start))
            emit(
                "Объект несоответствия",
                wrap_block(doc["product"], width, indent=" " * value_start),
            )
            kinds = doc["kinds"] or ["—"]
            emit(
                "Вид несоответствия",
                wrap_block("; ".join(kinds), width, indent=" " * value_start),
            )
            emit("Подразделение", [doc["supplier_dept"]])
            print(f"  └{thin[2:]}")


def print_table(docs: list[dict[str, Any]]) -> None:
    headers = ("Документ", "Дата", "Направление", "Объект несоответствия", "Вид несоответствия")
    rows = [
        (
            doc["number"],
            doc["date"],
            doc["direction_label"],
            doc["product"],
            "; ".join(doc["kinds"]) if doc["kinds"] else "—",
        )
        for doc in docs
    ]
    widths = {header: len(header) for header in headers}
    for row in rows:
        for header, value in zip(headers, row):
            widths[header] = max(widths[header], len(value))

    def line(char: str = "─") -> str:
        return "  " + "  ".join(char * (widths[header] + 2) for header in headers)

    print()
    print("  " + "  ".join(header.ljust(widths[header]) for header in headers))
    print(line())
    for row in rows:
        print(
            "  "
            + "  ".join(
                value.ljust(widths[header])[: widths[header]]
                for header, value in zip(headers, row)
            )
        )
    print(line())
    print(f"  Всего документов: {len(rows)}")


def write_csv(path: str, docs: list[dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "number",
                "date",
                "direction",
                "direction_label",
                "supplier_dept",
                "product",
                "kinds",
            ),
            delimiter=";",
        )
        writer.writeheader()
        for doc in docs:
            writer.writerow({**doc, "kinds": "; ".join(doc["kinds"])})


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--month", help="Период в формате YYYY-MM")
    parser.add_argument("--from", dest="date_from", help="Начало периода YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", help="Конец периода YYYY-MM-DD")
    parser.add_argument("--csv", dest="csv_path", help="Путь к CSV-файлу")
    parser.add_argument("--table", action="store_true", help="Табличный вывод вместо плиток")
    return parser.parse_args()


def run_report(config: ReportConfig) -> None:
    args = parse_args(config.description)
    date_from, date_to = resolve_period(args)

    session = requests.Session()
    session.auth = AUTH

    raw_docs = load_documents(session, config, date_from, date_to)
    kind_keys = {
        item.get("ВидНесоответствия_Key")
        for row in raw_docs
        for item in row.get("Несоответствия") or []
        if item.get("ВидНесоответствия_Key")
    }
    log("Загрузка видов несоответствий …")
    kind_names = load_kind_names(session, kind_keys)
    docs = normalize_documents(raw_docs, kind_names)

    if args.csv_path:
        write_csv(args.csv_path, docs)
        log(f"CSV сохранён: {args.csv_path}")

    print_tiles(docs, config, date_from, date_to)
    if args.table:
        print_table(docs)
    else:
        print_cards(docs)
