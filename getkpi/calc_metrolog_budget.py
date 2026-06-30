"""METD-M3.B: бюджет главного метролога по оплаченным заявкам на расход ДС."""
from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from .calc_budget_limit import (
    AUTH,
    EMPTY,
    FACT_RECORDER_TYPES,
    PLAN_RECORDER_TYPE,
    load_records,
    period_bounds,
)
from .calc_budget_techdir_m3 import REQUEST_DOC_ENTITY_CANDIDATES
from .fot_techdir_fact import BASE
from . import cache_manager

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_SOURCE_TAG = "metrolog_budget_paid_requests_v1"
CACHE_VERSION = 1

TARGET_CFO = "МС и Рем.участок"

BUDGET_PLAN_BY_MONTH = {
    1: 2_216_005,
    2: 2_094_994,
    3: 2_590_466,
    4: 2_194_576,
    5: 2_783_917,
    6: 2_168_163,
    7: 2_389_876,
    8: 2_261_878,
    9: 2_540_824,
    10: 1_989_007,
    11: 1_944_679,
    12: 2_476_756,
}

ALLOWED_DDS_ARTICLES = (
    "Оплата поставщику ТМЦ_1_МСиРУ_1.1.",
    "Инструмент_1_МСиРУ_3.10.",
    "Расходные материалы_1_МСиРУ_3.11.",
    "Ремонт оборудования_1_МСиРУ_3.12.",
    "Поверка эталонных СИ МС и РУ_1_МСиРУ_3.14.1.",
    "Аренда оборудования_1_МСиРУ_3.16.",
    "Оплата субподрядчику ОПМУ_1_МСиРУ_3.17.",
    "Поверка эталонных СИ по ГК_1_МСиРУ_3.14.2.",
    "Поверка СИ по ГК (новые приборы)_1_МСиРУ_3.14.3.",
    "Проекты (ТМЦ и услуги)_МС и РУ_1_МСиРУ_5.1",
)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def normalize_name(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    raw = re.sub(r"[^0-9a-zа-я]+", " ", raw)
    return " ".join(raw.split())


ALLOWED_DDS_NORMS = frozenset(normalize_name(value) for value in ALLOWED_DDS_ARTICLES)
TARGET_CFO_NORM = normalize_name(TARGET_CFO)


def cache_file_path_for_period(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"metrolog_budget_{int(year)}_{int(month):02d}.json"


def _load_cache(year: int, month: int, *, allow_stale: bool = False) -> dict | None:
    path = cache_file_path_for_period(year, month)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_source") != CACHE_SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if not allow_stale and data.get("cache_date") != date.today().isoformat():
        return None
    return data


def _save_cache(year: int, month: int, payload: dict) -> None:
    data = {
        **payload,
        "cache_source": CACHE_SOURCE_TAG,
        "cache_version": CACHE_VERSION,
        "cache_date": date.today().isoformat(),
    }
    try:
        cache_file_path_for_period(year, month).write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        logger.exception("Не удалось сохранить кэш METD-M3.B")


def _fetch_all(session: requests.Session, url: str, page: int = 5000, timeout: int = 120) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        response = session.get(f"{url}{sep}$top={page}&$skip={skip}", timeout=timeout)
        response.raise_for_status()
        batch = response.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += len(batch)


def _signed_payment_amount(row: dict[str, Any]) -> float:
    sign = -1 if row.get("Сторно") else 1
    amount = (
        float(row.get("СуммаОплаты") or 0)
        + float(row.get("СуммаПредоплаты") or 0)
        + float(row.get("СуммаПостоплаты") or 0)
    )
    return amount * sign


def _request_ref(row: dict[str, Any]) -> str:
    direct = str(row.get("ЗаявкаНаРасходованиеДенежныхСредств_Key") or "").strip()
    if direct and direct.lower() != EMPTY.lower():
        return direct.lower()
    recorder = str(row.get("Recorder") or "").strip()
    recorder_type = str(row.get("Recorder_Type") or "")
    if not recorder or recorder.lower() == EMPTY.lower():
        return ""
    if recorder_type == PLAN_RECORDER_TYPE:
        return recorder.lower()
    if "ЗаявкаНаРасходование" in recorder_type and "Денеж" in recorder_type:
        return recorder.lower()
    return ""


def _load_cfo_names(session: requests.Session, keys: set[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    refs = sorted(key for key in keys if key and key.lower() != EMPTY.lower())
    for idx in range(0, len(refs), 20):
        batch = refs[idx:idx + 20]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        url = (
            f"{BASE}/{quote('Catalog_ТД_ЦФО')}"
            f"?$format=json&$filter={quote(flt, safe='')}"
            f"&$select=Ref_Key,Description,DeletionMark"
        )
        for row in _fetch_all(session, url, page=100, timeout=60):
            key = str(row.get("Ref_Key") or "").strip()
            if key:
                result[key] = str(row.get("Description") or "").strip()
    return result


def _load_dds_article_names(session: requests.Session, keys: set[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    refs = sorted(key for key in keys if key and key.lower() != EMPTY.lower())
    for idx in range(0, len(refs), 20):
        batch = refs[idx:idx + 20]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        url = (
            f"{BASE}/{quote('Catalog_СтатьиДвиженияДенежныхСредств')}"
            f"?$format=json&$filter={quote(flt, safe='')}"
            f"&$select=Ref_Key,Description,Code,DeletionMark"
        )
        for row in _fetch_all(session, url, page=100, timeout=60):
            key = str(row.get("Ref_Key") or "").strip()
            if key:
                result[key] = str(row.get("Description") or row.get("Code") or "").strip()
    return result


def _load_request_headers(session: requests.Session, refs: set[str]) -> tuple[dict[str, dict], str]:
    result: dict[str, dict] = {}
    pending = sorted(ref for ref in refs if ref and ref.lower() != EMPTY.lower())
    used_entities: list[str] = []
    select = (
        "Ref_Key,Number,Date,Posted,DeletionMark,Статус,СуммаДокумента,"
        "ТД_ЦФО,ТД_ЦФО_Key,СтатьяДвиженияДенежныхСредств_Key"
    )
    for entity in REQUEST_DOC_ENTITY_CANDIDATES:
        if not pending:
            break
        entity_found = False
        for idx in range(0, len(pending), 12):
            batch = pending[idx:idx + 12]
            flt = " or ".join(f"Ref_Key eq guid'{ref}'" for ref in batch)
            url = (
                f"{BASE}/{quote(entity)}"
                f"?$format=json&$filter={quote(flt, safe='')}"
                f"&$select={quote(select, safe=',_')}"
            )
            try:
                rows = _fetch_all(session, url, page=100)
            except requests.RequestException:
                continue
            if rows:
                entity_found = True
            for row in rows:
                key = str(row.get("Ref_Key") or "").strip().lower()
                if key:
                    result[key] = row
        if entity_found:
            used_entities.append(entity)
        pending = [ref for ref in pending if ref not in result]
    return result, ",".join(used_entities or REQUEST_DOC_ENTITY_CANDIDATES[:1])


def _doc_cfo_matches(doc: dict, cfo_names: dict[str, str]) -> bool:
    raw = doc.get("ТД_ЦФО")
    if raw not in (None, ""):
        if normalize_name(raw) == TARGET_CFO_NORM:
            return True
    key = str(doc.get("ТД_ЦФО_Key") or "").strip()
    if key and key.lower() != EMPTY.lower():
        if normalize_name(cfo_names.get(key, "")) == TARGET_CFO_NORM:
            return True
    return False


def _doc_article_label(doc: dict, article_names: dict[str, str]) -> str:
    key = str(doc.get("СтатьяДвиженияДенежныхСредств_Key") or "").strip()
    return article_names.get(key, "")


def _doc_article_matches(doc: dict, article_names: dict[str, str]) -> bool:
    return normalize_name(_doc_article_label(doc, article_names)) in ALLOWED_DDS_NORMS


def compute_metrolog_budget_month(year: int, month: int) -> dict:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached
    if not cache_manager.is_force_compute_context():
        stale = _load_cache(year, month, allow_stale=True)
        if stale is not None:
            stale = dict(stale)
            stale["cache_refresh_status"] = "running"
            return stale

    session = requests.Session()
    session.auth = AUTH
    try:
        period_start, period_end = period_bounds(year, month)
        rows = load_records(session, period_start, period_end)
        payment_rows: list[dict] = []
        request_refs: set[str] = set()
        for row in rows:
            amount = _signed_payment_amount(row)
            if amount == 0:
                continue
            recorder_type = row.get("Recorder_Type") or ""
            if recorder_type not in FACT_RECORDER_TYPES and recorder_type != PLAN_RECORDER_TYPE:
                continue
            request_ref = _request_ref(row)
            if not request_ref:
                continue
            payment_rows.append(row)
            request_refs.add(request_ref)

        docs, doc_entity = _load_request_headers(session, request_refs)
        cfo_names = _load_cfo_names(
            session,
            {str(doc.get("ТД_ЦФО_Key") or "").strip() for doc in docs.values()},
        )
        article_names = _load_dds_article_names(
            session,
            {str(doc.get("СтатьяДвиженияДенежныхСредств_Key") or "").strip() for doc in docs.values()},
        )

        by_article = {article: 0.0 for article in ALLOWED_DDS_ARTICLES}
        counted_requests: set[str] = set()
        missing_headers = 0
        skipped_cfo = 0
        skipped_article = 0
        total_fact = 0.0

        for row in payment_rows:
            request_ref = _request_ref(row)
            doc = docs.get(request_ref)
            if not doc:
                missing_headers += 1
                continue
            if not _doc_cfo_matches(doc, cfo_names):
                skipped_cfo += 1
                continue
            if not _doc_article_matches(doc, article_names):
                skipped_article += 1
                continue

            amount = _signed_payment_amount(row)
            total_fact += amount
            counted_requests.add(request_ref)
            article_label = _doc_article_label(doc, article_names)
            for allowed in ALLOWED_DDS_ARTICLES:
                if normalize_name(allowed) == normalize_name(article_label):
                    by_article[allowed] += amount
                    break

        plan = float(BUDGET_PLAN_BY_MONTH.get(month, 0))
        payload = {
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
            "period": {"start": period_start[:10], "end": period_end[:10]},
            "plan": plan,
            "fact": round(total_fact, 2),
            "kpi_pct": round(total_fact / plan * 100, 1) if plan else None,
            "has_data": bool(counted_requests),
            "values_unit": "руб.",
            "by_article": {key: round(value, 2) for key, value in by_article.items()},
            "missing_articles": [
                article for article, value in by_article.items()
                if abs(value) == 0
            ],
            "debug": {
                "source": "Document_ЗаявкаНаРасходованиеДенежныхСредств + paid rows",
                "register_for_payment_period": "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент",
                "doc_entity": doc_entity,
                "target_cfo": TARGET_CFO,
                "allowed_articles": list(ALLOWED_DDS_ARTICLES),
                "payment_rows": len(payment_rows),
                "requests_found": len(docs),
                "requests_counted": len(counted_requests),
                "missing_headers": missing_headers,
                "skipped_cfo": skipped_cfo,
                "skipped_article": skipped_article,
                "recorder_types": dict(Counter(str(row.get("Recorder_Type") or "") for row in payment_rows)),
            },
        }
        _save_cache(year, month, payload)
        return payload
    finally:
        session.close()


def get_metrolog_budget_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_y = int(year or today.year)
    ref_m = max(1, min(12, int(month or today.month)))
    if ref_y == today.year and ref_m > today.month:
        ref_m = today.month

    months = []
    cache_refresh_running = False
    for m in range(1, ref_m + 1):
        payload = compute_metrolog_budget_month(ref_y, m)
        cache_refresh_running = cache_refresh_running or payload.get("cache_refresh_status") == "running"
        months.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": payload.get("plan"),
            "fact": payload.get("fact"),
            "kpi_pct": payload.get("kpi_pct"),
            "has_data": payload.get("has_data"),
            "values_unit": "руб.",
        })

    ref_row = months[-1] if months else None
    result = {
        "data_granularity": "monthly",
        "monthly_data": months,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
            "values_unit": "руб.",
        },
        "kpi_period": {
            "type": "current_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "debug": {
            "source": "Document_ЗаявкаНаРасходованиеДенежныхСредств",
            "plan_source": "БЮДЖЕТ ПЛАН Главного метролога",
        },
    }
    if cache_refresh_running:
        result["cache_refresh_status"] = "running"
    return result


def get_metrolog_budget_table(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_y = int(year or today.year)
    ref_m = max(1, min(12, int(month or today.month)))
    payload = compute_metrolog_budget_month(ref_y, ref_m)
    table = {
        "name": "Бюджет главного метролога",
        "periodicity": "ежемесячно",
        "description": (
            "Оплаченные/частично оплаченные заявки на расход ДС с ТД_ЦФО "
            '"МС и Рем.участок" и разрешёнными статьями ДДС.'
        ),
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
        "columns": ["Статья ДДС", "Факт, руб."],
        "rows": [
            {"Статья ДДС": article, "Факт, руб.": value}
            for article, value in payload["by_article"].items()
        ],
        "totals": {"plan": payload["plan"], "fact": payload["fact"]},
        "missing_articles": payload["missing_articles"],
        "debug": payload["debug"],
    }
    if payload.get("cache_refresh_status"):
        table["cache_refresh_status"] = payload.get("cache_refresh_status")
    return table
