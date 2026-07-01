from __future__ import annotations

import json
import logging
import os
from calendar import monthrange
from datetime import date, datetime, time
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from . import cache_manager
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_SOURCE_TAG = "metrolog_production_plan_monthly_v5_zero_is_real"
CACHE_VERSION = 5

BASE = DEFAULT_BASE_URL.rstrip("/") + "/odata/standard.odata"
if os.getenv("ONEC_BASE_URL"):
    raw_base = os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL).strip().rstrip("/")
    BASE = raw_base if raw_base.endswith("/odata/standard.odata") else f"{raw_base}/odata/standard.odata"

AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)

SCHEDULE_ENTITY_CANDIDATES = (
    "InformationRegister_ГрафикЭтаповПроизводства2_2",
)
STAGE_DOC_ENTITY = "Document_ЭтапПроизводства2_2"
DEPARTMENT_ENTITY = "Catalog_СтруктураПредприятия"
SURVEY_DOC_TYPES = (
    "Document_ТД_КартаЗаказаUFG",
    "Document_ТД_КартаЗаказаCFM",
    "Document_ТД_КартаЗаказаUFGH",
    "Document_ТД_КартаЗаказаTFG",
    "Document_ТД_КартаЗаказаUFL",
    "Document_ТД_КартаЗаказаПлотномер",
    "Document_ТД_КартаЗаказаГранд",
    "Document_ТД_КартаЗаказаСПУ3М",
    "Document_ТД_КартаЗаказаРаботУслуг",
)
SURVEY_DOC_LABELS = {
    "Document_ТД_КартаЗаказаUFG": "Карта заказа UFG",
    "Document_ТД_КартаЗаказаCFM": "Карта заказа CFM",
    "Document_ТД_КартаЗаказаUFGH": "Карта заказа UFGH",
    "Document_ТД_КартаЗаказаTFG": "Карта заказа TFG",
    "Document_ТД_КартаЗаказаUFL": "Карта заказа UFL",
    "Document_ТД_КартаЗаказаПлотномер": "Карта заказа Плотномер",
    "Document_ТД_КартаЗаказаГранд": "Карта заказа Гранд",
    "Document_ТД_КартаЗаказаСПУ3М": "Карта заказа СПУ3М",
    "Document_ТД_КартаЗаказаРаботУслуг": "Карта заказа Работ/услуг",
}

DEPARTMENT_NAME = "Метрологическая служба"
DEPARTMENT_KEY = "4668a58a-6eb1-11e2-afce-001e67112509"

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def cache_file_path_for_period(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"metrolog_production_plan_{int(year)}_{int(month):02d}.json"


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
        logger.exception("Не удалось сохранить кэш METD-M1")


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw or raw.startswith("0001-01-01"):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _fmt_ru_date(value: str | datetime | None) -> str:
    dt = value if isinstance(value, datetime) else _dt(str(value) if value else None)
    return dt.strftime("%d.%m.%Y") if dt else ""


def _late_by_calendar_day(
    plan_end: datetime | None,
    fact_end: datetime | None,
    evaluation_dt: datetime,
) -> bool:
    """День-в-день не просрочка: время внутри одной даты игнорируем."""
    if plan_end is None:
        return False
    if fact_end is not None:
        return fact_end.date() > plan_end.date()
    return plan_end.date() < evaluation_dt.date()


def _month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(int(year), int(month), 1)
    end = datetime.combine(date(int(year), int(month), monthrange(int(year), int(month))[1]), time.max)
    return start, end


def _odata_rows(session: requests.Session, entity: str, query: str, *, label: str) -> tuple[list[dict], int | None, str]:
    rows: list[dict] = []
    skip = 0
    last_status: int | None = None
    last_error = ""
    while True:
        page_query = f"{query}&$top=5000&$skip={skip}"
        url = f"{BASE}/{quote(entity)}?{page_query}"
        response = request_with_retry(session, url, timeout=120, retries=3, label=label)
        if response is None:
            return rows, last_status, "request_dropped"
        last_status = response.status_code
        if not response.ok:
            last_error = response.text[:500]
            return rows, last_status, last_error
        batch = response.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < 5000:
            return rows, last_status, ""
        skip += len(batch)


def _fetch_single(session: requests.Session, entity: str, key: str, select: str) -> dict | None:
    clean_key = str(key or "").strip()
    if not clean_key or clean_key == EMPTY_GUID:
        return None
    url = f"{BASE}/{quote(entity)}(guid'{clean_key}')?$format=json&$select={quote(select, safe=',_')}"
    response = request_with_retry(session, url, timeout=60, retries=3, label=f"METD-M1/{entity}")
    if response is None or not response.ok:
        return None
    data = response.json()
    return data if isinstance(data, dict) else None


def _resolve_department(session: requests.Session) -> dict:
    row = _fetch_single(session, DEPARTMENT_ENTITY, DEPARTMENT_KEY, "Ref_Key,Description,Code,DeletionMark")
    return {
        "entity": DEPARTMENT_ENTITY,
        "key": DEPARTMENT_KEY,
        "name": (row or {}).get("Description") or DEPARTMENT_NAME,
        "code": (row or {}).get("Code"),
        "deletion_mark": (row or {}).get("DeletionMark"),
    }


def _load_schedule_rows(
    session: requests.Session,
    period_start: datetime,
    period_end: datetime,
    current_dt: datetime,
) -> tuple[list[dict], dict]:
    select = "Этап,Этап_Type,Начало,Окончание,ЗаказНаПроизводство_Key"
    odata_filter = (
        f"Начало ge datetime'{_fmt(period_start)}' "
        f"and Окончание le datetime'{_fmt(period_end)}' "
        f"and Окончание le datetime'{_fmt(current_dt)}'"
    )
    query = (
        "$format=json"
        f"&$select={quote(select, safe=',_')}"
        f"&$filter={quote(odata_filter, safe='')}"
    )
    attempts = []
    for entity in SCHEDULE_ENTITY_CANDIDATES:
        rows, status, error = _odata_rows(session, entity, query, label=f"METD-M1/{entity}")
        attempts.append({
            "entity": entity,
            "http_status": status,
            "rows": len(rows),
            "error": error,
            "query": query,
            "filter": odata_filter,
        })
        if status is not None and 200 <= status < 300:
            return rows, {
                "entity": entity,
                "select": select,
                "filter": odata_filter,
                "query": query,
                "attempts": attempts,
            }
    return [], {
        "entity": None,
        "select": select,
        "filter": odata_filter,
        "query": query,
        "attempts": attempts,
        "error": "Регистр сведений ГрафикЭтаповПроизводства2_2 не опубликован в OData под ожидаемым именем.",
    }


def _stage_key(row: dict) -> str:
    return str(row.get("Этап") or "").strip()


def _load_stage_docs(session: requests.Session, stage_keys: set[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    keys = sorted(key for key in stage_keys if key and key != EMPTY_GUID)
    select = "Ref_Key,Number,Date,Подразделение_Key,ФактическоеОкончаниеЭтапа,ФактическоеНачалоЭтапа"
    for i in range(0, len(keys), 25):
        batch = keys[i:i + 25]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        query = (
            "$format=json"
            f"&$select={quote(select, safe=',_')}"
            f"&$filter={quote(flt, safe='')}"
        )
        rows, _status, _error = _odata_rows(session, STAGE_DOC_ENTITY, query, label="METD-M1/stages")
        for row in rows:
            key = str(row.get("Ref_Key") or "")
            if key:
                out[key] = row
    return out


def _load_production_orders(session: requests.Session, order_keys: set[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    keys = sorted(key for key in order_keys if key and key != EMPTY_GUID)
    select = "Ref_Key,Number,Date,ТД_ОпросныйЛист"
    for i in range(0, len(keys), 25):
        batch = keys[i:i + 25]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        query = (
            "$format=json"
            f"&$select={quote(select, safe=',_')}"
            f"&$filter={quote(flt, safe='')}"
        )
        rows, _status, _error = _odata_rows(
            session,
            "Document_ЗаказНаПроизводство2_2",
            query,
            label="METD-M1/production-orders",
        )
        for row in rows:
            key = str(row.get("Ref_Key") or "")
            if key:
                out[key] = row
    return out


def _survey_doc_title(entity: str, row: dict) -> str:
    number = str(row.get("Number") or "").strip()
    doc_date = _fmt_ru_date(row.get("Date"))
    label = SURVEY_DOC_LABELS.get(entity, entity.replace("Document_", ""))
    title = label
    if number:
        title = f"{title} №{number}"
    if doc_date:
        title = f"{title} от {doc_date}"
    return title


def _load_survey_titles(session: requests.Session, survey_keys: set[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    keys = sorted(key for key in survey_keys if key and key != EMPTY_GUID)
    if not keys:
        return out

    select = "Ref_Key,Number,Date"
    for entity in SURVEY_DOC_TYPES:
        missing = [key for key in keys if key not in out]
        if not missing:
            break
        for i in range(0, len(missing), 25):
            batch = missing[i:i + 25]
            flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
            query = (
                "$format=json"
                f"&$select={quote(select, safe=',_')}"
                f"&$filter={quote(flt, safe='')}"
            )
            rows, status, _error = _odata_rows(session, entity, query, label=f"METD-M1/survey/{entity}")
            if status is None or not (200 <= status < 300):
                continue
            for row in rows:
                key = str(row.get("Ref_Key") or "").strip()
                if key:
                    out[key] = _survey_doc_title(entity, row)
    return out


def _month_row(year: int, month: int) -> dict:
    period_start, period_end = _month_bounds(year, month)
    current_dt = datetime.now()
    evaluation_dt = min(current_dt, period_end)
    session = requests.Session()
    session.auth = AUTH

    department = _resolve_department(session)
    schedule_rows, query_protocol = _load_schedule_rows(session, period_start, period_end, evaluation_dt)
    unique_schedule: dict[str, dict] = {}
    population_rows: list[tuple[str, dict]] = []
    for row in schedule_rows:
        key = _stage_key(row)
        if not key or key == EMPTY_GUID:
            continue
        population_rows.append((key, row))
        if key not in unique_schedule:
            unique_schedule[key] = row

    stages = _load_stage_docs(session, set(unique_schedule.keys())) if unique_schedule else {}
    production_order_keys = {
        str(row.get("ЗаказНаПроизводство_Key") or "").strip()
        for row in schedule_rows
        if str(row.get("ЗаказНаПроизводство_Key") or "").strip()
        and str(row.get("ЗаказНаПроизводство_Key") or "").strip() != EMPTY_GUID
    }
    production_orders = (
        _load_production_orders(session, production_order_keys)
        if production_order_keys
        else {}
    )
    survey_keys_for_late_rows: set[str] = set()
    on_time = 0
    late = 0
    skipped_wrong_department = 0
    details: list[dict] = []
    late_stage_rows: list[dict] = []
    for key, schedule in population_rows:
        stage = stages.get(key) or {}
        if str(stage.get("Подразделение_Key") or "").strip() != DEPARTMENT_KEY:
            skipped_wrong_department += 1
            continue
        plan_end = _dt(schedule.get("Окончание"))
        fact_end = _dt(stage.get("ФактическоеОкончаниеЭтапа"))
        is_late = _late_by_calendar_day(plan_end, fact_end, evaluation_dt)
        if is_late:
            late += 1
        else:
            on_time += 1
        order_key = str(schedule.get("ЗаказНаПроизводство_Key") or "").strip()
        production_order = production_orders.get(order_key) or {}
        survey_key = str(production_order.get("ТД_ОпросныйЛист") or "").strip()
        if is_late:
            if survey_key and survey_key != EMPTY_GUID:
                survey_keys_for_late_rows.add(survey_key)
            late_stage_rows.append({
                "Этап": stage.get("Number") or key,
                "Начало": _fmt_ru_date(schedule.get("Начало")),
                "Окончание": _fmt_ru_date(schedule.get("Окончание")),
                "ЭтапФактическоеОкончание": _fmt_ru_date(stage.get("ФактическоеОкончаниеЭтапа")),
                "ЗаказНаПроизводствоТД_ОпросныйЛист": (
                    survey_key if survey_key and survey_key != EMPTY_GUID else ""
                ),
                "stage_key": key,
                "production_order_key": order_key,
                "survey_key": survey_key if survey_key != EMPTY_GUID else "",
            })
        details.append({
            "stage_key": key,
            "stage_number": stage.get("Number"),
            "plan_start": _fmt_ru_date(schedule.get("Начало")),
            "plan_end": _fmt_ru_date(schedule.get("Окончание")),
            "fact_end": _fmt_ru_date(stage.get("ФактическоеОкончаниеЭтапа")),
            "late": is_late,
            "production_order_key": order_key,
        })

    survey_titles = _load_survey_titles(session, survey_keys_for_late_rows)
    for row in late_stage_rows:
        survey_key = row.get("survey_key") or ""
        if survey_key:
            row["ЗаказНаПроизводствоТД_ОпросныйЛист"] = survey_titles.get(survey_key) or survey_key

    total = on_time + late
    pct = round((on_time / total) * 100, 2) if total else None
    return {
        "month": int(month),
        "year": int(year),
        "month_name": MONTH_NAMES.get(int(month), str(month)),
        "plan": total,
        "fact": on_time,
        "kpi_pct": pct,
        "has_data": True,
        "total_stages": total,
        "late_stages": late,
        "on_time_stages": on_time,
        "late_stage_rows": late_stage_rows,
        "check_sum_ok": total == late + on_time,
        "period_start": _fmt(period_start),
        "period_end": _fmt(period_end),
        "current_date": _fmt(evaluation_dt),
        "debug": {
            "source_register": "InformationRegister_ГрафикЭтаповПроизводства2_2",
            "stage_document": STAGE_DOC_ENTITY,
            "department": department,
            "schedule_rows_loaded": len(schedule_rows),
            "unique_stage_refs_from_schedule": len(unique_schedule),
            "population_schedule_rows": len(population_rows),
            "stage_docs_loaded": len(stages),
            "production_orders_loaded": len(production_orders),
            "survey_titles_loaded": len(survey_titles),
            "skipped_wrong_department": skipped_wrong_department,
            "query_protocol": query_protocol,
            "details_sample": details[:100],
            "formula": (
                "total = строки регистра с непустым Этап; late = дата(ФактическоеОкончаниеЭтапа) > дата(Окончание) "
                "или факт пустой и дата(Окончание) < дата(ТекущаяДата); on_time = остальные"
            ),
        },
    }


def get_metrolog_production_plan_monthly(year: int, month: int) -> dict:
    ref_y = int(year)
    ref_m = max(1, min(12, int(month)))
    cached = _load_cache(ref_y, ref_m)
    if cached is not None:
        return cached
    if not cache_manager.is_force_compute_context():
        stale = _load_cache(ref_y, ref_m, allow_stale=True)
        if stale is not None:
            stale = dict(stale)
            stale["cache_refresh_status"] = "running"
            return stale
    months = [_month_row(ref_y, m) for m in range(1, ref_m + 1)]
    last = months[-1] if months else None
    payload = {
        "data_granularity": "monthly",
        "monthly_data": months,
        "last_full_month_row": last,
        "ytd": {
            "total_plan": sum(float(row.get("plan") or 0) for row in months),
            "total_fact": sum(float(row.get("fact") or 0) for row in months),
            "kpi_pct": last.get("kpi_pct") if last else None,
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
        },
        "kpi_period": {
            "type": "selected_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES.get(ref_m, str(ref_m)),
        },
        "debug": last.get("debug") if last else {},
    }
    _save_cache(ref_y, ref_m, payload)
    return payload


def get_metrolog_late_stage_table(year: int, month: int) -> dict:
    payload = get_metrolog_production_plan_monthly(year, month)
    rows = []
    for row in payload.get("monthly_data") or []:
        if row.get("year") == int(year) and row.get("month") == int(month):
            rows = row.get("late_stage_rows") or []
            break
    table = {
        "name": f"Просроченные этапы метрологической службы за {MONTH_NAMES.get(int(month), month)} {int(year)}",
        "periodicity": "ежемесячно",
        "description": (
            "Строки регистра ГрафикЭтаповПроизводства2_2, где этап относится к метрологической службе. "
            "День-в-день не считается просрочкой: сравниваются календарные даты, а не время."
        ),
        "period": {
            "year": int(year),
            "month": int(month),
            "month_name": MONTH_NAMES.get(int(month), str(month)),
        },
        "columns": [
            "Этап",
            "Начало",
            "Окончание",
            "ЭтапФактическоеОкончание",
            "ЗаказНаПроизводствоТД_ОпросныйЛист",
        ],
        "rows": rows,
    }
    if payload.get("cache_refresh_status"):
        table["cache_refresh_status"] = payload.get("cache_refresh_status")
    return table
