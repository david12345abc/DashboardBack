"""METD-M1 — план производства в части МС из локальной копии 1С (erp_pm).

SQL:
  InformationRegister.ГрафикЭтаповПроизводства2_2 → dbo._InfoRg43704
  Document.ЭтапПроизводства2_2                    → dbo._Document1052
  Document.ЗаказНаПроизводство2_2                 → dbo._Document709
  Catalog.СтруктураПредприятия                    → dbo._Reference513

Даты 1С в SQL хранятся со смещением +2000 лет.
"""
from __future__ import annotations

import json
import logging
import uuid
from calendar import monthrange
from datetime import date, datetime, time
from pathlib import Path

from comdir.common import connect_ctx, to_1c_dt, uuid_to_1c_bytes

from . import cache_manager

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
EMPTY16 = bytes(16)
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_SOURCE_TAG = "metrolog_production_plan_monthly_v7_sql"
CACHE_VERSION = 7

SCHEDULE_TABLE = "_InfoRg43704"
STAGE_TABLE = "_Document1052"
ORDER_TABLE = "_Document709"
DEPT_TABLE = "_Reference513"

COL_STAGE_REF = "_Fld43707_RRRef"
COL_PLAN_START = "_Fld43708"
COL_PLAN_END = "_Fld43709"
COL_ORDER_REF = "_Fld43705RRef"
COL_STAGE_DEPT = "_Fld41036RRef"
COL_FACT_START = "_Fld41047"
COL_FACT_END = "_Fld41048"
COL_SURVEY_REF = "_Fld100560_RRRef"

DEPARTMENT_NAME = "Метрологическая служба"
DEPARTMENT_KEY = "4668a58a-6eb1-11e2-afce-001e67112509"

SURVEY_TABLES = (
    ("_Document86501", "Карта заказа UFG"),
    ("_Document132108", "Карта заказа CFM"),
    ("_Document166682", "Карта заказа UFGH"),
    ("_Document111938", "Карта заказа TFG"),
    ("_Document171890", "Карта заказа UFL"),
    ("_Document109400", "Карта заказа Плотномер"),
    ("_Document144170", "Карта заказа Гранд"),
    ("_Document122051", "Карта заказа СПУ3М"),
    ("_Document86500", "Карта заказа Работ/услуг"),
)

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


def _bin_to_guid(value) -> str:
    if value is None:
        return ""
    raw = bytes(value)
    if len(raw) != 16 or raw == EMPTY16:
        return ""
    guid_bytes = raw[12:16] + raw[10:12] + raw[8:10] + raw[0:8]
    return str(uuid.UUID(bytes=guid_bytes))


def _clean_number(value) -> str:
    return "".join(ch for ch in str(value or "") if ch.isprintable()).strip()


def _from_sql_dt(value) -> datetime | None:
    if not isinstance(value, datetime) or value.year <= YEAR_OFFSET + 1:
        return None
    try:
        return value.replace(year=value.year - YEAR_OFFSET, tzinfo=None)
    except ValueError:
        return None


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _fmt_ru_date(value: datetime | None) -> str:
    return value.strftime("%d.%m.%Y") if value else ""


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


def _to_sql_dt(dt: datetime) -> datetime:
    return dt.replace(year=dt.year + YEAR_OFFSET)


def _resolve_department(cur) -> dict:
    cur.execute(
        f"""
        SELECT _Description, _Code, _Marked
        FROM {DEPT_TABLE} WITH (NOLOCK)
        WHERE _IDRRef = ?
        """,
        uuid_to_1c_bytes(DEPARTMENT_KEY),
    )
    row = cur.fetchone()
    name = DEPARTMENT_NAME
    code = None
    marked = None
    if row:
        name = (row[0] or DEPARTMENT_NAME).strip() or DEPARTMENT_NAME
        code = _clean_number(row[1]) or None
        marked = bool(row[2] and bytes(row[2]) != b"\x00")
    return {
        "entity": DEPT_TABLE,
        "key": DEPARTMENT_KEY,
        "name": name,
        "code": code,
        "deletion_mark": marked,
    }


def _load_survey_titles(cur, survey_refs: list[bytes]) -> dict[str, str]:
    out: dict[str, str] = {}
    pending = [ref for ref in survey_refs if ref and ref != EMPTY16]
    if not pending:
        return out
    for table, label in SURVEY_TABLES:
        still = [ref for ref in pending if _bin_to_guid(ref) not in out]
        if not still:
            break
        still_ph = ",".join("?" for _ in still)
        try:
            cur.execute(
                f"""
                SELECT _IDRRef, _Number, _Date_Time
                FROM {table} WITH (NOLOCK)
                WHERE _IDRRef IN ({still_ph})
                """,
                still,
            )
        except Exception:
            logger.exception("METD-M1: не удалось прочитать %s", table)
            continue
        for ref, number, doc_date in cur.fetchall():
            key = _bin_to_guid(ref)
            if not key:
                continue
            title = label
            num = _clean_number(number)
            if num:
                title = f"{title} №{num}"
            ru_date = _fmt_ru_date(_from_sql_dt(doc_date))
            if ru_date:
                title = f"{title} от {ru_date}"
            out[key] = title
    return out


def _month_row(year: int, month: int, cur) -> dict:
    period_start, period_end = _month_bounds(year, month)
    current_dt = datetime.now().replace(microsecond=0)
    evaluation_dt = min(current_dt, period_end)
    department = _resolve_department(cur)

    cur.execute(
        f"""
        SELECT
            s.{COL_STAGE_REF} AS stage_ref,
            s.{COL_PLAN_START} AS plan_start,
            s.{COL_PLAN_END} AS plan_end,
            s.{COL_ORDER_REF} AS order_ref,
            st._Number AS stage_number,
            st.{COL_FACT_END} AS fact_end,
            ord.{COL_SURVEY_REF} AS survey_ref
        FROM {SCHEDULE_TABLE} s WITH (NOLOCK)
        INNER JOIN {STAGE_TABLE} st WITH (NOLOCK)
            ON st._IDRRef = s.{COL_STAGE_REF}
        LEFT JOIN {ORDER_TABLE} ord WITH (NOLOCK)
            ON ord._IDRRef = s.{COL_ORDER_REF}
        WHERE s.{COL_PLAN_START} >= ?
          AND s.{COL_PLAN_END} <= ?
          AND s.{COL_PLAN_END} <= ?
          AND st.{COL_STAGE_DEPT} = ?
          AND s.{COL_STAGE_REF} <> ?
        """,
        _to_sql_dt(period_start),
        _to_sql_dt(period_end),
        _to_sql_dt(evaluation_dt),
        uuid_to_1c_bytes(DEPARTMENT_KEY),
        EMPTY16,
    )
    rows = cur.fetchall()
    survey_refs: list[bytes] = []
    on_time = 0
    late = 0
    details: list[dict] = []
    stage_rows: list[dict] = []
    for stage_ref, plan_start_raw, plan_end_raw, order_ref, stage_number, fact_end_raw, survey_ref in rows:
        plan_start = _from_sql_dt(plan_start_raw)
        plan_end = _from_sql_dt(plan_end_raw)
        fact_end = _from_sql_dt(fact_end_raw)
        is_late = _late_by_calendar_day(plan_end, fact_end, evaluation_dt)
        if is_late:
            late += 1
        else:
            on_time += 1
        stage_key = _bin_to_guid(stage_ref)
        order_key = _bin_to_guid(order_ref)
        survey_key = _bin_to_guid(survey_ref)
        if survey_ref and bytes(survey_ref) != EMPTY16:
            survey_refs.append(bytes(survey_ref))
        stage_rows.append({
            "Этап": _clean_number(stage_number) or stage_key,
            "Начало": _fmt_ru_date(plan_start),
            "Окончание": _fmt_ru_date(plan_end),
            "ЭтапФактическоеОкончание": _fmt_ru_date(fact_end),
            "ЗаказНаПроизводствоТД_ОпросныйЛист": survey_key,
            "stage_key": stage_key,
            "production_order_key": order_key,
            "survey_key": survey_key,
            "late": is_late,
        })
        details.append({
            "stage_key": stage_key,
            "stage_number": _clean_number(stage_number),
            "plan_start": _fmt_ru_date(plan_start),
            "plan_end": _fmt_ru_date(plan_end),
            "fact_end": _fmt_ru_date(fact_end),
            "late": is_late,
            "production_order_key": order_key,
        })

    survey_titles = _load_survey_titles(cur, survey_refs)
    for row in stage_rows:
        survey_key = row.get("survey_key") or ""
        if survey_key:
            row["ЗаказНаПроизводствоТД_ОпросныйЛист"] = survey_titles.get(survey_key) or survey_key

    late_stage_rows = [
        {key: value for key, value in row.items() if key != "late"}
        for row in stage_rows
        if row.get("late")
    ]

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
        "stage_rows": stage_rows,
        "late_stage_rows": late_stage_rows,
        "check_sum_ok": total == late + on_time,
        "period_start": _fmt(period_start),
        "period_end": _fmt(period_end),
        "current_date": _fmt(evaluation_dt),
        "debug": {
            "source_register": SCHEDULE_TABLE,
            "stage_document": STAGE_TABLE,
            "department": department,
            "schedule_rows_loaded": len(rows),
            "unique_stage_refs_from_schedule": len({row[0] for row in rows}),
            "population_schedule_rows": len(rows),
            "stage_docs_loaded": len({row[0] for row in rows}),
            "production_orders_loaded": len({row[3] for row in rows if row[3] and bytes(row[3]) != EMPTY16}),
            "survey_titles_loaded": len(survey_titles),
            "skipped_wrong_department": 0,
            "query_protocol": {
                "entity": SCHEDULE_TABLE,
                "filter": (
                    f"{COL_PLAN_START} >= period_start and {COL_PLAN_END} <= period_end "
                    f"and {COL_PLAN_END} <= evaluation_dt and {STAGE_TABLE}.{COL_STAGE_DEPT} = МС"
                ),
            },
            "details_sample": details[:100],
            "formula": (
                "total = строки графика этапов МС с непустым Этап; late = дата(ФактическоеОкончаниеЭтапа) > дата(Окончание) "
                "или факт пустой и дата(Окончание) < дата(ТекущаяДата); on_time = остальные"
            ),
        },
    }


def _stage_row_from_detail(detail: dict, late_row: dict | None = None) -> dict:
    late_row = late_row or {}
    stage_key = str(detail.get("stage_key") or late_row.get("stage_key") or "").strip()
    return {
        "Этап": detail.get("stage_number") or late_row.get("Этап") or stage_key or "Этап",
        "Начало": detail.get("plan_start") or late_row.get("Начало") or "",
        "Окончание": detail.get("plan_end") or late_row.get("Окончание") or "",
        "ЭтапФактическоеОкончание": (
            detail.get("fact_end") or late_row.get("ЭтапФактическоеОкончание") or ""
        ),
        "ЗаказНаПроизводствоТД_ОпросныйЛист": (
            late_row.get("ЗаказНаПроизводствоТД_ОпросныйЛист") or ""
        ),
        "stage_key": stage_key,
        "production_order_key": (
            detail.get("production_order_key") or late_row.get("production_order_key") or ""
        ),
        "survey_key": late_row.get("survey_key") or "",
        "late": bool(detail.get("late") if "late" in detail else late_row),
    }


def hydrate_stage_rows_in_month_row(month_row: dict | None) -> dict | None:
    """Достроить stage_rows из details_sample / late_stage_rows для старых кэшей."""
    if not isinstance(month_row, dict):
        return month_row
    existing = month_row.get("stage_rows")
    if isinstance(existing, list) and existing:
        return month_row

    details = (month_row.get("debug") or {}).get("details_sample") or []
    late_rows = month_row.get("late_stage_rows") or []
    late_by_key = {
        str(row.get("stage_key") or "").strip(): row
        for row in late_rows
        if isinstance(row, dict) and str(row.get("stage_key") or "").strip()
    }
    stage_rows: list[dict] = []
    seen: set[str] = set()
    for detail in details:
        if not isinstance(detail, dict):
            continue
        key = str(detail.get("stage_key") or "").strip()
        row = _stage_row_from_detail(detail, late_by_key.get(key))
        stage_rows.append(row)
        if key:
            seen.add(key)
    for key, late_row in late_by_key.items():
        if key in seen:
            continue
        stage_rows.append(_stage_row_from_detail({"late": True, "stage_key": key}, late_row))

    if not stage_rows:
        return month_row
    out = dict(month_row)
    out["stage_rows"] = stage_rows
    return out


def ensure_stage_rows_in_payload(payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return payload
    out = dict(payload)
    months = []
    for row in out.get("monthly_data") or []:
        months.append(hydrate_stage_rows_in_month_row(row) or row)
    if months:
        out["monthly_data"] = months
    if out.get("last_full_month_row") is not None:
        out["last_full_month_row"] = hydrate_stage_rows_in_month_row(out.get("last_full_month_row"))
    return out


def get_metrolog_production_plan_monthly(year: int, month: int) -> dict:
    ref_y = int(year)
    ref_m = max(1, min(12, int(month)))
    cached = _load_cache(ref_y, ref_m)
    if cached is not None:
        return ensure_stage_rows_in_payload(cached) or cached
    if not cache_manager.is_force_compute_context():
        stale = _load_cache(ref_y, ref_m, allow_stale=True)
        if stale is not None:
            stale = dict(stale)
            stale["cache_refresh_status"] = "running"
            return ensure_stage_rows_in_payload(stale) or stale
    with connect_ctx() as cn:
        cur = cn.cursor()
        months = [_month_row(ref_y, m, cur) for m in range(1, ref_m + 1)]
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
            "Строки регистра ГрафикЭтаповПроизводства2_2 из erp_pm, где этап относится к метрологической службе. "
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
