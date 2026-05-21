"""METD-Q2: текучесть персонала метрологической службы по 1C:ERP."""
from __future__ import annotations

import calendar
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from .cache_manager import locked_call
from .techdir_tekuchet import (
    AUTH,
    BASE,
    MONTH_RU,
    _month_cell_matches,
    _normalize_viddokumenta,
    load_docs,
)
from .techdir_tekuchet_fact import (
    DISMISSAL_EVENT,
    EMPLOYEE_EXTRA_ENTITY,
    EXCLUDE_ATTR_KEY,
    HR_ENTITY,
    STAFFING_ENTITY,
    STAFF_HISTORY_ENTITY,
    build_org_structure_index,
    fetch_all,
    load_dismissals_for_month,
    load_excluded_employees,
    load_staff_history_for_positions,
    load_staffing_positions,
    month_end,
    staff_units_for_departments,
    turnover_percent,
)

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "metrolog_turnover_q2_v1"
CACHE_VERSION = 2
PLAN_FALLBACK_PCT = 5.0

STRUCTURE_TARGETS = [
    {
        "name": "Главный метролог",
        "structure_key": "433e13bb-f9a6-11ef-9623-6cb31113810c",
        "structure_code": "00-000149",
        "org_keys": [],
        "note": "В Catalog_ПодразделенияОрганизаций точного активного совпадения нет.",
    },
    {
        "name": "Отдел метрологии и сертификации",
        "structure_key": "0577c412-d5fd-11e9-829b-ac1f6b05524d",
        "structure_code": "00-000025",
        "org_keys": ["22882cd7-d5fc-11e9-829b-ac1f6b05524d"],
        "org_code": "НП00-0017",
        "note": "Точное активное совпадение в оргподразделениях.",
    },
    {
        "name": "Метрологическая служба",
        "structure_key": "4668a58a-6eb1-11e2-afce-001e67112509",
        "structure_code": "00-000056",
        "org_keys": [
            "8cf17e48-df91-11e9-829b-ac1f6b05524d",
            "d7584766-c46a-11e7-8267-ac1f6b05524d",
        ],
        "org_code": "НП00-0022 / АЛМ000002",
        "note": (
            "В Catalog_ПодразделенияОрганизаций два активных точных совпадения; "
            "оба включены в протокол без выбора наугад."
        ),
    },
]


def _cache_path(year: int, quarter: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"metrolog_turnover_q2_{year}_q{quarter}.json"


def _load_cache(year: int, quarter: int) -> dict | None:
    path = _cache_path(year, quarter)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    return data


def _save_cache(year: int, quarter: int, payload: dict) -> None:
    try:
        with _cache_path(year, quarter).open("w", encoding="utf-8") as f:
            json.dump(
                {
                    **payload,
                    "source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        logger.exception("Не удалось сохранить кэш METD-Q2")


def _quarter_months(quarter: int) -> list[int]:
    start = (quarter - 1) * 3 + 1
    return [start, start + 1, start + 2]


def _normalize_plan_percent(value: float) -> float:
    """В документе METD-Q2 план встречается как 0.5 при целевом отображении 5 %."""
    if 0 < value <= 1:
        return round(value * 10, 2)
    return round(value, 2)


def _completed_quarter_for_month(month: int) -> int:
    return max(1, min(4, (max(1, month) - 1) // 3))


def _plan_for_quarter(session: requests.Session, year: int, quarter: int) -> tuple[float, dict]:
    docs = load_docs(session)
    structure_keys = {target["structure_key"] for target in STRUCTURE_TARGETS}
    months = {f"{year}-{m:02d}" for m in _quarter_months(quarter)}
    plan_values: list[float] = []
    matched_docs: set[str] = set()
    matched_rows = 0

    for doc in docs:
        if doc.get("Подразделение_Key") not in structure_keys:
            continue
        if _normalize_viddokumenta(doc.get("ВидДокумента")) != "0":
            continue
        doc_matched = False
        for row in doc.get("Текучесть", []) or []:
            if not any(_month_cell_matches(row.get("Месяц"), month_str) for month_str in months):
                continue
            raw_plan = row.get("План")
            if raw_plan is None:
                continue
            plan_values.append(_normalize_plan_percent(float(raw_plan or 0)))
            matched_rows += 1
            doc_matched = True
        if doc_matched:
            matched_docs.add(doc.get("Ref_Key") or "")

    non_zero = [value for value in plan_values if value > 0]
    if non_zero:
        return round(max(non_zero), 2), {
            "plan_source": "Document_ТД_ТекучестьПерсонала:max_exact_department_plan",
            "plan_rows": matched_rows,
            "plan_documents": len(matched_docs),
        }

    return PLAN_FALLBACK_PCT, {
        "plan_source": "kpi_definition_fallback",
        "plan_rows": matched_rows,
        "plan_documents": len(matched_docs),
    }


def _metadata_protocol() -> dict[str, Any]:
    return {
        "entities": [
            {
                "entity": STAFFING_ENTITY,
                "fields": ["Подразделение_Key", "КоличествоСтавок", "Утверждена", "Закрыта", "ДатаЗакрытия"],
            },
            {
                "entity": STAFF_HISTORY_ENTITY,
                "fields": ["ПозицияШтатногоРасписания_Key", "Дата", "Используется", "КоличествоСтавок"],
            },
            {
                "entity": HR_ENTITY,
                "fields": ["Period", "Сотрудник_Key", "Подразделение_Key", "ВидСобытия"],
            },
            {
                "entity": EMPLOYEE_EXTRA_ENTITY,
                "fields": ["Ref_Key", "Свойство_Key", "Значение"],
            },
            {
                "entity": "Document_ТД_ТекучестьПерсонала",
                "fields": ["Подразделение_Key", "ВидДокумента", "Текучесть.План", "Текучесть.Месяц"],
            },
        ],
        "exclude_attribute": {
            "name": "НеУчитыватьПриТекучести",
            "key": EXCLUDE_ATTR_KEY,
            "rule": "Значение eq true исключается; false/пусто включается.",
        },
        "dismissal_filter": f"ВидСобытия eq '{DISMISSAL_EVENT}'",
        "children_included": False,
    }


def _query_protocol(year: int, quarter: int, as_of: date) -> list[dict]:
    months = _quarter_months(quarter)
    return [
        {
            "source": "Catalog_ШтатноеРасписание",
            "filter": "Подразделение_Key in exact org keys; DeletionMark eq false",
            "slice_date": as_of.isoformat(),
        },
        {
            "source": "InformationRegister_ИсторияИспользованияШтатногоРасписания",
            "filter": "Дата <= slice_date and Active eq true по найденным позициям",
            "slice_date": as_of.isoformat(),
        },
        {
            "source": "InformationRegister_КадроваяИсторияСотрудников",
            "filter": (
                f"{year}-{months[0]:02d}-01..{year}-{months[-1]:02d}-"
                f"{calendar.monthrange(year, months[-1])[1]:02d}; "
                f"Active eq true; {DISMISSAL_EVENT}; exact org keys"
            ),
        },
        {
            "source": "Catalog_Сотрудники_ДополнительныеРеквизиты",
            "filter": f"Свойство_Key eq {EXCLUDE_ATTR_KEY} and Значение eq true",
        },
        {
            "source": "Document_ТД_ТекучестьПерсонала",
            "filter": "DeletionMark eq false; ВидДокумента = план; exact structure keys",
        },
    ]


def compute_metrolog_turnover_quarter(year: int, quarter: int) -> dict:
    cached = _load_cache(year, quarter)
    if cached is not None:
        return cached

    session = requests.Session()
    session.auth = AUTH
    try:
        months = _quarter_months(quarter)
        as_of = month_end(year, months[-1])
        plan, plan_debug = _plan_for_quarter(session, year, quarter)

        departments = []
        for target in STRUCTURE_TARGETS:
            for org_key in target["org_keys"] or [""]:
                departments.append({
                    "group": target["name"],
                    "structure_key": target["structure_key"],
                    "structure_name": target["name"],
                    "structure_code": target.get("structure_code", ""),
                    "org_key": org_key,
                    "note": target.get("note", ""),
                })

        count_departments = [dept for dept in departments if dept.get("org_key")]
        org_keys = {dept["org_key"] for dept in count_departments}
        org_structure_index = build_org_structure_index(count_departments)
        excluded = load_excluded_employees(session)
        positions = load_staffing_positions(session, org_keys)
        history = load_staff_history_for_positions(session, list(positions.keys()), as_of)
        staff_by_org = staff_units_for_departments(
            session,
            count_departments,
            as_of,
            positions_cache=positions,
            history_cache=history,
        )

        all_dismissals = []
        seen_dismissals: set[tuple[str, str]] = set()
        for month in months:
            for row in load_dismissals_for_month(session, year, month, excluded, org_keys):
                key = (row.get("Сотрудник_Key") or "", row.get("Подразделение_Key") or "")
                if key in seen_dismissals:
                    continue
                seen_dismissals.add(key)
                all_dismissals.append(row)

        dismissed_by_org: dict[str, int] = {key: 0 for key in org_keys}
        for row in all_dismissals:
            org_key = row.get("Подразделение_Key") or ""
            if org_key in dismissed_by_org:
                dismissed_by_org[org_key] += 1

        rows = []
        for target in STRUCTURE_TARGETS:
            target_org_keys = target["org_keys"]
            staff = round(sum(staff_by_org.get(key, 0.0) for key in target_org_keys), 2)
            dismissed = sum(dismissed_by_org.get(key, 0) for key in target_org_keys)
            rows.append({
                "Подразделение": target["name"],
                "Штатные единицы": staff,
                "Уволено": dismissed,
                "Текучесть, %": turnover_percent(staff, dismissed),
                "Ref_Key структуры": target["structure_key"],
                "Ref_Key оргподразделения": ", ".join(target_org_keys) or "",
                "Сверка": target.get("note", ""),
            })

        total_staff = round(sum(row["Штатные единицы"] for row in rows), 2)
        total_dismissed = sum(int(row["Уволено"]) for row in rows)
        fact = turnover_percent(total_staff, total_dismissed)
        kpi_ratio = round(fact / plan * 100, 1) if plan else None

        payload = {
            "data_granularity": "quarterly",
            "quarter": quarter,
            "year": year,
            "label": f"Q{quarter} {year}",
            "plan": plan,
            "fact": fact,
            "kpi_pct": fact,
            "kpi_ratio_pct": kpi_ratio,
            "has_data": True,
            "values_unit": "%",
            "staff_units": total_staff,
            "dismissed_count": total_dismissed,
            "rows": rows,
            "totals": {
                "Штатные единицы": total_staff,
                "Уволено": total_dismissed,
                "Текучесть, %": fact,
            },
            "debug": {
                **plan_debug,
                "metadata_protocol": _metadata_protocol(),
                "query_protocol": _query_protocol(year, quarter, as_of),
                "excluded_employees": len(excluded),
                "staff_positions": len(positions),
                "org_structure_index": org_structure_index,
                "source": "1C:ERP OData; exact department names",
            },
        }
        _save_cache(year, quarter, payload)
        return payload
    finally:
        session.close()


def get_metrolog_turnover_ytd(year: int | None = None, month: int | None = None) -> dict:
    def _runner() -> dict:
        ref_date = date.today()
        ref_y = year or ref_date.year
        ref_m = month or ref_date.month
        last_q = _completed_quarter_for_month(ref_m)
        quarter_rows = []
        for quarter in range(1, last_q + 1):
            snapshot = compute_metrolog_turnover_quarter(ref_y, quarter)
            quarter_rows.append({
                "quarter": quarter,
                "year": ref_y,
                "label": f"Q{quarter} {ref_y}",
                "plan": snapshot["plan"],
                "fact": snapshot["fact"],
                "kpi_pct": snapshot["fact"],
                "kpi_ratio_pct": snapshot.get("kpi_ratio_pct"),
                "has_data": snapshot.get("has_data", True),
                "values_unit": "%",
                "staff_units": snapshot.get("staff_units"),
                "dismissed_count": snapshot.get("dismissed_count"),
            })

        ref_row = quarter_rows[-1] if quarter_rows else None
        return {
            "data_granularity": "quarterly",
            "quarterly_data": quarter_rows,
            "last_full_quarter_row": ref_row,
            "ytd": {
                "total_plan": ref_row.get("plan") if ref_row else None,
                "total_fact": ref_row.get("fact") if ref_row else None,
                "kpi_pct": ref_row.get("fact") if ref_row else None,
                "kpi_ratio_pct": ref_row.get("kpi_ratio_pct") if ref_row else None,
                "quarters_with_data": len(quarter_rows),
                "quarters_total": len(quarter_rows),
                "values_unit": "%",
            },
            "kpi_period": {
                "type": "last_full_quarter",
                "year": ref_y,
                "quarter": ref_row.get("quarter") if ref_row else last_q,
                "label": ref_row.get("label") if ref_row else f"Q{last_q} {ref_y}",
                "data_complete": ref_row is not None,
            },
            "debug": {
                "status": "ok",
                "kpi_id": "METD-Q2",
                "source": "1C:ERP staffing/dismissals + Document_ТД_ТекучестьПерсонала",
            },
        }

    ref_date = date.today()
    ref_y = year or ref_date.year
    ref_m = month or ref_date.month
    return locked_call(f"metrolog_turnover_ytd_{ref_y}_{ref_m:02d}", _runner)


def get_metrolog_turnover_table(year: int | None = None, month: int | None = None) -> dict:
    ref_date = date.today()
    ref_y = year or ref_date.year
    ref_m = month or ref_date.month
    quarter = _completed_quarter_for_month(ref_m)
    snapshot = compute_metrolog_turnover_quarter(ref_y, quarter)
    return {
        "name": "Текучесть персонала метрологической службы",
        "periodicity": "ежеквартально",
        "description": (
            "Штатные единицы на дату среза и увольнения за период по точным "
            "подразделениям из 1C:ERP, без включения вложенных подразделений."
        ),
        "period": {
            "year": ref_y,
            "quarter": quarter,
            "label": f"Q{quarter} {ref_y}",
            "months": [MONTH_RU[m] for m in _quarter_months(quarter)],
        },
        "columns": [
            "Подразделение",
            "Штатные единицы",
            "Уволено",
            "Текучесть, %",
            "Ref_Key структуры",
            "Ref_Key оргподразделения",
            "Сверка",
        ],
        "rows": snapshot["rows"],
        "totals": snapshot["totals"],
        "debug": snapshot["debug"],
    }
