from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from .cache_manager import locked_call
from .odata_http import request_with_retry
from .techdir_tekuchet import AUTH, BASE

logger = logging.getLogger(__name__)

CACHE_VERSION = 1
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"
EMPTY_DATE = "0001-01-01T00:00:00"

STRUCTURE_ENTITY = "Catalog_СтруктураПредприятия"
EMPLOYEE_ENTITY = "Catalog_Сотрудники"
HR_ENTITY = "InformationRegister_КадроваяИсторияСотрудников_RecordType"

STRUCTURE_SELECT = (
    "Ref_Key,Description,Parent_Key,DeletionMark,"
    "ТекущийРуководитель_Key,ТД_КонтролирующийРуководитель_Key"
)
EMPLOYEE_SELECT = "Ref_Key,Description,DeletionMark,ВАрхиве"
HR_SELECT = (
    "Period,LineNumber,Active,Сотрудник_Key,Подразделение_Key,ВидСобытия,"
    "ВидДоговора,ДействуетДо,Организация_Key,ГоловнойСотрудник_Key,ЭтоГоловнойСотрудник"
)

MANAGER_FIELD_WARNING = {
    "code": "FIELD_NOT_AVAILABLE",
    "message": (
        "В опубликованном OData регистре кадровой истории нет поля "
        "НепосредственныйРуководитель. Численность рассчитана по ветке "
        "Catalog_СтруктураПредприятия/Подразделение_Key, а не по персональной "
        "цепочке руководитель-сотрудник."
    ),
}

LOCAL_STRUCTURE_ALIASES: dict[str, tuple[str, ...]] = {
    "главный метролог": ("метрологическая служба", "отдел метрологии и сертификации"),
    "главный конструктор": ("конструкторское бюро",),
    "коммерческий директор": ("коммерческая служба", "коммерческий директор"),
    "начальник службы логистики": ("служба логистики", "логистика"),
    "директор по развитию": ("служба развития",),
    "главный бухгалтер": ("бухгалтерия", "финансово-бухгалтерская служба"),
    "начальник отдела автоматизации ит": ("отдел автоматизации ит", "служба автоматизации"),
    "начальник отдела сопровождения 1с": ("отдел сопровождения 1с", "сопровождение 1с"),
    "служба управления персоналом": ("служба управления персоналом", "отдел кадров"),
}


def normalize_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("ё", "е").replace("Ё", "Е")
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def month_cache_path(as_of: date) -> Path:
    return CACHE_DIR / f"employee_headcount_{as_of.year}_{as_of.month:02d}.json"


def _parse_date(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.startswith("0001-01-01") or text.startswith("0000-00-00"):
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00").split("+", 1)[0])
    except ValueError:
        return None


def _fetch_all(
    session: requests.Session,
    entity: str,
    *,
    query: str = "",
    page: int = 5000,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if query else ""
        page_query = f"{query}{sep}$top={page}&$skip={skip}&$format=json"
        url = f"{BASE}/{quote(entity)}?{page_query}"
        response = request_with_retry(
            session,
            url,
            timeout=timeout,
            retries=4,
            label=f"employee_headcount/{entity}",
        )
        if response is None:
            raise RuntimeError(f"Нет ответа OData для {entity}")
        if not response.ok:
            raise RuntimeError(f"OData {entity}: HTTP {response.status_code}: {response.text[:500]}")
        batch = response.json().get("value") or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += page
    return rows


def _load_structure_rows(session: requests.Session) -> tuple[dict[str, dict], dict[str, list[str]], dict[str, list[dict]]]:
    query = (
        f"$select={quote(STRUCTURE_SELECT, safe=',_')}"
        f"&$orderby={quote('Ref_Key', safe='')}"
    )
    by_key: dict[str, dict] = {}
    children: dict[str, list[str]] = defaultdict(list)
    by_name: dict[str, list[dict]] = defaultdict(list)
    for row in _fetch_all(session, STRUCTURE_ENTITY, query=query, timeout=60):
        key = str(row.get("Ref_Key") or "")
        if not key:
            continue
        by_key[key] = row
        children[str(row.get("Parent_Key") or EMPTY_GUID)].append(key)
        by_name[normalize_name(row.get("Description"))].append(row)
    return by_key, children, by_name


def _load_employee_flags(session: requests.Session) -> dict[str, dict]:
    query = f"$select={quote(EMPLOYEE_SELECT, safe=',_')}"
    return {
        str(row.get("Ref_Key") or ""): row
        for row in _fetch_all(session, EMPLOYEE_ENTITY, query=query, timeout=90)
        if row.get("Ref_Key")
    }


def _load_latest_hr_rows(session: requests.Session, as_of: date) -> tuple[dict[str, dict], int]:
    as_of_end = datetime.combine(as_of, time.max)
    query = (
        f"$filter={quote('Active eq true', safe='')}"
        f"&$select={quote(HR_SELECT, safe=',_')}"
        f"&$orderby={quote('Сотрудник_Key,Period,LineNumber', safe=',_')}"
    )
    latest: dict[str, dict] = {}
    read_rows = 0
    for row in _fetch_all(session, HR_ENTITY, query=query, timeout=180):
        read_rows += 1
        employee_key = str(row.get("Сотрудник_Key") or "")
        if not employee_key:
            continue
        period = _parse_date(row.get("Period"))
        if period and period > as_of_end:
            continue
        current = latest.get(employee_key)
        current_period = _parse_date(current.get("Period")) if current else None
        current_line = int(current.get("LineNumber") or 0) if current else 0
        line = int(row.get("LineNumber") or 0)
        if current is None or (period or datetime.min, line) >= (current_period or datetime.min, current_line):
            latest[employee_key] = row
    return latest, read_rows


def _is_active_structure_row(row: dict) -> bool:
    if row.get("DeletionMark"):
        return False
    return "ликв" not in normalize_name(row.get("Description"))


def _is_working_hr_row(row: dict, employee: dict | None, as_of: date) -> bool:
    if employee and (employee.get("DeletionMark") or employee.get("ВАрхиве")):
        return False
    if normalize_name(row.get("ВидСобытия")) == "увольнение":
        return False
    contract = normalize_name(row.get("ВидДоговора"))
    if "гпх" in contract or "гражданско" in contract:
        return False
    valid_until = _parse_date(row.get("ДействуетДо"))
    if valid_until and valid_until.date() < as_of:
        return False
    return bool(row.get("Подразделение_Key"))


def _descendant_keys(root_key: str, children: dict[str, list[str]]) -> set[str]:
    result: set[str] = set()
    stack = [root_key]
    while stack:
        key = stack.pop()
        if key in result:
            continue
        result.add(key)
        stack.extend(children.get(key, []))
    return result


def _pick_structure_match(
    name: str,
    structure_rows: dict[str, dict],
    by_name: dict[str, list[dict]],
) -> tuple[str | None, str]:
    normalized = normalize_name(name)
    search_names = [normalized, *LOCAL_STRUCTURE_ALIASES.get(normalized, ())]
    for candidate_name in search_names:
        exact = [row for row in by_name.get(candidate_name, []) if _is_active_structure_row(row)]
        if exact:
            best = sorted(exact, key=lambda row: len(str(row.get("Description") or "")))[0]
            return str(best.get("Ref_Key") or ""), "exact"
    for candidate_name in search_names:
        if not candidate_name:
            continue
        contains = [
            row
            for row in structure_rows.values()
            if _is_active_structure_row(row)
            and (
                candidate_name in normalize_name(row.get("Description"))
                or normalize_name(row.get("Description")) in candidate_name
            )
        ]
        if contains:
            best = sorted(contains, key=lambda row: len(str(row.get("Description") or "")))[0]
            return str(best.get("Ref_Key") or ""), "contains"
    return None, "not_found"


def _iter_structure_names(tree: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for name, child in (tree or {}).items():
        names.append(str(name))
        if isinstance(child, dict):
            names.extend(_iter_structure_names(child))
    return names


def _compute_payload(as_of: date, local_structure: dict[str, Any]) -> dict[str, Any]:
    session = requests.Session()
    session.auth = AUTH
    structure_rows, structure_children, by_name = _load_structure_rows(session)
    employees = _load_employee_flags(session)
    latest_hr, hr_rows_read = _load_latest_hr_rows(session, as_of)

    direct_by_dept: dict[str, int] = defaultdict(int)
    without_department = 0
    for employee_key, row in latest_hr.items():
        if not _is_working_hr_row(row, employees.get(employee_key), as_of):
            continue
        dept_key = str(row.get("Подразделение_Key") or "")
        if not dept_key or dept_key == EMPTY_GUID:
            without_department += 1
            continue
        direct_by_dept[dept_key] += 1

    counts_by_department: dict[str, int] = {}
    direct_counts_by_department: dict[str, int] = {}
    department_keys: dict[str, str] = {}
    match_quality: dict[str, str] = {}
    unresolved: list[str] = []

    for local_name in _iter_structure_names(local_structure):
        dept_key, quality = _pick_structure_match(local_name, structure_rows, by_name)
        match_quality[local_name] = quality
        if not dept_key:
            unresolved.append(local_name)
            continue
        subtree = _descendant_keys(dept_key, structure_children)
        department_keys[local_name] = dept_key
        direct_counts_by_department[local_name] = int(direct_by_dept.get(dept_key, 0))
        counts_by_department[local_name] = int(sum(direct_by_dept.get(key, 0) for key in subtree))

    total_working = int(sum(direct_by_dept.values()))
    payload = {
        "cache_version": CACHE_VERSION,
        "cache_date": date.today().isoformat(),
        "cached_at": datetime.now().isoformat(timespec="seconds"),
        "asOfDate": as_of.isoformat(),
        "cache_month": f"{as_of.year}-{as_of.month:02d}",
        "dataSource": {
            "system": "1C:ERP Управление предприятием 2",
            "release": "2.5.22.137",
            "entities": [STRUCTURE_ENTITY, EMPLOYEE_ENTITY, HR_ENTITY],
        },
        "mode": "DEPARTMENT_STRUCTURE_FALLBACK",
        "countsByDepartment": counts_by_department,
        "directCountsByDepartment": direct_counts_by_department,
        "departmentKeys": department_keys,
        "countsByManager": {},
        "quality": {
            "warnings": [MANAGER_FIELD_WARNING],
            "method": (
                "Берётся последний активный кадровый срез сотрудника на дату; "
                "уволенные и архивные сотрудники исключаются; ГПХ исключается при наличии "
                "такого значения в ВидДоговора; численность узла равна сумме по найденной "
                "ветке Catalog_СтруктураПредприятия."
            ),
            "hrRowsRead": hr_rows_read,
            "employeesInLatestSlice": len(latest_hr),
            "workingEmployeesTotal": total_working,
            "employeesWithoutDepartment": without_department,
            "employeesWithoutManagerInOrg": None,
            "unresolvedDepartments": unresolved,
            "matchQuality": match_quality,
        },
    }
    return payload


def _read_cache(path: Path, as_of: date) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("cache_month") != f"{as_of.year}-{as_of.month:02d}":
        return None
    return data


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


def get_employee_headcount(
    *,
    as_of: date | None = None,
    structure: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    as_of = as_of or date.today()
    structure = structure or {}
    path = month_cache_path(as_of)

    def _load_or_compute() -> dict[str, Any]:
        if not force:
            cached = _read_cache(path, as_of)
            if cached is not None:
                return cached
        payload = _compute_payload(as_of, structure)
        _write_cache(path, payload)
        return payload

    try:
        return locked_call(f"employee_headcount_{as_of.year}_{as_of.month:02d}", _load_or_compute)
    except Exception as exc:
        logger.exception("employee headcount calculation failed")
        cached = _read_cache(path, as_of)
        if cached is not None:
            cached = dict(cached)
            quality = dict(cached.get("quality") or {})
            warnings = list(quality.get("warnings") or [])
            warnings.append({"code": "STALE_CACHE_AFTER_ERROR", "message": str(exc)})
            quality["warnings"] = warnings
            cached["quality"] = quality
            return cached
        return {
            "cache_version": CACHE_VERSION,
            "asOfDate": as_of.isoformat(),
            "cache_month": f"{as_of.year}-{as_of.month:02d}",
            "dataSource": {"system": "1C:ERP Управление предприятием 2"},
            "mode": "ERROR",
            "countsByDepartment": {},
            "directCountsByDepartment": {},
            "countsByManager": {},
            "quality": {
                "warnings": [
                    MANAGER_FIELD_WARNING,
                    {"code": "HEADCOUNT_CALCULATION_ERROR", "message": str(exc)},
                ],
            },
        }
