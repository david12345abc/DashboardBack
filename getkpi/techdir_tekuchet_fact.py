"""
Факт текучести TD-Q2: штатные единицы и увольнения по подразделениям техдирекции.

План по-прежнему из Document_ТД_ТекучестьПерсонала (techdir_tekuchet).
Факт: уволено / штатные единицы × 100 % (на последний день месяца).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote

import requests

from .odata_http import request_with_retry
from .techdir_tekuchet import (
    AUTH,
    BASE,
    GROUP_ORDER,
    load_structure,
    normalize_name,
    resolve_group_department_keys,
)

logger = logging.getLogger(__name__)

EMPTY_DATE = "0001-01-01T00:00:00"

TEKUCHEST_ALLOWED_ORG_KEYS = {
    "d455d61b-f396-11e8-8283-ac1f6b05524d",
    "a37213b5-e6f1-11e7-826b-ac1f6b05524d",
    "75d572f4-e4bd-11f0-976f-6cb31113810e",
    "5d1da688-f077-11f0-977e-6cb31113810e",
    "22882cd7-d5fc-11e9-829b-ac1f6b05524d",
    "d6669ecf-e4bd-11f0-976f-6cb31113810e",
}

STAFF_COUNT_STRUCTURE_KEYS = frozenset({
    "3abd426b-53f8-11e6-8121-001e67112509",
    "12eea873-61d9-11e5-8104-001e67112509",
    "0b3f223a-f537-11f0-9784-6cb31113810e",
    "0577c412-d5fd-11e9-829b-ac1f6b05524d",
    "a885694e-f537-11f0-9784-6cb31113810e",
})

STRUCTURE_ORG_SKIP = {
    "6a367bba-2246-11eb-8474-ac1f6b05524d": "управленческий узел без ставок в штатном расписании",
    "5d0c35d2-b007-11f0-9723-6cb31113810c": "управленческий узел без ставок в штатном расписании",
    "4668a58a-6eb1-11e2-afce-001e67112509": (
        "родительский узел; ставки учитываются в Отделе метрологии и сертификации"
    ),
    "be038466-f514-11f0-9784-6cb31113810e": "не входит в перечень подразделений для расчёта текучести",
    "4182e761-f537-11f0-9784-6cb31113810e": "не входит в перечень подразделений для расчёта текучести",
    "dfa3dd25-d695-11ea-83c6-ac1f6b05524d": "не входит в перечень подразделений для расчёта текучести",
}

STRUCTURE_ORG_OVERRIDES: dict[str, str] = {
    "3abd426b-53f8-11e6-8121-001e67112509": "d455d61b-f396-11e8-8283-ac1f6b05524d",
    "12eea873-61d9-11e5-8104-001e67112509": "a37213b5-e6f1-11e7-826b-ac1f6b05524d",
}

STRUCTURE_ORG_ALIASES: dict[str, tuple[str, ...]] = {}

ORG_DEPT_ENTITY = "Catalog_ПодразделенияОрганизаций"
STAFFING_ENTITY = "Catalog_ШтатноеРасписание"
STAFF_HISTORY_ENTITY = "InformationRegister_ИсторияИспользованияШтатногоРасписания_RecordType"
HR_ENTITY = "InformationRegister_КадроваяИсторияСотрудников_RecordType"
EMPLOYEE_EXTRA_ENTITY = "Catalog_Сотрудники_ДополнительныеРеквизиты"

EXCLUDE_ATTR_KEY = "87fd0b0c-52c0-11f1-9805-6cb31113810e"
DISMISSAL_EVENT = "Увольнение"


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


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
        response = request_with_retry(
            session,
            page_url,
            timeout=timeout,
            retries=4,
            label="techdir_tekuchet_fact",
        )
        if response is None or not response.ok:
            break
        batch = response.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def is_active_dept(row: dict) -> bool:
    if row.get("DeletionMark"):
        return False
    return "ликв" not in normalize_name(row.get("Description", ""))


def load_hierarchy(session: requests.Session, entity: str) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    url = (
        f"{BASE}/{quote(entity)}"
        f"?$select={quote('Ref_Key,Description,Parent_Key,DeletionMark,Code', safe=',_')}"
    )
    by_key: dict[str, dict] = {}
    exact: dict[str, list[dict]] = defaultdict(list)
    for row in fetch_all(session, url, page=5000, timeout=60):
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        exact[normalize_name(row.get("Description", ""))].append(row)
    return by_key, exact


def pick_best_org_candidate(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    allowed = TEKUCHEST_ALLOWED_ORG_KEYS
    return sorted(
        candidates,
        key=lambda row: (
            1 if row.get("DeletionMark") else 0,
            0 if row.get("Ref_Key") in allowed else 1,
            len(normalize_name(row.get("Description", ""))),
            row.get("Description", ""),
        ),
    )[0]


def in_tekuchest_org_scope(org_key: str) -> bool:
    return org_key in TEKUCHEST_ALLOWED_ORG_KEYS


def map_structure_to_org(
    structure_key: str,
    structure_name: str,
    org_rows: list[dict],
    org_exact: dict[str, list[dict]],
    org_by_key: dict[str, dict],
) -> tuple[str | None, str]:
    if structure_key in STRUCTURE_ORG_SKIP:
        return None, STRUCTURE_ORG_SKIP[structure_key]

    override = STRUCTURE_ORG_OVERRIDES.get(structure_key)
    if override:
        row = org_by_key.get(override)
        if row and is_active_dept(row) and in_tekuchest_org_scope(override):
            return override, ""
        return None, "override не найден в оргструктуре"

    search_names = (structure_name,) + STRUCTURE_ORG_ALIASES.get(structure_key, ())
    for name in search_names:
        exact_matches = [
            row for row in org_exact.get(normalize_name(name), [])
            if is_active_dept(row) and in_tekuchest_org_scope(row["Ref_Key"])
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]["Ref_Key"], ""
        if len(exact_matches) > 1:
            best = pick_best_org_candidate(exact_matches)
            return best["Ref_Key"], f"выбрано из {len(exact_matches)} одноимённых"

    for name in search_names:
        alias_norm = normalize_name(name)
        contains_matches = [
            row for row in org_rows
            if is_active_dept(row)
            and alias_norm in normalize_name(row.get("Description", ""))
            and in_tekuchest_org_scope(row["Ref_Key"])
        ]
        if contains_matches:
            best = pick_best_org_candidate(contains_matches)
            return best["Ref_Key"], "частичное совпадение наименования"

    return None, "не найдено среди разрешённых оргподразделений ТД"


def build_department_map(session: requests.Session) -> tuple[list[dict], dict[str, str]]:
    structure_rows, _structure_by_key, structure_exact = load_structure(session)
    _group_keys, diagnostics = resolve_group_department_keys(structure_rows, structure_exact)
    org_by_key, org_exact = load_hierarchy(session, ORG_DEPT_ENTITY)
    org_rows = list(org_by_key.values())

    departments: list[dict] = []
    warnings: dict[str, str] = {}

    for group_name in GROUP_ORDER:
        matched = diagnostics.get(group_name, [])
        if not matched:
            departments.append({
                "group": group_name,
                "structure_key": "",
                "structure_name": group_name,
                "structure_code": "",
                "org_key": "",
                "note": "не найдено в Catalog_СтруктураПредприятия",
            })
            continue

        for structure_key, structure_name in matched:
            org_key, note = map_structure_to_org(
                structure_key,
                structure_name,
                org_rows,
                org_exact,
                org_by_key,
            )
            departments.append({
                "group": group_name,
                "structure_key": structure_key,
                "structure_name": structure_name,
                "structure_code": org_by_key.get(org_key, {}).get("Code", "") if org_key else "",
                "org_key": org_key or "",
                "note": note,
            })
            if note and not org_key:
                warnings[structure_key] = note

    return departments, warnings


def build_org_structure_index(departments: list[dict]) -> dict[str, str]:
    index: dict[str, str] = {}
    for dept in departments:
        structure_key = dept.get("structure_key") or ""
        org_key = dept.get("org_key") or ""
        if not structure_key:
            continue
        if org_key:
            index[org_key] = structure_key
        if structure_key == "0b3f223a-f537-11f0-9784-6cb31113810e":
            for alt_key in (
                "75d572f4-e4bd-11f0-976f-6cb31113810e",
                "5d1da688-f077-11f0-977e-6cb31113810e",
            ):
                index[alt_key] = structure_key
    return index


def staff_count_departments(departments: list[dict]) -> list[dict]:
    return [
        dept for dept in departments
        if dept.get("structure_key") in STAFF_COUNT_STRUCTURE_KEYS and dept.get("org_key")
    ]


def dismissal_org_keys(departments: list[dict]) -> set[str]:
    keys = set(TEKUCHEST_ALLOWED_ORG_KEYS)
    keys.update(dept["org_key"] for dept in departments if dept.get("org_key"))
    return keys


def load_excluded_employees(session: requests.Session) -> set[str]:
    flt = f"Свойство_Key eq guid'{EXCLUDE_ATTR_KEY}' and Значение eq true"
    url = (
        f"{BASE}/{quote(EMPLOYEE_EXTRA_ENTITY)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Ref_Key,Значение', safe=',_')}"
    )
    rows = fetch_all(session, url, page=5000, timeout=60)
    return {row["Ref_Key"] for row in rows if row.get("Ref_Key")}


def load_staffing_positions(session: requests.Session, org_keys: set[str]) -> dict[str, dict]:
    if not org_keys:
        return {}

    positions: dict[str, dict] = {}
    url = (
        f"{BASE}/{quote(STAFFING_ENTITY)}"
        f"?$select={quote('Ref_Key,Description,Подразделение_Key,КоличествоСтавок,Утверждена,Закрыта,ДатаЗакрытия,DeletionMark', safe=',_')}"
    )
    for row in fetch_all(session, url, page=5000, timeout=120):
        if row.get("DeletionMark"):
            continue
        dept_key = row.get("Подразделение_Key") or ""
        if dept_key not in org_keys:
            continue
        positions[row["Ref_Key"]] = row
    return positions


def load_staff_history_for_positions(
    session: requests.Session,
    position_keys: list[str],
    as_of: date,
) -> dict[str, dict]:
    if not position_keys:
        return {}

    as_of_ts = f"{as_of.isoformat()}T23:59:59"
    latest: dict[str, dict] = {}
    chunk_size = 20

    for idx in range(0, len(position_keys), chunk_size):
        chunk = position_keys[idx : idx + chunk_size]
        ref_filter = " or ".join(
            f"ПозицияШтатногоРасписания_Key eq guid'{key}'" for key in chunk
        )
        flt = f"({ref_filter}) and Дата le datetime'{as_of_ts}' and Active eq true"
        url = (
            f"{BASE}/{quote(STAFF_HISTORY_ENTITY)}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select={quote('ПозицияШтатногоРасписания_Key,Дата,Используется,КоличествоСтавок,Active', safe=',_')}"
        )
        for row in fetch_all(session, url, page=5000, timeout=120):
            if not row.get("Используется"):
                continue
            pos_key = row.get("ПозицияШтатногоРасписания_Key")
            if not pos_key:
                continue
            prev = latest.get(pos_key)
            if prev is None or (row.get("Дата") or "") > (prev.get("Дата") or ""):
                latest[pos_key] = row

    return latest


def is_position_active_on_date(position: dict, as_of: date) -> bool:
    if not position.get("Утверждена"):
        return False
    if not position.get("Закрыта"):
        return True
    close_raw = position.get("ДатаЗакрытия") or ""
    if not close_raw or close_raw.startswith(EMPTY_DATE):
        return False
    return datetime.fromisoformat(close_raw.replace("Z", "")).date() >= as_of


def staff_units_for_departments(
    session: requests.Session,
    departments: list[dict],
    as_of: date,
    positions_cache: dict[str, dict] | None = None,
    history_cache: dict[str, dict] | None = None,
) -> dict[str, float]:
    org_keys = {d["org_key"] for d in departments if d["org_key"]}
    positions = positions_cache or load_staffing_positions(session, org_keys)
    history = history_cache or load_staff_history_for_positions(
        session,
        list(positions.keys()),
        as_of,
    )

    by_org: dict[str, float] = defaultdict(float)
    for pos_key, position in positions.items():
        dept_key = position.get("Подразделение_Key") or ""
        if dept_key not in org_keys:
            continue

        hist = history.get(pos_key)
        if hist is not None:
            by_org[dept_key] += float(hist.get("КоличествоСтавок") or 0)
            continue

        if is_position_active_on_date(position, as_of):
            by_org[dept_key] += float(position.get("КоличествоСтавок") or 0)

    return by_org


def load_dismissals_for_month(
    session: requests.Session,
    year: int,
    month: int,
    excluded_employees: set[str],
    allowed_org_keys: set[str],
) -> list[dict]:
    if not allowed_org_keys:
        return []

    start = date(year, month, 1)
    end = month_end(year, month)
    base_flt = (
        f"Period ge datetime'{start.isoformat()}T00:00:00'"
        f" and Period le datetime'{end.isoformat()}T23:59:59'"
        f" and Active eq true"
        f" and ВидСобытия eq '{DISMISSAL_EVENT}'"
    )
    select = quote("Period,Сотрудник_Key,Подразделение_Key,ВидСобытия", safe=",_")

    rows: list[dict] = []
    org_list = sorted(allowed_org_keys)
    chunk_size = 15
    for idx in range(0, len(org_list), chunk_size):
        chunk = org_list[idx : idx + chunk_size]
        dept_filter = " or ".join(
            f"Подразделение_Key eq guid'{org_key}'" for org_key in chunk
        )
        flt = f"{base_flt} and ({dept_filter})"
        url = (
            f"{BASE}/{quote(HR_ENTITY)}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select={select}"
        )
        rows.extend(fetch_all(session, url, page=5000, timeout=120))

    result: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        employee_key = row.get("Сотрудник_Key") or ""
        dept_key = row.get("Подразделение_Key") or ""
        if not employee_key or not dept_key:
            continue
        if dept_key not in allowed_org_keys:
            continue
        if employee_key in excluded_employees:
            continue
        dedupe_key = (employee_key, dept_key)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        result.append(row)

    return result


def count_dismissals_by_structure(
    dismissals: list[dict],
    org_structure_index: dict[str, str],
    allowed_org_keys: set[str],
) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in dismissals:
        dept_key = row.get("Подразделение_Key") or ""
        if dept_key not in allowed_org_keys:
            continue
        structure_key = org_structure_index.get(dept_key)
        if structure_key:
            counts[structure_key] += 1
    return counts


def turnover_percent(staff: float, dismissed: int) -> float:
    if staff <= 0:
        return 0.0
    return round(dismissed / staff * 100, 1)


def compute_turnover_fact_percent(
    year: int,
    month: int,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """
    Факт TD-Q2: доля уволенных в % от штатных единиц на конец месяца.
    Возвращает total_fact (процент) и диагностику расчёта.
    """
    own_session = session is None
    if own_session:
        session = requests.Session()
        session.auth = AUTH

    try:
        departments, _warnings = build_department_map(session)
        count_departments = staff_count_departments(departments)
        org_keys = {d["org_key"] for d in count_departments}
        allowed_dismissal_org_keys = dismissal_org_keys(count_departments)
        org_structure_index = build_org_structure_index(count_departments)

        excluded = load_excluded_employees(session)
        positions = load_staffing_positions(session, org_keys)
        as_of = month_end(year, month)
        history = load_staff_history_for_positions(session, list(positions.keys()), as_of)
        staff_by_org = staff_units_for_departments(
            session,
            count_departments,
            as_of,
            positions_cache=positions,
            history_cache=history,
        )
        dismissals = load_dismissals_for_month(
            session, year, month, excluded, allowed_dismissal_org_keys,
        )
        dismissed_by_structure = count_dismissals_by_structure(
            dismissals, org_structure_index, allowed_dismissal_org_keys,
        )

        total_staff = sum(staff_by_org.get(org_key, 0.0) for org_key in org_keys)
        total_dismissed = sum(dismissed_by_structure.values())
        total_fact = turnover_percent(total_staff, total_dismissed)

        return {
            "total_fact": total_fact,
            "staff_units": round(total_staff, 2),
            "dismissed_count": total_dismissed,
            "excluded_employees": len(excluded),
            "staff_positions": len(positions),
            "fact_source": "hr_staff_dismissals_turnover_pct",
        }
    except Exception:
        logger.exception("TD-Q2: ошибка расчёта факта текучести за %s-%02d", year, month)
        raise
    finally:
        if own_session:
            session.close()
