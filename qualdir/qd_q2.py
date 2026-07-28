"""
QD-Q2 — факт текучести персонала службы качества.

По ТЗ: штатные единицы (ставки ШР на дату среза) и увольнения за период
с фильтром доп. реквизита НеУчитыватьПриТекучести.

Эталон (OData): DashboardBack/qualdir/turnover.py + getkpi/techdir_tekuchet_fact.py
  Catalog_СтруктураПредприятия
  Catalog_ПодразделенияОрганизаций
  Catalog_ШтатноеРасписание
  InformationRegister_ИсторияИспользованияШтатногоРасписания
  InformationRegister_КадроваяИсторияСотрудников (ВидСобытия = Увольнение)
  Catalog_Сотрудники_ДополнительныеРеквизиты (Свойство = НеУчитыватьПриТекучести)

Подразделения контура качества (без автоподтягивания вложенных сверх списка):
  ОТК-1
  ОТК-2
  Лаборатория неразрушающего контроля
  Отдел управления несоответствиями

Текучесть % = Уволено / Штатные единицы × 100 (до 1 знака).

Использование:
  python qualdir/qd_q2.py
  python qualdir/qd_q2.py 2026 6
  python qualdir/qd_q2.py 2026-01 2026-06
"""

from __future__ import annotations

import functools
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()
import logging as _logging

log = _logging.getLogger(__name__).info
SCRIPT_DIR = Path(__file__).resolve().parent

BASE = os.getenv("ONEC_BASE_URL", os.getenv("ODATA_BASE", "http://192.168.2.229:81/erp_pm/odata/standard.odata")).rstrip("/")
if not BASE.endswith("/odata/standard.odata"):
    BASE = f"{BASE.rstrip('/')}/odata/standard.odata"
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)
EMPTY_DATE = "0001-01-01T00:00:00"

# Как QD_Q2_GROUP_ALIASES в DashboardBack/qualdir/turnover.py
QD_Q2_DEPARTMENTS: list[tuple[str, tuple[str, ...]]] = [
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    (
        "Лаборатория неразрушающего контроля",
        ("лаборатория неразрушающего контроля", "лнк"),
    ),
    (
        "Отдел управления несоответствиями",
        (
            "отдел управления несоответствиями",
            "отдел управления несоотвествиями",
        ),
    ),
]
QD_Q2_GROUP_ORDER = [name for name, _ in QD_Q2_DEPARTMENTS]

# Родители в СтруктураПредприятия — для выбора при дублях имён.
QUALITY_PARENT_HINTS = (
    "заместитель директора по качеству",
    "зам технического директора по качеству",
    "зам. технического директора по качеству",
    "качество",
    "отк",
)

# Явные GUID оргподразделений (после сверки структура↔орг).
# Структура: ОТК-1 / ОТК-2; орг: «Отдел технического контроля №N».
STRUCTURE_ORG_OVERRIDES: dict[str, str] = {
    "37f2482d-62c5-11e6-8121-001e67112509": "42dfd1d2-e6f3-11e7-826b-ac1f6b05524d",  # ОТК-1
    "91111f82-62c5-11e6-8121-001e67112509": "85f3b385-04c2-11e8-826d-ac1f6b05524d",  # ОТК-2
    "ee502cb3-544c-11ed-89e3-ac1f6b05524d": "7ebffdf2-88b3-11eb-8577-ac1f6b05524d",  # ЛНК
}

# Имя в СтруктураПредприятия → варианты в ПодразделенияОрганизаций.
STRUCTURE_ORG_ALIASES: dict[str, tuple[str, ...]] = {
    "ОТК-1": ("Отдел технического контроля №1", "Отдел технического контроля N1"),
    "ОТК-2": ("Отдел технического контроля №2", "Отдел технического контроля N2"),
}

# Предпочтительные родители оргподразделений при дублях имён.
ORG_PARENT_PREFERENCE = (
    "служба качества",
    "обособленное подразделение",
)

ORG_DEPT_ENTITY = "Catalog_ПодразделенияОрганизаций"
STRUCTURE_ENTITY = "Catalog_СтруктураПредприятия"
STAFFING_ENTITY = "Catalog_ШтатноеРасписание"
STAFF_HISTORY_ENTITY = "InformationRegister_ИсторияИспользованияШтатногоРасписания_RecordType"
HR_ENTITY = "InformationRegister_КадроваяИсторияСотрудников_RecordType"
EMPLOYEE_EXTRA_ENTITY = "Catalog_Сотрудники_ДополнительныеРеквизиты"

EXCLUDE_ATTR_KEY = "87fd0b0c-52c0-11f1-9805-6cb31113810e"  # НеУчитыватьПриТекучести
DISMISSAL_EVENT = "Увольнение"

MONTH_NAMES = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

# Иерархия: только узлы из списка, без авто-включения детей.
HIERARCHY_MODE = "listed_only_no_auto_children"


def normalize_name(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in value).split())


def parse_month(value: str) -> tuple[int, int]:
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Месяц должен быть в формате ГГГГ-ММ")
    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def parse_period_args(argv: list[str] | None = None) -> tuple[tuple[int, int], tuple[int, int], str]:
    args = [arg.strip() for arg in (argv if argv is not None else sys.argv[1:]) if arg.strip()]
    now = datetime.now()
    # по умолчанию — последний полный месяц
    if now.month == 1:
        default = (now.year - 1, 12)
    else:
        default = (now.year, now.month - 1)

    if not args:
        return default, default, f"{default[0]:04d}-{default[1]:02d}"

    if len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        period = (int(args[0]), int(args[1]))
        if not 1 <= period[1] <= 12:
            raise ValueError("Месяц должен быть от 1 до 12")
        return period, period, f"{period[0]:04d}-{period[1]:02d}"

    if len(args) == 1 and len(args[0]) == 4 and args[0].isdigit():
        year = int(args[0])
        end_m = default[1] if year == default[0] else 12
        if year == now.year and now.month == 1:
            end_m = 12
        elif year == now.year:
            end_m = now.month - 1 or 1
        return (year, 1), (year, end_m), str(year)

    if len(args) == 1:
        period = parse_month(args[0])
        return period, period, args[0]

    if len(args) == 2:
        start = parse_month(args[0])
        end = parse_month(args[1])
        if start > end:
            raise ValueError("Дата начала должна быть не позже даты окончания")
        return start, end, f"{args[0]}_{args[1]}"

    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ, ГОД МЕСЯЦ или ГГГГ-ММ ГГГГ-ММ")


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    year, month = start
    result: list[tuple[int, int]] = []
    while (year, month) <= end:
        result.append((year, month))
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
            log(f"  {progress_label}: загружено {len(rows)}")
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def is_active_dept(row: dict) -> bool:
    if row.get("DeletionMark"):
        return False
    return "ликв" not in normalize_name(row.get("Description", ""))


def load_hierarchy(
    session: requests.Session,
    entity: str,
) -> tuple[dict[str, dict], dict[str, list[dict]]]:
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


def pick_best_candidate(candidates: list[dict]) -> dict:
    return sorted(
        candidates,
        key=lambda row: (
            1 if row.get("DeletionMark") else 0,
            len(normalize_name(row.get("Description", ""))),
            row.get("Description", ""),
            row.get("Code", ""),
        ),
    )[0]


def _is_under_quality(
    row: dict,
    structure_by_key: dict[str, dict],
) -> bool:
    """True, если узел или его предки относятся к контуру качества."""
    seen: set[str] = set()
    current: dict | None = row
    while current:
        key = current.get("Ref_Key") or ""
        if not key or key in seen:
            break
        seen.add(key)
        name = normalize_name(current.get("Description", ""))
        if any(hint in name for hint in QUALITY_PARENT_HINTS):
            return True
        parent_key = current.get("Parent_Key") or ""
        current = structure_by_key.get(parent_key)
    return False


def _org_parent_rank(row: dict, org_by_key: dict[str, dict]) -> int:
    parent = org_by_key.get(row.get("Parent_Key") or "", {})
    parent_name = normalize_name(parent.get("Description", ""))
    for idx, hint in enumerate(ORG_PARENT_PREFERENCE):
        if hint in parent_name:
            return idx
    return len(ORG_PARENT_PREFERENCE)


def pick_best_org_candidate(
    candidates: list[dict],
    org_by_key: dict[str, dict],
) -> dict:
    return sorted(
        candidates,
        key=lambda row: (
            1 if row.get("DeletionMark") else 0,
            _org_parent_rank(row, org_by_key),
            len(normalize_name(row.get("Description", ""))),
            row.get("Description", ""),
            row.get("Code", ""),
        ),
    )[0]


def map_structure_to_org(
    structure_key: str,
    structure_name: str,
    org_exact: dict[str, list[dict]],
    org_by_key: dict[str, dict],
) -> tuple[str | None, str, dict | None]:
    override = STRUCTURE_ORG_OVERRIDES.get(structure_key)
    if override:
        row = org_by_key.get(override)
        if row and is_active_dept(row):
            return override, "override по GUID (сверка имён структура↔орг)", row
        return None, "override GUID не найден в оргструктуре", None

    search_names = (structure_name,) + STRUCTURE_ORG_ALIASES.get(structure_name, ())
    for name in search_names:
        matches = [
            row
            for row in org_exact.get(normalize_name(name), [])
            if is_active_dept(row)
        ]
        if len(matches) == 1:
            note = ""
            if name != structure_name:
                note = f"алиас оргсправочника: «{name}»"
            return matches[0]["Ref_Key"], note, matches[0]
        if len(matches) > 1:
            best = pick_best_org_candidate(matches, org_by_key)
            note = f"выбрано из {len(matches)} одноимённых в ПодразделенияОрганизаций"
            if name != structure_name:
                note = f"алиас оргсправочника: «{name}»; {note}"
            return best["Ref_Key"], note, best
    return None, "не найдено в Catalog_ПодразделенияОрганизаций", None


def _find_structure_matches(
    display: str,
    aliases: tuple[str, ...],
    structure_exact: dict[str, list[dict]],
    structure_by_key: dict[str, dict],
) -> list[dict]:
    for name in (display,) + aliases:
        matches = [
            row
            for row in structure_exact.get(normalize_name(name), [])
            if is_active_dept(row)
        ]
        if matches:
            under_q = [r for r in matches if _is_under_quality(r, structure_by_key)]
            return under_q or matches
    return []


def build_department_map(session: requests.Session) -> list[dict[str, Any]]:
    log("[1/5] Сопоставление подразделений службы качества ...")
    structure_by_key, structure_exact = load_hierarchy(session, STRUCTURE_ENTITY)
    org_by_key, org_exact = load_hierarchy(session, ORG_DEPT_ENTITY)

    departments: list[dict[str, Any]] = []
    for display, aliases in QD_Q2_DEPARTMENTS:
        struct_matches = _find_structure_matches(
            display, aliases, structure_exact, structure_by_key
        )
        if not struct_matches:
            departments.append(
                {
                    "group": display,
                    "structure_key": "",
                    "structure_name": display,
                    "structure_code": "",
                    "org_key": "",
                    "org_name": "",
                    "org_code": "",
                    "note": "не найдено в Catalog_СтруктураПредприятия",
                }
            )
            continue

        note_dup = ""
        if len(struct_matches) > 1:
            note_dup = f"дубли в СтруктураПредприятия: {len(struct_matches)}"

        structure = pick_best_candidate(struct_matches)
        org_key, note, org_row = map_structure_to_org(
            structure["Ref_Key"],
            structure.get("Description", display),
            org_exact,
            org_by_key,
        )
        if note_dup:
            note = f"{note_dup}; {note}" if note else note_dup
        departments.append(
            {
                "group": display,
                "structure_key": structure["Ref_Key"],
                "structure_name": structure.get("Description", display),
                "structure_code": structure.get("Code", ""),
                "org_key": org_key or "",
                "org_name": (org_row or {}).get("Description", ""),
                "org_code": (org_row or {}).get("Code", ""),
                "note": note,
            }
        )

    mapped = sum(1 for d in departments if d["org_key"])
    log(f"  Эталонных узлов: {len(QD_Q2_GROUP_ORDER)}, сопоставлено с оргструктурой: {mapped}")
    return departments


def load_excluded_employees(session: requests.Session) -> set[str]:
    flt = f"Свойство_Key eq guid'{EXCLUDE_ATTR_KEY}' and Значение eq true"
    url = (
        f"{BASE}/{quote(EMPLOYEE_EXTRA_ENTITY)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Ref_Key,Значение', safe=',_')}"
    )
    rows = fetch_all(session, url, page=5000, timeout=60, progress_label="исключения")
    return {row["Ref_Key"] for row in rows if row.get("Ref_Key")}


def load_staffing_positions(
    session: requests.Session,
    org_keys: set[str],
) -> dict[str, dict]:
    if not org_keys:
        return {}
    positions: dict[str, dict] = {}
    url = (
        f"{BASE}/{quote(STAFFING_ENTITY)}"
        f"?$select={quote('Ref_Key,Description,Подразделение_Key,КоличествоСтавок,Утверждена,Закрыта,ДатаЗакрытия,DeletionMark', safe=',_')}"
    )
    for row in fetch_all(session, url, page=5000, timeout=120, progress_label="штатное расписание"):
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
            pos_key = row.get("ПозицияШтатногоРасписания_Key")
            if not pos_key:
                continue
            # Берём срез последних на дату — в т.ч. Используется=Ложь (закрытие позиции).
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


def staff_units_by_org(
    positions: dict[str, dict],
    history: dict[str, dict],
    org_keys: set[str],
    as_of: date,
) -> dict[str, float]:
    by_org: dict[str, float] = defaultdict(float)
    for pos_key, position in positions.items():
        dept_key = position.get("Подразделение_Key") or ""
        if dept_key not in org_keys:
            continue
        hist = history.get(pos_key)
        if hist is not None:
            # Срез истории + статус позиции (закрытые на дату среза не входят).
            if hist.get("Используется") and is_position_active_on_date(position, as_of):
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


def turnover_percent(staff: float, dismissed: int) -> float:
    if staff <= 0:
        return 0.0
    return round(dismissed / staff * 100, 1)


def calc_month(
    session: requests.Session,
    departments: list[dict[str, Any]],
    year: int,
    month: int,
    excluded: set[str],
    positions: dict[str, dict],
) -> dict[str, Any]:
    as_of = month_end(year, month)
    org_keys = {d["org_key"] for d in departments if d.get("org_key")}
    history = load_staff_history_for_positions(session, list(positions.keys()), as_of)
    staff_by_org = staff_units_by_org(positions, history, org_keys, as_of)
    dismissals = load_dismissals_for_month(session, year, month, excluded, org_keys)

    dismissed_by_org: dict[str, int] = defaultdict(int)
    for row in dismissals:
        dismissed_by_org[row["Подразделение_Key"]] += 1

    rows: list[dict[str, Any]] = []
    total_staff = 0.0
    total_dismissed = 0
    for dept in departments:
        org_key = dept.get("org_key") or ""
        staff = round(staff_by_org.get(org_key, 0.0), 2) if org_key else 0.0
        dismissed = dismissed_by_org.get(org_key, 0) if org_key else 0
        note = dept.get("note") or ""
        if org_key and staff == 0 and not note:
            note = "нет позиций в штатном расписании"
        rows.append(
            {
                "group": dept["group"],
                "structure_code": dept.get("structure_code") or "",
                "structure_name": dept.get("structure_name") or dept["group"],
                "org_code": dept.get("org_code") or "",
                "org_name": dept.get("org_name") or "",
                "org_key": org_key,
                "staff_units": staff,
                "dismissed": dismissed,
                "note": note,
            }
        )
        total_staff += staff
        total_dismissed += dismissed

    total_staff = round(total_staff, 2)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "as_of": as_of.isoformat(),
        "period_start": date(year, month, 1).isoformat(),
        "period_end": as_of.isoformat(),
        "rows": rows,
        "total_staff_units": total_staff,
        "total_dismissed": total_dismissed,
        "turnover_pct": turnover_percent(total_staff, total_dismissed),
        "dismissal_details": [
            {
                "period": r.get("Period"),
                "employee": r.get("Сотрудник_Key"),
                "department": r.get("Подразделение_Key"),
            }
            for r in dismissals
        ],
    }


def format_month_table(snapshot: dict[str, Any]) -> str:
    lines = [
        f"QD-Q2 факт текучести за {snapshot['month_name']} {snapshot['year']}",
        f"Дата среза штата: {snapshot['as_of']}",
        f"Период увольнений: {snapshot['period_start']} … {snapshot['period_end']}",
        f"Иерархия: {HIERARCHY_MODE}",
        "",
        f"{'Код орг':<12} {'Подразделение':<55} {'Штат':>8} {'Увол.':>6} {'Примечание'}",
        f"{'-' * 12} {'-' * 55} {'-' * 8} {'-' * 6} {'-' * 30}",
    ]
    for row in snapshot["rows"]:
        code = row["org_code"] or row["structure_code"] or "—"
        name = row["org_name"] or row["structure_name"]
        lines.append(
            f"{code:<12} {name:<55} {row['staff_units']:>8.2f} {row['dismissed']:>6} {row['note']}"
        )
    lines.extend(
        [
            f"{'-' * 12} {'-' * 55} {'-' * 8} {'-' * 6} {'-' * 30}",
            f"{'ИТОГО':<12} {'':<55} {snapshot['total_staff_units']:>8.2f} "
            f"{snapshot['total_dismissed']:>6}",
            "",
            f"Текучесть = {snapshot['total_dismissed']} / {snapshot['total_staff_units']} "
            f"× 100% = {snapshot['turnover_pct']:.1f}%",
            "",
        ]
    )
    return "\n".join(lines)


def build_protocol(departments: list[dict], excluded_count: int) -> str:
    lines = [
        "=== Протокол метаданных (1С:ERP / OData erp_pm) ===",
        "Справочник подразделений (управленческий): Catalog_СтруктураПредприятия",
        "Справочник подразделений (кадровый/ШР): Catalog_ПодразделенияОрганизаций",
        "Штатное расписание: Catalog_ШтатноеРасписание + "
        "InformationRegister_ИсторияИспользованияШтатногоРасписания",
        "Увольнения: InformationRegister_КадроваяИсторияСотрудников, ВидСобытия='Увольнение', поле Period",
        f"Доп. реквизит: Catalog_Сотрудники_ДополнительныеРеквизиты, "
        f"Свойство_Key={EXCLUDE_ATTR_KEY} (НеУчитыватьПриТекучести), тип Булево",
        "Значение реквизита: текущее в карточке сотрудника (Истина → исключить; пусто/Ложь → учесть)",
        f"Иерархия: {HIERARCHY_MODE}",
        f"Сотрудников с НеУчитыватьПриТекучести=Истина: {excluded_count}",
        "",
        "Сопоставление эталон → ИБ:",
    ]
    for d in departments:
        lines.append(
            f"  • {d['group']} → structure={d.get('structure_key') or '—'} "
            f"org={d.get('org_key') or '—'} [{d.get('note') or 'ok'}]"
        )
    lines.append("")
    return "\n".join(lines)


def run_report(
    start: tuple[int, int],
    end: tuple[int, int],
) -> dict[str, Any]:
    session = requests.Session()
    session.auth = AUTH
    try:
        departments = build_department_map(session)
        org_keys = {d["org_key"] for d in departments if d.get("org_key")}

        log("[2/5] Исключения по текучести ...")
        excluded = load_excluded_employees(session)
        log(f"  Исключено сотрудников: {len(excluded)}")

        log("[3/5] Позиции штатного расписания ...")
        positions = load_staffing_positions(session, org_keys)
        log(f"  Позиций в контуре: {len(positions)}")

        months = iter_months(start, end)
        snapshots: list[dict[str, Any]] = []
        for i, (year, month) in enumerate(months, 1):
            log(f"[4/5] Расчёт {year}-{month:02d} ({i}/{len(months)}) ...")
            snapshots.append(
                calc_month(session, departments, year, month, excluded, positions)
            )

        log("[5/5] Готово.")
        return {
            "departments": departments,
            "excluded_employees": len(excluded),
            "hierarchy_mode": HIERARCHY_MODE,
            "months": snapshots,
            "protocol": build_protocol(departments, len(excluded)),
            "odata_base": BASE,
            "exclude_attr_key": EXCLUDE_ATTR_KEY,
        }
    finally:
        session.close()


def save_report(period_slug: str, payload: dict[str, Any], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"qd_q2_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"qd_q2_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return txt_path, json_path


def build_qd_q2_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Payload в стиле DashboardBack QD-Q2 (факт из HR; план здесь не считается)."""
    now = datetime.now()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m

    report = run_report((year, 1), (year, month))
    monthly_rows: list[dict[str, Any]] = []
    for snap in report["months"]:
        monthly_rows.append(
            {
                "month": snap["month"],
                "year": snap["year"],
                "month_name": snap["month_name"],
                "plan": None,
                "fact": snap["turnover_pct"],
                "staff_units": snap["total_staff_units"],
                "dismissed": snap["total_dismissed"],
                "kpi_pct": snap["turnover_pct"],
                "has_data": True,
                "values_unit": "%",
                "by_department": snap["rows"],
            }
        )
    ref_row = monthly_rows[-1] if monthly_rows else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
        },
        "ytd": {
            "total_plan": None,
            "total_fact": ref_row["fact"] if ref_row else None,
            "kpi_pct": ref_row["kpi_pct"] if ref_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "%",
        },
        "debug": {
            "kpi_id": "QD-Q2",
            "status": "ok",
            "source": "qualdir.qd_q2.odata",
            "fact_source": "hr_staff_dismissals_turnover_pct",
            "target_departments": list(QD_Q2_GROUP_ORDER),
            "hierarchy_mode": HIERARCHY_MODE,
            "excluded_employees": report["excluded_employees"],
            "odata_base": BASE,
        },
    }


def main() -> None:
    try:
        start, end, slug = parse_period_args()
        report = run_report(start, end)
        parts = [report["protocol"]]
        for snap in report["months"]:
            parts.append(format_month_table(snap))
        if len(report["months"]) > 1:
            parts.append("Сводка по месяцам:")
            parts.append(f"{'Месяц':<10} {'Штат':>8} {'Увол.':>6} {'Текучесть %':>12}")
            parts.append(f"{'-' * 10} {'-' * 8} {'-' * 6} {'-' * 12}")
            for snap in report["months"]:
                parts.append(
                    f"{snap['year']:04d}-{snap['month']:02d} "
                    f"{snap['total_staff_units']:>8.2f} "
                    f"{snap['total_dismissed']:>6} "
                    f"{snap['turnover_pct']:>12.1f}"
                )
            parts.append("")
        text = "\n".join(parts)
        print(text)
        txt_path, json_path = save_report(slug, report, text)
        print(f"Отчёт сохранён: {txt_path}")
        print(f"JSON: {json_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

QD_Q2_YTD_CACHE_PREFIX = "qualdir_qd_q2_ytd"
QD_Q2_YTD_DISK_TAG = "qualdir_qd_q2_ytd_payload_v5"
QD_Q2_YTD_DISK_VERSION = 5


def turnover_month_cache_path(year: int, month: int) -> _Path:
    return month_cache_path("qualdir_tekuchet", year, month)


def qd_q2_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_Q2_YTD_CACHE_PREFIX, ry, rm)


def get_qd_q2_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_Q2_YTD_CACHE_PREFIX,
        source_tag=QD_Q2_YTD_DISK_TAG,
        version=QD_Q2_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_q2",
        compute_fn=lambda y, m: build_qd_q2_payload(y, m),
        kpi_id="QD-Q2",
    )
