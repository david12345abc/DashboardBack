"""
Вывод должностей и занимающих их сотрудников из 1С.

Логика:
  1. Берём активных сотрудников из Catalog_Сотрудники.
  2. Берём актуальную кадровую историю из
     InformationRegister_КадроваяИсторияСотрудников_RecordType.
  3. Разворачиваем Должность_Key через Catalog_Должности.
  4. Разворачиваем Подразделение_Key через Catalog_СтруктураПредприятия.
     Если кадровая история ссылается на Catalog_ПодразделенияОрганизаций,
     сопоставляем подразделение со структурой предприятия по названию.

Использование:
  python list_enterprise_positions.py
  python list_enterprise_positions.py "Технический директор"

Результат сохраняется в текстовый файл рядом со скриптом.

Переменные окружения:
  ONEC_BASE_URL     адрес базы 1С, по умолчанию http://192.168.2.229:81/erp_pm
  ODATA_USER        пользователь OData, по умолчанию odata.user
  ODATA_PASSWORD    пароль OData
"""

from __future__ import annotations

import functools
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
EMPLOYEE_ENTITY = "Catalog_Сотрудники"
HR_ENTITY = "InformationRegister_КадроваяИсторияСотрудников_RecordType"
POSITION_ENTITY = "Catalog_Должности"
STRUCTURE_ENTITY = "Catalog_СтруктураПредприятия"
ORG_DEPT_ENTITY = "Catalog_ПодразделенияОрганизаций"
EMPTY = "00000000-0000-0000-0000-000000000000"
EMPTY_DATE = "0001-01-01T00:00:00"
SCRIPT_DIR = Path(__file__).resolve().parent


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


def fetch_all(
    session: requests.Session,
    url: str,
    page: int = 1000,
    timeout: int = 120,
) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        response = session.get(page_url, timeout=timeout)
        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

        batch = response.json().get("value", [])
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)

    return rows


def normalize_text(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in value).split())


def slugify_filename(value: str | None) -> str:
    slug = normalize_text(value).replace(" ", "_")
    return slug or "all"


def is_empty_key(value: str | None) -> bool:
    return not value or value == EMPTY


def is_empty_date(value: str | None) -> bool:
    return not value or value.startswith(EMPTY_DATE)


def is_future_or_empty(value: str | None) -> bool:
    if is_empty_date(value):
        return True
    return value[:19] >= datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def is_marked_or_archived(row: dict) -> bool:
    for field in ("DeletionMark", "Недействителен", "ВАрхиве"):
        if row.get(field) is True:
            return True
    return False


def load_employees(session: requests.Session) -> dict[str, dict]:
    url = (
        f"{BASE}/{quote(EMPLOYEE_ENTITY)}"
        f"?$format=json"
        f"&$select={quote('Ref_Key,Description,DeletionMark,ВАрхиве,ФизическоеЛицо_Key', safe=',_')}"
    )
    employees = fetch_all(session, url)
    return {
        row["Ref_Key"]: row
        for row in employees
        if row.get("Ref_Key") and not is_marked_or_archived(row)
    }


def load_latest_hr_rows(session: requests.Session) -> dict[str, dict]:
    url = (
        f"{BASE}/{quote(HR_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote('Active eq true', safe='')}"
        f"&$select={quote('Period,Сотрудник_Key,ФизическоеЛицо_Key,Подразделение_Key,Должность_Key,ДействуетДо', safe=',_')}"
    )
    rows = fetch_all(session, url)
    latest: dict[str, dict] = {}

    for row in rows:
        if not is_future_or_empty(row.get("ДействуетДо")):
            continue

        period = row.get("Period") or ""
        for key_field in ("Сотрудник_Key", "ФизическоеЛицо_Key"):
            key = row.get(key_field)
            if is_empty_key(key):
                continue
            if key not in latest or period > (latest[key].get("Period") or ""):
                latest[key] = row

    return latest


def load_positions(session: requests.Session) -> dict[str, str]:
    url = (
        f"{BASE}/{quote(POSITION_ENTITY)}"
        f"?$format=json"
        f"&$select={quote('Ref_Key,Description,DeletionMark', safe=',_')}"
    )
    return {
        row["Ref_Key"]: (row.get("Description") or "").strip()
        for row in fetch_all(session, url)
        if row.get("Ref_Key") and not row.get("DeletionMark")
    }


def load_hierarchy(
    session: requests.Session,
    entity: str,
) -> tuple[dict[str, dict], dict[str, list[str]]]:
    url = (
        f"{BASE}/{quote(entity)}"
        f"?$format=json"
        f"&$select={quote('Ref_Key,Description,Parent_Key,DeletionMark', safe=',_')}"
    )
    by_key: dict[str, dict] = {}
    children: dict[str, list[str]] = defaultdict(list)

    for row in fetch_all(session, url):
        key = row.get("Ref_Key")
        if not key or row.get("DeletionMark"):
            continue
        by_key[key] = row
        children[row.get("Parent_Key") or EMPTY].append(key)

    return by_key, children


def build_name_index(hierarchy: dict[str, dict]) -> dict[str, list[str]]:
    index: dict[str, list[str]] = defaultdict(list)
    for key, row in hierarchy.items():
        name = normalize_text(row.get("Description"))
        if name:
            index[name].append(key)
    return index


def find_root_key(structure: dict[str, dict], root_name: str | None) -> str | None:
    if not root_name:
        return None

    target = normalize_text(root_name)
    exact = [
        key
        for key, row in structure.items()
        if normalize_text(row.get("Description")) == target
    ]
    if exact:
        return sorted(exact, key=lambda key: structure[key].get("Description") or "")[0]

    contains = [
        key
        for key, row in structure.items()
        if target in normalize_text(row.get("Description"))
    ]
    if contains:
        return sorted(contains, key=lambda key: structure[key].get("Description") or "")[0]

    return None


def collect_subtree(root_key: str, children: dict[str, list[str]]) -> set[str]:
    subtree: set[str] = set()
    stack = [root_key]
    while stack:
        key = stack.pop()
        if key in subtree:
            continue
        subtree.add(key)
        stack.extend(children.get(key, []))
    return subtree


def structure_path(key: str, structure: dict[str, dict]) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    current = key
    while not is_empty_key(current) and current not in seen and current in structure:
        seen.add(current)
        row = structure[current]
        name = (row.get("Description") or "").strip()
        if name:
            parts.append(name)
        current = row.get("Parent_Key") or EMPTY
    return " / ".join(reversed(parts))


def resolve_enterprise_dept_key(
    dept_key: str,
    structure: dict[str, dict],
    org_depts: dict[str, dict],
    structure_name_index: dict[str, list[str]],
) -> str | None:
    if dept_key in structure:
        return dept_key

    org_dept = org_depts.get(dept_key)
    if not org_dept:
        return None

    candidates = structure_name_index.get(normalize_text(org_dept.get("Description")), [])
    if not candidates:
        return None

    return sorted(candidates, key=lambda key: structure_path(key, structure))[0]


def resolve_department_display(
    dept_key: str,
    enterprise_dept_key: str | None,
    structure: dict[str, dict],
    org_depts: dict[str, dict],
) -> str:
    if enterprise_dept_key:
        return structure_path(enterprise_dept_key, structure)

    if dept_key in org_depts:
        return f"Оргструктура: {structure_path(dept_key, org_depts)}"

    return "(подразделение не найдено в структуре предприятия)"


def find_key_by_full_path(structure: dict[str, dict], path: str) -> str | None:
    """Найти узел Catalog_СтруктураПредприятия по полному пути «A / B / C»."""
    target = normalize_text(path)
    if not target:
        return None

    exact = [
        key
        for key in structure
        if normalize_text(structure_path(key, structure)) == target
    ]
    if exact:
        return sorted(exact, key=lambda key: structure_path(key, structure))[0]

    by_suffix: list[tuple[int, str]] = []
    for key in structure:
        node_path = normalize_text(structure_path(key, structure))
        if not node_path:
            continue
        if target == node_path or target.endswith(node_path) or node_path.endswith(target):
            by_suffix.append((len(node_path), key))
    if by_suffix:
        by_suffix.sort(key=lambda item: (-item[0], structure_path(item[1], structure)))
        return by_suffix[0][1]

    last_segment = path.rsplit("/", 1)[-1].strip()
    return find_root_key(structure, last_segment)


def build_report(
    session: requests.Session,
    root_name: str | None = None,
    root_key: str | None = None,
) -> list[dict]:
    employees = load_employees(session)
    latest_hr = load_latest_hr_rows(session)
    positions = load_positions(session)
    structure, children = load_hierarchy(session, STRUCTURE_ENTITY)
    org_depts, _org_children = load_hierarchy(session, ORG_DEPT_ENTITY)
    structure_name_index = build_name_index(structure)

    allowed_depts: set[str] | None = None
    if root_key is None:
        root_key = find_root_key(structure, root_name)
    elif root_key not in structure:
        raise RuntimeError(f"Подразделение не найдено в структуре предприятия: {root_key}")
    if (root_name or root_key) and not root_key:
        raise RuntimeError(f"Подразделение в структуре предприятия не найдено: {root_name}")
    if root_key:
        allowed_depts = collect_subtree(root_key, children)

    rows: list[dict] = []
    seen_assignments: set[tuple[str, str, str, str]] = set()
    for employee in employees.values():
        employee_key = employee.get("Ref_Key")
        person_key = employee.get("ФизическоеЛицо_Key")
        hr_row = latest_hr.get(employee_key) or latest_hr.get(person_key) or {}

        dept_key = hr_row.get("Подразделение_Key") or EMPTY
        if is_empty_key(dept_key):
            continue
        enterprise_dept_key = resolve_enterprise_dept_key(
            dept_key,
            structure,
            org_depts,
            structure_name_index,
        )
        if allowed_depts is not None and enterprise_dept_key not in allowed_depts:
            continue

        position_key = hr_row.get("Должность_Key") or EMPTY
        period = hr_row.get("Period", "")
        assignment_key = (
            person_key or employee_key or "",
            enterprise_dept_key or dept_key,
            position_key,
            period,
        )
        if assignment_key in seen_assignments:
            continue
        seen_assignments.add(assignment_key)

        position = positions.get(position_key) or "(должность не указана)"
        rows.append(
            {
                "department": resolve_department_display(
                    dept_key,
                    enterprise_dept_key,
                    structure,
                    org_depts,
                ),
                "position": position,
                "employee": (employee.get("Description") or "").strip(),
                "period": period,
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            normalize_text(row["department"]),
            normalize_text(row["position"]),
            normalize_text(row["employee"]),
        ),
    )


def format_report(rows: list[dict]) -> str:
    if not rows:
        return "Актуальные должности с сотрудниками не найдены.\n"

    lines = [f"Найдено назначений: {len(rows)}"]
    current_department = ""
    current_position = ""

    for row in rows:
        if row["department"] != current_department:
            current_department = row["department"]
            current_position = ""
            lines.append("")
            lines.append(current_department)

        if row["position"] != current_position:
            current_position = row["position"]
            lines.append(f"  {current_position}")

        period = row["period"][:10]
        suffix = f" с {period}" if period else ""
        lines.append(f"    - {row['employee']}{suffix}")

    return "\n".join(lines) + "\n"


def save_report(rows: list[dict], root_name: str | None) -> Path:
    suffix = slugify_filename(root_name)
    output_path = SCRIPT_DIR / f"enterprise_positions_report_{suffix}.txt"
    output_path.write_text(format_report(rows), encoding="utf-8-sig")
    return output_path


def main() -> None:
    root_name = " ".join(arg.strip() for arg in sys.argv[1:] if arg.strip()) or None
    try:
        session = requests.Session()
        session.auth = AUTH
        output_path = save_report(build_report(session, root_name), root_name)
        print(f"Отчёт сохранён: {output_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
