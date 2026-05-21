"""
Сопоставление отдела из стороннего проекта с 1С и вывод ФИО на должностях.

Использует:
  - source_department_holders_compact.json — source → matched_1c
  - list_enterprise_positions.py — актуальные назначения из кадровой истории

Примеры:
  python lookup_source_department.py "Технический директор"
  python lookup_source_department.py "ОДП"
  python lookup_source_department.py --list
  python lookup_source_department.py -i
  python lookup_source_department.py --departments custom.json "коммерческий директор"

Переменные окружения: ONEC_BASE_URL, ODATA_USER, ODATA_PASSWORD
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
from pathlib import Path

import requests

from list_enterprise_positions import (
    AUTH,
    build_report,
    find_key_by_full_path,
    load_hierarchy,
    normalize_text,
    STRUCTURE_ENTITY,
    structure_path,
)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DEPARTMENTS = ROOT_DIR / "source_department_holders_compact.json"


def load_departments(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, list):
        return data
    departments = data.get("departments")
    if isinstance(departments, list):
        return departments
    raise ValueError(f"ожидался массив departments в {path}")


def load_matches(path: Path) -> list[dict]:
    return load_departments(path)


def matched_paths(entry: dict) -> list[str]:
    value = entry.get("matched_1c")
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value:
        return [str(value).strip()]
    return []


def resolve_source(query: str, matches: list[dict]) -> tuple[list[dict], list[dict]]:
    """(точные совпадения, неоднозначные/частичные)."""
    norm = normalize_text(query)
    if not norm:
        raise ValueError("запрос пустой")

    exact = [row for row in matches if normalize_text(row.get("source")) == norm]
    if exact:
        return exact, []

    starts = [row for row in matches if normalize_text(row.get("source", "")).startswith(norm)]
    contains = [
        row
        for row in matches
        if norm in normalize_text(row.get("source", ""))
        and row not in starts
    ]
    partial = starts + contains
    return [], partial


def print_available(matches: list[dict]) -> None:
    print("Доступные source из сопоставления:")
    for row in matches:
        flag = " [review]" if row.get("review_required") else ""
        print(f"  - {row.get('source', '')}{flag}")


def print_result(
    query: str,
    entry: dict,
    onec_path: str,
    rows: list[dict],
) -> None:
    print()
    print(f"Запрос (source):     {query}")
    print(f"Сопоставление:       {entry.get('source', '')}")
    print(f"Путь в 1С:           {onec_path}")
    if entry.get("review_required"):
        print("Внимание:            сопоставление требует проверки")
        if entry.get("comment"):
            print(f"Комментарий:         {entry['comment']}")
    if not rows:
        print()
        print("  Сотрудники на должностях в этом подразделении не найдены.")
        return

    by_position: dict[str, list[dict]] = {}
    for row in rows:
        by_position.setdefault(row["position"], []).append(row)

    print()
    print(f"  Найдено назначений: {len(rows)}")
    for position in sorted(by_position, key=normalize_text):
        print(f"  {position}")
        for row in sorted(by_position[position], key=lambda item: normalize_text(item["employee"])):
            period = (row.get("period") or "")[:10]
            suffix = f" (с {period})" if period else ""
            dept = row.get("department") or ""
            if dept and normalize_text(dept) != normalize_text(onec_path):
                print(f"    - {row['employee']}{suffix}  [{dept}]")
            else:
                print(f"    - {row['employee']}{suffix}")


def query_one_path(
    session: requests.Session,
    structure: dict,
    onec_path: str,
) -> tuple[str | None, list[dict]]:
    root_key = find_key_by_full_path(structure, onec_path)
    if not root_key:
        return None, []
    rows = build_report(session, root_key=root_key)
    actual_path = structure_path(root_key, structure)
    return actual_path, rows


def run_query(session: requests.Session, query: str, matches: list[dict]) -> int:
    exact, partial = resolve_source(query, matches)
    if not exact and not partial:
        log(f"Не найдено сопоставление для «{query}»")
        print_available(matches)
        return 1

    if not exact and len(partial) > 1:
        log(f"Неоднозначный запрос «{query}». Уточните один из вариантов:")
        for row in partial:
            log(f"  - {row.get('source')}")
        return 2

    entries = exact or partial[:1]
    structure, _children = load_hierarchy(session, STRUCTURE_ENTITY)
    exit_code = 0

    for entry in entries:
        paths = matched_paths(entry)
        if not paths:
            log(f"Пустой matched_1c для «{entry.get('source')}»")
            exit_code = 1
            continue

        for onec_path in paths:
            actual_path, rows = query_one_path(session, structure, onec_path)
            if actual_path is None:
                log(f"Узел 1С не найден по пути: {onec_path}")
                exit_code = 1
                continue
            print_result(query, entry, actual_path, rows)
            if len(paths) > 1:
                print()

    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Отдел source → 1С → ФИО на должностях.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        help="Название отдела/должности из стороннего проекта (source)",
    )
    parser.add_argument(
        "--departments",
        type=Path,
        default=DEFAULT_DEPARTMENTS,
        help=f"JSON с департаментами source → matched_1c (по умолчанию {DEFAULT_DEPARTMENTS.name})",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Показать все source из файла сопоставления",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Интерактивный ввод названия отдела",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.departments.is_file():
        log(f"Файл не найден: {args.departments}")
        sys.exit(1)

    matches = load_departments(args.departments)

    if args.list:
        print_available(matches)
        return

    query = (args.query or "").strip()
    if args.interactive or not query:
        try:
            query = input("Отдел (source): ").strip()
        except EOFError:
            query = ""
    if not query:
        log("Укажите название отдела или используйте --list")
        sys.exit(2)

    session = requests.Session()
    session.auth = AUTH
    sys.exit(run_query(session, query, matches))


if __name__ == "__main__":
    main()
