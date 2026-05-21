"""
Просроченные задачи протоколов по департаменту (source).

Цепочка:
  1. source из source_department_holders_compact.json → matched_1c
  2. matched_1c + актуальные кадровые данные 1С → ФИО на должности
  3. ФИО → Ref_Key (Catalog_Пользователи)
  4. Ref_Key → просроченные задачи (InformationRegister_ТД_ЗадачиПротоколов)

Использование:
  python protocol_tasks_by_department.py "Технический директор"
  python protocol_tasks_by_department.py -i
  python protocol_tasks_by_department.py --list
  python protocol_tasks_by_department.py --date 2026-05-19 "Отдел ВЭД"
  python protocol_tasks_by_department.py --table "ОДП"
  python protocol_tasks_by_department.py --csv tasks.csv "Технический директор"
"""

from __future__ import annotations

import argparse
import functools
import sys
from pathlib import Path

import requests

from department_holders import rows_for_matched_path, select_holders
from list_enterprise_positions import AUTH, build_report, normalize_text
from lookup_source_department import (
    DEFAULT_DEPARTMENTS,
    load_departments,
    matched_paths,
    resolve_source,
)
from lookup_user_ref import (
    EMPTY,
    build_fio_index,
    load_persons,
    load_users,
    resolve_user_ref,
)
from protocol_tasks_by_leader import (
    build_users_by_key,
    load_tasks,
    normalize_row,
    parse_as_of,
    print_cards,
    print_table,
    write_csv,
)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)


def find_holders_live(
    source: str,
    onec_paths: list[str],
    all_rows: list[dict],
) -> list[dict]:
    holders: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for onec_path in onec_paths:
        scoped, resolved_path = rows_for_matched_path(all_rows, onec_path)
        if not scoped:
            continue
        found, method = select_holders(source, resolved_path, scoped)
        for holder in found:
            key = (holder["fio"], holder["position"])
            if key in seen:
                continue
            seen.add(key)
            holders.append(
                {
                    **holder,
                    "match_method": method,
                    "matched_1c": onec_path,
                    "resolved_1c": resolved_path,
                }
            )
    return holders


def print_available(departments: list[dict]) -> None:
    print("Доступные департаменты (source):")
    for row in departments:
        print(f"  - {row.get('source', '')}")


def print_department_header(
    source: str,
    matched_1c: str | list[str],
    holder: dict,
    leader_key: str,
) -> None:
    print()
    print("═" * 72)
    print(f"  Департамент:  {source}")
    if isinstance(matched_1c, list):
        print(f"  Путь 1С:      {holder.get('matched_1c') or matched_1c[0]}")
    else:
        print(f"  Путь 1С:      {matched_1c}")
    if holder.get("resolved_1c") and holder.get("resolved_1c") != holder.get("matched_1c"):
        print(f"  Уточнённый:   {holder['resolved_1c']}")
    print(f"  Должность:    {holder.get('position') or '—'}")
    print(f"  ФИО:          {holder['fio']}")
    print(f"  Ref_Key:      {leader_key}")
    print("═" * 72)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Просроченные задачи протоколов по департаменту (source).",
    )
    parser.add_argument(
        "department",
        nargs="?",
        help="Название департамента (source из source_department_holders_compact.json)",
    )
    parser.add_argument(
        "--departments",
        type=Path,
        default=DEFAULT_DEPARTMENTS,
        help=f"JSON source → matched_1c (по умолчанию {DEFAULT_DEPARTMENTS.name})",
    )
    parser.add_argument(
        "--date",
        dest="as_of",
        help="Дата «сегодня» в формате YYYY-MM-DD (по умолчанию — текущая дата)",
    )
    parser.add_argument("--csv", dest="csv_path", help="Путь к CSV-файлу для выгрузки")
    parser.add_argument(
        "--table",
        action="store_true",
        help="Широкая таблица (по умолчанию — компактные карточки)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Показать все source из compact JSON",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Интерактивный ввод департамента",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.departments.is_file():
        log(f"Файл не найден: {args.departments}")
        sys.exit(1)

    departments = load_departments(args.departments)

    if args.list:
        print_available(departments)
        return

    query = (args.department or "").strip()
    if args.interactive or not query:
        try:
            query = input("Департамент (source): ").strip()
        except EOFError:
            query = ""
    if not query:
        log("Укажите департамент или используйте --list")
        sys.exit(2)

    exact, partial = resolve_source(query, departments)
    if not exact and not partial:
        log(f"Департамент не найден: «{query}»")
        print_available(departments)
        sys.exit(1)
    if not exact and len(partial) > 1:
        log(f"Неоднозначный запрос «{query}». Уточните один из вариантов:")
        for row in partial:
            log(f"  - {row.get('source')}")
        sys.exit(2)

    entry = (exact or partial[:1])[0]
    source = entry.get("source", query)
    onec_paths = matched_paths(entry)
    if not onec_paths:
        log(f"Пустой matched_1c для «{source}»")
        sys.exit(1)

    as_of = parse_as_of(args.as_of)
    session = requests.Session()
    session.auth = AUTH

    log("Загрузка кадровых назначений из 1С …")
    all_rows = build_report(session)
    log(f"  Назначений: {len(all_rows)}")

    holders = find_holders_live(source, onec_paths, all_rows)
    if not holders:
        log(
            f"Не найден сотрудник на должности для «{source}» "
            f"(пути 1С: {len(onec_paths)})."
        )
        sys.exit(1)

    log(f"  Найдено ФИО: {len(holders)}")

    log("Загрузка пользователей 1С …")
    users = load_users(session)
    person_keys = {
        user.get("ФизическоеЛицо_Key")
        for user in users
        if user.get("ФизическоеЛицо_Key") and user.get("ФизическоеЛицо_Key") != EMPTY
    }
    persons = load_persons(session, person_keys)
    exact_index, ambiguous = build_fio_index(users, persons)
    users_by_key = build_users_by_key(session)

    all_task_rows: list[dict[str, str]] = []
    matched_1c = entry.get("matched_1c")
    resolved_count = 0

    for holder in holders:
        fio = holder["fio"]
        try:
            leader_key = resolve_user_ref(fio, exact_index, ambiguous, users)
        except (LookupError, ValueError) as exc:
            log(f"Ошибка для «{fio}»: {exc}")
            continue

        resolved_count += 1
        log(f"Загрузка задач для {fio} …")
        _entity, raw_rows = load_tasks(session, leader_key, as_of)
        rows = [normalize_row(row, users_by_key) for row in raw_rows]
        rows.sort(
            key=lambda row: (
                row.get("СрокИсполнения", ""),
                row.get("Протокол", ""),
                row.get("НомерПунктаПротокола", ""),
            ),
        )

        print_department_header(source, matched_1c, holder, leader_key)

        if args.csv_path:
            all_task_rows.extend(rows)
        elif args.table:
            print_table(rows)
        else:
            print_cards(rows)

    if args.csv_path:
        write_csv(args.csv_path, all_task_rows)
        log(f"CSV сохранён: {args.csv_path} ({len(all_task_rows)} задач)")

    if resolved_count == 0:
        log("Не удалось сопоставить ни одно ФИО с пользователем 1С.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except (LookupError, PermissionError, ValueError, RuntimeError) as exc:
        log(f"Ошибка: {exc}")
        sys.exit(1)
