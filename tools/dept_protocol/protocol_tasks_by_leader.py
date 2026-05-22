"""
Просроченные незавершённые задачи протоколов по руководителю.

Эквивалент запроса 1С к регистру сведений ТД_ЗадачиПротоколов:
  - Протокол.Руководитель = пользователь из Catalog_Пользователи
  - ДатаИсполнения = пустая дата (задача не выполнена)
  - СрокИсполнения < текущая дата

Использование:
  python protocol_tasks_by_leader.py "Абаза Елена Александровна"
  python protocol_tasks_by_leader.py --date 2026-05-19 "Фамилия Имя Отчество"
  python protocol_tasks_by_leader.py --csv tasks.csv "Фамилия Имя Отчество"
  python protocol_tasks_by_leader.py --table "Фамилия Имя Отчество"

На stdout — карточки задач (по умолчанию), таблица (--table) или CSV. Прогресс — в stderr.
"""

from __future__ import annotations

import argparse
import csv
import functools
import os
import re
import shutil
import sys
import textwrap
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import requests

from lookup_user_ref import (
    AUTH,
    BASE,
    EMPTY,
    build_fio_index,
    fetch_all,
    load_persons,
    load_users,
    resolve_user_ref,
)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)

REGISTER_ENTITY = "InformationRegister_ТД_ЗадачиПротоколов"
EMPTY_DATE = "0001-01-01T00:00:00"

COLUMNS = (
    ("Протокол", "Протокол"),
    ("ТемаСовещания", "ТемаСовещания"),
    ("Выполнена", "Выполнена"),
    ("Подтверждена", "Подтверждена"),
    ("ДатаПостановкиЗадачи", "ДатаПостановкиЗадачи"),
    ("СрокИсполнения", "СрокИсполнения"),
    ("ДатаИсполнения", "ДатаИсполнения"),
    ("Переносы", "Примечание"),
    ("НомерПунктаПротокола", "НомерПунктаПротокола"),
    ("Задача", "Задача"),
    ("Исполнитель", "Ответственный"),
    ("АвторЗадачи", "Автор"),
    ("РуководительПротокола", "РуководительПротокола"),
)

SELECT_FIELDS = (
    "Протокол",
    "ТемаСовещания",
    "Выполнена",
    "Подтверждена",
    "ДатаПостановкиЗадачи",
    "СрокИсполнения",
    "ДатаИсполнения",
    "Примечание",
    "НомерПунктаПротокола",
    "Задача",
    "Ответственный",
    "Автор",
    "Протокол_Key",
    "Ответственный_Key",
    "Автор_Key",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Просроченные задачи протоколов по руководителю (регистр ТД_ЗадачиПротоколов).",
    )
    parser.add_argument("fio", help="ФИО руководителя протокола (Catalog_Пользователи)")
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
    return parser.parse_args()


def parse_as_of(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def odata_datetime_start(day: date) -> str:
    return f"{day.isoformat()}T00:00:00"


def is_empty_date(value: Any) -> bool:
    if value in (None, "", EMPTY_DATE):
        return True
    text = str(value)
    return text.startswith("0001-01-01")


def fmt_date(value: Any) -> str:
    if is_empty_date(value):
        return ""
    text = str(value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return text[:10]


def fmt_bool(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    text = str(value).strip().lower()
    if text in {"true", "истина", "да", "1"}:
        return "Да"
    if text in {"false", "ложь", "нет", "0"}:
        return "Нет"
    return str(value)


def fmt_protocol(row: dict[str, Any]) -> str:
    protocol = row.get("Протокол")
    if isinstance(protocol, dict):
        number = str(protocol.get("Number") or "").strip()
        doc_date = fmt_date(protocol.get("Date"))
        if number and doc_date:
            prefix = "" if number.startswith("№") else "№"
            return f"{prefix}{number} от {doc_date}"
        if number:
            return number if number.startswith("№") else f"№{number}"
        if doc_date:
            return doc_date
    if isinstance(protocol, str) and protocol.strip():
        return protocol.strip()
    key = row.get("Протокол_Key") or ""
    return key[:8] + "..." if key and key != EMPTY else ""


def resolve_ref_name(value: Any, key: Any, users_by_key: dict[str, str]) -> str:
    if isinstance(value, dict):
        name = (value.get("Description") or "").strip()
        if name:
            return name
        key = value.get("Ref_Key") or key
    if isinstance(value, str) and value.strip() and not value.endswith("_Key"):
        return value.strip()
    if key and key != EMPTY and key in users_by_key:
        return users_by_key[key]
    return ""


def normalize_row(row: dict[str, Any], users_by_key: dict[str, str]) -> dict[str, str]:
    leader = ""
    protocol = row.get("Протокол")
    if isinstance(protocol, dict):
        leader_obj = protocol.get("Руководитель")
        leader = resolve_ref_name(
            leader_obj,
            protocol.get("Руководитель_Key"),
            users_by_key,
        )

    theme = row.get("ТемаСовещания")
    if isinstance(theme, dict):
        theme = (theme.get("Description") or "").strip()

    return {
        "Протокол": fmt_protocol(row),
        "ТемаСовещания": str(theme or "").strip(),
        "Выполнена": fmt_bool(row.get("Выполнена")),
        "Подтверждена": fmt_bool(row.get("Подтверждена")),
        "ДатаПостановкиЗадачи": fmt_date(row.get("ДатаПостановкиЗадачи")),
        "СрокИсполнения": fmt_date(row.get("СрокИсполнения")),
        "ДатаИсполнения": fmt_date(row.get("ДатаИсполнения")),
        "Примечание": str(row.get("Примечание") or "").strip(),
        "НомерПунктаПротокола": str(row.get("НомерПунктаПротокола") or "").strip(),
        "Задача": str(row.get("Задача") or "").strip(),
        "Ответственный": resolve_ref_name(
            row.get("Ответственный"),
            row.get("Ответственный_Key"),
            users_by_key,
        ),
        "Автор": resolve_ref_name(
            row.get("Автор"),
            row.get("Автор_Key"),
            users_by_key,
        ),
        "РуководительПротокола": leader,
    }


def build_users_by_key(session: requests.Session) -> dict[str, str]:
    users = load_users(session)
    person_keys = {
        user.get("ФизическоеЛицо_Key")
        for user in users
        if user.get("ФизическоеЛицо_Key") and user.get("ФизическоеЛицо_Key") != EMPTY
    }
    persons = load_persons(session, person_keys)
    index, _ambiguous = build_fio_index(users, persons)
    by_key = {user["Ref_Key"]: (user.get("Description") or "").strip() for user in users}
    for norm, ref_key in index.items():
        if ref_key not in by_key:
            by_key[ref_key] = norm
    return by_key


def build_filter(leader_key: str, as_of: date) -> str:
    parts = [
        f"Протокол/Руководитель_Key eq guid'{leader_key}'",
        f"ДатаИсполнения eq datetime'{EMPTY_DATE}'",
        f"СрокИсполнения lt datetime'{odata_datetime_start(as_of)}'",
    ]
    return " and ".join(parts)


def _fetch_register_all(
    session: requests.Session,
    url: str,
    *,
    page: int = 500,
    timeout: int,
) -> list[dict[str, Any]]:
    try:
        from getkpi.odata_http import request_with_retry
    except ImportError:
        return fetch_all(session, url, page=page, timeout=timeout)

    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}&$format=json"
        response = request_with_retry(
            session,
            page_url,
            timeout=timeout,
            retries=4,
            label="protocol_tasks",
        )
        if response is None:
            raise RuntimeError(f"OData request failed: {page_url[:160]}")
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


def load_tasks(
    session: requests.Session,
    leader_key: str,
    as_of: date,
) -> tuple[str, list[dict[str, Any]]]:
    filter_expr = quote(build_filter(leader_key, as_of), safe="")
    # «/» в $expand обязательно кодировать — иначе 1С воспринимает «Протокол/Руководитель» как сегмент URL.
    expand = quote("Протокол,Ответственный,Автор,Протокол/Руководитель", safe=",")
    select = quote(",".join(SELECT_FIELDS), safe=",_")

    entity = REGISTER_ENTITY
    url = (
        f"{BASE}/{quote(entity)}"
        f"?$format=json"
        f"&$filter={filter_expr}"
        f"&$select={select}"
        f"&$expand={expand}"
    )
    read_timeout = int(os.getenv("ODATA_READ_TIMEOUT", "240"))
    probe = session.get(f"{url}&$top=1", timeout=read_timeout)
    if probe.status_code == 404:
        raise LookupError(
            f"{entity}: не опубликован в OData (HTTP 404). "
            "Попросите администратора 1С добавить регистр "
            "InformationRegister_ТД_ЗадачиПротоколов в стандартный интерфейс OData."
        )
    if probe.status_code == 401:
        raise PermissionError(
            f"{entity}: доступ запрещён для текущего пользователя OData (HTTP 401)."
        )
    if not probe.ok:
        raise RuntimeError(f"{entity}: HTTP {probe.status_code}: {probe.text[:500]}")

    rows = _fetch_register_all(session, url, page=500, timeout=read_timeout)
    return entity, rows


def terminal_width(default: int = 100, maximum: int = 100) -> int:
    try:
        return min(maximum, max(72, shutil.get_terminal_size(fallback=(default, 24)).columns))
    except OSError:
        return default


def wrap_block(text: str, width: int, indent: str = "    ") -> list[str]:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return ["—"]
    usable = max(20, width - len(indent))
    return textwrap.wrap(text, width=usable) or ["—"]


def format_postponements(text: str) -> list[str]:
    """Разбить историю переносов на отдельные строки."""
    text = (text or "").strip()
    if not text:
        return []
    parts = re.split(r";(?=\s*Перенос\s)", text, flags=re.IGNORECASE)
    lines: list[str] = []
    for part in parts:
        part = part.strip(" ;")
        if part:
            lines.append(part)
    return lines


def short_fio(value: str) -> str:
    """Фамилия И.О. для компактного вывода."""
    parts = (value or "").split()
    if len(parts) >= 3:
        return f"{parts[0]} {parts[1][0]}.{parts[2][0]}."
    if len(parts) == 2:
        return f"{parts[0]} {parts[1][0]}."
    return value


def print_cards(rows: list[dict[str, str]]) -> None:
    width = terminal_width()
    bar_len = width - 2
    rule = "═" * bar_len
    thin = "─" * bar_len

    print()
    print(rule)
    print(f"  Просроченные задачи протоколов · всего: {len(rows)}")
    print(rule)

    if not rows:
        print("  Задач не найдено.")
        return

    label_width = 14
    value_start = 2 + label_width + 2

    for index, row in enumerate(rows, start=1):
        protocol = row.get("Протокол") or "—"
        point = row.get("НомерПунктаПротокола") or "—"
        deadline = row.get("СрокИсполнения") or "не указан"
        posted = row.get("ДатаПостановкиЗадачи") or "—"
        task = row.get("Задача") or "—"
        assignee = row.get("Ответственный") or "—"
        author = row.get("Автор") or "—"
        theme = row.get("ТемаСовещания") or ""
        postponements = format_postponements(row.get("Примечание") or "")

        print()
        print(f"  ┌─ {index}/{len(rows)} " + thin[6:])

        def emit(label: str, lines: list[str]) -> None:
            pad = " " * (label_width - len(label))
            first_prefix = f"  │ {label}{pad}  "
            next_prefix = " " * len(first_prefix)
            for line_no, line in enumerate(lines):
                prefix = first_prefix if line_no == 0 else next_prefix
                print(f"{prefix}{line}")

        header = f"{protocol} · пункт {point}"
        emit("Протокол", wrap_block(header, width, indent=" " * value_start))
        emit("Срок", [deadline])
        emit("Постановка", [posted])
        if theme:
            emit("Тема", wrap_block(theme, width, indent=" " * value_start))
        emit("Задача", wrap_block(task, width, indent=" " * value_start))
        emit("Исполнитель", [short_fio(assignee)])
        emit("Автор", [short_fio(author)])

        if postponements:
            post_lines: list[str] = []
            for item in postponements:
                post_lines.extend(wrap_block(item, width, indent=" " * value_start))
            emit("Переносы", post_lines)

        flags = []
        if row.get("Выполнена") == "Да":
            flags.append("выполнена")
        if row.get("Подтверждена") == "Да":
            flags.append("подтверждена")
        if flags:
            emit("Статус", [", ".join(flags)])

        print(f"  └{thin[2:]}")

    print()
    print(f"  Всего задач: {len(rows)}")
    print()


def print_table(rows: list[dict[str, str]]) -> None:
    headers = [title for title, _field in COLUMNS]
    widths = {title: len(title) for title in headers}
    for row in rows:
        for title, field in COLUMNS:
            widths[title] = max(widths[title], len(row.get(field, "")))

    def line(char: str = "─") -> str:
        parts = [char * (widths[title] + 2) for title in headers]
        return "  " + "  ".join(parts)

    print()
    print("  " + "  ".join(title.ljust(widths[title]) for title in headers))
    print(line())
    for row in rows:
        print(
            "  "
            + "  ".join(
                row.get(field, "").ljust(widths[title])[: widths[title]]
                for title, field in COLUMNS
            )
        )
    print(line())
    print(f"  Всего задач: {len(rows)}")


def write_csv(path: str, rows: list[dict[str, str]]) -> None:
    headers = [title for title, _field in COLUMNS]
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({title: row.get(field, "") for title, field in COLUMNS})


def main() -> None:
    args = parse_args()
    as_of = parse_as_of(args.as_of)

    session = requests.Session()
    session.auth = AUTH

    log("Поиск пользователя ...")
    users = load_users(session)
    person_keys = {
        user.get("ФизическоеЛицо_Key")
        for user in users
        if user.get("ФизическоеЛицо_Key") and user.get("ФизическоеЛицо_Key") != EMPTY
    }
    persons = load_persons(session, person_keys)
    exact_index, ambiguous = build_fio_index(users, persons)
    leader_key = resolve_user_ref(args.fio, exact_index, ambiguous, users)
    log(f"  Руководитель: {args.fio}")
    log(f"  Ref_Key: {leader_key}")
    log(f"  Дата отсечения: {as_of.isoformat()}")

    log("Загрузка задач протоколов ...")
    entity, raw_rows = load_tasks(session, leader_key, as_of)
    log(f"  Источник: {entity}")
    log(f"  Записей из OData: {len(raw_rows)}")

    users_by_key = build_users_by_key(session)
    rows = [normalize_row(row, users_by_key) for row in raw_rows]

    rows.sort(
        key=lambda row: (
            row.get("СрокИсполнения", ""),
            row.get("Протокол", ""),
            row.get("НомерПунктаПротокола", ""),
        ),
    )

    if args.csv_path:
        write_csv(args.csv_path, rows)
        log(f"CSV сохранён: {args.csv_path}")
    elif args.table:
        print_table(rows)
    else:
        print_cards(rows)


if __name__ == "__main__":
    try:
        main()
    except (LookupError, PermissionError, ValueError, RuntimeError) as exc:
        log(f"Ошибка: {exc}")
        sys.exit(1)
