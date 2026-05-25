"""
Поиск ссылки (Ref_Key) пользователя 1С по ФИО.

Справочник: Catalog_Пользователи
Сопоставление: поле Description (ФИО пользователя).

Использование:
  python lookup_user_ref.py "Абаза Елена Александровна"
  python lookup_user_ref.py "Иванов Иван Иванович" "Петров Петр Петрович"

На stdout выводится Ref_Key (GUID). Если пользователь не найден — код выхода 1.
"""

from __future__ import annotations

import functools
import os
import re
import sys
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"
DEFAULT_ODATA_TIMEOUT = int(os.getenv("ODATA_READ_TIMEOUT", "240"))

USER_ENTITY = "Catalog_Пользователи"
PERSON_ENTITY = "Catalog_ФизическиеЛица"


def normalize_name(value: str) -> str:
    value = (value or "").lower().replace("ё", "е")
    value = re.sub(r"[^0-9a-zа-я]+", " ", value)
    return " ".join(value.split())


def fetch_all(session: requests.Session, url: str, page: int = 500, timeout: int | None = None) -> list[dict]:
    read_timeout = DEFAULT_ODATA_TIMEOUT if timeout is None else int(timeout)
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}&$format=json"
        response = session.get(page_url, timeout=read_timeout)
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


def is_active_user(row: dict) -> bool:
    if row.get("DeletionMark"):
        return False
    description = (row.get("Description") or "").strip()
    return bool(description) and not description.startswith("<")


def load_users(session: requests.Session) -> list[dict]:
    url = (
        f"{BASE}/{quote(USER_ENTITY)}"
        f"?$select={quote('Ref_Key,Description,DeletionMark,Недействителен,ФизическоеЛицо_Key', safe=',_')}"
    )
    return [row for row in fetch_all(session, url) if is_active_user(row)]


def load_persons(session: requests.Session, person_keys: set[str]) -> dict[str, str]:
    if not person_keys:
        return {}
    keys = sorted(k for k in person_keys if k and k != EMPTY)
    persons: dict[str, str] = {}
    chunk_size = 20
    for idx in range(0, len(keys), chunk_size):
        chunk = keys[idx : idx + chunk_size]
        ref_filter = " or ".join(f"Ref_Key eq guid'{key}'" for key in chunk)
        url = (
            f"{BASE}/{quote(PERSON_ENTITY)}"
            f"?$filter={quote(ref_filter, safe='')}"
            f"&$select={quote('Ref_Key,Description', safe=',_')}"
        )
        for row in fetch_all(session, url, page=500):
            key = row.get("Ref_Key")
            if key:
                persons[key] = (row.get("Description") or "").strip()
    return persons


def build_fio_index(
    users: list[dict],
    persons: dict[str, str],
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """normalized FIO -> Ref_Key; also returns ambiguous matches."""
    exact: dict[str, str] = {}
    ambiguous: dict[str, list[str]] = {}

    for user in users:
        ref_key = user["Ref_Key"]
        names = [(user.get("Description") or "").strip()]
        person_key = user.get("ФизическоеЛицо_Key") or ""
        if person_key in persons and persons[person_key]:
            names.append(persons[person_key])

        seen_norm: set[str] = set()
        for name in names:
            norm = normalize_name(name)
            if not norm or norm in seen_norm:
                continue
            seen_norm.add(norm)
            if norm in exact and exact[norm] != ref_key:
                ambiguous.setdefault(norm, sorted({exact[norm], ref_key}))
            else:
                exact[norm] = ref_key

    return exact, ambiguous


def resolve_user_ref(
    fio: str,
    exact_index: dict[str, str],
    ambiguous: dict[str, list[str]],
    users: list[dict],
) -> str:
    norm = normalize_name(fio)
    if not norm:
        raise ValueError("ФИО не задано")

    if norm in ambiguous:
        refs = ", ".join(ambiguous[norm])
        raise LookupError(f"неоднозначное совпадение ФИО «{fio}»: {refs}")

    if norm in exact_index:
        return exact_index[norm]

    contains = [
        user for user in users
        if norm in normalize_name(user.get("Description") or "")
    ]
    if len(contains) == 1:
        return contains[0]["Ref_Key"]
    if len(contains) > 1:
        refs = ", ".join(sorted(u["Ref_Key"] for u in contains))
        raise LookupError(f"несколько частичных совпадений для «{fio}»: {refs}")

    raise LookupError(f"пользователь не найден: {fio}")


def main() -> None:
    fio_args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    if not fio_args:
        log("Использование: python lookup_user_ref.py \"Фамилия Имя Отчество\"")
        sys.exit(2)

    session = requests.Session()
    session.auth = AUTH

    log("Загрузка Catalog_Пользователи ...")
    users = load_users(session)
    person_keys = {
        user.get("ФизическоеЛицо_Key")
        for user in users
        if user.get("ФизическоеЛицо_Key") and user.get("ФизическоеЛицо_Key") != EMPTY
    }
    persons = load_persons(session, person_keys)
    exact_index, ambiguous = build_fio_index(users, persons)
    log(f"  Пользователей: {len(users)}")

    exit_code = 0
    for fio in fio_args:
        try:
            ref_key = resolve_user_ref(fio, exact_index, ambiguous, users)
            print(ref_key)
        except (LookupError, ValueError) as exc:
            log(f"Ошибка: {exc}")
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
