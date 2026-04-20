"""
calc_sudy_by_dept.py — таблица судов по подразделению за конкретный месяц.

Модуль больше не создаёт CSV. Он возвращает строки таблицы в памяти и может
использоваться из Django view.
"""
from __future__ import annotations

import functools
import json
import re
import sys
from datetime import date
from pathlib import Path

from .commercial_tiles import commercial_kpi_key, dept_guid_for_kpi_key
from .komdir_lawsuits import fetch_lawsuits_for_month

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


def _normalize_requested_department(department: str) -> tuple[str, str | None]:
    """
    Нормализует название подразделения пользователя в каноническое имя и GUID 1С.
    Для коммерческого директора GUID=None, что означает «все коммерческие отделы».
    """
    raw = str(department or "").strip()
    ck = commercial_kpi_key(raw)
    if isinstance(ck, str):
        return ck, dept_guid_for_kpi_key(ck)
    lowered = raw.lower()
    if "коммерческий" in lowered and "директор" in lowered:
        return "коммерческий директор", None
    return raw, None


def _dept_cache_token(canonical_dept: str, dept_guid: str | None) -> str:
    if dept_guid:
        return dept_guid
    normalized = re.sub(r"[^a-zA-Z0-9а-яА-Я_-]+", "_", canonical_dept.strip()).strip("_")
    return normalized or "all"


def _cache_path(year: int, month: int, canonical_dept: str, dept_guid: str | None) -> Path:
    token = _dept_cache_token(canonical_dept, dept_guid)
    return CACHE_DIR / f"lawsuits_by_dept_{token}_{year}_{month:02d}.json"


def _load_cache(year: int, month: int, canonical_dept: str, dept_guid: str | None) -> dict | None:
    path = _cache_path(year, month, canonical_dept, dept_guid)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None

    today = date.today()
    is_current_month = year == today.year and month == today.month
    if is_current_month and payload.get("cached_at") != today.isoformat():
        return None
    return payload


def _save_cache(
    year: int,
    month: int,
    canonical_dept: str,
    dept_guid: str | None,
    rows: list[dict],
) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "cached_at": date.today().isoformat(),
        "department": canonical_dept,
        "year": int(year),
        "month": int(month),
        "count": len(rows),
        "rows": rows,
    }
    try:
        with open(_cache_path(year, month, canonical_dept, dept_guid), "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
    except OSError:
        pass


def get_sudy_by_department(year: int, month: int, department: str) -> dict:
    """
    Возвращает таблицу судов за месяц по запрошенному подразделению.

    department:
    - дочерний отдел коммерческого блока → фильтрация по initiator_dept_key
    - коммерческий директор → все строки из коммерческого блока за месяц
    """
    canonical_dept, dept_guid = _normalize_requested_department(department)
    cached = _load_cache(year, month, canonical_dept, dept_guid)
    if cached is not None:
        return cached

    all_rows = fetch_lawsuits_for_month(year, month)
    if dept_guid:
        rows = [r for r in all_rows if (r.get("initiator_dept_key") or "") == dept_guid]
    else:
        rows = list(all_rows)
    rows.sort(key=lambda r: ((r.get("date") or ""), (r.get("number") or "")), reverse=True)
    result = {
        "department": canonical_dept,
        "year": int(year),
        "month": int(month),
        "count": len(rows),
        "rows": rows,
    }
    _save_cache(year, month, canonical_dept, dept_guid, rows)
    return result


def main() -> None:
    year = date.today().year
    month = date.today().month
    department = "коммерческий директор"
    data = get_sudy_by_department(year, month, department)
    print(f"Суды за {year}-{month:02d} · {data['department']}")
    print(f"Строк: {data['count']}")
    for row in data["rows"][:10]:
        print(
            f"{row.get('date', '')}  {row.get('number', ''):<12}  "
            f"{row.get('initiator_dept', '')}  {row.get('counterparty', '')}"
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
