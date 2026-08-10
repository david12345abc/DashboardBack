"""HRD-M5 — план/факт выполнения задач сотрудниками СУП.

Источник: 1С OData ``Task_ЗадачаИсполнителя``.

План месяца — число задач исполнителей из «Службы управления персоналом»,
у которых ``СрокИсполнения`` попадает в календарный месяц
(``DeletionMark = false``, срок не пустой).

Факт — из плана: задача выполнена вовремя
(``Executed = true`` и ``date(ДатаИсполнения) <= date(СрокИсполнения)``).

KPI % = факт / план × 100.
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from getkpi.list_enterprise_positions import (
    AUTH,
    BASE,
    EMPTY,
    employees_by_department,
    fetch_all,
    normalize_text,
)
from getkpi.odata_http import request_with_retry
from qualdir.sql_tile_cache import get_ytd_via_cache

logger = logging.getLogger(__name__)

KPI_ID = "HRD-M5"
CACHE_PREFIX = "sup_hrd_m5_tasks"
CACHE_SOURCE_TAG = "sup_hrd_m5_tasks_payload_v1"
CACHE_VERSION = 1

TASK_ENTITY = "Task_ЗадачаИсполнителя"
USER_ENTITY = "Catalog_Пользователи"
SUP_DEPARTMENT_PATH = "Служба управления персоналом"
EMPTY_DATE = "0001-01-01T00:00:00"

TASK_SELECT = ",".join(
    (
        "Ref_Key",
        "Number",
        "Date",
        "Description",
        "Executed",
        "СрокИсполнения",
        "ДатаИсполнения",
        "ПредметСтрокой",
        "Предмет_Type",
        "DeletionMark",
    )
)


def _parse_date(value: Any) -> date | None:
    if value in (None, "", EMPTY_DATE):
        return None
    text = str(value)
    if text.startswith("0001-01-01"):
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _kpi_pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(100.0 * fact / plan, 1)


def _odata_session() -> requests.Session:
    session = requests.Session()
    session.auth = AUTH
    session.headers.update({"Accept": "application/json"})
    return session


def _resolve_user_refs(session: requests.Session, fios: list[str]) -> dict[str, str]:
    url = (
        f"{BASE}/{quote(USER_ENTITY)}"
        f"?$select={quote('Ref_Key,Description,DeletionMark', safe=',_')}"
        f"&$format=json"
    )
    users = fetch_all(session, url)
    by_norm: dict[str, str] = {}
    for user in users:
        if user.get("DeletionMark"):
            continue
        name = (user.get("Description") or "").strip()
        ref = user.get("Ref_Key")
        if not name or not ref or ref == EMPTY:
            continue
        by_norm[normalize_text(name)] = ref

    resolved: dict[str, str] = {}
    for fio in fios:
        ref = by_norm.get(normalize_text(fio))
        if ref:
            resolved[fio] = ref
        else:
            logger.warning("%s: пользователь 1С не найден для «%s»", KPI_ID, fio)
    return resolved


def _fetch_tasks_for_user(
    session: requests.Session,
    user_ref: str,
    year: int,
) -> list[dict[str, Any]]:
    filt = (
        f"Исполнитель eq cast(guid'{user_ref}','Catalog_Пользователи') "
        f"and DeletionMark eq false "
        f"and СрокИсполнения ge datetime'{year:04d}-01-01T00:00:00' "
        f"and СрокИсполнения lt datetime'{year + 1:04d}-01-01T00:00:00'"
    )
    url = (
        f"{BASE}/{quote(TASK_ENTITY)}"
        f"?$format=json"
        f"&$filter={quote(filt, safe='')}"
        f"&$select={quote(TASK_SELECT, safe=',_')}"
        f"&$orderby={quote('СрокИсполнения asc')}"
    )
    rows: list[dict[str, Any]] = []
    skip = 0
    page = 500
    read_timeout = int(os.getenv("ODATA_READ_TIMEOUT", "240"))
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        response = request_with_retry(
            session,
            page_url,
            timeout=read_timeout,
            retries=4,
            label=f"{KPI_ID}_tasks",
        )
        if response is None:
            raise RuntimeError(f"{KPI_ID}: OData недоступен для {TASK_ENTITY}")
        if not response.ok:
            raise RuntimeError(
                f"{KPI_ID}: HTTP {response.status_code}: {response.text[:400]}"
            )
        batch = response.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def _classify_task(row: dict[str, Any]) -> tuple[date | None, str]:
    """Вернуть (срок, статус): ontime | late | open | skip."""
    deadline = _parse_date(row.get("СрокИсполнения"))
    if deadline is None:
        return None, "skip"
    done = _parse_date(row.get("ДатаИсполнения"))
    executed = bool(row.get("Executed"))
    if executed and done is not None:
        if done <= deadline:
            return deadline, "ontime"
        return deadline, "late"
    return deadline, "open"


def _load_year_tasks(
    session: requests.Session,
    year: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fios = employees_by_department(SUP_DEPARTMENT_PATH, session=session)
    user_refs = _resolve_user_refs(session, fios)
    details: list[dict[str, Any]] = []
    missing = [fio for fio in fios if fio not in user_refs]

    for fio, ref in user_refs.items():
        for row in _fetch_tasks_for_user(session, ref, year):
            deadline, status = _classify_task(row)
            if deadline is None or status == "skip":
                continue
            done = _parse_date(row.get("ДатаИсполнения"))
            details.append(
                {
                    "executor": fio,
                    "executor_key": ref,
                    "number": str(row.get("Number") or "").strip(),
                    "description": str(row.get("Description") or "").strip(),
                    "deadline": deadline.isoformat(),
                    "done": done.isoformat() if done else "",
                    "executed": bool(row.get("Executed")),
                    "status": status,
                    "subject": str(row.get("ПредметСтрокой") or "").strip(),
                    "subject_type": str(row.get("Предмет_Type") or "")
                    .replace("StandardODATA.", "")
                    .strip(),
                    "month": deadline.month,
                    "year": deadline.year,
                }
            )

    debug = {
        "department": SUP_DEPARTMENT_PATH,
        "staff_count": len(fios),
        "staff": fios,
        "resolved_users": len(user_refs),
        "missing_users": missing,
        "tasks_total": len(details),
    }
    return details, debug


def build_hrd_m5_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    session = _odata_session()
    try:
        details, staff_debug = _load_year_tasks(session, ref_y)
    finally:
        session.close()

    by_month: dict[int, dict[str, int]] = defaultdict(
        lambda: {"plan": 0, "fact": 0, "late": 0, "open": 0}
    )
    for item in details:
        if item["year"] != ref_y or item["month"] > ref_m:
            continue
        bucket = by_month[int(item["month"])]
        bucket["plan"] += 1
        if item["status"] == "ontime":
            bucket["fact"] += 1
        elif item["status"] == "late":
            bucket["late"] += 1
        else:
            bucket["open"] += 1

    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        stats = by_month[m]
        plan = int(stats["plan"])
        fact = int(stats["fact"])
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES[m],
                "plan": plan,
                "fact": fact,
                "late": int(stats["late"]),
                "open": int(stats["open"]),
                "kpi_pct": _kpi_pct(fact, plan),
                "has_data": plan > 0,
                "values_unit": "шт.",
            }
        )

    ref_row = next((row for row in monthly_rows if row["month"] == ref_m), None)
    if ref_row is None:
        ref_row = {
            "month": ref_m,
            "year": ref_y,
            "month_name": MONTH_NAMES[ref_m],
            "plan": 0,
            "fact": 0,
            "late": 0,
            "open": 0,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": "шт.",
        }

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "total_late": ref_row.get("late"),
            "total_open": ref_row.get("open"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": KPI_ID,
            "status": "ok",
            "source": "Task_ЗадачаИсполнителя / OData",
            "rule": (
                "plan = tasks of SUP assignees with deadline in month; "
                "fact = Executed and date(ДатаИсполнения) <= date(СрокИсполнения); "
                "kpi = fact / plan × 100"
            ),
            "rows_by_month": [
                {
                    "month": row["month"],
                    "plan": row["plan"],
                    "fact": row["fact"],
                    "late": row["late"],
                    "open": row["open"],
                    "kpi_pct": row["kpi_pct"],
                }
                for row in monthly_rows
            ],
            **staff_debug,
        },
    }


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    from devdir import ytd_json_cache

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, ref_y, ref_m)


def get_hrd_m5_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=CACHE_PREFIX,
        source_tag=CACHE_SOURCE_TAG,
        version=CACHE_VERSION,
        lock_key_prefix="sup_hrd_m5_tasks",
        compute_fn=lambda y, m: build_hrd_m5_payload(y, m),
        kpi_id=KPI_ID,
    )


def main() -> int:
    import json
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    args = [a for a in sys.argv[1:] if a.strip()]
    year = int(args[0]) if args else None
    month = int(args[1]) if len(args) > 1 else None
    payload = build_hrd_m5_payload(year=year, month=month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
