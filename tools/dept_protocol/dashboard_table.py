"""
Просроченные задачи протоколов для дашборда (табличная часть API).

Обёртка над protocol_tasks_by_department: source из compact JSON → ФИО → Ref_Key → задачи 1С.
"""
from __future__ import annotations

import logging
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests

from . import _bootstrap  # noqa: F401 — sys.path для sibling-модулей

from lookup_source_department import (  # type: ignore[import-untyped]
    DEFAULT_DEPARTMENTS,
    load_departments,
    matched_paths,
    resolve_source,
)
from lookup_user_ref import (  # type: ignore[import-untyped]
    build_fio_index,
    load_persons,
    load_users,
    resolve_user_ref,
)
from protocol_tasks_by_department import find_holders_live  # type: ignore[import-untyped]
from protocol_tasks_by_leader import (  # type: ignore[import-untyped]
    load_tasks,
    normalize_row,
    normalized_task_in_scope,
    task_deadline_in_month,
)
from tools.dept_protocol.table_cache import (
    load_month_block,
    save_month_block,
)

logger = logging.getLogger(__name__)

TABLE_ID = "DEPT-T-PROTOCOL-OVERDUE"
PROTOCOL_START_YEAR = 2026
PROTOCOL_START_MONTH = 1

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

TABLE_COLUMNS = [
    "Протокол",
    "НомерПунктаПротокола",
    "Задача",
    "СрокИсполнения",
    "ДатаПостановкиЗадачи",
    "Ответственный",
    "Автор",
    "РуководительПротокола",
    "ТемаСовещания",
    "Выполнена",
    "Подтверждена",
    "Примечание",
]

EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


@dataclass
class ProtocolOneCContext:
    session: requests.Session
    hr_rows: list[dict]
    users: list[dict]
    exact_index: dict[str, str]
    ambiguous: dict[str, list[str]]
    users_by_key: dict[str, str]
    owns_session: bool = True

    def close(self) -> None:
        if self.owns_session:
            self.session.close()


def create_protocol_context(session: requests.Session | None = None) -> ProtocolOneCContext:
    from list_enterprise_positions import AUTH, build_report  # type: ignore[import-untyped]

    owns_session = session is None
    if session is None:
        session = requests.Session()
        session.auth = AUTH

    hr_rows = build_report(session)
    users = load_users(session)
    person_keys = {
        user.get("ФизическоеЛицо_Key")
        for user in users
        if user.get("ФизическоеЛицо_Key") and str(user.get("ФизическоеЛицо_Key")) != EMPTY_GUID
    }
    persons = load_persons(session, person_keys)
    exact_index, ambiguous = build_fio_index(users, persons)
    users_by_key = {
        str(user["Ref_Key"]): str(user.get("Description") or "").strip()
        for user in users
        if user.get("Ref_Key")
    }
    for norm, ref_key in exact_index.items():
        if ref_key not in users_by_key:
            users_by_key[ref_key] = norm

    return ProtocolOneCContext(
        session=session,
        hr_rows=hr_rows,
        users=users,
        exact_index=exact_index,
        ambiguous=ambiguous,
        users_by_key=users_by_key,
        owns_session=owns_session,
    )


def normalize_ref_period(
    year: int | None = None,
    month: int | None = None,
) -> tuple[int, int]:
    today = date.today()
    ref_y = int(year) if year is not None else today.year
    ref_m = int(month) if month is not None else today.month
    ref_m = max(1, min(12, ref_m))
    if (ref_y, ref_m) > (today.year, today.month):
        ref_y, ref_m = today.year, today.month
    return ref_y, ref_m


def month_pairs_from_start(ref_y: int, ref_m: int) -> list[tuple[int, int]]:
    """Месяцы от PROTOCOL_START (2026-01) до ref_y/ref_m включительно."""
    if (ref_y, ref_m) < (PROTOCOL_START_YEAR, PROTOCOL_START_MONTH):
        return []
    pairs: list[tuple[int, int]] = []
    y, m = PROTOCOL_START_YEAR, PROTOCOL_START_MONTH
    while (y, m) <= (ref_y, ref_m):
        pairs.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return pairs


def month_as_of(year: int, month: int, *, today: date | None = None) -> date | None:
    """Дата «сегодня» для расчёта просрочки внутри месяца."""
    today = today or date.today()
    if (year, month) > (today.year, today.month):
        return None
    if (year, month) == (today.year, today.month):
        return today
    return date(year, month, monthrange(year, month)[1])


def sanitize_month_block(block: dict[str, Any]) -> dict[str, Any]:
    """Отсечь чужие месяцы, протоколы до 2026 и задачи без срока."""
    year = block.get("year")
    month = block.get("month")
    if not isinstance(year, int) or not isinstance(month, int):
        return block
    as_of_date = month_as_of(year, month)
    if as_of_date is None:
        return block
    rows = block.get("rows") or []
    filtered = [
        row for row in rows
        if isinstance(row, dict)
        and normalized_task_in_scope(row, as_of_date)
        and task_deadline_in_month(row, year, month)
    ]
    if filtered == rows:
        return block
    out = dict(block)
    out["rows"] = filtered
    out["row_count"] = len(filtered)
    return out


def _resolve_department_entry(department: str) -> dict | None:
    if not (department or "").strip():
        return None
    if not DEFAULT_DEPARTMENTS.is_file():
        logger.warning("dept_protocol: файл сопоставлений не найден: %s", DEFAULT_DEPARTMENTS)
        return None
    departments = load_departments(DEFAULT_DEPARTMENTS)
    exact, partial = resolve_source(department.strip(), departments)
    if exact:
        return exact[0]
    if len(partial) == 1:
        return partial[0]
    return None


def fetch_overdue_protocol_task_rows(
    department: str,
    *,
    as_of: date | None = None,
    ctx: ProtocolOneCContext | None = None,
) -> list[dict[str, str]] | None:
    """
    Просроченные задачи протоколов для department (source из compact JSON).

    None — департамент не в справочнике или не удалось определить руководителя.
    [] — руководитель найден, просроченных задач нет.
    """
    entry = _resolve_department_entry(department)
    if entry is None:
        return None

    source = str(entry.get("source") or department).strip()
    onec_paths = matched_paths(entry)
    if not onec_paths:
        return None

    as_of_date = as_of or date.today()
    own_ctx = ctx is None
    if ctx is None:
        ctx = create_protocol_context()

    try:
        holders = find_holders_live(source, onec_paths, ctx.hr_rows)
        if not holders:
            return None

        all_rows_out: list[dict[str, str]] = []
        seen_task_keys: set[tuple[str, ...]] = set()

        for holder in holders:
            fio = holder["fio"]
            try:
                leader_key = resolve_user_ref(
                    fio,
                    ctx.exact_index,
                    ctx.ambiguous,
                    ctx.users,
                )
            except (LookupError, ValueError) as exc:
                logger.warning(
                    "dept_protocol: не найден пользователь 1С для «%s»: %s",
                    fio,
                    exc,
                )
                continue

            _entity, raw_rows = load_tasks(ctx.session, leader_key, as_of_date)
            for raw in raw_rows:
                row = normalize_row(raw, ctx.users_by_key)
                if not normalized_task_in_scope(row, as_of_date):
                    continue
                dedupe_key = (
                    row.get("Протокол", ""),
                    row.get("НомерПунктаПротокола", ""),
                    row.get("Задача", ""),
                    row.get("СрокИсполнения", ""),
                    row.get("Ответственный", ""),
                )
                if dedupe_key in seen_task_keys:
                    continue
                seen_task_keys.add(dedupe_key)
                all_rows_out.append(row)

        all_rows_out.sort(
            key=lambda row: (
                row.get("СрокИсполнения", ""),
                row.get("Протокол", ""),
                row.get("НомерПунктаПротокола", ""),
            ),
        )
        return all_rows_out
    finally:
        if own_ctx:
            ctx.close()


def build_month_block(
    department: str,
    year: int,
    month: int,
    *,
    ctx: ProtocolOneCContext | None = None,
    force: bool = False,
) -> dict[str, Any] | None:
    if not force:
        cached = load_month_block(department, year, month)
        if cached is not None:
            return cached

    as_of_date = month_as_of(year, month)
    if as_of_date is None:
        return None

    rows = fetch_overdue_protocol_task_rows(department, as_of=as_of_date, ctx=ctx)
    if rows is None:
        return None

    block = sanitize_month_block({
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "as_of": as_of_date.isoformat(),
        "rows": rows,
        "row_count": len(rows),
        "has_data": True,
        "columns": TABLE_COLUMNS,
    })
    save_month_block(department, year, month, block)
    return block


def assemble_ytd_table(
    entry: dict,
    department: str,
    monthly_data: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> dict[str, Any] | None:
    if not monthly_data:
        return None

    ref_block = next(
        (block for block in monthly_data if block.get("year") == ref_y and block.get("month") == ref_m),
        monthly_data[-1],
    )
    source = str(entry.get("source") or department).strip()
    matched = entry.get("matched_1c")
    ref_rows = ref_block.get("rows") or []

    return {
        "kpi_id": TABLE_ID,
        "name": f"Просроченные задачи протоколов ({source})",
        "periodicity": "ежемесячно",
        "data_granularity": "monthly",
        "description": (
            "Незавершённые просроченные задачи протоколов 1С: протокол не ранее "
            f"{PROTOCOL_START_YEAR}-{PROTOCOL_START_MONTH:02d}, срок исполнения "
            "внутри отчётного месяца; снимок на конец месяца (или на сегодня "
            "для текущего месяца)."
        ),
        "monthly_data": monthly_data,
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
            "as_of": ref_block.get("as_of"),
            "source": source,
            "matched_1c": matched,
        },
        "columns": TABLE_COLUMNS,
        "rows": ref_rows,
        "row_count": ref_block.get("row_count", len(ref_rows)),
        "months_with_data": sum(1 for block in monthly_data if block.get("has_data")),
        "months_total": len(monthly_data),
    }


def build_from_cached_months(
    department: str,
    ref_y: int,
    ref_m: int,
    *,
    allow_stale: bool = False,
) -> dict[str, Any] | None:
    entry = _resolve_department_entry(department)
    if entry is None:
        return None
    monthly_data: list[dict[str, Any]] = []
    for y, m in month_pairs_from_start(ref_y, ref_m):
        block = load_month_block(department, y, m, allow_stale=allow_stale)
        if block is not None:
            monthly_data.append(block)
    if not monthly_data:
        return None
    if not any(
        block.get("year") == ref_y and block.get("month") == ref_m
        for block in monthly_data
    ):
        return None
    return assemble_ytd_table(entry, department, monthly_data, ref_y, ref_m)


def build_protocol_overdue_table(
    department: str,
    *,
    year: int | None = None,
    month: int | None = None,
    force: bool = False,
    ctx: ProtocolOneCContext | None = None,
) -> dict[str, Any] | None:
    """Payload таблицы с monthly_data с января 2026 до ref_y/ref_m."""
    ref_y, ref_m = normalize_ref_period(year, month)
    entry = _resolve_department_entry(department)
    if entry is None:
        return None

    pairs = month_pairs_from_start(ref_y, ref_m)
    if not pairs:
        return None

    if not force:
        cached_table = build_from_cached_months(
            department, ref_y, ref_m, allow_stale=True,
        )
        if cached_table is not None and cached_table.get("months_total") == len(pairs):
            return cached_table

    own_ctx = ctx is None
    if ctx is None:
        ctx = create_protocol_context()

    try:
        monthly_data: list[dict[str, Any]] = []
        for y, m in pairs:
            try:
                block = build_month_block(department, y, m, ctx=ctx, force=force)
            except requests.RequestException as exc:
                logger.warning(
                    "DEPT-T-PROTOCOL-OVERDUE: OData error «%s» %04d-%02d: %s",
                    department,
                    y,
                    m,
                    exc,
                )
                block = load_month_block(department, y, m, allow_stale=True)
            if block is not None:
                monthly_data.append(block)

        if not monthly_data:
            return build_from_cached_months(department, ref_y, ref_m, allow_stale=True)

        return assemble_ytd_table(entry, department, monthly_data, ref_y, ref_m)
    finally:
        if own_ctx:
            ctx.close()


def warm_all_department_tables(*, force: bool = False) -> None:
    """Последовательный прогрев кэша всех подразделений × месяцев с 2026-01."""
    from tools.dept_protocol.table_cache import (
        all_sources_cached_for_months,
        is_warm_complete_today,
        iter_configured_sources,
        load_month_block,
        mark_warm_complete,
    )

    today = date.today()
    pairs = month_pairs_from_start(today.year, today.month)
    if not pairs:
        return

    if not force and is_warm_complete_today() and all_sources_cached_for_months(pairs):
        return

    ctx = create_protocol_context()
    try:
        for source in iter_configured_sources():
            for y, m in pairs:
                if not force and load_month_block(source, y, m) is not None:
                    continue
                try:
                    build_month_block(source, y, m, ctx=ctx, force=force)
                except Exception:
                    logger.exception(
                        "DEPT-T-PROTOCOL-OVERDUE: прогрев не удался для «%s» %04d-%02d",
                        source,
                        y,
                        m,
                    )
    finally:
        ctx.close()

    mark_warm_complete()
