"""Среднесписочная численность по организациям (календарные дни месяца).

ССЧ_месяц = Σ N_d / D, где N_d — число сотрудников организации,
у которых на день d последнее кадровое событие ≠ увольнение.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Any

from hr_turnover_sql import (
    ORG_TABLE,
    HR_HIST,
    COL_HR_EMP,
    COL_HR_DEPT,
    COL_HR_EVENT,
    DISMISSAL_EVENT_BIN,
    EMPTY_SQL_YEAR,
    YEAR_OFFSET,
    guid_to_1c,
    is_true_bin,
)
from sql_connection import SqlConnection

NPO_ORG_GUID = "fbca2148-6cfd-11e7-812d-001e67112509"
ALMAZ_ORG_GUID = "fbca2146-6cfd-11e7-812d-001e67112509"

ORG_LABELS: dict[str, str] = {
    NPO_ORG_GUID: "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
    ALMAZ_ORG_GUID: "АЛМАЗ ООО",
}

# Периметр производительности: выручка FND-T1 считается по НПО+Алмаз суммарно.
PRODUCTIVITY_ORG_GUIDS: tuple[str, ...] = (NPO_ORG_GUID, ALMAZ_ORG_GUID)


def _load_org_hr_events(
    sql: SqlConnection,
    org_guids: list[str] | tuple[str, ...],
    *,
    until: date,
) -> tuple[list[bytes], dict[bytes, list[tuple[date, bool]]]]:
    """Подразделения указанных организаций + хронология (дата, employed)."""
    owner_bins = [guid_to_1c(g) for g in org_guids]
    until_sql = datetime(until.year + YEAR_OFFSET, until.month, until.day, 23, 59, 59)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        ph_owners = ",".join("?" * len(owner_bins))
        cur.execute(
            f"""
            SELECT _IDRRef FROM dbo.[{ORG_TABLE}] WITH (NOLOCK)
            WHERE [_OwnerIDRRef] IN ({ph_owners})
            """,
            owner_bins,
        )
        org_bins = [bytes(r[0]) for r in cur.fetchall()]
        if not org_bins:
            return [], {}

        ph = ",".join("?" * len(org_bins))
        cur.execute(
            f"""
            SELECT _Period, [{COL_HR_EMP}], [{COL_HR_EVENT}], _Active
            FROM dbo.[{HR_HIST}] WITH (NOLOCK)
            WHERE [{COL_HR_DEPT}] IN ({ph})
              AND _Period <= ?
            ORDER BY _Period
            """,
            [*org_bins, until_sql],
        )
        by_emp: dict[bytes, list[tuple[date, bool]]] = {}
        for period, emp, event, active in cur.fetchall():
            if period is None or period.year <= EMPTY_SQL_YEAR:
                continue
            cal = date(period.year - YEAR_OFFSET, period.month, period.day)
            employed = is_true_bin(active) and (
                bytes(event) if event else b""
            ) != DISMISSAL_EVENT_BIN
            by_emp.setdefault(bytes(emp), []).append((cal, employed))

    return org_bins, by_emp


def _headcount_on_date(
    by_emp: dict[bytes, list[tuple[date, bool]]],
    as_of: date,
) -> int:
    count = 0
    for timeline in by_emp.values():
        status = None
        for cal, employed in timeline:
            if cal <= as_of:
                status = employed
            else:
                break
        if status:
            count += 1
    return count


def _ssc_for_month(
    by_emp: dict[bytes, list[tuple[date, bool]]],
    year: int,
    month: int,
) -> float:
    days_in_month = monthrange(year, month)[1]
    month_start = date(year, month, 1)
    month_end_d = date(year, month, days_in_month)
    total = 0
    d = month_start
    while d <= month_end_d:
        total += _headcount_on_date(by_emp, d)
        d += timedelta(days=1)
    return round(total / days_in_month, 1)


def calc_org_headcount_month_end(
    year: int,
    through_month: int,
    org_guids: list[str] | tuple[str, ...] = (NPO_ORG_GUID,),
    *,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    """Численность неуволенных на конец каждого месяца 1..through_month."""
    sql = sql or SqlConnection()
    through_month = max(1, min(12, int(through_month)))
    guids = tuple(org_guids)
    until = date(year, through_month, monthrange(year, through_month)[1])
    org_bins, by_emp = _load_org_hr_events(sql, guids, until=until)
    months: dict[int, int] = {}
    for m in range(1, through_month + 1):
        as_of = date(year, m, monthrange(year, m)[1])
        months[m] = _headcount_on_date(by_emp, as_of) if by_emp else 0
    labels = [ORG_LABELS.get(g, g) for g in guids]
    return {
        "year": year,
        "months": months,
        "departments_count": len(org_bins),
        "employees_tracked": len(by_emp),
        "organization_keys": list(guids),
        "organizations": labels,
        "organization_key": guids[0] if len(guids) == 1 else "+".join(guids),
        "organization": " + ".join(labels),
    }


def calc_org_ssc_by_month(
    year: int,
    through_month: int,
    org_guids: list[str] | tuple[str, ...] = PRODUCTIVITY_ORG_GUIDS,
    *,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    """ССЧ по одной или нескольким организациям за месяцы 1..through_month.

    При нескольких org_guids — объединённый периметр (сумма списков сотрудников).
    """
    sql = sql or SqlConnection()
    through_month = max(1, min(12, int(through_month)))
    guids = tuple(org_guids)
    until = date(year, through_month, monthrange(year, through_month)[1])
    org_bins, by_emp = _load_org_hr_events(sql, guids, until=until)
    months: dict[int, float] = {}
    for m in range(1, through_month + 1):
        months[m] = _ssc_for_month(by_emp, year, m) if by_emp else 0.0

    labels = [ORG_LABELS.get(g, g) for g in guids]
    return {
        "year": year,
        "months": months,
        "departments_count": len(org_bins),
        "employees_tracked": len(by_emp),
        "organization_keys": list(guids),
        "organizations": labels,
        # совместимость со старым полем
        "organization_key": guids[0] if len(guids) == 1 else "+".join(guids),
        "organization": " + ".join(labels),
    }


def calc_npo_ssc_by_month(
    year: int,
    through_month: int,
    *,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    """ССЧ только НПО (обёртка для совместимости)."""
    return calc_org_ssc_by_month(
        year, through_month, org_guids=(NPO_ORG_GUID,), sql=sql,
    )


def calc_productivity_ssc_by_month(
    year: int,
    through_month: int,
    *,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    """ССЧ НПО + Алмаз — периметр выручки FND-T1 / HRD-M7."""
    return calc_org_ssc_by_month(
        year, through_month, org_guids=PRODUCTIVITY_ORG_GUIDS, sql=sql,
    )
