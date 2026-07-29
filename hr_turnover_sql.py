"""
Общий SQL-расчёт текучести персонала (erp_pm).

OData → SQL:
  Catalog_ПодразделенияОрганизаций              → _Reference358
  Catalog_ШтатноеРасписание                    → _Reference613X1
  InformationRegister_ИсторияИспользованияШР  → _InfoRg45070
  InformationRegister_КадроваяИсторияСотрудников → _InfoRg45157
  Catalog_Сотрудники.ДополнительныеРеквизиты   → _Reference486_VT14110
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from sql_connection import SqlConnection

YEAR_OFFSET = 2000
EMPTY_SQL_YEAR = 2001

ORG_TABLE = "_Reference358"
STAFF_TABLE = "_Reference613X1"
STAFF_HIST = "_InfoRg45070"
HR_HIST = "_InfoRg45157"
EMP_EXTRA_VT = "_Reference486_VT14110"

COL_STAFF_DEPT = "_Fld16326RRef"
COL_STAFF_RATES = "_Fld16330"
COL_STAFF_APPROVED = "_Fld16331"  # binary 0x01
COL_STAFF_CLOSED = "_Fld16333"  # binary 0x01
COL_STAFF_CLOSE_DATE = "_Fld16334"

COL_HIST_POS = "_Fld45071RRef"
COL_HIST_DATE = "_Fld45072"
COL_HIST_USED = "_Fld45073"
COL_HIST_RATES = "_Fld45074"

COL_HR_EMP = "_Fld45158RRef"
COL_HR_DEPT = "_Fld45162RRef"
COL_HR_EVENT = "_Fld45166RRef"

COL_EXTRA_EMP = "_Reference486_IDRRef"
COL_EXTRA_PROP = "_Fld14112RRef"
COL_EXTRA_BOOL = "_Fld14113_L"

EXCLUDE_ATTR_GUID = "87fd0b0c-52c0-11f1-9805-6cb31113810e"
# ВидСобытия = Увольнение (binary fingerprint from OData↔SQL)
DISMISSAL_EVENT_BIN = bytes.fromhex("b33dfc9b1619f433433e45be20908286")

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


def guid_to_1c(guid_str: str) -> bytes:
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


def bin_to_guid(b: bytes) -> str:
    raw = b[12:16] + b[10:12] + b[8:10] + b[0:8]
    return str(uuid.UUID(bytes=raw))


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def to_sql_dt(d: date, end_of_day: bool = False) -> datetime:
    if end_of_day:
        return datetime(d.year + YEAR_OFFSET, d.month, d.day, 23, 59, 59)
    return datetime(d.year + YEAR_OFFSET, d.month, d.day)


def from_sql_dt(dt: datetime | None) -> date | None:
    if dt is None:
        return None
    if dt.year <= EMPTY_SQL_YEAR:
        return None
    return date(dt.year - YEAR_OFFSET, dt.month, dt.day)


def is_true_bin(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value) not in (b"\x00", b"")
    return bool(value)


def turnover_percent(staff: float, dismissed: int) -> float:
    if staff <= 0:
        return 0.0
    return round(dismissed / staff * 100, 1)


@dataclass(frozen=True)
class DeptSpec:
    """Одна строка отчёта текучести."""

    group: str
    org_key: str  # GUID ПодразделенияОрганизаций; пусто = нет маппинга
    note: str = ""
    structure_code: str = ""
    structure_name: str = ""


EXCLUDE_PROP_BIN = guid_to_1c(EXCLUDE_ATTR_GUID)


def load_org_meta(
    sql: SqlConnection,
    org_keys: set[str],
) -> dict[str, dict[str, str]]:
    """org_key → {org_name, org_code}."""
    if not org_keys:
        return {}
    bins = [guid_to_1c(k) for k in org_keys]
    ph = ",".join("?" * len(bins))
    out: dict[str, dict[str, str]] = {}
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT _IDRRef, _Description, _Code
            FROM dbo.[{ORG_TABLE}] WITH (NOLOCK)
            WHERE _IDRRef IN ({ph})
            """,
            bins,
        )
        for idr, desc, code in cur.fetchall():
            key = bin_to_guid(bytes(idr))
            out[key] = {
                "org_name": str(desc or ""),
                "org_code": str(code or "").strip(),
            }
    return out


def load_excluded_employees(sql: SqlConnection) -> set[bytes]:
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT [{COL_EXTRA_EMP}]
            FROM dbo.[{EMP_EXTRA_VT}] WITH (NOLOCK)
            WHERE [{COL_EXTRA_PROP}] = ?
              AND [{COL_EXTRA_BOOL}] = 0x01
            """,
            [EXCLUDE_PROP_BIN],
        )
        return {bytes(r[0]) for r in cur.fetchall()}


def load_positions(
    sql: SqlConnection,
    org_bins: list[bytes],
) -> list[dict[str, Any]]:
    if not org_bins:
        return []
    ph = ",".join("?" * len(org_bins))
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                _IDRRef,
                [{COL_STAFF_DEPT}],
                [{COL_STAFF_RATES}],
                [{COL_STAFF_APPROVED}],
                [{COL_STAFF_CLOSED}],
                [{COL_STAFF_CLOSE_DATE}]
            FROM dbo.[{STAFF_TABLE}] WITH (NOLOCK)
            WHERE _Marked = 0x00
              AND [{COL_STAFF_DEPT}] IN ({ph})
            """,
            org_bins,
        )
        rows = []
        for idr, dept, rates, approved, closed, close_dt in cur.fetchall():
            rows.append(
                {
                    "ref": bytes(idr),
                    "dept": bytes(dept),
                    "rates": float(rates or 0),
                    "approved": is_true_bin(approved),
                    "closed": is_true_bin(closed),
                    "close_date": from_sql_dt(close_dt),
                }
            )
        return rows


def load_history_latest(
    sql: SqlConnection,
    position_refs: list[bytes],
    as_of: date,
) -> dict[bytes, dict[str, Any]]:
    if not position_refs:
        return {}
    as_of_sql = to_sql_dt(as_of, end_of_day=True)
    latest: dict[bytes, dict[str, Any]] = {}
    chunk = 200
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        for i in range(0, len(position_refs), chunk):
            part = position_refs[i : i + chunk]
            ph = ",".join("?" * len(part))
            cur.execute(
                f"""
                SELECT
                    [{COL_HIST_POS}],
                    [{COL_HIST_DATE}],
                    [{COL_HIST_USED}],
                    [{COL_HIST_RATES}]
                FROM dbo.[{STAFF_HIST}] WITH (NOLOCK)
                WHERE _Active = 0x01
                  AND [{COL_HIST_DATE}] <= ?
                  AND [{COL_HIST_POS}] IN ({ph})
                """,
                [as_of_sql, *part],
            )
            for pos, dt, used, rates in cur.fetchall():
                pos_b = bytes(pos)
                prev = latest.get(pos_b)
                if prev is None or dt > prev["date"]:
                    latest[pos_b] = {
                        "date": dt,
                        "used": is_true_bin(used),
                        "rates": float(rates or 0),
                    }
    return latest


def is_position_active(pos: dict[str, Any], as_of: date) -> bool:
    if not pos["approved"]:
        return False
    if not pos["closed"]:
        return True
    close_d = pos["close_date"]
    if close_d is None:
        return False
    return close_d >= as_of


def staff_units_by_dept(
    positions: list[dict[str, Any]],
    history: dict[bytes, dict[str, Any]],
    as_of: date,
) -> dict[bytes, float]:
    by_dept: dict[bytes, float] = defaultdict(float)
    for pos in positions:
        hist = history.get(pos["ref"])
        if hist is not None:
            if hist["used"] and is_position_active(pos, as_of):
                by_dept[pos["dept"]] += hist["rates"]
            continue
        if is_position_active(pos, as_of):
            by_dept[pos["dept"]] += pos["rates"]
    return by_dept


def load_dismissals(
    sql: SqlConnection,
    year: int,
    month: int,
    org_bins: list[bytes],
    excluded: set[bytes],
) -> list[dict[str, Any]]:
    if not org_bins:
        return []
    start = to_sql_dt(date(year, month, 1))
    end = to_sql_dt(month_end(year, month), end_of_day=True)
    ph = ",".join("?" * len(org_bins))
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT [{COL_HR_EMP}], [{COL_HR_DEPT}], _Period
            FROM dbo.[{HR_HIST}] WITH (NOLOCK)
            WHERE _Active = 0x01
              AND [{COL_HR_EVENT}] = ?
              AND _Period >= ? AND _Period <= ?
              AND [{COL_HR_DEPT}] IN ({ph})
            """,
            [DISMISSAL_EVENT_BIN, start, end, *org_bins],
        )
        seen: set[tuple[bytes, bytes]] = set()
        out: list[dict[str, Any]] = []
        for emp, dept, period in cur.fetchall():
            emp_b, dept_b = bytes(emp), bytes(dept)
            if emp_b in excluded:
                continue
            key = (emp_b, dept_b)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "employee": bin_to_guid(emp_b),
                    "department": bin_to_guid(dept_b),
                    "period": period.replace(year=period.year - YEAR_OFFSET).isoformat(sep="T")
                    if period and period.year > EMPTY_SQL_YEAR
                    else str(period),
                }
            )
        return out


def calc_month(
    sql: SqlConnection,
    departments: list[DeptSpec],
    year: int,
    month: int,
    *,
    positions: list[dict[str, Any]] | None = None,
    excluded: set[bytes] | None = None,
    org_meta: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    org_keys = {d.org_key for d in departments if d.org_key}
    org_bins = [guid_to_1c(k) for k in sorted(org_keys)]
    org_meta = org_meta if org_meta is not None else load_org_meta(sql, org_keys)
    excluded = excluded if excluded is not None else load_excluded_employees(sql)
    positions = positions if positions is not None else load_positions(sql, org_bins)

    as_of = month_end(year, month)
    history = load_history_latest(sql, [p["ref"] for p in positions], as_of)
    staff_by_dept = staff_units_by_dept(positions, history, as_of)
    dismissals = load_dismissals(sql, year, month, org_bins, excluded)

    dismissed_by_dept: dict[str, int] = defaultdict(int)
    for row in dismissals:
        dismissed_by_dept[row["department"]] += 1

    rows: list[dict[str, Any]] = []
    total_staff = 0.0
    total_dismissed = 0
    for dept in departments:
        meta = org_meta.get(dept.org_key, {}) if dept.org_key else {}
        dept_bin = guid_to_1c(dept.org_key) if dept.org_key else None
        staff = round(staff_by_dept.get(dept_bin, 0.0), 2) if dept_bin else 0.0
        dismissed = dismissed_by_dept.get(dept.org_key, 0) if dept.org_key else 0
        note = dept.note
        if dept.org_key and staff == 0 and not note:
            note = "нет позиций в штатном расписании"
        rows.append(
            {
                "group": dept.group,
                "structure_code": dept.structure_code,
                "structure_name": dept.structure_name or dept.group,
                "org_code": meta.get("org_code", ""),
                "org_name": meta.get("org_name", ""),
                "org_key": dept.org_key,
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
        "dismissal_details": dismissals,
        "source": "sql",
    }


def build_report(
    departments: list[DeptSpec],
    start: tuple[int, int],
    end: tuple[int, int],
    *,
    sql: SqlConnection | None = None,
    hierarchy_mode: str = "listed_only_no_auto_children",
    kpi_label: str = "TURN",
) -> dict[str, Any]:
    sql = sql or SqlConnection()
    org_keys = {d.org_key for d in departments if d.org_key}
    org_bins = [guid_to_1c(k) for k in sorted(org_keys)]
    org_meta = load_org_meta(sql, org_keys)
    excluded = load_excluded_employees(sql)
    positions = load_positions(sql, org_bins)

    months: list[dict[str, Any]] = []
    y, m = start
    while (y, m) <= end:
        months.append(
            calc_month(
                sql,
                departments,
                y,
                m,
                positions=positions,
                excluded=excluded,
                org_meta=org_meta,
            )
        )
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    return {
        "kpi": kpi_label,
        "departments": [
            {
                "group": d.group,
                "structure_key": "",
                "structure_name": d.structure_name or d.group,
                "structure_code": d.structure_code,
                "org_key": d.org_key,
                "org_name": org_meta.get(d.org_key, {}).get("org_name", ""),
                "org_code": org_meta.get(d.org_key, {}).get("org_code", ""),
                "note": d.note,
            }
            for d in departments
        ],
        "excluded_employees": len(excluded),
        "hierarchy_mode": hierarchy_mode,
        "months": months,
        "source": "sql",
        "sql_tables": {
            "org": ORG_TABLE,
            "staffing": STAFF_TABLE,
            "staff_history": STAFF_HIST,
            "hr_history": HR_HIST,
            "employee_extra": EMP_EXTRA_VT,
        },
    }
