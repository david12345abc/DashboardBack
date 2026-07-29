"""
1С-M1 — SLA заявок в отдел сопровождения 1С (SQL-бэкап erp_pm).

План — число заявок за месяц:
  • _Marked = 0
  • _Date_Time в календарном месяце (в SQL год + 2000)
  • ПодразделениеИсполнитель = «Отдел сопровождения 1С»

Факт — из плана со статусом «Отработано»:
  • Статус ∈ {Исполнен, Аннулирован}

SQL:
  Document_ТД_ЗаявкаВСлужбуСопровождения → dbo._Document76754X1
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sql_connection import SqlConnection

EXECUTOR_DEPARTMENT_LABEL = "Отдел сопровождения 1С"
EXECUTOR_DEPARTMENT_GUID = "4668a58d-6eb1-11e2-afce-001e67112509"

FACT_STATUSES = frozenset({"Исполнен", "Аннулирован"})

YEAR_OFFSET = 2000
DOC_SQL = "_Document76754X1"
COL_EXECUTOR = "_Fld76800RRef"
COL_STATUS = "_Fld76789RRef"
COL_HOURS = "_Fld96856"

STATUS_GUIDS = {
    "Исполнен": "cf98f404-ffff-43ac-9872-f67940f6a5b9",
    "НаИсполнении": "cf4fc467-7eed-4c34-b6d7-3274f4f4ca76",
    "Подготовлен": "2bd07cf4-b389-4098-a3c6-f0706a0844bf",
    "Аннулирован": "d8a68847-6d0f-47da-b7ba-b3ba877193c7",
    "Отклонен": "0465869c-8837-49fc-a379-dc9a2e1dc610",
}

# Эталон из TestKPIDump/1cauto/1c_m1.py (отчёт 1С); SQL-бэкап может отставать.
REFERENCE_2026: dict[int, tuple[float, float]] = {
    1: (309.0, 271.0),
    2: (395.0, 368.0),
    3: (439.0, 394.0),
    4: (392.0, 343.0),
    5: (360.0, 320.0),
    6: (368.0, 315.0),
    7: (324.0, 259.0),
}

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def sql_period_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    y = year + YEAR_OFFSET
    start = datetime(y, month, 1)
    if month == 12:
        end = datetime(y + 1, 1, 1)
    else:
        end = datetime(y, month + 1, 1)
    return start, end


def guid_to_1c(guid_str: str) -> bytes:
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


EXECUTOR_BIN = guid_to_1c(EXECUTOR_DEPARTMENT_GUID)
FACT_STATUS_BINS = tuple(guid_to_1c(STATUS_GUIDS[s]) for s in sorted(FACT_STATUSES))


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100.0, 1)


def compute_month(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)
    fact_ph = ",".join("?" * len(FACT_STATUS_BINS))
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS plan_cnt,
                SUM(CASE WHEN d.[{COL_STATUS}] IN ({fact_ph}) THEN 1 ELSE 0 END) AS fact_cnt,
                SUM(CASE WHEN d.[{COL_STATUS}] IN ({fact_ph})
                         THEN CAST(d.[{COL_HOURS}] AS float) ELSE 0 END) AS hours_fact,
                SUM(CAST(d.[{COL_HOURS}] AS float)) AS hours_total
            FROM dbo.[{DOC_SQL}] d WITH (NOLOCK)
            WHERE d._Marked = 0x00
              AND d._Date_Time >= ?
              AND d._Date_Time < ?
              AND d.[{COL_EXECUTOR}] = ?
            """,
            [*FACT_STATUS_BINS, *FACT_STATUS_BINS, p_start, p_end, EXECUTOR_BIN],
        )
        plan_cnt, fact_cnt, hours_fact, hours_total = cur.fetchone()

        cur.execute(
            f"""
            SELECT d.[{COL_STATUS}], COUNT(*)
            FROM dbo.[{DOC_SQL}] d WITH (NOLOCK)
            WHERE d._Marked = 0x00
              AND d._Date_Time >= ?
              AND d._Date_Time < ?
              AND d.[{COL_EXECUTOR}] = ?
            GROUP BY d.[{COL_STATUS}]
            """,
            [p_start, p_end, EXECUTOR_BIN],
        )
        guid_to_status = {guid_to_1c(g): name for name, g in STATUS_GUIDS.items()}
        by_status: dict[str, int] = {}
        for val, cnt in cur.fetchall():
            label = guid_to_status.get(bytes(val), bytes(val).hex())
            by_status[label] = int(cnt)

    plan = float(plan_cnt or 0)
    fact = float(fact_cnt or 0)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "plan": plan,
        "fact": fact,
        "kpi_pct": kpi_pct(plan, fact),
        "has_data": plan > 0,
        "values_unit": "шт.",
        "hours_fact": round(float(hours_fact or 0), 2),
        "hours_total": round(float(hours_total or 0), 2),
        "counts": {
            "docs_plan": int(plan),
            "docs_fact": int(fact),
            "by_status": by_status,
        },
        "period_start": p_start.isoformat(sep="T"),
        "period_end": p_end.isoformat(sep="T"),
        "executor_department": EXECUTOR_DEPARTMENT_LABEL,
        "executor_department_key": EXECUTOR_DEPARTMENT_GUID,
        "source": "sql",
    }


def build_c1_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    now = date.today()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m

    sql = SqlConnection()
    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, month + 1):
        row = compute_month(year, m, sql=sql)
        monthly_rows.append(
            {
                "month": row["month"],
                "year": row["year"],
                "month_name": row["month_name"],
                "plan": row["plan"],
                "fact": row["fact"],
                "kpi_pct": row["kpi_pct"],
                "has_data": row["has_data"],
                "values_unit": "шт.",
            }
        )
    ref_row = monthly_rows[-1] if monthly_rows else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "1C-M1",
            "status": "ok",
            "source": "getkpi.c1auto.c1_m1_core.sql",
            "plan_source": f"все заявки «{EXECUTOR_DEPARTMENT_LABEL}» за месяц по Date",
            "fact_source": "Статус Исполнен/Аннулирован (Отработано в отчёте 1С)",
            "executor_department": EXECUTOR_DEPARTMENT_LABEL,
            "executor_department_key": EXECUTOR_DEPARTMENT_GUID,
            "document": DOC_SQL,
        },
    }
