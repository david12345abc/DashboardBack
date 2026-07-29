"""
IT-M1 — SLA заявок в службу ИТ (Отдел информационных технологий).

Сверка с отчётом 1С «Отчет по Заявкам»
(Подразделение-исполнитель = Отдел информационных технологий).

План — число заявок за месяц:
  • _Marked = 0
  • _Date_Time в календарном месяце (в SQL год + 2000)
  • ПодразделениеИсполнитель = «Отдел информационных технологий»
  (= «Итого» / Количество в отчёте)

Факт — из плана со статусом «Отработано» в отчёте:
  • Статус ∈ {Исполнен, Аннулирован}

KPI % = факт / план × 100 (до 1 знака).

SQL (erp_pm):
  Document_ТД_ЗаявкаВСлужбуСопровождения → dbo._Document76754X1
    _Fld76800RRef — ПодразделениеИсполнитель
    _Fld76789RRef — Статус (enum)
    _Fld96856     — ВремяВыполнения (часы)

SQL-бэкап может отставать от отчёта 1С (особенно текущий месяц).

Замечание: DashboardBack/support_sla_data считал факт по
ПодтверждениеАвтораОбИсполнении в том же месяце — это ниже
строки «Отработано» отчёта. Здесь факт = отчёт.

Использование:
  python autoit/it_m1.py
  python autoit/it_m1.py --check
  python autoit/it_m1.py 2026
  python autoit/it_m1.py 2026-05
  python autoit/it_m1.py 2026-01 2026-07
"""

from __future__ import annotations

import json
import logging as _logging
import re
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

log = _logging.getLogger(__name__).info
SCRIPT_DIR = Path(__file__).resolve().parent

EXECUTOR_DEPARTMENT_LABEL = "Отдел информационных технологий"
EXECUTOR_DEPARTMENT_GUID = "bafd0e4c-1f39-11e2-855f-001e67112509"

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

# Live SQL REFERENCE (erp_pm). Текущий месяц может отставать от отчёта 1С.
REFERENCE_2026: dict[int, tuple[float, float]] = {
    1: (105.0, 98.0),
    2: (131.0, 127.0),
    3: (147.0, 142.0),
    4: (137.0, 128.0),
    5: (128.0, 125.0),
    6: (143.0, 142.0),
    7: (147.0, 141.0),
}

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


def parse_month(value: str) -> tuple[int, int]:
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Месяц должен быть в формате ГГГГ-ММ")
    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def parse_period_args(argv: list[str] | None = None) -> tuple[tuple[int, int], tuple[int, int], str]:
    args = [arg.strip() for arg in (argv if argv is not None else sys.argv[1:]) if arg.strip()]
    args = [a for a in args if not a.startswith("-")]
    now = datetime.now()

    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)

    if len(args) == 2 and args[0].isdigit() and args[1].isdigit() and len(args[0]) == 4:
        period = (int(args[0]), int(args[1]))
        if not 1 <= period[1] <= 12:
            raise ValueError("Месяц должен быть от 1 до 12")
        return period, period, f"{period[0]:04d}-{period[1]:02d}"

    if len(args) == 1 and len(args[0]) == 4 and args[0].isdigit():
        year = int(args[0])
        return (year, 1), (year, 12), args[0]

    if len(args) == 1:
        period = parse_month(args[0])
        return period, period, args[0]

    if len(args) == 2:
        start = parse_month(args[0])
        end = parse_month(args[1])
        if start > end:
            raise ValueError("Дата начала должна быть не позже даты окончания")
        return start, end, f"{args[0]}_{args[1]}"

    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ, ГОД МЕСЯЦ или ГГГГ-ММ ГГГГ-ММ")


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    year, month = start
    result: list[tuple[int, int]] = []
    while (year, month) <= end:
        result.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return result


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


def build_monthly_report(
    start: tuple[int, int],
    end: tuple[int, int],
) -> dict[str, Any]:
    log("[SQL] Расчёт IT-M1 ...")
    sql = SqlConnection()
    months = iter_months(start, end)
    rows: list[dict[str, Any]] = []
    for i, (year, month) in enumerate(months, 1):
        log(f"  {year}-{month:02d} ({i}/{len(months)}) ...")
        rows.append(compute_month(year, month, sql=sql))
    return {
        "executor_department": EXECUTOR_DEPARTMENT_LABEL,
        "executor_department_key": EXECUTOR_DEPARTMENT_GUID,
        "months": rows,
        "document": DOC_SQL,
        "source": "sql",
    }


def format_report(payload: dict[str, Any]) -> str:
    rows = payload["months"]
    lines = [
        "IT-M1 — SLA заявок в службу ИТ (SQL)",
        f"Источник: {DOC_SQL} (erp_pm, год+{YEAR_OFFSET})",
        f"Исполнитель: {payload['executor_department']}",
        "План: Date в месяце + ПодразделениеИсполнитель = ОИТ",
        "Факт: Статус ∈ {Исполнен, Аннулирован} (= «Отработано» в отчёте 1С)",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'KPI %':>8} {'Часы факт':>10}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 10}",
    ]
    for row in rows:
        pct = row["kpi_pct"]
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{int(row['plan']):>8} "
            f"{int(row['fact']):>8} "
            f"{pct_s:>8} "
            f"{float(row.get('hours_fact') or 0):>10.2f}"
        )

    plan_sum = sum(float(r["plan"]) for r in rows)
    fact_sum = sum(float(r["fact"]) for r in rows)
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 10}",
            f"{'ИТОГО':<10} {int(plan_sum):>8} {int(fact_sum):>8} {'':>8} {'':>10}",
            "",
        ]
    )
    return "\n".join(lines)


def save_report(period_slug: str, payload: dict[str, Any], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"it_m1_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"it_m1_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return txt_path, json_path


def build_it_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Payload в стиле DashboardBack IT-M1."""
    now = datetime.now()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m

    report = build_monthly_report((year, 1), (year, month))
    monthly_rows = [
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
        for row in report["months"]
    ]
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
            "kpi_id": "IT-M1",
            "status": "ok",
            "source": "autoit.it_m1.sql",
            "plan_source": "все заявки ОИТ за месяц по Date",
            "fact_source": "Статус Исполнен/Аннулирован (Отработано в отчёте 1С)",
            "executor_department": report["executor_department"],
            "executor_department_key": report["executor_department_key"],
            "document": DOC_SQL,
        },
    }


def run_check() -> int:
    print(f"Сверка IT-M1 SLA · SQL {DOC_SQL} · 2026")
    sql = SqlConnection()
    all_ok = True
    for month, (ref_plan, ref_fact) in sorted(REFERENCE_2026.items()):
        snap = compute_month(2026, month, sql=sql)
        plan = float(snap["plan"])
        fact = float(snap["fact"])
        ok = plan == ref_plan and fact == ref_fact
        if not ok:
            all_ok = False
        mark = "OK" if ok else "РАСХОЖДЕНИЕ"
        print(
            f"  {MONTH_NAMES[month]}: plan={int(plan)}/{int(ref_plan)} "
            f"fact={int(fact)}/{int(ref_fact)} ({mark})"
        )
    return 0 if all_ok else 2


