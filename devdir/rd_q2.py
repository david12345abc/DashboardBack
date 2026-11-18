"""
RD-Q2 — факт текучести персонала контура директора по развитию.

Источник (SQL erp_pm): см. hr_turnover_sql.py

Использование:
  python devdir/rd_q2.py
  python devdir/rd_q2.py 2026-01 2026-07
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from hr_turnover_sql import DeptSpec, SqlConnection, build_report, headcount_month
from turnover_report import (
    apply_plan_table_to_turnover_payload,
    dashboard_payload_from_report,
    run_turnover_cli,
)

from devdir.rd_q2_tekuchest_plan import plan_for_month

SCRIPT_DIR = Path(__file__).resolve().parent
HIERARCHY_MODE = "listed_only_no_auto_children"
KPI_ID = "RD-Q2"
TITLE = "RD-Q2 факт текучести"

# Увольнения 17.11.2026 в секторе ИИ: в отчёте есть, в дампе кадровой истории ещё нет.
NOVEMBER_DISMISSALS_NOT_IN_DUMP = (
    ("44b05d83-58c6-11f1-980d-6cb31113810c", "c478df52-ddff-11ef-95fc-6cb31113810e"),
    ("d3e8a703-07d4-11f1-979e-6cb31113810c", "c478df52-ddff-11ef-95fc-6cb31113810e"),
    ("988b096a-def9-11f0-9767-6cb31113810e", "c478df52-ddff-11ef-95fc-6cb31113810e"),
    ("cca67597-fff8-11f0-9792-6cb31113810c", "c478df52-ddff-11ef-95fc-6cb31113810e"),
)

RD_Q2_DEPARTMENTS = [
    "ДИРЕКТОР ПО РАЗВИТИЮ",
    "Служба развития",
    "Сектор по развитию новых продуктов",
    "Сектор по внедрению искусственного интеллекта",
    "Сектор обучения и развития",
]

DEPARTMENTS = [
    DeptSpec("ДИРЕКТОР ПО РАЗВИТИЮ", "", note="нет орг. GUID / ставки в дочерних"),
    DeptSpec("Служба развития", "eee813d7-f395-11e8-8283-ac1f6b05524d"),
    DeptSpec("Сектор по развитию новых продуктов", "eb25e5c4-1af8-11ee-93af-6cb31113810c"),
    DeptSpec("Сектор по внедрению искусственного интеллекта", "c478df52-ddff-11ef-95fc-6cb31113810e"),
    DeptSpec("Сектор обучения и развития", "2fc48070-f110-11f0-977f-6cb31113810c"),
]


def build_rd_q2_payload(year: int | None = None, month: int | None = None) -> dict:
    now = datetime.now()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m
    report = build_report(DEPARTMENTS, (year, 1), (year, month), hierarchy_mode=HIERARCHY_MODE, kpi_label=KPI_ID)
    if year == 2026:
        sql = SqlConnection()
        for snap in report["months"]:
            if snap["month"] != 11:
                continue
            counted = headcount_month(
                sql,
                DEPARTMENTS,
                year,
                11,
                extra_dismissals=NOVEMBER_DISMISSALS_NOT_IN_DUMP,
            )
            snap["rows"] = counted["rows"]
            snap["total_staff_units"] = counted["total_staff_units"]
            snap["total_dismissed"] = counted["total_dismissed"]
            snap["turnover_pct"] = counted["turnover_pct"]
    payload = dashboard_payload_from_report(
        report,
        kpi_id=KPI_ID,
        year=year,
        month=month,
        source_module="devdir.rd_q2.sql",
        target_departments=list(RD_Q2_DEPARTMENTS),
        hierarchy_mode=HIERARCHY_MODE,
    )
    return apply_plan_table_to_turnover_payload(
        payload,
        year,
        month,
        plan_for_month=plan_for_month,
        plan_source="devdir.rd_q2_tekuchest_plan",
    )


def main() -> None:
    try:
        run_turnover_cli(
            kpi_id=KPI_ID,
            title=TITLE,
            departments=DEPARTMENTS,
            script_dir=SCRIPT_DIR,
            file_prefix="rd_q2",
            hierarchy_mode=HIERARCHY_MODE,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
