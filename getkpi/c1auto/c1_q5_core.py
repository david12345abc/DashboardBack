"""
1С-Q5 — факт текучести персонала отдела сопровождения 1С.

Источник (SQL erp_pm): см. hr_turnover_sql.py

Подразделение контура:
  Отдел сопровождения 1С

Использование:
  python 1cauto/1c_q5.py
  python 1cauto/1c_q5.py 2026-01 2026-07
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from hr_turnover_sql import DeptSpec, build_report
from turnover_report import (
    apply_plan_table_to_turnover_payload,
    dashboard_payload_from_report,
    run_turnover_cli,
)

from getkpi.c1auto.c1_q5_tekuchest_plan import plan_for_month

SCRIPT_DIR = Path(__file__).resolve().parent
HIERARCHY_MODE = "listed_only_no_auto_children"
KPI_ID = "1С-Q5"
TITLE = "1С-Q5 факт текучести"

C1_Q5_DEPARTMENTS = ("Отдел сопровождения 1С",)

DEPARTMENTS = [
    DeptSpec(
        group="Отдел сопровождения 1С",
        org_key="79456e06-f396-11e8-8283-ac1f6b05524d",
        structure_name="Отдел сопровождения 1С",
        note="орг GUID из сверки структура↔ШР",
    ),
]


def build_c1_q5_payload(year: int | None = None, month: int | None = None) -> dict:
    now = datetime.now()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m
    report = build_report(DEPARTMENTS, (year, 1), (year, month), hierarchy_mode=HIERARCHY_MODE, kpi_label=KPI_ID)
    payload = dashboard_payload_from_report(
        report,
        kpi_id=KPI_ID,
        year=year,
        month=month,
        source_module="1cauto.1c_q5.sql",
        target_departments=list(C1_Q5_DEPARTMENTS),
        hierarchy_mode=HIERARCHY_MODE,
    )
    return apply_plan_table_to_turnover_payload(
        payload,
        year,
        month,
        plan_for_month=plan_for_month,
        plan_source="getkpi.c1auto.c1_q5_tekuchest_plan",
    )


def main() -> None:
    try:
        run_turnover_cli(
            kpi_id=KPI_ID,
            title=TITLE,
            departments=DEPARTMENTS,
            script_dir=SCRIPT_DIR,
            file_prefix="1c_q5",
            hierarchy_mode=HIERARCHY_MODE,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
