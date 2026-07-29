"""
IT-Q2 — факт текучести персонала службы ИТ.

Принцип: штатные единицы (ставки ШР на дату среза) и увольнения
за период с фильтром доп. реквизита НеУчитыватьПриТекучести.

Источник (SQL erp_pm):
  Catalog_ПодразделенияОрганизаций              → _Reference358
  Catalog_ШтатноеРасписание                    → _Reference613X1
  InformationRegister_ИсторияИспользованияШР  → _InfoRg45070
  InformationRegister_КадроваяИсторияСотрудников → _InfoRg45157
  Catalog_Сотрудники.ДопРеквизиты              → _Reference486_VT14110

Подразделение контура:
  Отдел информационных технологий

Текучесть % = Уволено / Штатные единицы × 100 (до 1 знака).

Использование:
  python autoit/it_q2.py
  python autoit/it_q2.py 2026 6
  python autoit/it_q2.py 2026-01 2026-07
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

from getkpi.autoit.it_q2_tekuchest_plan import plan_for_month

SCRIPT_DIR = Path(__file__).resolve().parent
HIERARCHY_MODE = "listed_only_no_auto_children"
KPI_ID = "IT-Q2"
TITLE = "IT-Q2 факт текучести"

IT_Q2_DEPARTMENTS = (
    "Отдел информационных технологий",
)

DEPARTMENTS = [
    DeptSpec(
        group="Отдел информационных технологий",
        org_key="8b51d336-f396-11e8-8283-ac1f6b05524d",
        structure_code="00-000057",
        structure_name="Отдел информационных технологий",
        note="орг GUID из сверки структура↔ШР",
    ),
]


def build_it_q2_payload(year: int | None = None, month: int | None = None) -> dict:
    now = datetime.now()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m
    report = build_report(
        DEPARTMENTS,
        (year, 1),
        (year, month),
        hierarchy_mode=HIERARCHY_MODE,
        kpi_label=KPI_ID,
    )
    payload = dashboard_payload_from_report(
        report,
        kpi_id=KPI_ID,
        year=year,
        month=month,
        source_module="autoit.it_q2.sql",
        target_departments=list(IT_Q2_DEPARTMENTS),
        hierarchy_mode=HIERARCHY_MODE,
    )
    return apply_plan_table_to_turnover_payload(
        payload,
        year,
        month,
        plan_for_month=plan_for_month,
        plan_source="getkpi.autoit.it_q2_tekuchest_plan",
    )


def main() -> None:
    try:
        run_turnover_cli(
            kpi_id=KPI_ID,
            title=TITLE,
            departments=DEPARTMENTS,
            script_dir=SCRIPT_DIR,
            file_prefix="it_q2",
            hierarchy_mode=HIERARCHY_MODE,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
