"""
TD-Q2 — факт текучести персонала техдирекции.

Только факт (SQL erp_pm): штат / увольнения, см. hr_turnover_sql.py
Текучесть % = Уволено / Штатные единицы × 100 (до 1 знака).

Использование:
  python techdir/td_q2.py
  python techdir/td_q2.py --check
  python techdir/td_q2.py 2026-01 2026-07
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from hr_turnover_sql import DeptSpec, MONTH_NAMES, SqlConnection, build_report, headcount_month
from turnover_report import (
    apply_plan_table_to_turnover_payload,
    dashboard_payload_from_report,
    run_turnover_cli,
)

from getkpi.td_q2_tekuchest_plan import plan_for_month

SCRIPT_DIR = Path(__file__).resolve().parent
HIERARCHY_MODE = "techdir_listed_org_guids"
KPI_ID = "TD-Q2"
TITLE = "TD-Q2 факт текучести"
ROUND_TOLERANCE = 0.05

TD_Q2_DEPARTMENTS = (
    "Отдел технической поддержки",
    "Сервисная служба",
    "Сектор качества разработки",
    "Метрологическая служба",
    "Отдел метрологии и сертификации",
    "Проектный офис",
    "Сектор промышленной безопасности",
    "Эксплуатационная служба",
)

# Контур отчёта 1С за октябрь и ноябрь 2026: численность людей, не ставки.
REPORT_DEPARTMENTS = [
    DeptSpec("Административно-хозяйственный отдел", "af350276-e6e4-11e7-826b-ac1f6b05524d"),
    DeptSpec("Лаборатория неразрушающего контроля", "7ebffdf2-88b3-11eb-8577-ac1f6b05524d"),
    DeptSpec("Отдел по работе с рекламациями", "f5a56328-68b5-11e8-8276-ac1f6b05524d"),
    DeptSpec("Отдел технической поддержки", "d455d61b-f396-11e8-8283-ac1f6b05524d"),
    DeptSpec("Сектор качества разработки", "75d572f4-e4bd-11f0-976f-6cb31113810e"),
    DeptSpec("Сектор промышленной безопасности", "d6669ecf-e4bd-11f0-976f-6cb31113810e"),
    DeptSpec("Сектор разработки технических решений", "af809fa2-e4bd-11f0-976f-6cb31113810e"),
    # 16.11.2026 часть сотрудников переведена на новую карточку с тем же названием.
    DeptSpec("Сектор разработки технических решений", "88ae80e7-e24a-11f1-98c9-6cb31113810e"),
    DeptSpec("Сервисная служба", "a37213b5-e6f1-11e7-826b-ac1f6b05524d"),
    DeptSpec("Служба технического директора", "f348c51e-d57f-11ea-83c5-ac1f6b05524c"),
    DeptSpec("Эксплуатационная служба", "07cf0cbf-d580-11ea-83c5-ac1f6b05524c"),
]

# Мещерюк, 17.11.2026, сектор промбезопасности: в отчёте есть, в дампе увольнения ещё нет.
NOVEMBER_DISMISSALS_NOT_IN_DUMP = (
    ("2710c121-b5f6-11ef-95c8-6cb31113810e", "d6669ecf-e4bd-11f0-976f-6cb31113810e"),
)

DEPARTMENTS = [
    DeptSpec("Отдел технической поддержки", "d455d61b-f396-11e8-8283-ac1f6b05524d"),
    DeptSpec("Сервисная служба", "a37213b5-e6f1-11e7-826b-ac1f6b05524d"),
    DeptSpec("Сектор качества разработки", "75d572f4-e4bd-11f0-976f-6cb31113810e"),
    DeptSpec("Метрологическая служба", "8cf17e48-df91-11e9-829b-ac1f6b05524d"),
    DeptSpec("Отдел метрологии и сертификации", "22882cd7-d5fc-11e9-829b-ac1f6b05524d"),
    DeptSpec("Проектный офис", "6077b515-f396-11e8-8283-ac1f6b05524d"),
    DeptSpec("Сектор промышленной безопасности", "d6669ecf-e4bd-11f0-976f-6cb31113810e"),
    DeptSpec("Эксплуатационная служба", "07cf0cbf-d580-11ea-83c5-ac1f6b05524c"),
]

# Факт % (SQL HR) · 2026
REFERENCE_2026: dict[int, float] = {
    1: 0.0,
    2: 7.7,
    3: 0.0,
    4: 2.6,
    5: 0.0,
    6: 2.6,
    7: 2.6,
}


def _merge_department_rows(rows: list[dict]) -> list[dict]:
    """Одноимённые карточки подразделения складываются в одну строку штата."""
    merged: list[dict] = []
    index: dict[str, int] = {}
    for row in rows:
        key = str(row.get("group") or "")
        pos = index.get(key)
        if pos is None:
            index[key] = len(merged)
            merged.append(dict(row))
            continue
        target = merged[pos]
        target["staff_units"] = float(target.get("staff_units") or 0) + float(row.get("staff_units") or 0)
        target["dismissed"] = int(target.get("dismissed") or 0) + int(row.get("dismissed") or 0)
    return merged


def build_td_q2_payload(
    year: int | None = None,
    month: int | None = None,
    *,
    strict: bool = False,  # noqa: ARG001 — совместимость со старым вызовом
) -> dict:
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
    if year == 2026:
        sql = SqlConnection()
        for snap in report["months"]:
            if snap["month"] not in (10, 11):
                continue
            extra = NOVEMBER_DISMISSALS_NOT_IN_DUMP if snap["month"] == 11 else None
            counted = headcount_month(
                sql,
                REPORT_DEPARTMENTS,
                year,
                snap["month"],
                extra_dismissals=extra,
            )
            snap["rows"] = _merge_department_rows(counted["rows"])
            snap["total_staff_units"] = counted["total_staff_units"]
            snap["total_dismissed"] = counted["total_dismissed"]
            snap["turnover_pct"] = counted["turnover_pct"]
    payload = dashboard_payload_from_report(
        report,
        kpi_id=KPI_ID,
        year=year,
        month=month,
        source_module="getkpi.td_q2.sql",
        target_departments=list(TD_Q2_DEPARTMENTS),
        hierarchy_mode=HIERARCHY_MODE,
    )
    return apply_plan_table_to_turnover_payload(
        payload,
        year,
        month,
        plan_for_month=plan_for_month,
        plan_source="getkpi.td_q2_tekuchest_plan",
    )


def run_check() -> int:
    print("Сверка TD-Q2 · факт SQL · 2026")
    report = build_report(DEPARTMENTS, (2026, 1), (2026, 7), hierarchy_mode=HIERARCHY_MODE, kpi_label=KPI_ID)
    by_month = {(s["year"], s["month"]): s for s in report["months"]}
    all_ok = True
    for month, ref_fact in sorted(REFERENCE_2026.items()):
        snap = by_month[(2026, month)]
        fact = float(snap["turnover_pct"])
        ok = abs(fact - ref_fact) <= ROUND_TOLERANCE
        if not ok:
            all_ok = False
        mark = "OK" if ok else "РАСХОЖДЕНИЕ"
        print(
            f"  {MONTH_NAMES[month]}: fact={fact:.1f}/{ref_fact:.1f} "
            f"staff={snap['total_staff_units']} disc={snap['total_dismissed']} ({mark})"
        )
    return 0 if all_ok else 2


def main() -> None:
    try:
        argv = sys.argv[1:]
        if "--check" in argv:
            sys.exit(run_check())
        run_turnover_cli(
            kpi_id=KPI_ID,
            title=TITLE,
            departments=DEPARTMENTS,
            script_dir=SCRIPT_DIR,
            file_prefix="td_q2",
            hierarchy_mode=HIERARCHY_MODE,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
