"""
QD-Q2 — факт текучести персонала службы качества.

Источник (SQL erp_pm): см. hr_turnover_sql.py

Использование:
  python qualdir/qd_q2.py
  python qualdir/qd_q2.py 2026-01 2026-07
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from hr_turnover_sql import DeptSpec, build_report
from turnover_report import dashboard_payload_from_report, run_turnover_cli

from qualdir.qd_q2_tekuchest_plan import plan_for_month

SCRIPT_DIR = Path(__file__).resolve().parent
HIERARCHY_MODE = "listed_only_no_auto_children"
KPI_ID = "QD-Q2"
TITLE = "QD-Q2 факт текучести"

QD_Q2_DEPARTMENTS = [
    "ОТК-1",
    "ОТК-2",
    "Лаборатория неразрушающего контроля",
    "Отдел управления несоответствиями",
]

DEPARTMENTS = [
    DeptSpec("ОТК-1", "42dfd1d2-e6f3-11e7-826b-ac1f6b05524d"),
    DeptSpec("ОТК-2", "85f3b385-04c2-11e8-826d-ac1f6b05524d"),
    DeptSpec("Лаборатория неразрушающего контроля", "7ebffdf2-88b3-11eb-8577-ac1f6b05524d"),
    DeptSpec(
        "Отдел управления несоответствиями",
        "",
        note="нет активного орг. GUID в ШР",
    ),
]


def apply_qd_q2_plan(payload: dict[str, Any], year: int, month: int) -> dict[str, Any]:
    """План из утверждённой таблицы (раньше top2 Document_ТД_ТекучестьПерсонала)."""
    for row in payload.get("monthly_data") or []:
        try:
            ry, rm = int(row["year"]), int(row["month"])
        except (KeyError, TypeError, ValueError):
            continue
        plan = plan_for_month(ry, rm)
        row["plan"] = plan
        row["plan_max_turnover_pct"] = plan
        # Как в старом turnover.py: плитка «готова», когда есть и план, и факт.
        row["has_data"] = plan is not None and row.get("fact") is not None
    ref = payload.get("last_full_month_row")
    if isinstance(ref, dict):
        ref_plan = plan_for_month(year, month)
        if ref_plan is None:
            try:
                ref_plan = plan_for_month(int(ref["year"]), int(ref["month"]))
            except (KeyError, TypeError, ValueError):
                ref_plan = None
        ref["plan"] = ref_plan
        ref["plan_max_turnover_pct"] = ref_plan
        ref["has_data"] = ref_plan is not None and ref.get("fact") is not None
        payload["last_full_month_row"] = ref
    ytd = payload.setdefault("ytd", {})
    ytd["total_plan"] = plan_for_month(year, month)
    debug = payload.setdefault("debug", {})
    debug["plan_source"] = "qualdir.qd_q2_tekuchest_plan"
    debug["metric"] = "plan_table_plus_sql_fact"
    return payload


def build_qd_q2_payload(year: int | None = None, month: int | None = None) -> dict:
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
        source_module="qualdir.qd_q2.sql",
        target_departments=list(QD_Q2_DEPARTMENTS),
        hierarchy_mode=HIERARCHY_MODE,
    )
    return apply_qd_q2_plan(payload, year, month)


def main() -> None:
    try:
        run_turnover_cli(
            kpi_id=KPI_ID,
            title=TITLE,
            departments=DEPARTMENTS,
            script_dir=SCRIPT_DIR,
            file_prefix="qd_q2",
            hierarchy_mode=HIERARCHY_MODE,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

QD_Q2_YTD_CACHE_PREFIX = "qualdir_qd_q2_ytd"
QD_Q2_YTD_DISK_TAG = "qualdir_qd_q2_ytd_sql_payload_v2"
QD_Q2_YTD_DISK_VERSION = 2


def turnover_month_cache_path(year: int, month: int) -> _Path:
    return month_cache_path("qualdir_tekuchet_sql", year, month)


def qd_q2_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_Q2_YTD_CACHE_PREFIX, ry, rm)


def get_qd_q2_ytd(year: int | None = None, month: int | None = None) -> dict:
    ref_y, ref_m = normalize_period(year, month)
    payload = get_ytd_via_cache(
        year=ref_y,
        month=ref_m,
        cache_prefix=QD_Q2_YTD_CACHE_PREFIX,
        source_tag=QD_Q2_YTD_DISK_TAG,
        version=QD_Q2_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_q2_sql",
        compute_fn=lambda y, m: build_qd_q2_payload(y, m),
        kpi_id="QD-Q2",
    )
    return apply_qd_q2_plan(payload, ref_y, ref_m)
