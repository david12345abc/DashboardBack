"""
ГСП-Q5 — факт текучести персонала ГСПП.

Источник (SQL erp_pm): см. hr_turnover_sql.py

Подразделения:
  ГСПП
  Сектор сопровождения продаж
  Сектор сопровождения производства и продаж
  Сектор доработки и улучшения продукции
  Сектор постановки на производство несерийной продукции

Использование:
  python gspp/gsp_q5.py
  python gspp/gsp_q5.py 2026-01 2026-07
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from hr_turnover_sql import DeptSpec, build_report
from turnover_report import dashboard_payload_from_report, run_turnover_cli

from gspp.q5_tekuchest_plan import plan_for_month

SCRIPT_DIR = Path(__file__).resolve().parent
HIERARCHY_MODE = "listed_only_no_auto_children"
KPI_ID = "ГСП-Q5"
TITLE = "ГСП-Q5 факт текучести"

GSPP_TURNOVER_DEPARTMENTS = (
    "ГСПП",
    "Сектор сопровождения продаж",
    "Сектор сопровождения производства и продаж",
    "Сектор доработки и улучшения продукции",
    "Сектор постановки на производство несерийной продукции",
)

DEPARTMENTS = [
    DeptSpec("ГСПП", "e6cb6900-88cc-11ec-8801-ac1f6b05524d", structure_code="00-000113", structure_name="ГСПП"),
    DeptSpec("Сектор сопровождения продаж", "da45bdbd-f081-11f0-977e-6cb31113810e", structure_code="00-000158"),
    DeptSpec("Сектор сопровождения производства и продаж", "e9485e57-f081-11f0-977e-6cb31113810e", structure_code="00-000159"),
    DeptSpec("Сектор доработки и улучшения продукции", "50dda32e-f082-11f0-977e-6cb31113810e", structure_code="00-000160"),
    DeptSpec("Сектор постановки на производство несерийной продукции", "80cab7eb-f121-11f0-977f-6cb31113810c", structure_code="00-000162"),
]


def apply_gspp_q5_plan(payload: dict[str, Any], year: int, month: int) -> dict[str, Any]:
    """План из утверждённой таблицы (раньше top2 Document_ТД_ТекучестьПерсонала)."""
    for row in payload.get("monthly_data") or []:
        try:
            ry, rm = int(row["year"]), int(row["month"])
        except (KeyError, TypeError, ValueError):
            continue
        plan = plan_for_month(ry, rm)
        row["plan"] = plan
        row["plan_max_turnover_pct"] = plan
        row["has_data"] = plan is not None or row.get("fact") is not None
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
        ref["has_data"] = ref_plan is not None or ref.get("fact") is not None
        payload["last_full_month_row"] = ref
    ytd = payload.setdefault("ytd", {})
    ytd["total_plan"] = plan_for_month(year, month)
    debug = payload.setdefault("debug", {})
    debug["plan_source"] = "gspp.q5_tekuchest_plan"
    debug["metric"] = "plan_table_plus_sql_fact"
    return payload


def build_gspp_q5_payload(year: int | None = None, month: int | None = None) -> dict:
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
        source_module="gspp.gsp_q5.sql",
        target_departments=list(GSPP_TURNOVER_DEPARTMENTS),
        hierarchy_mode=HIERARCHY_MODE,
    )
    return apply_gspp_q5_plan(payload, year, month)


def main() -> None:
    try:
        run_turnover_cli(
            kpi_id=KPI_ID,
            title=TITLE,
            departments=DEPARTMENTS,
            script_dir=SCRIPT_DIR,
            file_prefix="gsp_q5",
            hierarchy_mode=HIERARCHY_MODE,
        )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

GSPP_Q5_CACHE_PREFIX = "gspp_q5_tekuchest"
GSPP_Q5_DISK_TAG = "gspp_q5_sql_payload_v3"
GSPP_Q5_DISK_VERSION = 3


def gspp_q5_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(GSPP_Q5_CACHE_PREFIX, ry, rm)


def get_gspp_q5_ytd(year: int | None = None, month: int | None = None) -> dict:
    ref_y, ref_m = normalize_period(year, month)
    payload = get_ytd_via_cache(
        year=ref_y,
        month=ref_m,
        cache_prefix=GSPP_Q5_CACHE_PREFIX,
        source_tag=GSPP_Q5_DISK_TAG,
        version=GSPP_Q5_DISK_VERSION,
        lock_key_prefix="gspp_q5_sql",
        compute_fn=lambda y, m: build_gspp_q5_payload(y, m),
        kpi_id="ГСП-Q5",
    )
    # Даже если на диске лежит старый fact-only кэш — план подставляем при отдаче.
    return apply_gspp_q5_plan(payload, ref_y, ref_m)
