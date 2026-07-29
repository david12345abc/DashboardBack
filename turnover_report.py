"""Общие CLI/форматирование отчётов текучести на SQL."""

from __future__ import annotations

import functools
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from hr_turnover_sql import (
    EXCLUDE_ATTR_GUID,
    MONTH_NAMES,
    DeptSpec,
    build_report,
)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)


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
    if now.month == 1:
        default = (now.year - 1, 12)
    else:
        default = (now.year, now.month - 1)

    if not args:
        return default, default, f"{default[0]:04d}-{default[1]:02d}"

    if len(args) == 2 and args[0].isdigit() and args[1].isdigit() and len(args[0]) == 4:
        period = (int(args[0]), int(args[1]))
        if not 1 <= period[1] <= 12:
            raise ValueError("Месяц должен быть от 1 до 12")
        return period, period, f"{period[0]:04d}-{period[1]:02d}"

    if len(args) == 1 and len(args[0]) == 4 and args[0].isdigit():
        year = int(args[0])
        end_m = default[1] if year == default[0] else 12
        if year == now.year and now.month == 1:
            end_m = 12
        elif year == now.year:
            end_m = now.month - 1 or 1
        return (year, 1), (year, end_m), str(year)

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


def format_month_table(snapshot: dict[str, Any], title: str, hierarchy_mode: str) -> str:
    lines = [
        f"{title} за {snapshot['month_name']} {snapshot['year']}",
        f"Дата среза штата: {snapshot['as_of']}",
        f"Период увольнений: {snapshot['period_start']} … {snapshot['period_end']}",
        f"Иерархия: {hierarchy_mode}",
        "Источник: SQL erp_pm",
        "",
        f"{'Код орг':<12} {'Подразделение':<55} {'Штат':>8} {'Увол.':>6} {'Примечание'}",
        f"{'-' * 12} {'-' * 55} {'-' * 8} {'-' * 6} {'-' * 30}",
    ]
    for row in snapshot["rows"]:
        code = row["org_code"] or row["structure_code"] or "—"
        name = row["org_name"] or row["structure_name"]
        lines.append(
            f"{code:<12} {name:<55} {row['staff_units']:>8.2f} {row['dismissed']:>6} {row['note']}"
        )
    lines.extend(
        [
            f"{'-' * 12} {'-' * 55} {'-' * 8} {'-' * 6} {'-' * 30}",
            f"{'ИТОГО':<12} {'':<55} {snapshot['total_staff_units']:>8.2f} "
            f"{snapshot['total_dismissed']:>6}",
            "",
            f"Текучесть = {snapshot['total_dismissed']} / {snapshot['total_staff_units']} "
            f"× 100% = {snapshot['turnover_pct']:.1f}%",
            "",
        ]
    )
    return "\n".join(lines)


def build_protocol(
    departments: list[dict],
    excluded_count: int,
    hierarchy_mode: str,
    sql_tables: dict[str, str],
) -> str:
    lines = [
        "=== Протокол метаданных (1С:ERP / SQL erp_pm) ===",
        f"ПодразделенияОрганизаций → {sql_tables['org']}",
        f"ШтатноеРасписание → {sql_tables['staffing']}",
        f"ИсторияИспользованияШР → {sql_tables['staff_history']}",
        f"КадроваяИсторияСотрудников → {sql_tables['hr_history']} (ВидСобытия=Увольнение)",
        f"Доп.реквизиты сотрудников → {sql_tables['employee_extra']}",
        f"Свойство НеУчитыватьПриТекучести = {EXCLUDE_ATTR_GUID}",
        f"Иерархия: {hierarchy_mode}",
        f"Сотрудников с НеУчитыватьПриТекучести=Истина: {excluded_count}",
        "",
        "Контур (орг. GUID):",
    ]
    for d in departments:
        lines.append(
            f"  • {d['group']} → org={d.get('org_key') or '—'} [{d.get('note') or 'ok'}]"
        )
    lines.append("")
    return "\n".join(lines)


def run_turnover_cli(
    *,
    kpi_id: str,
    title: str,
    departments: list[DeptSpec],
    script_dir: Path,
    file_prefix: str,
    hierarchy_mode: str = "listed_only_no_auto_children",
    build_dashboard_payload: Callable[..., dict[str, Any]] | None = None,
) -> None:
    start, end, slug = parse_period_args()
    log(f"[{kpi_id}] SQL-расчёт текучести ...")
    report = build_report(
        departments,
        start,
        end,
        hierarchy_mode=hierarchy_mode,
        kpi_label=kpi_id,
    )
    protocol = build_protocol(
        report["departments"],
        report["excluded_employees"],
        hierarchy_mode,
        report["sql_tables"],
    )
    report["protocol"] = protocol

    parts = [protocol]
    for snap in report["months"]:
        parts.append(format_month_table(snap, title, hierarchy_mode))
    if len(report["months"]) > 1:
        parts.append("Сводка по месяцам:")
        parts.append(f"{'Месяц':<10} {'Штат':>8} {'Увол.':>6} {'Текучесть %':>12}")
        parts.append(f"{'-' * 10} {'-' * 8} {'-' * 6} {'-' * 12}")
        for snap in report["months"]:
            parts.append(
                f"{snap['year']:04d}-{snap['month']:02d} "
                f"{snap['total_staff_units']:>8.2f} "
                f"{snap['total_dismissed']:>6} "
                f"{snap['turnover_pct']:>12.1f}"
            )
        parts.append("")
    text = "\n".join(parts)
    print(text)

    txt_path = script_dir / f"{file_prefix}_{slug}.txt"
    json_path = script_dir / f"{file_prefix}_{slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Отчёт сохранён: {txt_path}")
    print(f"JSON: {json_path}")


def apply_plan_table_to_turnover_payload(
    payload: dict[str, Any],
    year: int,
    month: int,
    *,
    plan_for_month,
    plan_source: str,
    require_plan_and_fact: bool = False,
) -> dict[str, Any]:
    """Подмешивает помесячный план в SQL fact-only payload текучести."""
    for row in payload.get("monthly_data") or []:
        try:
            ry, rm = int(row["year"]), int(row["month"])
        except (KeyError, TypeError, ValueError):
            continue
        plan = plan_for_month(ry, rm)
        row["plan"] = plan
        row["plan_max_turnover_pct"] = plan
        if require_plan_and_fact:
            row["has_data"] = plan is not None and row.get("fact") is not None
        else:
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
        if require_plan_and_fact:
            ref["has_data"] = ref_plan is not None and ref.get("fact") is not None
        else:
            ref["has_data"] = ref_plan is not None or ref.get("fact") is not None
        payload["last_full_month_row"] = ref
    ytd = payload.setdefault("ytd", {})
    ytd["total_plan"] = plan_for_month(year, month)
    debug = payload.setdefault("debug", {})
    debug["plan_source"] = plan_source
    debug["metric"] = "plan_table_plus_sql_fact"
    return payload


def dashboard_payload_from_report(
    report: dict[str, Any],
    *,
    kpi_id: str,
    year: int,
    month: int,
    source_module: str,
    target_departments: list[str],
    hierarchy_mode: str,
) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    for snap in report["months"]:
        monthly_rows.append(
            {
                "month": snap["month"],
                "year": snap["year"],
                "month_name": snap["month_name"],
                "plan": None,
                "fact": snap["turnover_pct"],
                "staff_units": snap["total_staff_units"],
                "dismissed": snap["total_dismissed"],
                "kpi_pct": snap["turnover_pct"],
                "has_data": True,
                "values_unit": "%",
                "by_department": snap["rows"],
            }
        )
    ref_row = monthly_rows[-1] if monthly_rows else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
        },
        "ytd": {
            "total_plan": None,
            "total_fact": ref_row["fact"] if ref_row else None,
            "kpi_pct": ref_row["kpi_pct"] if ref_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "%",
        },
        "debug": {
            "kpi_id": kpi_id,
            "status": "ok",
            "source": source_module,
            "metric": "fact_only",
            "fact_source": "hr_staff_dismissals_turnover_pct",
            "target_departments": target_departments,
            "hierarchy_mode": hierarchy_mode,
            "excluded_employees": report["excluded_employees"],
            "sql_tables": report.get("sql_tables"),
        },
    }
