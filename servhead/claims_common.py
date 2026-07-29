"""Общая SQL-логика претензий (Catalog_Претензии) для плиток SH-M*."""

from __future__ import annotations

import functools
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

CLAIMS_TABLE = "_Reference389"
ENUM_TABLE = "_Enum1688"

COL_MARKED = "_Marked"
COL_DATE_REG = "_Fld11617"          # ДатаРегистрации
COL_DATE_FACT = "_Fld11618"         # ДатаОкончания
COL_DATE_PLAN = "_Fld132055"        # ТД_ДатаОкончанияПлан
COL_STATUS = "_Fld11625RRef"

FIELD_DATE_FACT = "ДатаОкончания"
FIELD_DATE_PLAN = "ТД_ДатаОкончанияПлан"

# _Enum1688._EnumOrder → имя статуса (OData Статус / PredefinedDataName)
STATUS_BY_ORDER: dict[int, str] = {
    0: "Зарегистрирована",
    1: "Обрабатывается",
    2: "На контроле",
    3: "Удовлетворена",
    4: "НеУдовлетворена",
}

STATUS_SATISFIED = "Удовлетворена"
STATUS_REGISTERED = "Зарегистрирована"
# OData/DashboardBack: «Обрабатывается» и алиас «В обработке»
STATUS_IN_PROCESS = frozenset({"Обрабатывается", "В обработке"})

YEAR_OFFSET = 2000
SCRIPT_DIR = Path(__file__).resolve().parent
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
    now = datetime.now()

    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)

    if len(args) == 1 and len(args[0]) == 4:
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

    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ или ГГГГ-ММ ГГГГ-ММ")


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[str]:
    year, month = start
    result: list[str] = []
    while (year, month) <= end:
        result.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return result


def to_sql_dt(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(year=value.year + YEAR_OFFSET)
    return datetime(value.year + YEAR_OFFSET, value.month, value.day)


def kpi_pct(plan: int, fact: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100.0, 2)


def load_status_bins(cur) -> dict[str, bytes]:
    """Имя статуса → binary из _Enum1688 (по _EnumOrder)."""
    cur.execute(
        f"""
        SELECT _IDRRef, _EnumOrder
        FROM [{ENUM_TABLE}] WITH (NOLOCK)
        """
    )
    result: dict[str, bytes] = {}
    for idr, order in cur.fetchall():
        name = STATUS_BY_ORDER.get(int(order))
        if name:
            result[name] = bytes(idr)
    return result


def resolve_fact_bins(status_bins: dict[str, bytes], fact_statuses: frozenset[str]) -> set[bytes]:
    bins: set[bytes] = set()
    for name in fact_statuses:
        blob = status_bins.get(name)
        if blob is not None:
            bins.add(blob)
    if not bins:
        raise RuntimeError(
            f"Не найдены статусы {sorted(fact_statuses)} в {ENUM_TABLE}; "
            f"доступны: {sorted(status_bins)}"
        )
    return bins


def sql_to_date(raw: datetime | date | None) -> date | None:
    """SQL datetime (+2000) → календарная date; пустые даты 1С отбрасываются."""
    if raw is None:
        return None
    year = raw.year - YEAR_OFFSET
    if year < 1900:
        return None
    try:
        return date(year, raw.month, raw.day)
    except ValueError:
        return None


def is_completed_on_time(fact_d: date | None, plan_d: date | None) -> bool:
    """Факт исполнения ≤ плановой даты окончания (как в DashboardBack)."""
    if fact_d is None or plan_d is None:
        return False
    return fact_d <= plan_d


def is_completed_late(fact_d: date | None, plan_d: date | None) -> bool:
    """Факт исполнения > плановой даты окончания (как в DashboardBack)."""
    if fact_d is None or plan_d is None:
        return False
    return fact_d > plan_d


def load_monthly_counts(
    cur,
    start_dt: date,
    end_dt: date,
    fact_bins: set[bytes],
) -> dict[str, dict[str, int]]:
    sql_start = to_sql_dt(start_dt)
    sql_end_exclusive = to_sql_dt(end_dt + timedelta(days=1))
    cur.execute(
        f"""
        SELECT
            [{COL_DATE_REG}],
            [{COL_STATUS}]
        FROM [{CLAIMS_TABLE}] WITH (NOLOCK)
        WHERE [{COL_MARKED}] = 0x00
          AND [{COL_DATE_REG}] >= ?
          AND [{COL_DATE_REG}] < ?
        """,
        sql_start,
        sql_end_exclusive,
    )

    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"plan": 0, "fact": 0})
    for date_raw, status_bin in cur.fetchall():
        if date_raw is None:
            continue
        year = date_raw.year - YEAR_OFFSET
        month = date_raw.month
        if year < 1:
            continue
        month_key = f"{year:04d}-{month:02d}"
        stats[month_key]["plan"] += 1
        if status_bin is not None and bytes(status_bin) in fact_bins:
            stats[month_key]["fact"] += 1
    return stats


def load_monthly_sla_counts(
    cur,
    start_dt: date,
    end_dt: date,
    *,
    late: bool,
) -> dict[str, dict[str, int]]:
    """План — все обращения; факт — в срок (late=False) или не в срок (late=True)."""
    sql_start = to_sql_dt(start_dt)
    sql_end_exclusive = to_sql_dt(end_dt + timedelta(days=1))
    cur.execute(
        f"""
        SELECT
            [{COL_DATE_REG}],
            [{COL_DATE_FACT}],
            [{COL_DATE_PLAN}]
        FROM [{CLAIMS_TABLE}] WITH (NOLOCK)
        WHERE [{COL_MARKED}] = 0x00
          AND [{COL_DATE_REG}] >= ?
          AND [{COL_DATE_REG}] < ?
        """,
        sql_start,
        sql_end_exclusive,
    )

    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"plan": 0, "fact": 0})
    for reg_raw, fact_raw, plan_raw in cur.fetchall():
        reg_d = sql_to_date(reg_raw)
        if reg_d is None:
            continue
        month_key = f"{reg_d.year:04d}-{reg_d.month:02d}"
        stats[month_key]["plan"] += 1
        fact_d = sql_to_date(fact_raw)
        plan_d = sql_to_date(plan_raw)
        matched = is_completed_late(fact_d, plan_d) if late else is_completed_on_time(fact_d, plan_d)
        if matched:
            stats[month_key]["fact"] += 1
    return stats


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
    fact_statuses: frozenset[str],
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        status_bins = load_status_bins(cur)
        fact_bins = resolve_fact_bins(status_bins, fact_statuses)
        stats = load_monthly_counts(
            cur,
            month_start(*start_period),
            month_end(*end_period),
            fact_bins,
        )

    report_rows: list[dict[str, Any]] = []
    for month_key in iter_months(start_period, end_period):
        plan = int(stats[month_key]["plan"])
        fact = int(stats[month_key]["fact"])
        report_rows.append(
            {
                "month": month_key,
                "plan": plan,
                "fact": fact,
                "kpi_pct": kpi_pct(plan, fact),
                "has_data": plan > 0,
            }
        )
    return report_rows


def build_monthly_sla_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
    *,
    late: bool,
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        stats = load_monthly_sla_counts(
            cur,
            month_start(*start_period),
            month_end(*end_period),
            late=late,
        )

    report_rows: list[dict[str, Any]] = []
    for month_key in iter_months(start_period, end_period):
        plan = int(stats[month_key]["plan"])
        fact = int(stats[month_key]["fact"])
        report_rows.append(
            {
                "month": month_key,
                "plan": plan,
                "fact": fact,
                "kpi_pct": kpi_pct(plan, fact),
                "has_data": plan > 0,
            }
        )
    return report_rows


def format_report(
    rows: list[dict[str, Any]],
    *,
    title: str,
    fact_label: str,
    source_line: str | None = None,
) -> str:
    lines = [
        title,
        source_line or f"Источник: {CLAIMS_TABLE} + {ENUM_TABLE}",
        f"План: все обращения за месяц по {COL_DATE_REG} (ДатаРегистрации)",
        f"Факт: {fact_label}",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        pct = row["kpi_pct"]
        pct_s = f"{pct:.2f}" if pct is not None else "—"
        lines.append(
            f"{row['month']:<10} "
            f"{row['plan']:>8} "
            f"{row['fact']:>8} "
            f"{pct_s:>8}"
        )
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8}",
            f"{'ИТОГО':<10} "
            f"{sum(row['plan'] for row in rows):>8} "
            f"{sum(row['fact'] for row in rows):>8} "
            f"{'':>8}",
            "",
        ]
    )
    return "\n".join(lines)


def save_report(file_prefix: str, period_slug: str, report: str) -> Path:
    output_path = SCRIPT_DIR / f"{file_prefix}_{period_slug}.txt"
    output_path.write_text(report, encoding="utf-8-sig")
    return output_path


def _payload_from_rows(
    rows: list[dict[str, Any]],
    *,
    kpi_id: str,
    source_module: str,
    ref_y: int,
    ref_m: int,
    debug_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        month_num = int(str(row["month"])[5:7])
        plan = int(row["plan"])
        fact = int(row["fact"])
        monthly_rows.append(
            {
                "month": month_num,
                "year": ref_y,
                "month_name": MONTH_NAMES[month_num].lower(),
                "plan": plan,
                "fact": fact,
                "kpi_pct": kpi_pct(plan, fact),
                "has_data": plan > 0,
                "values_unit": "шт.",
            }
        )

    ref_row = next(
        (item for item in monthly_rows if item["month"] == ref_m),
        monthly_rows[-1] if monthly_rows else {
            "month": ref_m,
            "year": ref_y,
            "month_name": MONTH_NAMES[ref_m].lower(),
            "plan": 0,
            "fact": 0,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": "шт.",
        },
    )
    today = date.today()
    period_type = (
        "current_month"
        if (ref_y, ref_m) >= (today.year, today.month)
        else "last_full_month"
    )
    debug: dict[str, Any] = {
        "kpi_id": kpi_id,
        "status": "ok",
        "source": source_module,
        "odata_entity": "Catalog_Претензии",
        "tables": {
            "claims": CLAIMS_TABLE,
            "date_reg_col": COL_DATE_REG,
            "date_fact_col": COL_DATE_FACT,
            "date_plan_col": COL_DATE_PLAN,
            "status_col": COL_STATUS,
            "status_enum": ENUM_TABLE,
        },
        "rows_by_month": [
            {"month": item["month"], "plan": item["plan"], "fact": item["fact"]}
            for item in monthly_rows
        ],
    }
    if debug_extra:
        debug.update(debug_extra)
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": period_type,
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m].lower(),
        },
        "ytd": {
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": debug,
    }


def build_claims_status_payload(
    *,
    kpi_id: str,
    fact_statuses: frozenset[str],
    source_module: str,
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    now = datetime.now()
    ref_y = year or now.year
    ref_m = month or now.month
    if year is None and month is None:
        ref_y, ref_m = now.year, now.month

    rows = build_monthly_report((ref_y, 1), (ref_y, ref_m), fact_statuses)
    status_list = sorted(fact_statuses)
    return _payload_from_rows(
        rows,
        kpi_id=kpi_id,
        source_module=source_module,
        ref_y=ref_y,
        ref_m=ref_m,
        debug_extra={
            "fact_statuses": status_list,
            "rule": (
                "plan = all claims in month by ДатаРегистрации; "
                f"fact = status in {status_list}"
            ),
        },
    )


def build_claims_sla_payload(
    *,
    kpi_id: str,
    late: bool,
    source_module: str,
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    now = datetime.now()
    ref_y = year or now.year
    ref_m = month or now.month
    if year is None and month is None:
        ref_y, ref_m = now.year, now.month

    rows = build_monthly_sla_report((ref_y, 1), (ref_y, ref_m), late=late)
    op = ">" if late else "<="
    return _payload_from_rows(
        rows,
        kpi_id=kpi_id,
        source_module=source_module,
        ref_y=ref_y,
        ref_m=ref_m,
        debug_extra={
            "date_fact_field": FIELD_DATE_FACT,
            "date_plan_field": FIELD_DATE_PLAN,
            "rule": (
                "plan = all claims in month by ДатаРегистрации; "
                f"fact = {FIELD_DATE_FACT} {op} {FIELD_DATE_PLAN}"
            ),
        },
    )


def run_cli(
    *,
    kpi_id: str,
    file_prefix: str,
    title: str,
    fact_label: str,
    fact_statuses: frozenset[str],
) -> None:
    start_period, end_period, period_slug = parse_period_args()
    rows = build_monthly_report(start_period, end_period, fact_statuses)
    report = format_report(
        rows,
        title=title,
        fact_label=f"{fact_label} ({COL_STATUS})",
    )
    print(report)
    output_path = save_report(file_prefix, period_slug, report)
    print(f"Отчёт сохранён: {output_path}")


def run_sla_cli(
    *,
    kpi_id: str,
    file_prefix: str,
    title: str,
    late: bool,
) -> None:
    start_period, end_period, period_slug = parse_period_args()
    rows = build_monthly_sla_report(start_period, end_period, late=late)
    op = ">" if late else "≤"
    report = format_report(
        rows,
        title=title,
        source_line=f"Источник: {CLAIMS_TABLE}",
        fact_label=(
            f"{FIELD_DATE_FACT} {op} {FIELD_DATE_PLAN} "
            f"({COL_DATE_FACT} {op} {COL_DATE_PLAN})"
        ),
    )
    print(report)
    output_path = save_report(file_prefix, period_slug, report)
    print(f"Отчёт сохранён: {output_path}")
