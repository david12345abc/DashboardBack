"""
QD-M6 — предъявления продукции на входной контроль (все организации).

Эталон (OData): DashboardBack/qualdir/qd_m6.py
  → qualdir.otk_predyavlenie_data.compute_otk_predyavlenie_month
  Document_ТД_ПредъявлениеТМЦнаОТК

Логика за месяц (по Date документа):
  plan     — DeletionMark = false, Posted = true;
  fact     — из plan с заполненной ДатаПроверкиОТК;
  rejected — строки ТЧ ТоварыДляОТК с НеПринятоОТК > 0
             или заполненным АктОТКоНесоответствии;
  in_work  — документы со СрокИсполнения = сегодня (на дату запуска).

SQL (erp_pm):
  Document_ТД_ПредъявлениеТМЦнаОТК → dbo._Document122024
    _Date_Time                     — Date (+2000 лет)
    _Marked / _Posted              — DeletionMark / Posted
    _Fld122581                     — ДатаПроверкиОТК
    _Fld122490                     — СрокИсполнения
    _Fld122026RRef                 — Организация
  ТЧ ТоварыДляОТК                  → dbo._Document122024_VT122491
    _Fld122580                     — НеПринятоОТК
    _Fld122498RRef                 — АктОТКоНесоответствии

Даты в SQL хранятся со смещением YEAR_OFFSET = 2000.

Использование:
  python qualdir/qd_m6.py
  python qualdir/qd_m6.py 2026
  python qualdir/qd_m6.py 2026-01
  python qualdir/qd_m6.py 2026-01 2026-05
"""

from __future__ import annotations

import functools
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

DOC_TABLE = "_Document122024"
VT_TABLE = "_Document122024_VT122491"
VT_DOC_COL = "_Document122024_IDRRef"

COL_DATE = "_Date_Time"
COL_MARKED = "_Marked"
COL_POSTED = "_Posted"
COL_CHECKED = "_Fld122581"
COL_DEADLINE = "_Fld122490"
COL_ORG = "_Fld122026RRef"
COL_REJECTED_QTY = "_Fld122580"
COL_REJECT_ACT = "_Fld122498RRef"

YEAR_OFFSET = 2000
EMPTY_SQL_YEAR = 2001
EMPTY_BIN = bytes(16)
SCRIPT_DIR = Path(__file__).resolve().parent

# Организации (OData Ref_Key → 1C binary в _Fld122026RRef)
ORG_NPO_KEY = "fbca2148-6cfd-11e7-812d-001e67112509"
ORG_ALMAZ_KEY = "fbca2146-6cfd-11e7-812d-001e67112509"
ORG_NPO_NAME = "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО"
ORG_ALMAZ_NAME = "АЛМАЗ ООО"


def guid_to_1c(guid_str: str) -> bytes:
    b = uuid.UUID(guid_str).bytes
    return b[8:16] + b[6:8] + b[4:6] + b[0:4]


ORG_NPO_BIN = guid_to_1c(ORG_NPO_KEY)
ORG_ALMAZ_BIN = guid_to_1c(ORG_ALMAZ_KEY)

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


def to_sql_dt(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(year=value.year + YEAR_OFFSET)
    return datetime(value.year + YEAR_OFFSET, value.month, value.day)


def month_bounds_sql(year: int, month: int) -> tuple[datetime, datetime]:
    start = to_sql_dt(date(year, month, 1))
    if month == 12:
        end = to_sql_dt(date(year + 1, 1, 1))
    else:
        end = to_sql_dt(date(year, month + 1, 1))
    return start, end


def kpi_pct(plan: int | None, fact: int | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(fact / plan * 100.0, 1)


def calc_month(
    year: int,
    month: int,
    *,
    sql: SqlConnection | None = None,
    organization_bin: bytes | None = None,
) -> dict[str, Any]:
    """Метрики предъявлений на ОТК за календарный месяц."""
    sql = sql or SqlConnection()
    start, end = month_bounds_sql(year, month)
    empty_dt = datetime(EMPTY_SQL_YEAR, 1, 1)

    org_clause = ""
    params: list[Any] = [empty_dt, start, end]
    if organization_bin is not None:
        org_clause = f" AND doc.[{COL_ORG}] = ?"
        params.append(organization_bin)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              COUNT(*) AS plan_cnt,
              SUM(CASE WHEN doc.[{COL_CHECKED}] > ? THEN 1 ELSE 0 END) AS fact_cnt
            FROM [{DOC_TABLE}] doc WITH (NOLOCK)
            WHERE doc.[{COL_MARKED}] = 0x00
              AND doc.[{COL_POSTED}] = 0x01
              AND doc.[{COL_DATE}] >= ?
              AND doc.[{COL_DATE}] < ?
              {org_clause}
            """,
            params,
        )
        plan_cnt, fact_cnt = cur.fetchone()
        plan = int(plan_cnt or 0)
        fact = int(fact_cnt or 0)

        rej_params: list[Any] = [start, end, EMPTY_BIN]
        if organization_bin is not None:
            rej_params.append(organization_bin)
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM [{DOC_TABLE}] doc WITH (NOLOCK)
            JOIN [{VT_TABLE}] vt WITH (NOLOCK)
              ON vt.[{VT_DOC_COL}] = doc._IDRRef
            WHERE doc.[{COL_MARKED}] = 0x00
              AND doc.[{COL_POSTED}] = 0x01
              AND doc.[{COL_DATE}] >= ?
              AND doc.[{COL_DATE}] < ?
              AND (
                vt.[{COL_REJECTED_QTY}] > 0
                OR (vt.[{COL_REJECT_ACT}] IS NOT NULL AND vt.[{COL_REJECT_ACT}] <> ?)
              )
              {org_clause}
            """,
            rej_params,
        )
        rejected = int(cur.fetchone()[0] or 0)

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "plan": plan,
        "fact": fact,
        "docs_count": plan,
        "executed_count": fact,
        "rejected_items_count": rejected,
        "kpi_pct": kpi_pct(plan, fact),
        "has_data": True,
        "values_unit": "шт.",
    }


def calc_in_work_today(
    *,
    as_of: date | None = None,
    sql: SqlConnection | None = None,
    organization_bin: bytes | None = None,
) -> int:
    """Документы со сроком исполнения = as_of (по умолчанию сегодня)."""
    sql = sql or SqlConnection()
    day = as_of or date.today()
    start = to_sql_dt(day)
    end = to_sql_dt(day + timedelta(days=1))
    params: list[Any] = [start, end]
    org_clause = ""
    if organization_bin is not None:
        org_clause = f" AND [{COL_ORG}] = ?"
        params.append(organization_bin)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM [{DOC_TABLE}] WITH (NOLOCK)
            WHERE [{COL_MARKED}] = 0x00
              AND [{COL_POSTED}] = 0x01
              AND [{COL_DEADLINE}] >= ?
              AND [{COL_DEADLINE}] < ?
              {org_clause}
            """,
            params,
        )
        return int(cur.fetchone()[0] or 0)


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
    *,
    organization_bin: bytes | None = None,
) -> tuple[list[dict[str, Any]], int]:
    sql = SqlConnection()
    rows = [
        calc_month(year, month, sql=sql, organization_bin=organization_bin)
        for year, month in iter_months(start_period, end_period)
    ]
    in_work = calc_in_work_today(sql=sql, organization_bin=organization_bin)
    return rows, in_work


def format_report(
    rows: list[dict[str, Any]],
    in_work_today: int,
    *,
    kpi_id: str = "QD-M6",
    organization_name: str | None = None,
) -> str:
    org_label = organization_name or "все организации"
    lines = [
        f"Предъявления продукции на входной контроль ({kpi_id} / SQL)",
        f"Источник: {DOC_TABLE} + {VT_TABLE}, {org_label}",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'Брак':>8} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        pct = row["kpi_pct"]
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{row['plan']:>8} "
            f"{row['fact']:>8} "
            f"{row['rejected_items_count']:>8} "
            f"{pct_s:>8}"
        )
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}",
            f"{'ИТОГО':<10} "
            f"{sum(row['plan'] for row in rows):>8} "
            f"{sum(row['fact'] for row in rows):>8} "
            f"{sum(row['rejected_items_count'] for row in rows):>8} "
            f"{'':>8}",
            "",
            f"В работе сегодня (СрокИсполнения = {date.today().isoformat()}): {in_work_today}",
            "",
        ]
    )
    return "\n".join(lines)


def save_report(
    period_slug: str,
    rows: list[dict[str, Any]],
    in_work_today: int,
    *,
    file_prefix: str = "qd_m6",
    kpi_id: str = "QD-M6",
    organization_name: str | None = None,
) -> Path:
    output_path = SCRIPT_DIR / f"{file_prefix}_{period_slug}.txt"
    output_path.write_text(
        format_report(
            rows,
            in_work_today,
            kpi_id=kpi_id,
            organization_name=organization_name,
        ),
        encoding="utf-8-sig",
    )
    return output_path


def build_otk_payload(
    year: int | None = None,
    month: int | None = None,
    *,
    kpi_id: str = "QD-M6",
    organization_bin: bytes | None = None,
    organization_key: str | None = None,
    organization_name: str | None = None,
    source_module: str = "qualdir.qd_m6.sql",
) -> dict[str, Any]:
    now = date.today()
    ref_y = year or now.year
    ref_m = month or (now.month - 1 if now.month > 1 else 12)
    if month is None and now.month == 1 and year is None:
        ref_y = now.year - 1
        ref_m = 12

    rows, in_work = build_monthly_report(
        (ref_y, 1),
        (ref_y, ref_m),
        organization_bin=organization_bin,
    )
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        item = {
            "month": row["month"],
            "year": row["year"],
            "month_name": MONTH_NAMES[row["month"]].lower(),
            "plan": row["plan"],
            "fact": row["fact"],
            "docs_count": row["docs_count"],
            "executed_count": row["executed_count"],
            "rejected_items_count": row["rejected_items_count"],
            "kpi_pct": row["kpi_pct"],
            "has_data": True,
            "values_unit": "шт.",
            "organization_key": organization_key,
            "organization_name": organization_name,
        }
        monthly_rows.append(item)

    ref_row = next(
        (item for item in monthly_rows if item["month"] == ref_m),
        monthly_rows[-1] if monthly_rows else None,
    )
    if ref_row is not None:
        ref_row = dict(ref_row)
        ref_row["in_work_today"] = in_work

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
            "data_complete": bool(ref_row and ref_row.get("has_data")),
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "rejected_items_count": ref_row.get("rejected_items_count") if ref_row else None,
            "in_work_today": in_work,
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "status": "ok",
            "kpi_id": kpi_id,
            "source": source_module,
            "odata_entity": "Document_ТД_ПредъявлениеТМЦнаОТК",
            "organization_key": organization_key,
            "organization_name": organization_name,
            "tables": {
                "document": DOC_TABLE,
                "tabular": VT_TABLE,
                "date_col": COL_DATE,
                "checked_col": COL_CHECKED,
                "deadline_col": COL_DEADLINE,
                "org_col": COL_ORG,
                "rejected_qty_col": COL_REJECTED_QTY,
                "reject_act_col": COL_REJECT_ACT,
            },
            "rule": (
                "plan = Posted docs by Date"
                + (f", Организация = {organization_name}" if organization_name else ", все организации")
                + "; fact = plan with ДатаПроверкиОТК filled; "
                "rejected = VT lines with НеПринятоОТК>0 or act filled"
            ),
            "in_work_today": in_work,
            "rows_by_month": rows,
        },
    }


def build_qd_m6_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    return build_otk_payload(year, month, kpi_id="QD-M6", source_module="qualdir.qd_m6.sql")


def run_cli(
    *,
    kpi_id: str = "QD-M6",
    file_prefix: str = "qd_m6",
    organization_bin: bytes | None = None,
    organization_name: str | None = None,
) -> None:
    start_period, end_period, period_slug = parse_period_args()
    rows, in_work = build_monthly_report(
        start_period,
        end_period,
        organization_bin=organization_bin,
    )
    report = format_report(
        rows,
        in_work,
        kpi_id=kpi_id,
        organization_name=organization_name,
    )
    print(report)
    output_path = save_report(
        period_slug,
        rows,
        in_work,
        file_prefix=file_prefix,
        kpi_id=kpi_id,
        organization_name=organization_name,
    )
    print(f"Отчёт сохранён: {output_path}")


def main() -> None:
    try:
        run_cli()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

QD_M6_YTD_CACHE_PREFIX = "qualdir_qd_m6_ytd"
QD_M6_YTD_DISK_TAG = "qualdir_qd_m6_ytd_payload_sql_v1"
QD_M6_YTD_DISK_VERSION = 10


def otk_predyavlenie_month_cache_path(year: int, month: int) -> _Path:
    return month_cache_path("qualdir_otk_predyavlenie_sql_all", year, month)


def legacy_otk_predyavlenie_month_cache_path(year: int, month: int) -> _Path | None:
    return None


def qd_m6_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M6_YTD_CACHE_PREFIX, ry, rm)


def qd_m6_tile_cache_path(year: int, month: int) -> _Path:
    return qd_m6_ytd_cache_path(year, month)


def compute_qd_m6_month(year: int, month: int) -> dict:
    return calc_month(year, month, organization_bin=None)


def get_qd_m6_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M6_YTD_CACHE_PREFIX,
        source_tag=QD_M6_YTD_DISK_TAG,
        version=QD_M6_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m6_sql",
        compute_fn=lambda y, m: build_qd_m6_payload(y, m),
        kpi_id="QD-M6",
    )
