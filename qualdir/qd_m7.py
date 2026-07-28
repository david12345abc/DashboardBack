"""
QD-M7 — предъявления продукции на выходной контроль.

Эталон (OData): DashboardBack/qualdir/qd_m7.py
  → qualdir.vyhod_kontrol_data
  Document_ТД_ПредъявлениеПродукцииНаВыходнойКонтроль

Логика:
  fact (месяц)              — число документов по Date, DeletionMark = false;
  accepted_to_work_today    — ДатаПринятоВРаботу = сегодня (или --as-of);
  checked_otk_today         — ДатаПроверкиОТК = сегодня (или --as-of).

  План / KPI % на плитке нет (fact-only).

SQL (erp_pm):
  Document_ТД_ПредъявлениеПродукцииНаВыходнойКонтроль
      → dbo._Document140962
        _Date_Time   = Date (+2000 лет)
        _Marked      = DeletionMark (0x00 = нет)
        _Fld141048   = ДатаПринятоВРаботу
        _Fld144091   = ДатаПроверкиОТК

Даты в SQL хранятся со смещением YEAR_OFFSET = 2000
(2026-03-01 → 4026-03-01; пустая дата 0001-01-01 → 2001-01-01).

Использование:
  python qualdir/qd_m7.py
  python qualdir/qd_m7.py 2026
  python qualdir/qd_m7.py 2026-03
  python qualdir/qd_m7.py 2026-01 2026-06
  python qualdir/qd_m7.py 2026-06 --as-of 2026-07-21
"""

from __future__ import annotations

import functools
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

DOC_TABLE = "_Document140962"
COL_DATE = "_Date_Time"
COL_MARKED = "_Marked"
COL_ACCEPTED = "_Fld141048"
COL_CHECKED = "_Fld144091"

YEAR_OFFSET = 2000
EMPTY_SQL_YEAR = 2001
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


def parse_day(value: str) -> date:
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise ValueError("Дата должна быть в формате ГГГГ-ММ-ДД")
    return date(int(value[:4]), int(value[5:7]), int(value[8:10]))


def parse_cli_args(
    argv: list[str] | None = None,
) -> tuple[tuple[int, int], tuple[int, int], str, date]:
    raw = list(argv if argv is not None else sys.argv[1:])
    as_of = date.today()
    if "--as-of" in raw:
        idx = raw.index("--as-of")
        if idx + 1 >= len(raw):
            raise ValueError("После --as-of укажите дату ГГГГ-ММ-ДД")
        as_of = parse_day(raw[idx + 1])
        del raw[idx : idx + 2]

    args = [a.strip() for a in raw if a.strip()]
    now = datetime.now()
    if not args:
        start, end, slug = (now.year, 1), (now.year, 12), str(now.year)
    elif len(args) == 1 and len(args[0]) == 4:
        year = int(args[0])
        start, end, slug = (year, 1), (year, 12), args[0]
    elif len(args) == 1:
        period = parse_month(args[0])
        start, end, slug = period, period, args[0]
    elif len(args) == 2:
        start = parse_month(args[0])
        end = parse_month(args[1])
        if start > end:
            raise ValueError("Дата начала должна быть не позже даты окончания")
        slug = f"{args[0]}_{args[1]}"
    else:
        raise ValueError("Используйте: ГГГГ, ГГГГ-ММ или ГГГГ-ММ ГГГГ-ММ [--as-of ГГГГ-ММ-ДД]")
    return start, end, slug, as_of


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


def from_sql_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.year <= EMPTY_SQL_YEAR:
        return None
    return value.replace(year=value.year - YEAR_OFFSET)


def count_docs_by_date(cur, start_dt: date, end_dt: date) -> dict[str, int]:
    """Счётчик документов по Date в разрезе месяцев."""
    sql_start = to_sql_dt(start_dt)
    sql_end_exclusive = to_sql_dt(end_dt + timedelta(days=1))
    cur.execute(
        f"""
        SELECT [{COL_DATE}]
        FROM [{DOC_TABLE}] WITH (NOLOCK)
        WHERE [{COL_MARKED}] = 0x00
          AND [{COL_DATE}] >= ?
          AND [{COL_DATE}] < ?
        """,
        sql_start,
        sql_end_exclusive,
    )
    stats: dict[str, int] = {}
    for (date_raw,) in cur.fetchall():
        dt = from_sql_dt(date_raw)
        if dt is None:
            continue
        key = f"{dt.year:04d}-{dt.month:02d}"
        stats[key] = stats.get(key, 0) + 1
    return stats


def count_by_field_day(cur, field: str, day: date) -> int:
    sql_start = to_sql_dt(day)
    sql_end = to_sql_dt(day + timedelta(days=1))
    cur.execute(
        f"""
        SELECT COUNT_BIG(*)
        FROM [{DOC_TABLE}] WITH (NOLOCK)
        WHERE [{COL_MARKED}] = 0x00
          AND [{field}] >= ?
          AND [{field}] < ?
        """,
        sql_start,
        sql_end,
    )
    return int(cur.fetchone()[0] or 0)


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
    *,
    as_of: date | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    as_of = as_of or date.today()
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        stats = count_docs_by_date(
            cur,
            month_start(*start_period),
            month_end(*end_period),
        )
        accepted = count_by_field_day(cur, COL_ACCEPTED, as_of)
        checked = count_by_field_day(cur, COL_CHECKED, as_of)

    rows: list[dict[str, Any]] = []
    for month_key in iter_months(start_period, end_period):
        fact = int(stats.get(month_key, 0))
        year = int(month_key[:4])
        month = int(month_key[5:7])
        rows.append(
            {
                "month": month_key,
                "year": year,
                "month_num": month,
                "month_name": MONTH_NAMES[month],
                "plan": None,
                "fact": fact,
                "kpi_pct": None,
                "has_data": True,
                "values_unit": "шт.",
            }
        )

    extras = {
        "as_of": as_of.isoformat(),
        "accepted_to_work_today": accepted,
        "checked_otk_today": checked,
    }
    return rows, extras


def format_report(rows: list[dict[str, Any]], extras: dict[str, Any]) -> str:
    lines = [
        "Предъявления продукции на выходной контроль (QD-M7 / SQL)",
        f"Источник: {DOC_TABLE}",
        f"На дату {extras['as_of']}: "
        f"принято в работу = {extras['accepted_to_work_today']}, "
        f"проверено ОТК = {extras['checked_otk_today']}",
        "",
        f"{'Месяц':<10} {'Факт':>8}",
        f"{'-' * 10} {'-' * 8}",
    ]
    for row in rows:
        lines.append(f"{row['month']:<10} {row['fact']:>8}")
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8}",
            f"{'ИТОГО':<10} {sum(int(r['fact']) for r in rows):>8}",
            "",
        ]
    )
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]], extras: dict[str, Any]) -> Path:
    output_path = SCRIPT_DIR / f"qd_m7_{period_slug}.txt"
    output_path.write_text(format_report(rows, extras), encoding="utf-8-sig")
    return output_path


def build_qd_m7_payload(
    year: int | None = None,
    month: int | None = None,
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    now = datetime.now()
    ref_y = year or now.year
    ref_m = month or (now.month - 1 if now.month > 1 else 12)
    if month is None and now.month == 1 and year is None:
        ref_y = now.year - 1
        ref_m = 12
    as_of = as_of or date.today()

    rows, extras = build_monthly_report((ref_y, 1), (ref_y, ref_m), as_of=as_of)
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        monthly_rows.append(
            {
                "month": row["month_num"],
                "year": row["year"],
                "month_name": row["month_name"].lower(),
                "plan": None,
                "fact": int(row["fact"]),
                "kpi_pct": None,
                "has_data": True,
                "values_unit": "шт.",
            }
        )

    ref_row = next(
        (item for item in monthly_rows if item["month"] == ref_m),
        monthly_rows[-1] if monthly_rows else None,
    )
    if ref_row is not None:
        ref_row = dict(ref_row)
        ref_row["accepted_to_work_today"] = extras["accepted_to_work_today"]
        ref_row["checked_otk_today"] = extras["checked_otk_today"]

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
            "total_plan": None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "accepted_to_work_today": extras["accepted_to_work_today"],
            "checked_otk_today": extras["checked_otk_today"],
            "kpi_pct": None,
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            **({"values_unit": "шт."} if ref_row and ref_row.get("has_data") else {}),
        },
        "debug": {
            "kpi_id": "QD-M7",
            "status": "ok",
            "source": "qualdir.qd_m7.sql",
            "odata_entity": "Document_ТД_ПредъявлениеПродукцииНаВыходнойКонтроль",
            "as_of": extras["as_of"],
            "tables": {
                "document": DOC_TABLE,
                "date_col": COL_DATE,
                "accepted_col": COL_ACCEPTED,
                "checked_col": COL_CHECKED,
                "marked_col": COL_MARKED,
            },
            "rule": (
                "fact = documents with Date in month, DeletionMark=false; "
                "accepted_to_work_today = ДатаПринятоВРаботу on as_of; "
                "checked_otk_today = ДатаПроверкиОТК on as_of"
            ),
            "rows_by_month": rows,
        },
    }


def main() -> None:
    try:
        start_period, end_period, period_slug, as_of = parse_cli_args()
        rows, extras = build_monthly_report(start_period, end_period, as_of=as_of)
        report = format_report(rows, extras)
        print(report)
        output_path = save_report(period_slug, rows, extras)
        print(f"Отчёт сохранён: {output_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

QD_M7_YTD_CACHE_PREFIX = "qualdir_qd_m7_ytd"
QD_M7_YTD_DISK_TAG = "qualdir_qd_m7_ytd_payload_sql_v1"
QD_M7_YTD_DISK_VERSION = 10


def vyhod_kontrol_month_cache_path(year: int, month: int) -> _Path:
    return month_cache_path("qualdir_vyhod_kontrol_sql", year, month)


def qd_m7_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M7_YTD_CACHE_PREFIX, ry, rm)


def qd_m7_tile_cache_path(year: int, month: int) -> _Path:
    return qd_m7_ytd_cache_path(year, month)


def get_qd_m7_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M7_YTD_CACHE_PREFIX,
        source_tag=QD_M7_YTD_DISK_TAG,
        version=QD_M7_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m7_sql",
        compute_fn=lambda y, m: build_qd_m7_payload(y, m),
        kpi_id="QD-M7",
    )
