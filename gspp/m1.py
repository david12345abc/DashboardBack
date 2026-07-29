"""
ГСП-M1 — ТКП выставлены в срок (расчёт по SQL-таблицам erp_pm).

Алгоритм из 1С (личный KPI / мотивация):

  ВЫБРАТЬ Регистратор, ДатаЗавершенияПлан, ДатаЗавершенияФакт
  ИЗ РегистрНакопления.ТД_МониторингЭтаповОпросныхЛистов
  ГДЕ ДатаЗавершенияПлан МЕЖДУ НачалоМесяца И КонецМесяца
    И ТочкаЭтапа = СогласованиеАнализОЛГруппойГСПП

  План  = Количество() строк результата
  Не в срок = НачалоДня(Факт) > НачалоДня(План)
  Факт  = План − Не в срок
  %     = 100 − НеВСрок / План × 100

SQL (erp_pm):
  Catalog_ТД_ТочкиЭтапов → _Reference100508
  AccumulationRegister → _AccumRg127619
    _Fld127620RRef = ТочкаЭтапа
    _Fld127622     = ДатаЗавершенияПлан
    _Fld127623     = ДатаЗавершенияФакт

Даты в SQL со смещением +2000 лет.
"""

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

STAGE_TABLE = "_Reference100508"
REGISTER_TABLE = "_AccumRg127619"
COL_STAGE = "_Fld127620RRef"
COL_PLAN = "_Fld127622"
COL_FACT = "_Fld127623"

STAGE_PREDEFINED = "СогласованиеАнализОЛГруппойГСПП"
STAGE_DESCRIPTION = "Анализ ОЛ группой ГСПП"
# OData Ref_Key 605622b5-0004-11ec-86a3-ac1f6b05524d
STAGE_BIN = bytes.fromhex("86a3ac1f6b05524d11ec0004605622b5")

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


def from_sql_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.year <= EMPTY_SQL_YEAR:
        return None
    return value.replace(year=value.year - YEAR_OFFSET)


def normalize_text(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in value).split())


def resolve_stage_point(cur) -> tuple[bytes, str]:
    cur.execute(
        f"""
        SELECT _IDRRef, _Description
        FROM [{STAGE_TABLE}] WITH (NOLOCK)
        WHERE _IDRRef = ?
           OR _Description = ?
        """,
        STAGE_BIN,
        STAGE_DESCRIPTION,
    )
    row = cur.fetchone()
    if row:
        return bytes(row[0]), row[1] or STAGE_DESCRIPTION

    cur.execute(
        f"""
        SELECT _IDRRef, _Description
        FROM [{STAGE_TABLE}] WITH (NOLOCK)
        WHERE _Description LIKE N'%Анализ%ОЛ%ГСПП%'
           OR _Description LIKE N'%группой ГСПП%'
        """
    )
    for idr, description in cur.fetchall():
        if normalize_text(description) == normalize_text(STAGE_DESCRIPTION):
            return bytes(idr), description or STAGE_DESCRIPTION

    raise RuntimeError(f"Не найдена точка этапа: {STAGE_PREDEFINED}")


def is_late(plan_dt: datetime | None, fact_dt: datetime | None) -> bool:
    """Как в 1С: НачалоДня(Факт) > НачалоДня(План). Пустой факт — не просрочка."""
    if plan_dt is None or fact_dt is None:
        return False
    return fact_dt.date() > plan_dt.date()


def kpi_pct(fact: int, plan: int) -> float | None:
    if plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def load_register_rows(
    cur,
    stage_bin: bytes,
    start_dt: date,
    end_dt: date,
) -> list[tuple[datetime, datetime | None]]:
    """Все строки регистра (без дедупликации — как РезультатЗапроса.Количество())."""
    sql_start = to_sql_dt(start_dt)
    sql_end_exclusive = to_sql_dt(end_dt + timedelta(days=1))
    cur.execute(
        f"""
        SELECT [{COL_PLAN}], [{COL_FACT}]
        FROM [{REGISTER_TABLE}] WITH (NOLOCK)
        WHERE [{COL_STAGE}] = ?
          AND [{COL_PLAN}] >= ?
          AND [{COL_PLAN}] < ?
        """,
        stage_bin,
        sql_start,
        sql_end_exclusive,
    )
    rows: list[tuple[datetime, datetime | None]] = []
    for plan_raw, fact_raw in cur.fetchall():
        if plan_raw is None:
            continue
        plan_dt = from_sql_dt(plan_raw)
        if plan_dt is None:
            continue
        fact_dt = from_sql_dt(fact_raw)
        rows.append((plan_dt, fact_dt))
    return rows


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> tuple[str, list[dict[str, Any]]]:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        stage_bin, stage_name = resolve_stage_point(cur)
        rows = load_register_rows(
            cur,
            stage_bin,
            month_start(*start_period),
            month_end(*end_period),
        )

    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"plan": 0, "late": 0})
    for plan_dt, fact_dt in rows:
        month_key = f"{plan_dt.year:04d}-{plan_dt.month:02d}"
        stats[month_key]["plan"] += 1
        if is_late(plan_dt, fact_dt):
            stats[month_key]["late"] += 1

    report_rows: list[dict[str, Any]] = []
    for month_key in iter_months(start_period, end_period):
        plan = stats[month_key]["plan"]
        late = stats[month_key]["late"]
        fact = plan - late
        report_rows.append(
            {
                "month": month_key,
                "plan": plan,
                "fact": fact,
                "late": late,
                "kpi_pct": kpi_pct(fact, plan),
            }
        )
    return stage_name, report_rows


def format_report(point_name: str, rows: list[dict[str, Any]]) -> str:
    lines = [
        "ТКП выставлены в срок (ГСП-M1 / SQL)",
        f"Точка этапа: {point_name} ({STAGE_PREDEFINED})",
        f"Источник: {REGISTER_TABLE} + {STAGE_TABLE}",
        "Критерий: НачалоДня(Факт) > НачалоДня(План) → не в срок",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'Не в срок':>10} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 10} {'-' * 8}",
    ]
    for row in rows:
        pct = row["kpi_pct"]
        pct_s = f"{pct:.2f}" if pct is not None else "—"
        lines.append(
            f"{row['month']:<10} "
            f"{row['plan']:>8} "
            f"{row['fact']:>8} "
            f"{row['late']:>10} "
            f"{pct_s:>8}"
        )
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 10} {'-' * 8}",
            f"{'ИТОГО':<10} "
            f"{sum(row['plan'] for row in rows):>8} "
            f"{sum(row['fact'] for row in rows):>8} "
            f"{sum(row['late'] for row in rows):>10} "
            f"{'':>8}",
            "",
        ]
    )
    return "\n".join(lines)


def build_gspp_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    now = datetime.now()
    ref_y = year or now.year
    ref_m = month or (now.month - 1 if now.month > 1 else 12)
    if month is None and now.month == 1 and year is None:
        ref_y = now.year - 1
        ref_m = 12

    point_name, rows = build_monthly_report((ref_y, 1), (ref_y, ref_m))
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        month_num = int(str(row["month"])[5:7])
        plan = int(row["plan"])
        fact = int(row["fact"])
        late = int(row["late"])
        monthly_rows.append(
            {
                "month": month_num,
                "year": ref_y,
                "month_name": MONTH_NAMES[month_num],
                "plan": plan,
                "fact": fact,
                "late": late,
                "kpi_pct": kpi_pct(fact, plan),
                "has_data": plan > 0,
                "values_unit": "шт.",
            }
        )

    ref_row = monthly_rows[-1] if monthly_rows else {
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "plan": 0,
        "fact": 0,
        "late": 0,
        "kpi_pct": None,
        "has_data": False,
        "values_unit": "шт.",
    }
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": ref_row.get("plan"),
            "total_fact": ref_row.get("fact"),
            "total_late": ref_row.get("late"),
            "kpi_pct": ref_row.get("kpi_pct"),
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "ГСП-M1",
            "status": "ok",
            "source": "gspp.m1.sql",
            "stage_point": point_name,
            "tables": {
                "stage_catalog": STAGE_TABLE,
                "register": REGISTER_TABLE,
                "stage_col": COL_STAGE,
                "plan_col": COL_PLAN,
                "fact_col": COL_FACT,
            },
            "rule": (
                "plan = register rows with planned completion in month "
                "(stage = Анализ ОЛ группой ГСПП); "
                "late = fact date strictly after plan date (time ignored); "
                "fact = plan - late"
            ),
            "rows_by_month": rows,
        },
    }


def main() -> int:
    try:
        start_period, end_period, label = parse_period_args()
        stage_name, rows = build_monthly_report(start_period, end_period)
        text = format_report(stage_name, rows)
        print(text)
        out = SCRIPT_DIR / f"gsp_m1_{label}.txt"
        out.write_text(text, encoding="utf-8")
        print(f"Сохранено: {out}")
        return 0
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

GSPP_M1_CACHE_PREFIX = "gspp_m1_ytd"
GSPP_M1_DISK_TAG = "gspp_m1_sql_payload_v1"
GSPP_M1_DISK_VERSION = 1


def gspp_m1_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(GSPP_M1_CACHE_PREFIX, ry, rm)


def get_gspp_m1_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=GSPP_M1_CACHE_PREFIX,
        source_tag=GSPP_M1_DISK_TAG,
        version=GSPP_M1_DISK_VERSION,
        lock_key_prefix="gspp_m1_sql",
        compute_fn=lambda y, m: build_gspp_m1_payload(y, m),
        kpi_id="ГСП-M1",
    )
