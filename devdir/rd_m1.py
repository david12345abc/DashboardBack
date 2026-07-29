"""
RD-M1 — Обработка инициатив / ЗПР в срок.

Источник: Document_ТД_ЗаявкаОПотребностиРынка → dbo._Document76753 (SQL-бэкап).

План — все ЗПР с ДатаПриемаВРазработку в месяце, DeletionMark=false
  (включая статусы «ВРаботе» / «ВРаботеУСмежногоПодразделения» / «Закрыта»).

Факт — из плана, без срыва срока отработки:
  СрокОтработкиФакт пустой ИЛИ дата(СрокОтработкиФакт) ≤ дата(СрокОтработки).

KPI % = факт / план × 100 (до 1 знака).

SQL (erp_pm):
  Document_ТД_ЗаявкаОПотребностиРынка → dbo._Document76753
    _Fld76769  — ДатаПриемаВРазработку
    _Fld76770  — СрокОтработки
    _Fld86492  — СрокОтработкиФакт (пусто = 2001-01-01)
  Период в SQL = календарный год + 2000.

Эталон (премия мотивацииТД, Соломичева С.В., Служба развития):
  июнь 2026 — 61 / 43 / 70.49%
  июль 2026 — 35 / 33 / 94.29% (бэкап за вчера может дать 33/32).

Использование:
  python devdir/rd_m1.py
  python devdir/rd_m1.py --check
  python devdir/rd_m1.py 2026
  python devdir/rd_m1.py 2026-04
  python devdir/rd_m1.py 2026-01 2026-07
"""

from __future__ import annotations

import functools
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sql_connection import SqlConnection  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

SCRIPT_DIR = Path(__file__).resolve().parent
YEAR_OFFSET = 2000
EMPTY_SQL_YEAR = 2001

DOC = "_Document76753"
COL_ACCEPTED = "_Fld76769"
COL_DEADLINE_PLAN = "_Fld76770"
COL_DEADLINE_FACT = "_Fld86492"

# Эталон: премия мотивацииТД (Соломичева С.В., Служба развития).
REFERENCE_2026: dict[int, tuple[float, float]] = {
    6: (61.0, 43.0),
    7: (35.0, 33.0),
}

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
    args = [a for a in args if not a.startswith("-")]
    now = datetime.now()

    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)

    if len(args) == 2 and args[0].isdigit() and args[1].isdigit() and len(args[0]) == 4:
        period = (int(args[0]), int(args[1]))
        if not 1 <= period[1] <= 12:
            raise ValueError("Месяц должен быть от 1 до 12")
        return period, period, f"{period[0]:04d}-{period[1]:02d}"

    if len(args) == 1 and len(args[0]) == 4 and args[0].isdigit():
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

    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ, ГОД МЕСЯЦ или ГГГГ-ММ ГГГГ-ММ")


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


def sql_period_bounds(year: int, month: int) -> tuple[str, str]:
    y = year + YEAR_OFFSET
    start = f"{y}{month:02d}01"
    if month == 12:
        end = f"{y + 1}0101"
    else:
        end = f"{y}{month + 1:02d}01"
    return start, end


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100.0, 1)


def compute_month(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS plan_cnt,
                SUM(
                    CASE
                        WHEN d.[{COL_DEADLINE_FACT}] IS NULL
                          OR YEAR(d.[{COL_DEADLINE_FACT}]) <= {EMPTY_SQL_YEAR}
                        THEN 1
                        WHEN d.[{COL_DEADLINE_PLAN}] IS NULL
                          OR YEAR(d.[{COL_DEADLINE_PLAN}]) <= {EMPTY_SQL_YEAR}
                        THEN 1
                        WHEN CAST(d.[{COL_DEADLINE_FACT}] AS date)
                             <= CAST(d.[{COL_DEADLINE_PLAN}] AS date)
                        THEN 1
                        ELSE 0
                    END
                ) AS fact_cnt
            FROM dbo.[{DOC}] d WITH (NOLOCK)
            WHERE d.[{COL_ACCEPTED}] >= ?
              AND d.[{COL_ACCEPTED}] < ?
              AND d._Marked = 0x00
            """,
            [p_start, p_end],
        )
        plan_cnt, fact_cnt = cur.fetchone()

    plan = float(plan_cnt or 0)
    fact = float(fact_cnt or 0)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "plan": plan,
        "fact": fact,
        "kpi_pct": kpi_pct(plan, fact),
        "has_data": plan > 0 or fact > 0,
        "values_unit": "шт.",
        "period_start": p_start,
        "period_end": p_end,
    }


def build_monthly_report(
    start: tuple[int, int],
    end: tuple[int, int],
    *,
    quiet: bool = False,
) -> dict[str, Any]:
    sql = SqlConnection()
    months = iter_months(start, end)
    rows: list[dict[str, Any]] = []
    for i, (year, month) in enumerate(months, 1):
        if not quiet:
            print(f"[{i}/{len(months)}] Расчёт {year}-{month:02d} ...")
        rows.append(compute_month(year, month, sql=sql))
    return {
        "months": rows,
        "document": DOC,
        "source": "sql",
    }


def format_report(payload: dict[str, Any]) -> str:
    rows = payload["months"]
    lines = [
        "RD-M1 — Обработка инициатив / ЗПР в срок (SQL)",
        f"Источник: {DOC}",
        f"План: {COL_ACCEPTED} (ДатаПриемаВРазработку) в месяце",
        f"Факт: {COL_DEADLINE_FACT} ≤ {COL_DEADLINE_PLAN} (пустой факт = ок)",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        pct = row["kpi_pct"]
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{int(row['plan']):>8} "
            f"{int(row['fact']):>8} "
            f"{pct_s:>8}"
        )

    plan_sum = sum(float(r["plan"]) for r in rows)
    fact_sum = sum(float(r["fact"]) for r in rows)
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8}",
            f"{'ИТОГО':<10} {int(plan_sum):>8} {int(fact_sum):>8} {'':>8}",
            "",
        ]
    )
    return "\n".join(lines)


def save_report(period_slug: str, payload: dict[str, Any], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"rd_m1_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"rd_m1_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return txt_path, json_path


def build_rd_m1_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Payload в стиле DashboardBack RD-M1."""
    now = date.today()
    if year is None or month is None:
        if now.month == 1:
            ref_y, ref_m = now.year - 1, 12
        else:
            ref_y, ref_m = now.year, now.month - 1
        year = year or ref_y
        month = month or ref_m

    report = build_monthly_report((year, 1), (year, month), quiet=True)
    monthly_rows = [
        {
            "month": row["month"],
            "year": row["year"],
            "month_name": row["month_name"],
            "plan": row["plan"],
            "fact": row["fact"],
            "kpi_pct": row["kpi_pct"],
            "has_data": row["has_data"],
            "values_unit": "шт.",
        }
        for row in report["months"]
    ]
    ref_row = monthly_rows[-1] if monthly_rows else None
    plan_sum = sum(float(r["plan"]) for r in monthly_rows)
    fact_sum = sum(float(r["fact"]) for r in monthly_rows)
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": year,
            "month": month,
            "month_name": MONTH_NAMES[month],
        },
        "ytd": {
            "total_plan": plan_sum if monthly_rows else None,
            "total_fact": fact_sum if monthly_rows else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_id": "RD-M1",
            "status": "ok",
            "source": "devdir.rd_m1.sql",
            "document": DOC,
            "plan_source": f"{COL_ACCEPTED} (ДатаПриемаВРазработку), все статусы",
            "fact_source": (
                f"{COL_DEADLINE_FACT} ≤ {COL_DEADLINE_PLAN} (пустой = в срок)"
            ),
            "reference": "премия мотивацииТД: июнь 61/43, июль 35/33",
        },
    }


def run_check() -> int:
    print("Сверка RD-M1 · 2026 (премия мотивацииТД / SQL-бэкап)")
    all_ok = True
    sql = SqlConnection()
    for month, (ref_plan, ref_fact) in sorted(REFERENCE_2026.items()):
        row = compute_month(2026, month, sql=sql)
        plan, fact = float(row["plan"]), float(row["fact"])
        ok = plan == ref_plan and fact == ref_fact
        # Июль: бэкап/live может отличаться на 1–2 документа.
        near = month == 7 and abs(plan - ref_plan) <= 2 and abs(fact - ref_fact) <= 1
        mark = "OK" if ok else ("OK~" if near else "РАСХОЖДЕНИЕ")
        if not ok and not near:
            all_ok = False
        print(
            f"  {MONTH_NAMES[month]}: plan={int(plan)}/{int(ref_plan)} "
            f"fact={int(fact)}/{int(ref_fact)} %={row['kpi_pct']} ({mark})"
        )
    return 0 if all_ok else 2


def main() -> None:
    try:
        argv = sys.argv[1:]
        if "--check" in argv:
            sys.exit(run_check())

        start, end, slug = parse_period_args(argv)
        report = build_monthly_report(start, end)
        text = format_report(report)
        print(text)
        txt_path, json_path = save_report(slug, report, text)
        print(f"Отчёт сохранён: {txt_path}")
        print(f"JSON: {json_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
