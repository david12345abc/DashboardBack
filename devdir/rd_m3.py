"""
RD-M3 — бюджет контура директора по развитию в пределах лимита.

Логика факта (SQL, бэкап erp_pm):
  Факт = Σ СуммаДокумента (_Fld22781)
  по Document_ЗаявкаНаРасходованиеДенежныхСредств за календарный месяц,
  где:
    • DeletionMark = false
    • Date документа попадает в календарный месяц
    • Подразделение ∈ «ДИРЕКТОР ПО РАЗВИТИЮ» и поддерево
      (Служба развития, новые продукты, ИИ, обучение)

План 2026 — константы RD_M3_BUDGET_PLAN_BY_MONTH_2026.
KPI % = факт / план × 100 (до 1 знака).

SQL (erp_pm):
  Document_ЗаявкаНаРасходованиеДенежныхСредств → dbo._Document726
    _Fld22781      = СуммаДокумента
    _Fld22796RRef  = Подразделение → _Reference513
  Catalog_СтруктураПредприятия → dbo._Reference513
  Период в SQL = календарный год + 2000.

Использование:
  python devdir/rd_m3.py
  python devdir/rd_m3.py --check
  python devdir/rd_m3.py 2026
  python devdir/rd_m3.py 2026-03
  python devdir/rd_m3.py 2026-01 2026-07
"""

from __future__ import annotations

import functools
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
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
ROUND_TOLERANCE = 0.01

DOC = "_Document726"
STRUCT = "_Reference513"
COL_SUM = "_Fld22781"
COL_DEPT = "_Fld22796RRef"

DEPARTMENT_ROOT = "ДИРЕКТОР ПО РАЗВИТИЮ"
DEPARTMENT_ROOT_ALIASES: tuple[str, ...] = (
    "директор по развитию",
)

# План 2026, руб./мес. (DashboardBack/devdir/rd_m3_budget_plan.py).
RD_M3_BUDGET_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 60_000,
    2: 50_000,
    3: 1_065_996,
    4: 606_320,
    5: 207_680,
    6: 384_000,
    7: 115_000,
    8: 264_800,
    9: 289_000,
    10: 240_000,
    11: 130_000,
    12: 139_000,
}

# Эталон факта из OData-отчёта TestKPIDump (июнь–июль 2026).
REFERENCE_FACT_2026: dict[int, float] = {
    6: 182_910.00,
    7: 314_818.84,
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


def normalize_name(value: str | None) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    text = text.lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return " ".join(text.split())


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def sql_period_bounds(year: int, month: int) -> tuple[str, str]:
    y = year + YEAR_OFFSET
    start = f"{y}{month:02d}01"
    if month == 12:
        end = f"{y + 1}0101"
    else:
        end = f"{y}{month + 1:02d}01"
    return start, end


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in RD_M3_BUDGET_PLAN_BY_MONTH_2026:
        return float(RD_M3_BUDGET_PLAN_BY_MONTH_2026[month])
    return None


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        if fact <= 0:
            return 100.0
        return None
    return round(fact / plan * 100.0, 1)


def money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.2f}".replace(",", " ")


def parse_month(value: str) -> tuple[int, int]:
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Месяц должен быть в формате ГГГГ-ММ")
    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def parse_period_args(argv: list[str] | None = None) -> tuple[tuple[int, int], tuple[int, int], str]:
    args = [a.strip() for a in (argv if argv is not None else sys.argv[1:]) if a.strip()]
    args = [a for a in args if not a.startswith("-")]
    now = datetime.now()
    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)
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
    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ или ГГГГ-ММ ГГГГ-ММ")


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    year, month = start
    out: list[tuple[int, int]] = []
    while (year, month) <= end:
        out.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return out


def _pick_best(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda row: (
            1 if row["marked"] else 0,
            len(normalize_name(row["desc"])),
            row["desc"],
        ),
    )[0]


def resolve_devdir_department_map(cur) -> tuple[dict[bytes, str], dict[str, str], list[str]]:
    """Поддерево корня «ДИРЕКТОР ПО РАЗВИТИЮ»: id → имя узла."""
    cur.execute(
        f"""
        SELECT _IDRRef, _Description, _ParentIDRRef, _Marked
        FROM dbo.[{STRUCT}] WITH (NOLOCK)
        """
    )
    rows: list[dict[str, Any]] = []
    for idr, desc, parent, marked in cur.fetchall():
        mb = bytes(marked) if marked is not None else b"\x00"
        rows.append(
            {
                "id": bytes(idr),
                "desc": desc or "",
                "parent": bytes(parent) if parent else None,
                "marked": mb != b"\x00",
            }
        )

    by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_norm.setdefault(normalize_name(row["desc"]), []).append(row)
    by_parent: dict[bytes | None, list[dict[str, Any]]] = {}
    for row in rows:
        by_parent.setdefault(row["parent"], []).append(row)

    found = None
    for alias in (DEPARTMENT_ROOT,) + DEPARTMENT_ROOT_ALIASES:
        found = _pick_best(by_norm.get(normalize_name(alias), []))
        if found:
            break
    if not found:
        raise RuntimeError(f"Корень не найден в {STRUCT}: {DEPARTMENT_ROOT}")

    subtree: set[bytes] = set()
    stack = [found["id"]]
    while stack:
        cur_id = stack.pop()
        if cur_id in subtree:
            continue
        subtree.add(cur_id)
        for child in by_parent.get(cur_id, []):
            stack.append(child["id"])

    id_to_group: dict[bytes, str] = {}
    labels: dict[str, str] = {DEPARTMENT_ROOT: found["desc"]}
    group_order: list[str] = []
    for row in rows:
        if row["id"] not in subtree or row["marked"]:
            continue
        name = row["desc"] or row["id"].hex()
        id_to_group[row["id"]] = name
        labels[name] = name
        group_order.append(name)
    group_order = sorted(group_order, key=lambda s: normalize_name(s))
    return id_to_group, labels, group_order


def compute_rd_m3_fact_monthly(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
    *,
    id_to_group: dict[bytes, str] | None = None,
    labels: dict[str, str] | None = None,
    group_order: list[str] | None = None,
) -> dict[str, Any]:
    """Σ СуммаДокумента заявок ДС по поддереву ДИРЕКТОР ПО РАЗВИТИЮ."""
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        if id_to_group is None or labels is None or group_order is None:
            id_to_group, labels, group_order = resolve_devdir_department_map(cur)

        dept_ids = list(id_to_group.keys())
        if not dept_ids:
            raise RuntimeError("Не найдены подразделения контура развития")

        # Как в OData-эталоне: без фильтра Posted — только не помеченные на удаление.
        cur.execute(
            f"""
            SELECT d.[{COL_DEPT}], d.[{COL_SUM}], d._Number
            FROM dbo.[{DOC}] d WITH (NOLOCK)
            WHERE d._Date_Time >= ? AND d._Date_Time < ?
              AND d._Marked = 0x00
              AND d.[{COL_DEPT}] IN ({",".join("?" * len(dept_ids))})
            """,
            [p_start, p_end, *dept_ids],
        )
        rows = cur.fetchall()

    groups_out: dict[str, dict[str, float | int]] = {
        name: {"fact_total": 0.0, "docs": 0} for name in group_order
    }
    total_fact = 0.0
    for dept_id, amount, _number in rows:
        group = id_to_group.get(bytes(dept_id))
        if not group:
            continue
        amt = _as_float(amount)
        total_fact += amt
        bucket = groups_out.setdefault(group, {"fact_total": 0.0, "docs": 0})
        bucket["fact_total"] = round(float(bucket["fact_total"]) + amt, 2)
        bucket["docs"] = int(bucket["docs"]) + 1

    total_fact = round(total_fact, 2)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "total_fact": total_fact,
        "groups": groups_out,
        "group_order": group_order,
        "counts": {
            "docs_included": len(rows),
            "department_nodes": len(id_to_group),
        },
        "debug": {
            "status": "ok",
            "kpi_id": "RD-M3-FACT",
            "document": DOC,
            "sum_field": COL_SUM,
            "period_start": p_start,
            "period_end": p_end,
            "structure_labels": labels,
            "root": labels.get(DEPARTMENT_ROOT, DEPARTMENT_ROOT),
            "rule": (
                "fact = sum(СуммаДокумента) for unmarked requests "
                "in subtree of ДИРЕКТОР ПО РАЗВИТИЮ"
            ),
        },
    }


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        id_to_group, labels, group_order = resolve_devdir_department_map(cur)

    report: list[dict[str, Any]] = []
    for year, month in iter_months(start_period, end_period):
        fact_payload = compute_rd_m3_fact_monthly(
            year,
            month,
            sql,
            id_to_group=id_to_group,
            labels=labels,
            group_order=group_order,
        )
        plan = plan_for_month(year, month)
        fact = float(fact_payload["total_fact"] or 0)
        report.append(
            {
                "year": year,
                "month": month,
                "month_name": MONTH_NAMES[month],
                "plan": plan,
                "fact": fact,
                "kpi_pct": kpi_pct(plan, fact),
                "has_data": plan is not None and fact is not None,
                "values_unit": "руб.",
                "groups": fact_payload.get("groups") or {},
                "group_order": group_order,
                "counts": fact_payload.get("counts") or {},
                "structure_labels": labels,
            }
        )
    return report


def format_report(rows: list[dict[str, Any]]) -> str:
    labels = (rows[-1].get("structure_labels") or {}) if rows else {}
    root_label = labels.get(DEPARTMENT_ROOT, DEPARTMENT_ROOT)
    group_order = (rows[-1].get("group_order") or []) if rows else []
    lines = [
        "RD-M3 — бюджет контура директора по развитию (SQL)",
        f"Корень: {root_label}",
        f"Поддерево ({len(group_order)}): {', '.join(group_order)}",
        f"Источник: {DOC}.{COL_SUM} (СуммаДокумента), дата документа, без Posted",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8} {'Заявок':>8}",
        f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        plan = row["plan"]
        fact = row["fact"]
        pct = row["kpi_pct"]
        docs = (row.get("counts") or {}).get("docs_included", 0)
        plan_s = f"{plan:,.2f}".replace(",", " ") if plan is not None else "—"
        fact_s = f"{fact:,.2f}".replace(",", " ") if fact is not None else "—"
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{plan_s:>14} "
            f"{fact_s:>14} "
            f"{pct_s:>8} "
            f"{docs:>8}"
        )

    plan_sum = sum(float(r["plan"]) for r in rows if r.get("plan") is not None)
    fact_sum = sum(float(r["fact"]) for r in rows if r.get("fact") is not None)
    lines.extend(
        [
            f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}",
            f"{'ИТОГО':<10} {money(plan_sum):>14} {money(fact_sum):>14} {'':>8}",
            "",
        ]
    )

    ref = rows[-1] if rows else None
    if ref and ref.get("groups"):
        lines.append(f"По подразделениям ({ref['year']:04d}-{ref['month']:02d}):")
        order = ref.get("group_order") or sorted(ref["groups"].keys())
        for name in order:
            bucket = ref["groups"].get(name) or {}
            total = float(bucket.get("fact_total") or 0)
            docs = int(bucket.get("docs") or 0)
            if total == 0 and docs == 0:
                continue
            lines.append(f"  {money(total):>14}  {name} (заявок={docs})")
        lines.append("")
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"rd_m3_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"rd_m3_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return txt_path, json_path


def build_rd_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    now = date.today()
    ref_y = year or now.year
    ref_m = month or (now.month - 1 if now.month > 1 else 12)
    if month is None and now.month == 1 and year is None:
        ref_y = now.year - 1
        ref_m = 12

    rows = build_monthly_report((ref_y, 1), (ref_y, ref_m))
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        monthly_rows.append(
            {
                "month": row["month"],
                "year": row["year"],
                "month_name": MONTH_NAMES[row["month"]],
                "plan": row.get("plan"),
                "fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "has_data": row.get("has_data"),
                "values_unit": "руб.",
                "groups": dict(row.get("groups") or {}),
            }
        )

    ref_row = next(
        (item for item in monthly_rows if item["month"] == ref_m),
        monthly_rows[-1] if monthly_rows else None,
    )
    plan_sum = sum(float(r["plan"]) for r in monthly_rows if r.get("plan") is not None)
    fact_sum = sum(float(r["fact"]) for r in monthly_rows if r.get("fact") is not None)
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": round(plan_sum, 2) if monthly_rows else None,
            "total_fact": round(fact_sum, 2) if monthly_rows else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok",
            "kpi_id": "RD-M3",
            "source": "devdir.rd_m3.sql",
            "plan_source": "RD_M3_BUDGET_PLAN_BY_MONTH_2026",
            "fact_source": (
                f"{DOC}.{COL_SUM} unmarked requests, subtree of {DEPARTMENT_ROOT}"
            ),
            "root": DEPARTMENT_ROOT,
        },
    }


def run_check() -> int:
    print("Сверка RD-M3 факт · 2026 (REFERENCE / OData-эталон)")
    all_ok = True
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        id_to_group, labels, group_order = resolve_devdir_department_map(cur)
    print(f"  Корень: {labels.get(DEPARTMENT_ROOT, DEPARTMENT_ROOT)}")
    print(f"  Узлов в поддереве: {len(id_to_group)}")
    print(f"  Поддерево: {', '.join(group_order)}")

    for month, ref in sorted(REFERENCE_FACT_2026.items()):
        snap = compute_rd_m3_fact_monthly(
            2026,
            month,
            sql,
            id_to_group=id_to_group,
            labels=labels,
            group_order=group_order,
        )
        fact = float(snap["total_fact"] or 0)
        ok = abs(fact - ref) <= ROUND_TOLERANCE
        if not ok:
            all_ok = False
        mark = "OK" if ok else "РАСХОЖДЕНИЕ"
        print(
            f"  {MONTH_NAMES[month]}: {money(fact)} / {money(ref)} "
            f"(d {money(fact - ref)}) ({mark})"
        )
    return 0 if all_ok else 2


def main() -> None:
    try:
        argv = sys.argv[1:]
        if "--check" in argv:
            sys.exit(run_check())

        start, end, slug = parse_period_args(argv)
        rows = build_monthly_report(start, end)
        text = format_report(rows)
        print(text)
        txt_path, json_path = save_report(slug, rows, text)
        print(f"Отчёт сохранён: {txt_path}")
        print(f"JSON: {json_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
