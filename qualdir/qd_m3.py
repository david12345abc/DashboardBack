"""
QD-M3 — бюджет блока в пределах лимита (факт из MSSQL-дампа 1С).

Логика факта (сверка со списком заявок в 1С):
  Факт = Σ СуммаДокумента (_Fld22781)
  по Document_ЗаявкаНаРасходованиеДенежныхСредств за календарный месяц,
  где:
    • DeletionMark = false, Posted = true
    • Дата документа (дата заявки) попадает в месяц
    • Подразделение ∈ контур качества (7 п/п QD-M4)
      или ТД_ЦФО ∈ метки качества
      (плюс потомки п/п — маппинг к ближайшей карточке)

План 2026 — константы из DashboardBack/qualdir/qd_m3.py.

SQL (erp_pm):
  Document_ЗаявкаНаРасходованиеДенежныхСредств → dbo._Document726
    _Fld22781      = СуммаДокумента
    _Fld22796RRef  = Подразделение → _Reference513
    _Fld127709RRef = ТД_ЦФО → _Reference127708
  Catalog_СтруктураПредприятия → dbo._Reference513
  Период в SQL = календарный год + 2000.

Использование:
  python qualdir/qd_m3.py
  python qualdir/qd_m3.py 2026
  python qualdir/qd_m3.py 2026-06
  python qualdir/qd_m3.py 2026-01 2026-07
"""

from __future__ import annotations

import argparse
import functools
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

SCRIPT_DIR = Path(__file__).resolve().parent
YEAR_OFFSET = 2000

DOC = "_Document726"
STRUCT = "_Reference513"
CFO_CAT = "_Reference127708"
COL_SUM = "_Fld22781"
COL_DEPT = "_Fld22796RRef"
COL_CFO = "_Fld127709RRef"
EMPTY = b"\x00" * 16

# План 2026, руб./мес. (DashboardBack/qualdir/qd_m3.py).
QD_M3_PLAN_BY_MONTH_2026: dict[int, int] = {
    1: 219_250,
    2: 161_100,
    3: 549_950,
    4: 148_350,
    5: 198_950,
    6: 203_950,
    7: 574_450,
    8: 387_450,
    9: 225_950,
    10: 249_950,
    11: 217_450,
    12: 221_300,
}

# ЦФО: эталон + фактическое имя в _Reference127708.
QD_M3_CFO_LABELS: tuple[str, ...] = (
    "Зам.директора по качеству",
    "Директор по качеству",
    "Зам. технического директора по качеству",
    "ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ",
    "Заместитель директора по качеству",
    "Контур качества (зам по кач.)",
)

# Подразделения контура качества (как QD-M4 / SEVEN_DEPTS_FOT_SPEC).
QD_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    (
        "Зам. технического директора по качеству",
        (
            "зам. технического директора по качеству",
            "зам технического директора по качеству",
            "заместитель тех. директора по качеству",
            "заместитель технического директора по качеству",
        ),
    ),
    ("Специалист по процессному управлению", ("специалист по процессному управлению",)),
    ("ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ", ("заместитель директора по качеству",)),
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    ("Лаборатория неразрушающего контроля", ("лаборатория неразрушающего контроля", "лнк")),
    (
        "Отдел управления несоответствиями",
        (
            "отдел управления несоответствиями",
            "отдел управления несоотвествиями",
        ),
    ),
]
QD_GROUP_ORDER = [t[0] for t in QD_FOT_SPEC]

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


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in QD_M3_PLAN_BY_MONTH_2026:
        return float(QD_M3_PLAN_BY_MONTH_2026[month])
    return None


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(fact / plan * 100.0, 1)


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


def _label_matches(norm: str, needles: frozenset[str]) -> bool:
    if not norm:
        return False
    for needle in needles:
        if len(needle) < 3:
            continue
        if norm == needle or needle in norm or norm in needle:
            return True
    return False


def _cfo_norms() -> frozenset[str]:
    return frozenset(n for n in (normalize_name(s) for s in QD_M3_CFO_LABELS) if n)


CFO_NORMS = _cfo_norms()


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


def resolve_quality_department_map(cur) -> tuple[dict[bytes, str], dict[str, str]]:
    """id подразделения → карточка из 7 п/п качества (ближайший предок)."""
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

    by_id = {row["id"]: row for row in rows}
    by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_norm.setdefault(normalize_name(row["desc"]), []).append(row)

    roots: dict[str, bytes] = {}
    labels: dict[str, str] = {}
    for display, aliases in QD_FOT_SPEC:
        found = None
        for alias in (display,) + aliases:
            found = _pick_best(by_norm.get(normalize_name(alias), []))
            if found:
                break
        if not found:
            alias_norm = normalize_name(display)
            contains = [
                r for r in rows if alias_norm and alias_norm in normalize_name(r["desc"])
            ]
            found = _pick_best(contains)
        if not found:
            raise RuntimeError(f"Подразделение не найдено в {STRUCT}: {display}")
        roots[display] = found["id"]
        labels[display] = found["desc"]

    root_ids = {root_id: display for display, root_id in roots.items()}
    id_to_group: dict[bytes, str] = {}
    for row in rows:
        if row["marked"]:
            continue
        cur_id = row["id"]
        seen: set[bytes] = set()
        while cur_id and cur_id not in seen:
            seen.add(cur_id)
            if cur_id in root_ids:
                id_to_group[row["id"]] = root_ids[cur_id]
                break
            parent = by_id.get(cur_id, {}).get("parent")
            if not parent or parent == EMPTY:
                break
            cur_id = parent

    return id_to_group, labels


def _load_cfo_ids(cur) -> dict[bytes, str]:
    cur.execute(f"SELECT _IDRRef, _Description FROM dbo.[{CFO_CAT}] WITH (NOLOCK)")
    out: dict[bytes, str] = {}
    for rid, desc in cur.fetchall():
        if _label_matches(normalize_name(desc), CFO_NORMS):
            out[bytes(rid)] = desc or ""
    return out


def compute_qd_m3_fact_monthly(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
    *,
    id_to_group: dict[bytes, str] | None = None,
    labels: dict[str, str] | None = None,
    cfo_ids: dict[bytes, str] | None = None,
) -> dict[str, Any]:
    """Σ СуммаДокумента заявок ДС контура качества за месяц (по дате заявки)."""
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        if id_to_group is None or labels is None:
            id_to_group, labels = resolve_quality_department_map(cur)
        if cfo_ids is None:
            cfo_ids = _load_cfo_ids(cur)

        dept_ids = list(id_to_group.keys())
        cfo_id_list = list(cfo_ids.keys())
        if not dept_ids and not cfo_id_list:
            raise RuntimeError("Не найден контур качества (п/п / ЦФО)")

        # Подразделение ∈ контур ИЛИ ТД_ЦФО ∈ метки качества.
        clauses: list[str] = []
        params: list[Any] = [p_start, p_end]
        if dept_ids:
            clauses.append(f"d.[{COL_DEPT}] IN ({','.join('?' * len(dept_ids))})")
            params.extend(dept_ids)
        if cfo_id_list:
            clauses.append(f"d.[{COL_CFO}] IN ({','.join('?' * len(cfo_id_list))})")
            params.extend(cfo_id_list)
        where_contour = "(" + " OR ".join(clauses) + ")"

        cur.execute(
            f"""
            SELECT d.[{COL_DEPT}], d.[{COL_CFO}], d.[{COL_SUM}], d._Number
            FROM dbo.[{DOC}] d WITH (NOLOCK)
            WHERE d._Date_Time >= ? AND d._Date_Time < ?
              AND d._Marked = 0x00
              AND d._Posted = 0x01
              AND {where_contour}
            """,
            params,
        )
        rows = cur.fetchall()

    groups_out: dict[str, dict[str, float | int]] = {
        name: {"fact_total": 0.0, "docs": 0} for name in QD_GROUP_ORDER
    }
    by_cfo_only = 0.0
    total_fact = 0.0
    for dept_id, cfo_id, amount, _number in rows:
        amt = _as_float(amount)
        total_fact += amt
        group = id_to_group.get(bytes(dept_id)) if dept_id else None
        if group:
            bucket = groups_out[group]
            bucket["fact_total"] = round(float(bucket["fact_total"]) + amt, 2)
            bucket["docs"] = int(bucket["docs"]) + 1
        else:
            by_cfo_only = round(by_cfo_only + amt, 2)

    total_fact = round(total_fact, 2)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "total_fact": total_fact,
        "groups": groups_out,
        "counts": {
            "docs_included": len(rows),
            "department_nodes": len(id_to_group),
            "cfo_nodes": len(cfo_ids),
            "by_cfo_only_amount": by_cfo_only,
        },
        "debug": {
            "status": "ok",
            "kpi_id": "QD-M3-FACT",
            "document": DOC,
            "sum_field": COL_SUM,
            "period_start": p_start,
            "period_end": p_end,
            "structure_labels": labels,
            "cfo_labels": list(QD_M3_CFO_LABELS),
            "rule": (
                "fact = sum(СуммаДокумента) by request Date for Posted docs "
                "in quality departments or quality ТД_ЦФО"
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
        id_to_group, labels = resolve_quality_department_map(cur)
        cfo_ids = _load_cfo_ids(cur)

    report: list[dict[str, Any]] = []
    for year, month in iter_months(start_period, end_period):
        fact_payload = compute_qd_m3_fact_monthly(
            year,
            month,
            sql,
            id_to_group=id_to_group,
            labels=labels,
            cfo_ids=cfo_ids,
        )
        plan = _plan_for_month(year, month)
        fact = float(fact_payload["total_fact"] or 0)
        report.append(
            {
                "year": year,
                "month": month,
                "month_name": MONTH_NAMES[month],
                "plan": plan,
                "fact": fact,
                "kpi_pct": _kpi_pct(plan, fact),
                "has_data": plan is not None,
                "values_unit": "руб.",
                "groups": fact_payload.get("groups") or {},
                "counts": fact_payload.get("counts") or {},
                "structure_labels": labels,
            }
        )
    return report


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "QD-M3 — бюджет блока в пределах лимита (SQL)",
        f"Источник: {DOC}.{COL_SUM} (СуммаДокумента), дата заявки, Posted, контур качества",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8} {'Заявок':>8}",
        f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        plan = row["plan"]
        fact = row["fact"]
        pct = row["kpi_pct"]
        docs = (row.get("counts") or {}).get("docs_included", 0)
        plan_s = f"{plan:,.2f}" if plan is not None else "—"
        fact_s = f"{fact:,.2f}" if fact is not None else "—"
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{plan_s:>14} {fact_s:>14} {pct_s:>8} {docs:>8}"
        )
    lines.append(f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}")
    if rows:
        labels = rows[0].get("structure_labels") or {}
        lines.extend(
            [
                "",
                "Контур подразделений:",
                *[f"  • {name} → {labels.get(name, '—')}" for name in QD_GROUP_ORDER],
            ]
        )
        # Разбивка последнего месяца с ненулевым фактом или последнего в отчёте
        last = rows[-1]
        groups = last.get("groups") or {}
        if any(float(g.get("fact_total") or 0) for g in groups.values()):
            lines.extend(
                [
                    "",
                    f"По подразделениям ({last['year']:04d}-{last['month']:02d}):",
                ]
            )
            for name in QD_GROUP_ORDER:
                g = groups.get(name) or {}
                amt = float(g.get("fact_total") or 0)
                n = int(g.get("docs") or 0)
                if amt or n:
                    lines.append(f"  {amt:>14,.2f}  {name} (заявок={n})")
    lines.append("")
    return "\n".join(lines)


def build_qd_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    today = date.today()
    ref_y = year or today.year
    ref_m = month or today.month
    if year is None and month is None:
        if today.month == 1:
            ref_y, ref_m = today.year - 1, 12
        else:
            ref_y, ref_m = today.year, today.month - 1

    rows = build_monthly_report((ref_y, 1), (ref_y, ref_m))
    monthly_rows = [
        {
            "month": r["month"],
            "year": r["year"],
            "month_name": r["month_name"],
            "plan": r["plan"],
            "fact": r["fact"],
            "kpi_pct": r["kpi_pct"],
            "has_data": r["has_data"],
            "values_unit": "руб.",
        }
        for r in rows
    ]
    ref_row = monthly_rows[-1] if monthly_rows else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok",
            "kpi_id": "QD-M3",
            "source": "qualdir.qd_m3.sql",
            "plan_source": "QD_M3_PLAN_BY_MONTH_2026",
            "fact_source": f"{DOC}.{COL_SUM} by request Date",
            "etalon_fixes": [
                "fact = Σ СуммаДокумента по дате заявки (не регистр ДДС)",
                "контур: 7 п/п качества или ТД_ЦФО качества",
                "Posted=true, DeletionMark=false",
            ],
            "monthly_counts": [
                {"year": r["year"], "month": r["month"], "counts": r.get("counts")}
                for r in rows
            ],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="QD-M3: бюджет блока (факт SQL).")
    parser.add_argument("period", nargs="*", help="ГГГГ | ГГГГ-ММ | ГГГГ-ММ ГГГГ-ММ")
    parser.add_argument("--json", action="store_true", help="JSON payload")
    args = parser.parse_args()

    try:
        if args.json and not args.period:
            payload = build_qd_m3_payload()
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        start, end, slug = parse_period_args(args.period)
        if args.json and start == end:
            payload = build_qd_m3_payload(start[0], start[1])
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            out = SCRIPT_DIR / f"qd_m3_{slug}.json"
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"JSON сохранён: {out}", file=sys.stderr)
            return 0

        rows = build_monthly_report(start, end)
        report = format_report(rows)
        print(report)
        out = SCRIPT_DIR / f"qd_m3_{slug}.txt"
        out.write_text(report, encoding="utf-8-sig")
        print(f"Отчёт сохранён: {out}")
        return 0
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

QD_M3_YTD_CACHE_PREFIX = "qualdir_qd_m3_ytd"
QD_M3_YTD_DISK_TAG = "qualdir_qd_m3_ytd_payload_sql_v3"
QD_M3_YTD_DISK_VERSION = 12


def qd_m3_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M3_YTD_CACHE_PREFIX, ry, rm)


def get_qd_m3_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M3_YTD_CACHE_PREFIX,
        source_tag=QD_M3_YTD_DISK_TAG,
        version=QD_M3_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m3_sql",
        compute_fn=lambda y, m: build_qd_m3_payload(y, m),
        kpi_id="QD-M3",
    )
