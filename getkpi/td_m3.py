"""
TD-M3 — бюджет затрат блока техдирекции в пределах лимита.

Эталон экрана 1С: «Списания ДС по статьям ДДС», группа статей
«Технический директор», организация НПО, проведённые списания.

Логика факта (SQL):
  Факт = Σ Сумма расшифровки платежа
  по Document_СписаниеБезналичныхДенежныхСредств за календарный месяц,
  где:
    • DeletionMark = false, Posted = true
    • Организация — НПО или Турбулентность-Дон
    • Статья ДДС ∈ поддерево «Технический директор»

KPI % = MIN(100; Факт/План·100).

План 2026: янв–май — TD_M3_PLAN_TARGET_2026; июнь–июль — кэш плитки.

SQL (erp_pm):
  Document_СписаниеБезналичныхДенежныхСредств → dbo._Document980
  РасшифровкаПлатежа                          → dbo._Document980_VT37251
  Catalog_СтатьиДвиженияДенежныхСредств       → dbo._Reference503
  Период в SQL = календарный год + 2000.

Использование:
  python techdir/td_m3.py
  python techdir/td_m3.py --check
  python techdir/td_m3.py 2026
  python techdir/td_m3.py 2026-05
  python techdir/td_m3.py 2026-01 2026-07
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

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

SCRIPT_DIR = Path(__file__).resolve().parent
YEAR_OFFSET = 2000
ROUND_TOLERANCE = 0.05

DOC = "_Document726"
STRUCT = "_Reference513"
COL_SUM = "_Fld22781"
COL_DEPT = "_Fld22796RRef"
EMPTY = b"\x00" * 16
WRITEOFF = "_Document980"
WRITEOFF_VT = "_Document980_VT37251"
ART_CAT = "_Reference503"
COL_WO_SUM = "_Fld37256"
COL_WO_ART = "_Fld37254RRef"
COL_WO_ORG = "_Fld37189RRef"
ARTICLE_GROUP = "Технический директор"
ORG_GUIDS = (
    "fbca2148-6cfd-11e7-812d-001e67112509",  # ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО
    "fbca2143-6cfd-11e7-812d-001e67112509",  # Турбулентность-Дон ООО
)

# План: янв–май — calc_budget_techdir_m3.TD_M3_PLAN_TARGET_2026;
# июнь–июль — techdir_m3_monthly_2026_*.json.
TD_M3_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 6_227_199,
    2: 6_208_765,
    3: 7_557_205,
    4: 7_805_028,
    5: 7_363_581,
    6: 21_406_993.27,
    7: 23_651_044.73,
}

TD_BUDGET_ROOT = "ТЕХНИЧЕСКИЙ ДИРЕКТОР"
TD_BUDGET_ROOT_ALIASES: tuple[str, ...] = (
    "технический директор",
    "техдиректор",
)

# Эталон: списания ДС по статьям группы «Технический директор».
REFERENCE_FACT_2026: dict[int, float] = {
    1: 1_911_235.02,
    2: 1_979_883.69,
    3: 3_089_112.67,
    4: 1_768_114.90,
    5: 3_738_792.67,
    6: 2_486_104.10,
    7: 2_956_581.98,
    8: 2_749_946.73,
    9: 2_107_130.85,
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
    if year == 2026 and month in TD_M3_PLAN_BY_MONTH_2026:
        return float(TD_M3_PLAN_BY_MONTH_2026[month])
    return None


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    """MIN(100; Факт/План·100) по методике TD-M3."""
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(min(100.0, fact / plan * 100.0), 1)


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


def resolve_techdir_department_map(cur) -> tuple[dict[bytes, str], dict[str, str], list[str]]:
    """Поддерево корня «ТЕХНИЧЕСКИЙ ДИРЕКТОР»: id → имя узла."""
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
    for alias in (TD_BUDGET_ROOT,) + TD_BUDGET_ROOT_ALIASES:
        found = _pick_best(by_norm.get(normalize_name(alias), []))
        if found:
            break
    if not found:
        alias_norm = normalize_name(TD_BUDGET_ROOT)
        found = _pick_best(
            [r for r in rows if alias_norm and alias_norm in normalize_name(r["desc"])]
        )
    if not found:
        raise RuntimeError(f"Корень не найден в {STRUCT}: {TD_BUDGET_ROOT}")

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
    labels: dict[str, str] = {TD_BUDGET_ROOT: found["desc"]}
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


def guid_to_1c_binary(guid: str) -> bytes:
    b = bytes.fromhex(guid.replace("-", ""))
    return b[8:10] + b[10:16] + b[6:8] + b[4:6] + b[0:4]


def resolve_td_article_map(cur) -> tuple[dict[bytes, str], list[str]]:
    """Статьи ДДС из поддерева «Технический директор» (без самого корня)."""
    cur.execute(
        f"""
        SELECT _IDRRef, _Description, _ParentIDRRef, _Marked
        FROM dbo.[{ART_CAT}] WITH (NOLOCK)
        """
    )
    rows = []
    for idr, desc, parent, marked in cur.fetchall():
        parent_b = bytes(parent) if parent is not None else EMPTY
        rows.append(
            {
                "id": bytes(idr),
                "desc": (desc or "").strip(),
                "parent": None if parent_b == EMPTY else parent_b,
                "marked": bytes(marked) != b"\x00" if marked is not None else False,
            }
        )
    by_parent: dict[bytes | None, list[dict[str, Any]]] = {}
    root = None
    for row in rows:
        by_parent.setdefault(row["parent"], []).append(row)
        if normalize_name(row["desc"]) == normalize_name(ARTICLE_GROUP):
            root = row
    if root is None:
        raise RuntimeError(f"Группа статей не найдена в {ART_CAT}: {ARTICLE_GROUP}")

    id_to_name: dict[bytes, str] = {}
    stack = [root["id"]]
    seen: set[bytes] = set()
    while stack:
        cur_id = stack.pop()
        if cur_id in seen:
            continue
        seen.add(cur_id)
        for child in by_parent.get(cur_id, []):
            stack.append(child["id"])
            if child["marked"] or child["id"] == root["id"]:
                continue
            id_to_name[child["id"]] = child["desc"] or child["id"].hex()
    order = sorted(set(id_to_name.values()), key=normalize_name)
    return id_to_name, order


def compute_td_m3_fact_monthly(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
    *,
    id_to_group: dict[bytes, str] | None = None,
    labels: dict[str, str] | None = None,
    group_order: list[str] | None = None,
) -> dict[str, Any]:
    """Σ списаний ДС по статьям группы «Технический директор»."""
    del id_to_group, labels, group_order
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)
    org_bins = [guid_to_1c_binary(g) for g in ORG_GUIDS]

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        id_to_name, article_order = resolve_td_article_map(cur)
        article_ids = list(id_to_name.keys())
        if not article_ids:
            raise RuntimeError("Не найдены статьи группы «Технический директор»")

        cur.execute(
            f"""
            SELECT vt.[{COL_WO_ART}], SUM(vt.[{COL_WO_SUM}]), COUNT(*)
            FROM dbo.[{WRITEOFF_VT}] vt WITH (NOLOCK)
            INNER JOIN dbo.[{WRITEOFF}] d WITH (NOLOCK)
                ON d._IDRRef = vt._Document980_IDRRef
            WHERE d._Date_Time >= ? AND d._Date_Time < ?
              AND d._Marked = 0x00
              AND d._Posted = 0x01
              AND d.[{COL_WO_ORG}] IN ({",".join("?" * len(org_bins))})
              AND vt.[{COL_WO_ART}] IN ({",".join("?" * len(article_ids))})
            GROUP BY vt.[{COL_WO_ART}]
            """,
            [p_start, p_end, *org_bins, *article_ids],
        )
        rows = cur.fetchall()

    groups_out: dict[str, dict[str, float | int]] = {}
    total_fact = 0.0
    line_count = 0
    for art_id, amount, cnt in rows:
        name = id_to_name.get(bytes(art_id))
        if not name:
            continue
        amt = round(_as_float(amount), 2)
        n = int(cnt or 0)
        total_fact += amt
        line_count += n
        bucket = groups_out.setdefault(name, {"fact_total": 0.0, "docs": 0})
        bucket["fact_total"] = round(float(bucket["fact_total"]) + amt, 2)
        bucket["docs"] = int(bucket["docs"]) + n

    total_fact = round(total_fact, 2)
    used_order = [name for name in article_order if name in groups_out]
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "total_fact": total_fact,
        "groups": groups_out,
        "group_order": used_order,
        "counts": {
            "docs_included": line_count,
            "article_nodes": len(id_to_name),
        },
        "debug": {
            "status": "ok",
            "kpi_id": "TD-M3-FACT",
            "document": WRITEOFF,
            "sum_field": COL_WO_SUM,
            "period_start": p_start,
            "period_end": p_end,
            "root": ARTICLE_GROUP,
            "rule": (
                "fact = sum(Сумма) of posted bank write-offs "
                "whose DDS article is in subtree of Технический директор"
            ),
        },
    }


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    report: list[dict[str, Any]] = []
    for year, month in iter_months(start_period, end_period):
        fact_payload = compute_td_m3_fact_monthly(year, month, sql)
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
                "has_data": True,
                "values_unit": "руб.",
                "groups": fact_payload.get("groups") or {},
                "group_order": fact_payload.get("group_order") or [],
                "counts": fact_payload.get("counts") or {},
            }
        )
    return report


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "TD-M3 — бюджет затрат техдирекции (SQL)",
        f"Источник: {WRITEOFF} / {WRITEOFF_VT}.{COL_WO_SUM}, Posted",
        f"Контур: статьи ДДС, группа «{ARTICLE_GROUP}»",
        "KPI % = MIN(100; Факт/План·100)",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8} {'Строк':>8}",
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
        lines.append(f"По статьям ДДС ({ref['year']:04d}-{ref['month']:02d}):")
        order = ref.get("group_order") or sorted(ref["groups"].keys())
        for name in order:
            bucket = ref["groups"].get(name) or {}
            total = float(bucket.get("fact_total") or 0)
            docs = int(bucket.get("docs") or 0)
            if total == 0 and docs == 0:
                continue
            lines.append(f"  {money(total):>14}  {name} (строк={docs})")
        lines.append("")
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"td_m3_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"td_m3_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return txt_path, json_path


def build_td_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
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
            "kpi_id": "TD-M3",
            "source": "techdir.td_m3.sql",
            "plan_source": "TD_M3_PLAN_BY_MONTH_2026",
            "fact_source": (
                f"{WRITEOFF}.{COL_WO_SUM} posted write-offs, "
                f"DDS subtree of {ARTICLE_GROUP}"
            ),
            "root": ARTICLE_GROUP,
        },
    }


def run_check() -> int:
    print("Сверка TD-M3 факт · 2026 (списания по группе статей)")
    all_ok = True
    sql = SqlConnection()
    print(f"  Группа статей: {ARTICLE_GROUP}")

    for month, ref in sorted(REFERENCE_FACT_2026.items()):
        snap = compute_td_m3_fact_monthly(2026, month, sql)
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
