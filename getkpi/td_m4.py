"""
TD-M4 — ФОТ технического директора в пределах лимита.

Методика факта (по ОСВ контура «ТЕХНИЧЕСКИЙ ДИРЕКТОР»):
  • сч. 26 — статьи АУП НПО + ГАРАНТИИ;
  • сч. 25 — статьи НПО и АЛМАЗ;
  • подразделения — карточки контура (вкл. корень «ТЕХНИЧЕСКИЙ ДИРЕКТОР»)
    и узлы, мапящиеся к ближайшей карточке.

План — константы TD_M4_FOT_PLAN_BY_MONTH_2026 (руб./мес.).

SQL (erp_pm):
  _AccRg2005          — движения Хозрасчётный
  _Acc25              — план счетов (коды 25, 26)
  _Reference513       — Catalog_СтруктураПредприятия
  _Chrc1945           — ChartOfCharacteristicTypes_СтатьиРасходов
  _Fld2010            — Сумма
  _Fld2017            — Сторно (0x01 → знак −)
  _Fld2008DtRRef      — ПодразделениеDr
  _ValueDt1_RRRef     — субконто «Статья расходов»
  Период в SQL = календарный год + 2000.

Сверка ОСВ май 2026: 6_037_063.74
  (выделенное на скринах 6_030_382.14 + ГАРАНТИИ Отдела техподдержки 6_681.60).

Использование:
  python techdir/td_m4.py
  python techdir/td_m4.py --check
  python techdir/td_m4.py 2026
  python techdir/td_m4.py 2026-05
  python techdir/td_m4.py 2026-01 2026-07
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

# План ФОТ 2026: янв–май — PLANNED_FOT_TARGET_2026 (fot_techdir_plan),
# июнь–июль — кэш techdir_m4_monthly_2026_*.json.
TD_M4_FOT_PLAN_BY_MONTH_2026: dict[int, float] = {
    1: 8_426_198,
    2: 8_555_629,
    3: 9_649_847,
    4: 10_358_694,
    5: 9_899_562,
    6: 10_292_249.02,
    7: 10_175_176.74,
}

# Статьи со скринов ОСВ (сч. 26 АУП/ГАРАНТИИ + сч. 25 НПО/АЛМАЗ).
FOT_SPEC_ARTICLES: tuple[str, ...] = (
    "Оплата труда (26 сч) НПО АУП!",
    "Страховые взносы (26 сч) НПО АУП!",
    "Оплата труда ГАРАНТИИ (26 сч) пр-во НПО!",
    "Страховые взносы ГАРАНТИИ (26 сч) НПО!",
    "Оплата труда (25 сч) НПО!",
    "Страховые взносы (25 сч) НПО!",
    "Оплата труда (сч 25) АЛМАЗ!",
    "Страховые взносы (сч 25) АЛМАЗ!",
)
SALARY_ARTICLES = frozenset(a for a in FOT_SPEC_ARTICLES if a.startswith("Оплата труда"))
FOT_ACCOUNTS: tuple[str, ...] = ("25", "26")

# Карточки контура техдиректора (+ корень, на который падают начисления АУП).
TECHDIR_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    (
        "ТЕХНИЧЕСКИЙ ДИРЕКТОР",
        ("технический директор",),
    ),
    ("Эксплуатационная служба", ("эксплуатационная служба",)),
    ("Электрик/энергетик", ("электрик/энергетик", "электрик энергетик")),
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
    (
        "Лаборатория неразрушающего контроля",
        ("лаборатория неразрушающего контроля",),
    ),
    (
        "Отдел управления несоответствиями",
        ("отдел управления несоответствиями",),
    ),
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    ("АХО", ("ахо",)),
    (
        "Зам. технического директора по сервису",
        (
            "зам. технического директора по сервису",
            "зам технического директора по сервису",
            "заместитель технического директора по сервису",
        ),
    ),
    (
        "Отдел по работе с рекламациями",
        ("отдел по работе с рекламациями",),
    ),
    ("Отдел технической поддержки", ("отдел технической поддержки",)),
    ("Сервисная служба", ("сервисная служба",)),
    (
        "Отдел сервисного обслуживания",
        ("отдел сервисного обслуживания",),
    ),
    ("Служба технического директора", ("служба технического директора",)),
    ("Сектор качества разработки", ("сектор качества разработки",)),
    (
        "Сектор разработки тех. Решений",
        (
            "сектор разработки тех. решений",
            "сектор разработки тех решений",
        ),
    ),
    (
        "Сектор промышленной безопасности",
        ("сектор промышленной безопасности",),
    ),
]
TECHDIR_GROUP_ORDER = [t[0] for t in TECHDIR_FOT_SPEC]

# Эталон факта по ОСВ (май/июнь проверены скринами).
REFERENCE_FACT_2026: dict[int, float] = {
    5: 6_037_063.74,
    6: 6_509_701.90,
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

YEAR_OFFSET = 2000
SCRIPT_DIR = Path(__file__).resolve().parent
ROUND_TOLERANCE = 0.05

TBL_ACC = "_Acc25"
TBL_RG = "_AccRg2005"
TBL_STRUCT = "_Reference513"
TBL_ARTICLES = "_Chrc1945"


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


def sql_period_bounds(year: int, month: int) -> tuple[str, str]:
    y = year + YEAR_OFFSET
    start = f"{y}{month:02d}01"
    if month == 12:
        end = f"{y + 1}0101"
    else:
        end = f"{y}{month + 1:02d}01"
    return start, end


def normalize_name(value: str | None) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    text = text.lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return " ".join(text.split())


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 1)


def as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.2f}".replace(",", " ")


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in TD_M4_FOT_PLAN_BY_MONTH_2026:
        return float(TD_M4_FOT_PLAN_BY_MONTH_2026[month])
    return None


def fetch_accounts(cur) -> dict[str, bytes]:
    cur.execute(
        f"""
        SELECT _IDRRef, _Code
        FROM dbo.[{TBL_ACC}]
        WHERE _Code IN ({",".join("?" * len(FOT_ACCOUNTS))})
        """,
        list(FOT_ACCOUNTS),
    )
    out = {str(row[1]): bytes(row[0]) for row in cur.fetchall()}
    missing = [code for code in FOT_ACCOUNTS if code not in out]
    if missing:
        raise RuntimeError(f"Счета не найдены в {TBL_ACC}: {missing}")
    return out


def fetch_article_ids(cur) -> dict[bytes, str]:
    cur.execute(
        f"""
        SELECT _IDRRef, _Description
        FROM dbo.[{TBL_ARTICLES}]
        WHERE _Description IN ({",".join("?" * len(FOT_SPEC_ARTICLES))})
        """,
        list(FOT_SPEC_ARTICLES),
    )
    out = {bytes(row[0]): row[1] for row in cur.fetchall()}
    missing = [name for name in FOT_SPEC_ARTICLES if name not in out.values()]
    if missing:
        raise RuntimeError(f"Не найдены статьи затрат: {missing}")
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


def resolve_techdir_department_map(cur) -> tuple[dict[bytes, str], dict[str, str]]:
    """id подразделения → карточка ФОТ (ближайший предок из списка)."""
    cur.execute(
        f"""
        SELECT _IDRRef, _Description, _ParentIDRRef, _Marked
        FROM dbo.[{TBL_STRUCT}] WITH (NOLOCK)
        """
    )
    rows: list[dict[str, Any]] = []
    for idr, desc, parent, marked in cur.fetchall():
        rows.append(
            {
                "id": bytes(idr),
                "desc": desc or "",
                "parent": bytes(parent) if parent else None,
                "marked": bytes(marked) != b"\x00" if marked is not None else False,
            }
        )

    by_id = {row["id"]: row for row in rows}
    by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_norm.setdefault(normalize_name(row["desc"]), []).append(row)

    roots: dict[str, bytes] = {}
    labels: dict[str, str] = {}
    for display, aliases in TECHDIR_FOT_SPEC:
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
            raise RuntimeError(f"Подразделение не найдено в {TBL_STRUCT}: {display}")
        roots[display] = found["id"]
        labels[display] = found["desc"]

    root_ids = {root_id: display for display, root_id in roots.items()}

    # Как build_struct_key_to_fot_group: идём вверх до ближайшей карточки из списка.
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
            if not parent or parent == b"\x00" * 16:
                break
            cur_id = parent

    return id_to_group, labels


def calc_td_m4_fot_month(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
) -> dict[str, Any]:
    """Факт ФОТ контура технического директора за месяц + план."""
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        accounts = fetch_accounts(cur)
        articles = fetch_article_ids(cur)
        id_to_group, labels = resolve_techdir_department_map(cur)

        dept_ids = list(id_to_group.keys())
        art_ids = list(articles.keys())
        acc_ids = list(accounts.values())
        if not dept_ids:
            raise RuntimeError("Не найдены подразделения контура техдирекции")

        cur.execute(
            f"""
            SELECT
                r._Fld2008DtRRef AS dept_id,
                a._Description AS article,
                SUM(
                    CASE WHEN r._Fld2017 = 0x01 THEN -r._Fld2010 ELSE r._Fld2010 END
                ) AS amount
            FROM dbo.[{TBL_RG}] r
            JOIN dbo.[{TBL_ARTICLES}] a ON a._IDRRef = r._ValueDt1_RRRef
            WHERE r._AccountDtRRef IN ({",".join("?" * len(acc_ids))})
              AND r._Period >= ? AND r._Period < ?
              AND r._Active = 0x01
              AND r._Fld2008DtRRef IN ({",".join("?" * len(dept_ids))})
              AND r._ValueDt1_RRRef IN ({",".join("?" * len(art_ids))})
            GROUP BY r._Fld2008DtRRef, a._Description
            """,
            [*acc_ids, p_start, p_end, *dept_ids, *art_ids],
        )
        rows = cur.fetchall()

    groups_out: dict[str, dict[str, float]] = {
        name: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
        for name in TECHDIR_GROUP_ORDER
    }
    by_article: dict[str, float] = {}
    total_fact = 0.0
    total_salary = 0.0
    total_insurance = 0.0

    for dept_id, article, amount in rows:
        amt = round(as_float(amount), 2)
        group = id_to_group.get(bytes(dept_id))
        if not group:
            continue
        total_fact += amt
        by_article[article] = round(by_article.get(article, 0.0) + amt, 2)
        bucket = groups_out[group]
        if article in SALARY_ARTICLES:
            bucket["fact_salary"] = round(bucket["fact_salary"] + amt, 2)
            total_salary += amt
        else:
            bucket["fact_insurance"] = round(bucket["fact_insurance"] + amt, 2)
            total_insurance += amt
        bucket["fact_total"] = round(bucket["fact_total"] + amt, 2)

    total_fact = round(total_fact, 2)
    plan = plan_for_month(year, month)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES.get(month, str(month)),
        "plan": plan,
        "fact": total_fact,
        "total_fact": total_fact,
        "fact_salary": round(total_salary, 2),
        "fact_insurance": round(total_insurance, 2),
        "kpi_pct": kpi_pct(plan, total_fact),
        "groups": groups_out,
        "article_totals": by_article,
        "has_data": plan is not None,
        "values_unit": "руб.",
        "debug": {
            "status": "ok",
            "kpi_id": "TD-M4",
            "component": "techdir.td_m4.calc_td_m4_fot_month",
            "sql_tables": {
                "register": TBL_RG,
                "accounts": TBL_ACC,
                "structure": TBL_STRUCT,
                "cost_articles": TBL_ARTICLES,
            },
            "sql_period": {"start": p_start, "end": p_end, "year_offset": YEAR_OFFSET},
            "accounts": list(FOT_ACCOUNTS),
            "cost_articles": list(FOT_SPEC_ARTICLES),
            "departments": list(TECHDIR_GROUP_ORDER),
            "structure_labels": labels,
            "department_nodes": len(id_to_group),
        },
    }


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    rows: list[dict[str, Any]] = []
    for year, month in iter_months(start_period, end_period):
        rows.append(calc_td_m4_fot_month(year, month, sql=sql))
    return rows


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "ФОТ подразделения в пределах лимита · технический директор (TD-M4 / SQL)",
        f"Источник: {TBL_RG} + {TBL_STRUCT} + {TBL_ARTICLES}, "
        f"сч. {'/'.join(FOT_ACCOUNTS)}, статьи АУП/ГАРАНТИИ/НПО/АЛМАЗ",
        f"Контур: {', '.join(TECHDIR_GROUP_ORDER)}",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8}",
    ]
    for row in rows:
        plan = row.get("plan")
        fact = row.get("fact")
        pct = row.get("kpi_pct")
        plan_s = f"{plan:,.2f}".replace(",", " ") if plan is not None else "—"
        fact_s = f"{fact:,.2f}".replace(",", " ") if fact is not None else "—"
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{plan_s:>14} "
            f"{fact_s:>14} "
            f"{pct_s:>8}"
        )

    plan_sum = sum(float(r["plan"]) for r in rows if r.get("plan") is not None)
    fact_sum = sum(float(r["fact"]) for r in rows if r.get("fact") is not None)
    lines.extend(
        [
            f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8}",
            f"{'ИТОГО':<10} {money(plan_sum):>14} {money(fact_sum):>14} {'':>8}",
            "",
        ]
    )

    ref = rows[-1] if rows else None
    if ref and ref.get("groups"):
        lines.append(f"По подразделениям ({ref['year']:04d}-{ref['month']:02d}):")
        for name in TECHDIR_GROUP_ORDER:
            bucket = ref["groups"].get(name) or {}
            total = float(bucket.get("fact_total") or 0)
            salary = float(bucket.get("fact_salary") or 0)
            insurance = float(bucket.get("fact_insurance") or 0)
            lines.append(
                f"  {money(total):>14}  {name} "
                f"(от={money(salary)}, св={money(insurance)})"
            )
        lines.append("")
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"td_m4_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"td_m4_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return txt_path, json_path


def build_td_m4_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    now = date.today()
    ref_y = year or now.year
    ref_m = month or (now.month - 1 if now.month > 1 else 12)
    if month is None and now.month == 1 and year is None:
        ref_y = now.year - 1
        ref_m = 12

    rows = build_monthly_report((ref_y, 1), (ref_y, ref_m))
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        plan = row.get("plan")
        fact = row.get("fact")
        has_data = plan is not None or fact is not None
        monthly_rows.append(
            {
                "month": row["month"],
                "year": row["year"],
                "month_name": MONTH_NAMES[row["month"]],
                "plan": plan,
                "fact": fact,
                "kpi_pct": row.get("kpi_pct"),
                "has_data": has_data,
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
            "kpi_id": "TD-M4",
            "source": "techdir.td_m4.sql",
            "plan_source": "TD_M4_FOT_PLAN_BY_MONTH_2026",
            "fact_source": (
                "Хозрасчётный сч.25/26, контур техдирекции, "
                "статьи АУП + ГАРАНТИИ + НПО/АЛМАЗ"
            ),
            "departments_in_fact_sum": list(TECHDIR_GROUP_ORDER),
        },
    }


def run_check() -> int:
    print("Сверка ФОТ факт · TD-M4 · 2026 (ОСВ / REFERENCE)")
    all_ok = True
    sql = SqlConnection()
    for month in sorted(REFERENCE_FACT_2026):
        payload = calc_td_m4_fot_month(2026, month, sql=sql)
        fact = float(payload["fact"] or 0)
        reference = REFERENCE_FACT_2026[month]
        diff = fact - reference
        mark = "OK" if abs(diff) <= ROUND_TOLERANCE else "РАСХОЖДЕНИЕ"
        if mark != "OK":
            all_ok = False
        print(
            f"  {MONTH_NAMES[month]}: fact={money(fact)} "
            f"ref={money(reference)} Δ={money(diff)} ({mark})"
        )
    return 0 if all_ok else 2


def main() -> None:
    try:
        if "--check" in sys.argv[1:]:
            sys.exit(run_check())

        start_period, end_period, period_slug = parse_period_args()
        rows = build_monthly_report(start_period, end_period)
        report = format_report(rows)
        print(report)
        txt_path, json_path = save_report(period_slug, rows, report)
        print(f"Отчёт сохранён: {txt_path}")
        print(f"JSON: {json_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
