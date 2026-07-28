"""
QD-M4 — ФОТ подразделения в пределах лимита (директор по качеству).

Эталон (OData): DashboardBack/qualdir/qd_m4.py + qd_m4_fact.py
  → getkpi.fot_quality_seven_depts.compute_seven_depts_register26_fot_monthly
  AccountingRegister_Хозрасчетный / RecordsWithExtDimensions
  дебет сч. 26, Сумма, сторно инвертируется.

План — константы QD_M4_PLAN_BY_MONTH_2026 (руб./мес.).
Факт — Σ дебетовых оборотов сч. 26 по 2 статьям (п. 4.2) по семи п/п
контура качества и их поддеревьям в структуре предприятия.

SQL (erp_pm):
  _AccRg2005          — движения Хозрасчётный
  _Acc25              — план счетов (код 26)
  _Reference513       — Catalog_СтруктураПредприятия
  _Chrc1945           — ChartOfCharacteristicTypes_СтатьиРасходов
  _Fld2010            — Сумма
  _Fld2017            — Сторно (0x01 → знак −)
  _Fld2008DtRRef      — ПодразделениеDr
  _ValueDt1_RRRef     — субконто «Статья расходов»
  Период в SQL = календарный год + 2000 (май 2026 → 4026-05).

Сверка с кэшем OData: январь 2026 fact = 482_740.08.

Использование:
  python qualdir/qd_m4.py
  python qualdir/qd_m4.py 2026
  python qualdir/qd_m4.py 2026-05
  python qualdir/qd_m4.py 2026-01 2026-05
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

from sql_connection import SqlConnection

# Плановый ФОТ контура качества, 2026, руб./мес. (сумма трёх строк по столбцу месяца).
QD_M4_PLAN_BY_MONTH_2026: dict[int, int] = {
    1: 511_958 + 507_692 + 61_890,
    2: 508_161 + 416_331 + 100_169,
    3: 525_100 + 964_361 + 112_724,
    4: 649_510 + 1_154_788 + 127_162,
    5: 532_549 + 1_143_118 + 132_887,
    6: 570_589 + 1_429_682 + 160_353,
    7: 549_005 + 1_406_250 + 158_705,
    8: 634_292 + 1_049_684 + 196_695,
    9: 537_874 + 1_293_779 + 184_608,
    10: 525_597 + 1_096_186 + 170_822,
    11: 522_884 + 919_101 + 112_025,
    12: 574_639 + 1_446_782 + 95_580,
}

# Две статьи п. 4.2 (как FOT_SPEC_ARTICLES в fot_techdir_fact).
FOT_SPEC_ARTICLES: tuple[str, ...] = (
    "Оплата труда (26 сч) НПО АУП!",
    "Страховые взносы (26 сч) НПО АУП!",
)
SALARY_ARTICLE = FOT_SPEC_ARTICLES[0]

# Семь подразделений контура качества + алиасы поиска.
SEVEN_DEPTS_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
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
SEVEN_DEPTS_GROUP_ORDER = [t[0] for t in SEVEN_DEPTS_FOT_SPEC]

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
    return round(fact / plan * 100, 2)


def as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in QD_M4_PLAN_BY_MONTH_2026:
        return float(QD_M4_PLAN_BY_MONTH_2026[month])
    return None


def fetch_account_26(cur) -> bytes:
    cur.execute(f"SELECT _IDRRef FROM dbo.[{TBL_ACC}] WHERE _Code = N'26'")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Счёт 26 не найден в _Acc25")
    return bytes(row[0])


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


def resolve_quality_department_map(cur) -> tuple[dict[bytes, str], dict[str, str]]:
    """id подразделения → карточка ФОТ; display → фактическое имя в структуре."""
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

    by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_norm.setdefault(normalize_name(row["desc"]), []).append(row)

    roots: dict[str, bytes] = {}
    labels: dict[str, str] = {}
    for display, aliases in SEVEN_DEPTS_FOT_SPEC:
        found = None
        for alias in (display,) + aliases:
            found = _pick_best(by_norm.get(normalize_name(alias), []))
            if found:
                break
        if not found:
            alias_norm = normalize_name(display)
            contains = [r for r in rows if alias_norm and alias_norm in normalize_name(r["desc"])]
            found = _pick_best(contains)
        if not found:
            raise RuntimeError(f"Подразделение не найдено в {TBL_STRUCT}: {display}")
        roots[display] = found["id"]
        labels[display] = found["desc"]

    children: dict[bytes, list[bytes]] = {}
    for row in rows:
        if row["marked"] or not row["parent"]:
            continue
        children.setdefault(row["parent"], []).append(row["id"])

    id_to_group: dict[bytes, str] = {}
    for display, root_id in roots.items():
        stack = [root_id]
        seen: set[bytes] = set()
        while stack:
            cur_id = stack.pop()
            if cur_id in seen:
                continue
            seen.add(cur_id)
            id_to_group[cur_id] = display
            stack.extend(children.get(cur_id, []))

    return id_to_group, labels


def calc_qd_m4_fot_month(year: int, month: int, sql: SqlConnection | None = None) -> dict[str, Any]:
    """Факт ФОТ контура качества за календарный месяц + план из констант."""
    own_sql = sql is None
    sql = sql or SqlConnection()
    p_start, p_end = sql_period_bounds(year, month)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        acc26 = fetch_account_26(cur)
        articles = fetch_article_ids(cur)
        id_to_group, labels = resolve_quality_department_map(cur)

        dept_ids = list(id_to_group.keys())
        art_ids = list(articles.keys())
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
            WHERE r._AccountDtRRef = ?
              AND r._Period >= ? AND r._Period < ?
              AND r._Active = 0x01
              AND r._Fld2008DtRRef IN ({",".join("?" * len(dept_ids))})
              AND r._ValueDt1_RRRef IN ({",".join("?" * len(art_ids))})
            GROUP BY r._Fld2008DtRRef, a._Description
            """,
            [acc26, p_start, p_end, *dept_ids, *art_ids],
        )
        rows = cur.fetchall()

    groups_out: dict[str, dict[str, float]] = {
        name: {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
        for name in SEVEN_DEPTS_GROUP_ORDER
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
        if article == SALARY_ARTICLE:
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
            "kpi_id": "QD-M4",
            "component": "qualdir.qd_m4.calc_qd_m4_fot_month",
            "sql_tables": {
                "register": TBL_RG,
                "accounts": TBL_ACC,
                "structure": TBL_STRUCT,
                "cost_articles": TBL_ARTICLES,
            },
            "sql_period": {"start": p_start, "end": p_end, "year_offset": YEAR_OFFSET},
            "account": "26",
            "cost_articles": list(FOT_SPEC_ARTICLES),
            "departments": list(SEVEN_DEPTS_GROUP_ORDER),
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
        rows.append(calc_qd_m4_fot_month(year, month, sql=sql))
    return rows


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "ФОТ подразделения в пределах лимита · контур качества (QD-M4 / SQL)",
        f"Источник: {TBL_RG} + {TBL_STRUCT} + {TBL_ARTICLES}, сч. 26, 2 статьи АУП, 7 п/п",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8}",
    ]
    for row in rows:
        plan = row.get("plan")
        fact = row.get("fact")
        pct = row.get("kpi_pct")
        plan_s = f"{plan:,.2f}" if plan is not None else "—"
        fact_s = f"{fact:,.2f}" if fact is not None else "—"
        pct_s = f"{pct:.2f}" if pct is not None else "—"
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{plan_s:>14} "
            f"{fact_s:>14} "
            f"{pct_s:>8}"
        )
    lines.extend(
        [
            f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8}",
            "",
        ]
    )

    ref = rows[-1] if rows else None
    if ref and ref.get("groups"):
        lines.append(f"По подразделениям ({ref['year']:04d}-{ref['month']:02d}):")
        for name in SEVEN_DEPTS_GROUP_ORDER:
            bucket = ref["groups"].get(name) or {}
            total = float(bucket.get("fact_total") or 0)
            if total:
                lines.append(f"  {total:>14,.2f}  {name}")
        lines.append("")
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]]) -> Path:
    output_path = SCRIPT_DIR / f"qd_m4_{period_slug}.txt"
    output_path.write_text(format_report(rows), encoding="utf-8-sig")
    return output_path


def build_qd_m4_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
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
        has_data = plan is not None and fact is not None
        item: dict[str, Any] = {
            "month": row["month"],
            "year": row["year"],
            "month_name": MONTH_NAMES[row["month"]].lower(),
            "plan": plan,
            "fact": fact,
            "kpi_pct": row.get("kpi_pct") if has_data else None,
            "has_data": has_data,
            "groups": dict(row.get("groups") or {}),
        }
        if plan is not None:
            item["values_unit"] = "руб."
        monthly_rows.append(item)

    ref_row = next(
        (item for item in monthly_rows if item["month"] == ref_m),
        monthly_rows[-1] if monthly_rows else None,
    )
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("plan") is not None else None,
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
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            **({"values_unit": "руб."} if ref_row and ref_row.get("has_data") else {}),
        },
        "debug": {
            "status": "ok" if any(item.get("has_data") for item in monthly_rows) else "no_data",
            "kpi_id": "QD-M4",
            "source": "qualdir.qd_m4.sql",
            "plan_source": "QD_M4_PLAN_BY_MONTH_2026",
            "fact_source": "Хозрасчётный сч.26, 7 п/п + поддеревья, 2 статьи АУП",
            "rows_by_month": [
                {
                    "month": f"{r['year']:04d}-{r['month']:02d}",
                    "plan": r.get("plan"),
                    "fact": r.get("fact"),
                    "kpi_pct": r.get("kpi_pct"),
                }
                for r in rows
            ],
        },
    }


def main() -> None:
    try:
        start_period, end_period, period_slug = parse_period_args()
        rows = build_monthly_report(start_period, end_period)
        report = format_report(rows)
        print(report)
        output_path = save_report(period_slug, rows)
        print(f"Отчёт сохранён: {output_path}")
        # удобный JSON для сверки одного месяца
        if start_period == end_period:
            print(json.dumps(rows[0], ensure_ascii=False, indent=2, default=str))
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

QD_M4_YTD_CACHE_PREFIX = "qualdir_qd_m4_ytd"
QD_M4_YTD_DISK_TAG = "qualdir_qd_m4_ytd_payload_sql_v1"
QD_M4_YTD_DISK_VERSION = 10


def qd_m4_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M4_YTD_CACHE_PREFIX, ry, rm)


def get_qd_m4_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M4_YTD_CACHE_PREFIX,
        source_tag=QD_M4_YTD_DISK_TAG,
        version=QD_M4_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m4_sql",
        compute_fn=lambda y, m: build_qd_m4_payload(y, m),
        kpi_id="QD-M4",
    )
