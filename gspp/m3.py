"""ГСП-M3 — ФОТ подразделения в пределах лимита (факт из MSSQL-дампа 1С).

Эталон (OData): DashboardBack/gspp/m3.py → getkpi.fot_techdir_fact
  AccountingRegister_Хозрасчетный / RecordsWithExtDimensions
  дебет сч. 26, Сумма, сторно инвертируется.

SQL-маппинг (erp_pm):
  _AccRg2005          — движения Хозрасчётный
  _Acc25              — план счетов (код 26)
  _Reference513       — Catalog_СтруктураПредприятия
  _Chrc1945           — ChartOfCharacteristicTypes_СтатьиРасходов
  _Fld2010            — Сумма
  _Fld2017            — Сторно (0x01 → знак −)
  _Fld2008DtRRef      — ПодразделениеDr
  _ValueDt1_RRRef     — субконто «Статья расходов»
  Период в SQL = календарный год + 2000 (май 2026 → 4026-05).

Факт ФОТ = Σ дебетовых оборотов сч. 26 по 4 статьям по ГСПП и дочерним п/п.
Сверка со скрином ОСВ 26 за май 2026: 3_800_482.08.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sql_connection import SqlConnection  # noqa: E402

# Плановые значения — как в DashboardBack/gspp/m3.py
GSPP_M3_PLAN_TARGET_2026: dict[int, int] = {
    1: 3_935_148,
    2: 4_007_859,
    3: 4_083_612,
    4: 4_349_149,
    5: 3_965_452,
    6: 4_123_100,
    7: 4_278_019,
    8: 4_293_323,
    9: 4_194_200,
    10: 4_313_961,
    11: 4_091_539,
    12: 4_124_167,
}

FOT_ARTICLES: tuple[str, ...] = (
    "Оплата труда (26 сч) НПО АУП!",
    "Оплата труда (26 сч) пр-во ПРОЕКТЫ!",
    "Страховые взносы (26 сч) НПО АУП!",
    "Страховые взносы (26 сч) НПО пр-во ПРОЕКТЫ!",
)

SALARY_ARTICLES = frozenset(
    {
        "Оплата труда (26 сч) НПО АУП!",
        "Оплата труда (26 сч) пр-во ПРОЕКТЫ!",
    }
)

# Корень структуры; сектора подтягиваются как дети (см. скрин ОСВ).
GSPP_ROOT_NAME = "ГСПП"
GSPP_SECTOR_ALIASES: tuple[str, ...] = (
    "Сектор сопровождения продаж",
    "Сектор сопровождения производства",
    "Сектор сопровождения производства и продаж",
    "Сектор сопровождения продаж (производство №1)",
    "Сектор сопровождения продаж (производство №2)",
    "Сектор доработки и улучшения продукции",
    "Сектор постановки на производство несерийной продукции",
)

MONTH_RU = {
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

# 1С в SQL Server хранит даты со сдвигом +2000 лет.
_1C_YEAR_OFFSET = 2000

TBL_ACC = "_Acc25"
TBL_RG = "_AccRg2005"
TBL_STRUCT = "_Reference513"
TBL_ARTICLES = "_Chrc1945"


def _sql_period_bounds(year: int, month: int) -> tuple[str, str]:
    y = year + _1C_YEAR_OFFSET
    start = f"{y}{month:02d}01"
    if month == 12:
        end = f"{y + 1}0101"
    else:
        end = f"{y}{month + 1:02d}01"
    return start, end


def _last_full_month(today: date | None = None) -> tuple[int, int]:
    today = today or date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _fetch_account_26(cur) -> bytes:
    cur.execute(f"SELECT _IDRRef FROM dbo.[{TBL_ACC}] WHERE _Code = N'26'")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Счёт 26 не найден в _Acc25")
    return row[0]


def _fetch_article_ids(cur) -> dict[bytes, str]:
    cur.execute(
        f"""
        SELECT _IDRRef, _Description
        FROM dbo.[{TBL_ARTICLES}]
        WHERE _Description IN ({",".join("?" * len(FOT_ARTICLES))})
        """,
        list(FOT_ARTICLES),
    )
    out = {row[0]: row[1] for row in cur.fetchall()}
    missing = [name for name in FOT_ARTICLES if name not in out.values()]
    if missing:
        raise RuntimeError(f"Не найдены статьи затрат: {missing}")
    return out


def _resolve_gspp_department_ids(cur) -> dict[bytes, str]:
    """ГСПП + всё поддерево; плюс сектора по имени, если вдруг вне дерева."""
    cur.execute(
        f"SELECT _IDRRef, _Description FROM dbo.[{TBL_STRUCT}] WHERE _Description = ?",
        [GSPP_ROOT_NAME],
    )
    root = cur.fetchone()
    if not root:
        raise RuntimeError("Подразделение «ГСПП» не найдено в _Reference513")

    out: dict[bytes, str] = {root[0]: root[1]}
    frontier = [root[0]]
    while frontier:
        cur.execute(
            f"""
            SELECT _IDRRef, _Description
            FROM dbo.[{TBL_STRUCT}]
            WHERE _ParentIDRRef IN ({",".join("?" * len(frontier))})
              AND _Marked = 0x00
            """,
            frontier,
        )
        children = cur.fetchall()
        frontier = []
        for cid, cname in children:
            if cid not in out:
                out[cid] = cname
                frontier.append(cid)

    # На случай переименований / узлов вне дерева — добираем алиасы со скрина.
    cur.execute(
        f"""
        SELECT _IDRRef, _Description
        FROM dbo.[{TBL_STRUCT}]
        WHERE _Description IN ({",".join("?" * len(GSPP_SECTOR_ALIASES))})
          AND _Marked = 0x00
        """,
        list(GSPP_SECTOR_ALIASES),
    )
    for rid, name in cur.fetchall():
        out.setdefault(rid, name)
    return out


def calc_gspp_fot_fact(year: int, month: int, sql: SqlConnection | None = None) -> dict[str, Any]:
    """Факт ФОТ ГСПП за календарный месяц."""
    own_sql = sql is None
    sql = sql or SqlConnection()
    p_start, p_end = _sql_period_bounds(year, month)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        acc26 = _fetch_account_26(cur)
        articles = _fetch_article_ids(cur)
        depts = _resolve_gspp_department_ids(cur)

        art_ids = list(articles.keys())
        dept_ids = list(depts.keys())

        cur.execute(
            f"""
            SELECT
                d._Description AS dept,
                a._Description AS article,
                SUM(
                    CASE WHEN r._Fld2017 = 0x01 THEN -r._Fld2010 ELSE r._Fld2010 END
                ) AS amount
            FROM dbo.[{TBL_RG}] r
            JOIN dbo.[{TBL_STRUCT}] d ON d._IDRRef = r._Fld2008DtRRef
            JOIN dbo.[{TBL_ARTICLES}] a ON a._IDRRef = r._ValueDt1_RRRef
            WHERE r._AccountDtRRef = ?
              AND r._Period >= ? AND r._Period < ?
              AND r._Active = 0x01
              AND r._Fld2008DtRRef IN ({",".join("?" * len(dept_ids))})
              AND r._ValueDt1_RRRef IN ({",".join("?" * len(art_ids))})
            GROUP BY d._Description, a._Description
            ORDER BY d._Description, a._Description
            """,
            [acc26, p_start, p_end, *dept_ids, *art_ids],
        )
        rows = cur.fetchall()

    by_dept: dict[str, dict[str, float]] = {}
    by_article: dict[str, float] = {}
    total_fact = 0.0
    total_salary = 0.0
    total_insurance = 0.0

    for dept, article, amount in rows:
        amt = round(_as_float(amount), 2)
        total_fact += amt
        by_article[article] = round(by_article.get(article, 0.0) + amt, 2)
        bucket = by_dept.setdefault(
            dept, {"fact_salary": 0.0, "fact_insurance": 0.0, "fact_total": 0.0}
        )
        if article in SALARY_ARTICLES:
            bucket["fact_salary"] = round(bucket["fact_salary"] + amt, 2)
            total_salary += amt
        else:
            bucket["fact_insurance"] = round(bucket["fact_insurance"] + amt, 2)
            total_insurance += amt
        bucket["fact_total"] = round(bucket["fact_total"] + amt, 2)

    total_fact = round(total_fact, 2)
    plan = (
        float(GSPP_M3_PLAN_TARGET_2026[month])
        if year == 2026 and month in GSPP_M3_PLAN_TARGET_2026
        else None
    )

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU.get(month, str(month)),
        "plan": plan,
        "fact": total_fact,
        "fact_salary": round(total_salary, 2),
        "fact_insurance": round(total_insurance, 2),
        "kpi_pct": _kpi_pct(plan, total_fact),
        "groups": by_dept,
        "article_totals": by_article,
        "debug": {
            "status": "ok",
            "component": "gspp.gsp_m3.calc_gspp_fot_fact",
            "sql_tables": {
                "register": TBL_RG,
                "accounts": TBL_ACC,
                "structure": TBL_STRUCT,
                "cost_articles": TBL_ARTICLES,
            },
            "sql_period": {"start": p_start, "end": p_end, "year_offset": _1C_YEAR_OFFSET},
            "account": "26",
            "cost_articles": list(FOT_ARTICLES),
            "departments": sorted(depts.values()),
            "department_count": len(depts),
        },
    }


def get_gspp_m3(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    if year is None or month is None:
        y, m = _last_full_month()
        year = year or y
        month = month or m
    return calc_gspp_fot_fact(year, month)


# --- YTD-плитка для дашборда (gspp.views / getkpi.cache_manager) ---
GSPP_M3_CACHE_PREFIX = "gspp_m3_ytd"
GSPP_M3_DISK_TAG = "gspp_m3_fot_payload_sql_v1"
GSPP_M3_DISK_VERSION = 1


def _build_gspp_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    from getkpi.autoit.it_monthly_period import pick_fot_display_row
    from getkpi.kpi_periods import last_full_month as lfm

    today = date.today()
    if year is not None and month is not None:
        ref_y, ref_m = int(year), max(1, min(12, int(month)))
    else:
        ref_y, ref_m = lfm(today)
    if ref_y == today.year:
        ref_m = min(ref_m, today.month)

    monthly_rows: list[dict[str, Any]] = []
    for m in range(1, ref_m + 1):
        snap = calc_gspp_fot_fact(ref_y, m)
        plan = snap.get("plan")
        fact = snap.get("fact")
        has_data = plan is not None and fact is not None
        monthly_rows.append(
            {
                "month": m,
                "year": ref_y,
                "month_name": MONTH_RU[m].lower(),
                "plan": round(float(plan), 2) if plan is not None else None,
                "fact": round(float(fact), 2) if fact is not None else None,
                "kpi_pct": snap.get("kpi_pct") if has_data else None,
                "has_data": has_data,
                "values_unit": "руб.",
            }
        )

    display_row = pick_fot_display_row(monthly_rows, ref_m, ref_year=ref_y)
    display_m = int(display_row["month"]) if display_row and display_row.get("month") else ref_m
    with_data = [row for row in monthly_rows if row.get("has_data")]
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(display_row) if display_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": display_m,
            "month_name": MONTH_RU[display_m].lower(),
        },
        "ytd": {
            "total_plan": display_row.get("plan") if display_row else None,
            "total_fact": display_row.get("fact") if display_row else None,
            "kpi_pct": display_row.get("kpi_pct") if display_row else None,
            "months_with_data": len(with_data),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok" if with_data else "no_data",
            "kpi_id": "ГСП-M3",
            "plan_source": "GSPP_M3_PLAN_TARGET_2026",
            "fact_source": "gspp.m3.calc_gspp_fot_fact (SQL сч.26)",
        },
    }


def gspp_m3_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    from getkpi.kpi_periods import last_full_month as lfm
    from devdir import ytd_json_cache

    today = date.today()
    if year is not None and month is not None:
        ref_y, ref_m = int(year), max(1, min(12, int(month)))
    else:
        ref_y, ref_m = lfm(today)
    if ref_y == today.year:
        ref_m = min(ref_m, today.month)
    return ytd_json_cache.cache_path(GSPP_M3_CACHE_PREFIX, ref_y, ref_m)


def get_gspp_m3_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any] | None:
    from devdir import ytd_json_cache
    from getkpi.kpi_periods import last_full_month as lfm

    today = date.today()
    if year is not None and month is not None:
        ref_y, ref_m = int(year), max(1, min(12, int(month)))
    else:
        ref_y, ref_m = lfm(today)
    if ref_y == today.year:
        ref_m = min(ref_m, today.month)

    cache_path = gspp_m3_ytd_cache_path(ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict[str, Any] | None:
        payload = _build_gspp_m3_payload(year=ref_y, month=ref_m)
        if (payload.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                cache_path,
                payload,
                source_tag=GSPP_M3_DISK_TAG,
                version=GSPP_M3_DISK_VERSION,
            )
        return payload

    return ytd_json_cache.resolve_payload(
        cache_path,
        source_tag=GSPP_M3_DISK_TAG,
        version=GSPP_M3_DISK_VERSION,
        perpetual=perpetual,
        lock_key=f"gspp_m3_fot_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="ГСП-M3: факт ФОТ ГСПП из SQL.")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()

    payload = get_gspp_m3(year=args.year, month=args.month)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
