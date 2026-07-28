# -*- coding: utf-8 -*-
"""
План / факт «ТКП в SLA» для коммерческой службы за месяц.

Источник логики — запрос ВТ_ОпросныеЛистыОтработанные (мониторинг этапов ОЛ).

План = число отработанных ОЛ текущего периода
       (Отработан = Истина, ТекущийПериод = Истина).

Факт = из них с ЖизненныйЦиклОЛ <= 3
       (группа «1. Жизненный цикл до 3-х дней»).

Жизненный цикл (календарные дни «Пятидневка»):
  ДниСНачалаГода(ДатаЗавершенияФакт) - ДниСНачалаГода(ДатаНачала)
  + 247 * (ГОД(факт) - ГОД(начала))

Дата начала: в рабочей БД регистр ВерсииОбъектов пуст,
поэтому используется Дата документа ОЛ (_Date_Time).

Отработанный ОЛ: есть факт завершения на точке
  ФормированиеТКП (или АннулированиеОЛ при статусе Аннулирован).

Отдел: Подразделение пользователя-ответственного ОЛ.

Запуск:
  python calc_plan_fact_tkp_sla.py 2026-07
  python calc_plan_fact_tkp_sla.py 2026-07 --dept 96f96cb31113810e11f092f67587c178
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pyodbc

YEAR_OFFSET = 2000
DAYS_IN_CALENDAR_YEAR = 247
EMPTY_DATE = datetime(2001, 1, 1)

POINT_TKP = bytes.fromhex("8283ac1f6b05524d11e8de55314a18fa")  # Формирование ТКП
POINT_ANN = bytes.fromhex("8286ac1f6b05524d11e901de284f6e13")  # Аннулирование ОЛ
STATUS_ANN = bytes.fromhex("884c4dc6ff636bfa402f43efc860994a")  # Аннулирован
CALENDAR = bytes.fromhex("812d001e6711250911e76313d658bace")  # Пятидневка

# Карты заказа (опросные листы): tref, table, status, responsible
OL_DOCS: list[tuple[str, str, str, str]] = [
    ("000151e5", "_Document86501", "_Fld86703RRef", "_Fld86701RRef"),  # UFG
    ("0001b542", "_Document111938", "_Fld112011RRef", "_Fld111996RRef"),  # TFG
    ("0002040c", "_Document132108", "_Fld132312RRef", "_Fld132278RRef"),  # CFM
    ("0002332a", "_Document144170", "_Fld144359RRef", "_Fld144325RRef"),  # Плотномер
    ("00028b1a", "_Document166682", "_Fld166819RRef", "_Fld166788RRef"),  # UFGH
    ("00029f72", "_Document171890", "_Fld173318RRef", "_Fld173264RRef"),  # UFL
]

COMMERCIAL_DEPTS: list[tuple[str, str]] = [
    ("Отдел по работе с ПАО Газпром", "80da001e6711250911e49f9cbd7b5184"),
    ("Отдел дилерских продаж", "96f96cb31113810e11f092f67587c178"),
    ("Отдел по работе с ключевыми клиентами", "8523ac1f6b05524d11eb67b6639ec87b"),
    ("Отдел продаж эталонного оборудования и услуг", "80d6001e6711250911e4810f34497ef7"),
    ("Отдел внешнеэкономической деятельности", "8283ac1f6b05524d11e8e40149480c10"),
    ("Отдел продаж БМИ", "93d36cb31113810e11ee37a59edaa7d4"),
]

FACT_ORDER = [n for n, _ in COMMERCIAL_DEPTS]
OUT_DIR = Path(__file__).resolve().parent


def connect() -> pyodbc.Connection:
    for driver in (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ):
        try:
            cn = pyodbc.connect(
                f"Driver={{{driver}}};Server=localhost;Database=erp_pm;"
                "Trusted_Connection=yes;TrustServerCertificate=yes;",
                autocommit=True,
            )
            cn.timeout = 0
            cur = cn.cursor()
            cur.execute("SET LOCK_TIMEOUT 600000")
            cur.close()
            return cn
        except Exception:
            continue
    raise RuntimeError("Не найден ODBC-драйвер SQL Server")


def to_1c_dt(d: date) -> datetime:
    return datetime(d.year + YEAR_OFFSET, d.month, d.day)


def parse_month(s: str) -> tuple[int, int]:
    parts = s.strip().split("-")
    if len(parts) != 2:
        raise ValueError("Ожидается месяц в формате YYYY-MM")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError("Месяц должен быть 01..12")
    return y, m


def pct(fact: float, plan: float) -> str:
    if not plan:
        return "—"
    return f"{fact / plan * 100:.1f}%"


def processed_ol_sql() -> tuple[str, list]:
    """UNION of processed OLs across document types. Placeholders for p0,p1 per doc."""
    parts: list[str] = []
    # params filled by caller in same order
    for tref, table, status, resp in OL_DOCS:
        parts.append(
            f"""
            SELECT DISTINCT
              m._RecorderRRef AS ol,
              d.{resp} AS resp,
              d._Date_Time AS life_dt,
              m._Fld127623 AS fact_dt
            FROM _AccumRg127619 m WITH (NOLOCK)
            INNER JOIN {table} d WITH (NOLOCK) ON d._IDRRef = m._RecorderRRef
            WHERE m._RecorderTRef = ?
              AND m._Active = 0x01
              AND d._Date_Time >= ? AND d._Date_Time < ?
              AND m._Fld127623 > ?
              AND (
                (d.{status} = ? AND m._Fld127620RRef = ?)
                OR (d.{status} <> ? AND m._Fld127620RRef = ?)
              )
            """
        )
    return " UNION ALL ".join(parts), []


def doc_params(p0: datetime, p_next: datetime) -> list:
    params: list = []
    for tref, *_ in OL_DOCS:
        params.extend(
            [
                bytes.fromhex(tref),
                p0,
                p_next,
                EMPTY_DATE,
                STATUS_ANN,
                POINT_ANN,
                STATUS_ANN,
                POINT_TKP,
            ]
        )
    return params


def calc_by_dept(cur, p0: datetime, p_next: datetime) -> dict[str, tuple[int, int]]:
    union_sql, _ = processed_ol_sql()
    params = doc_params(p0, p_next)

    cur.execute("IF OBJECT_ID('tempdb..#depts') IS NOT NULL DROP TABLE #depts")
    cur.execute(
        "CREATE TABLE #depts (id binary(16) PRIMARY KEY, name nvarchar(255))"
    )
    for name, hx in COMMERCIAL_DEPTS:
        cur.execute(
            "INSERT INTO #depts(id, name) VALUES (?, ?)",
            bytes.fromhex(hx),
            name,
        )

    sql = f"""
    ;WITH x AS (
      {union_sql}
    ),
    y AS (
      SELECT DISTINCT ol, resp, life_dt, fact_dt FROM x
    ),
    z AS (
      SELECT
        y.ol,
        y.resp,
        CAST(ISNULL(c1._Fld45245, 0) AS int)
          - CAST(ISNULL(c0._Fld45245, 0) AS int)
          + {DAYS_IN_CALENDAR_YEAR} * (YEAR(y.fact_dt) - YEAR(y.life_dt)) AS life_days
      FROM y
      LEFT JOIN _InfoRg45240 c0 WITH (NOLOCK)
        ON c0._Fld45241RRef = ?
       AND c0._Fld45243 = CAST(CAST(y.life_dt AS date) AS datetime)
      LEFT JOIN _InfoRg45240 c1 WITH (NOLOCK)
        ON c1._Fld45241RRef = ?
       AND c1._Fld45243 = CAST(CAST(y.fact_dt AS date) AS datetime)
    )
    SELECT d.name,
           COUNT(*) AS plan_cnt,
           SUM(CASE WHEN z.life_days <= 3 THEN 1 ELSE 0 END) AS fact_cnt
    FROM z
    INNER JOIN _Reference366 u WITH (NOLOCK) ON u._IDRRef = z.resp
    INNER JOIN #depts d ON d.id = u._Fld10996RRef
    GROUP BY d.name
    """
    cur.execute(sql, *params, CALENDAR, CALENDAR)
    return {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in cur.fetchall()}


def calc_for_dept(
    cur, p0: datetime, p_next: datetime, dept: bytes
) -> tuple[int, int]:
    union_sql, _ = processed_ol_sql()
    params = doc_params(p0, p_next)
    sql = f"""
    ;WITH x AS (
      {union_sql}
    ),
    y AS (
      SELECT DISTINCT ol, resp, life_dt, fact_dt FROM x
    ),
    z AS (
      SELECT
        y.ol,
        y.resp,
        CAST(ISNULL(c1._Fld45245, 0) AS int)
          - CAST(ISNULL(c0._Fld45245, 0) AS int)
          + {DAYS_IN_CALENDAR_YEAR} * (YEAR(y.fact_dt) - YEAR(y.life_dt)) AS life_days
      FROM y
      LEFT JOIN _InfoRg45240 c0 WITH (NOLOCK)
        ON c0._Fld45241RRef = ?
       AND c0._Fld45243 = CAST(CAST(y.life_dt AS date) AS datetime)
      LEFT JOIN _InfoRg45240 c1 WITH (NOLOCK)
        ON c1._Fld45241RRef = ?
       AND c1._Fld45243 = CAST(CAST(y.fact_dt AS date) AS datetime)
    )
    SELECT COUNT(*),
           SUM(CASE WHEN z.life_days <= 3 THEN 1 ELSE 0 END)
    FROM z
    INNER JOIN _Reference366 u WITH (NOLOCK) ON u._IDRRef = z.resp
    WHERE u._Fld10996RRef = ?
    """
    cur.execute(sql, *params, CALENDAR, CALENDAR, dept)
    row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def dept_name(cur, dept_id: bytes) -> str:
    cur.execute(
        "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef = ?",
        dept_id,
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else dept_id.hex()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="План/факт ТКП в SLA за месяц")
    ap.add_argument("month", help="Месяц YYYY-MM")
    ap.add_argument("--dept", default=None, help="GUID подразделения (32 hex)")
    args = ap.parse_args(argv)

    y, m = parse_month(args.month)
    p0 = to_1c_dt(date(y, m, 1))
    p_next = to_1c_dt(date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    lines = [
        f"ТКП в SLA за {y}-{m:02d}",
        "План = отработанные ОЛ текущего периода",
        "Факт = из них с жизненным циклом ≤ 3 раб. дней (Пятидневка)",
        "Дата начала жизни ОЛ: _Date_Time документа (ВерсииОбъектов в БД пуст)",
        "",
    ]

    print(f"ТКП в SLA за {y}-{m:02d}")
    print("БД: erp_pm @ localhost\n")

    if args.dept:
        dept = bytes.fromhex(args.dept)
        name = dept_name(cur, dept)
        plan, fact = calc_for_dept(cur, p0, p_next, dept)
        print(f"Отдел: {name}")
        print(f"План: {plan}")
        print(f"Факт: {fact}")
        print(f"%:    {pct(fact, plan)}")
        lines += [
            f"Отдел: {name}",
            f"Подразделение_Key: {args.dept}",
            f"План: {plan}",
            f"Факт: {fact}",
            f"%: {pct(fact, plan)}",
        ]
        out = OUT_DIR / f"plan_fact_tkp_sla_{y}_{m:02d}_{args.dept[:8]}.txt"
        out.write_text("\n".join(lines), encoding="utf-8")
        print(f"\nОтчёт: {out}")
        cn.close()
        return 0

    by_dept = calc_by_dept(cur, p0, p_next)
    plan_total = sum(by_dept.get(n, (0, 0))[0] for n in FACT_ORDER)
    fact_total = sum(by_dept.get(n, (0, 0))[1] for n in FACT_ORDER)

    print("=" * 78)
    print(f"{'Отдел':<55} {'План':>8} {'Факт':>8} {'%':>8}")
    print("-" * 78)
    lines.append(f"{'Отдел':<55} {'План':>8} {'Факт':>8} {'%':>8}")
    lines.append("-" * 78)

    for name in FACT_ORDER:
        plan, fact = by_dept.get(name, (0, 0))
        print(f"{name:<55} {plan:>8} {fact:>8} {pct(fact, plan):>8}")
        lines.append(f"{name:<55} {plan:>8} {fact:>8} {pct(fact, plan):>8}")

    print("-" * 78)
    print(
        f"{'ИТОГО коммерческий директор':<55} "
        f"{plan_total:>8} {fact_total:>8} {pct(fact_total, plan_total):>8}"
    )
    lines += [
        "-" * 78,
        f"{'ИТОГО коммерческий директор':<55} "
        f"{plan_total:>8} {fact_total:>8} {pct(fact_total, plan_total):>8}",
        "",
        f"План итого: {plan_total}",
        f"Факт итого: {fact_total}",
    ]

    out = OUT_DIR / f"plan_fact_tkp_sla_{y}_{m:02d}.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nОтчёт сохранён: {out}")
    cn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        raise
