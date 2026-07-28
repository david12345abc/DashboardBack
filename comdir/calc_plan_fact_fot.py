# -*- coding: utf-8 -*-
"""
Факт ФОТ для коммерческой службы за месяц.

Источник: регистр бухгалтерии Хозрасчётный (_AccRg2005).

Условия:
  • Active = true
  • дебет счёта 44 / 44.01 / 44.02
  • ПодразделениеDr ∈ коммерческие отделы
    (ВЭД, ОПЭОиУ, БМИ, ОРКК, ОДП, Газпром, PR, тендерный)
  • статья затрат (ExtDimensionDr1 / ValueDt1) одна из четырёх:
      — Оплата труда (44 сч) возмещение НПО!
      — Оплата труда (44 сч) ТД НПО!
      — Страховые взносы (44.01) возмещение НПО!
      — Страховые взносы (44.01) ТД НПО!
  • Сумма = Сумма проводки; если Сторно — минус

Комдир: сумма по всем этим отделам.
Дочерний отдел: фильтр по GUID подразделения.

Запуск:
  python calc_plan_fact_fot.py 2026-07
  python calc_plan_fact_fot.py 2026-07 --dept 96f96cb31113810e11f092f67587c178
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pyodbc

YEAR_OFFSET = 2000

# Счета _Acc25
ACCOUNTS = [
    ("44", "812d001e6711250911e76250fb2bde54"),
    ("44.01", "812d001e6711250911e76250fb2bde55"),
    ("44.02", "812d001e6711250911e76250fb2bde56"),
]

# Статьи затрат (_Chrc1945)
ARTICLES = [
    ("Оплата труда (44 сч) возмещение НПО!", "854dac1f6b05524d11eb782a992a514f"),
    ("Оплата труда (44 сч) ТД НПО!", "82f3ac1f6b05524d11ea3767cc7c4aa6"),
    ("Страховые взносы (44.01) возмещение НПО!", "854dac1f6b05524d11eb782aa04a98f8"),
    ("Страховые взносы (44.01) ТД НПО!", "82f3ac1f6b05524d11ea37680360bf95"),
]

# Коммерческие отделы для ФОТ (8 шт.)
FOT_DEPTS: list[tuple[str, str]] = [
    ("Отдел внешнеэкономической деятельности", "8283ac1f6b05524d11e8e40149480c10"),
    ("Отдел продаж эталонного оборудования и услуг", "80d6001e6711250911e4810f34497ef7"),
    ("Отдел продаж БМИ", "93d36cb31113810e11ee37a59edaa7d4"),
    ("Отдел по работе с ключевыми клиентами", "8523ac1f6b05524d11eb67b6639ec87b"),
    ("Отдел дилерских продаж", "96f96cb31113810e11f092f67587c178"),
    ("Отдел по работе с ПАО Газпром", "80da001e6711250911e49f9cbd7b5184"),
    ("Сектор рекламы и PR", "93d36cb31113810e11ee37a495dfd1c6"),
    ("Тендерный офис", "8129cd2988c3db2d11e0d91b1c9f9419"),
]

FACT_ORDER = [n for n, _ in FOT_DEPTS]

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


def fmt(x) -> str:
    return f"{float(x or 0):,.2f}".replace(",", " ").replace(".", ",")


def parse_month(s: str) -> tuple[int, int]:
    parts = s.strip().split("-")
    if len(parts) != 2:
        raise ValueError("Ожидается месяц в формате YYYY-MM")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError("Месяц должен быть 01..12")
    return y, m


def load_temp(cur, table: str, rows: list[tuple[str, str]]) -> None:
    cur.execute(f"IF OBJECT_ID('tempdb..{table}') IS NOT NULL DROP TABLE {table}")
    cur.execute(
        f"CREATE TABLE {table} (id binary(16) PRIMARY KEY, name nvarchar(255))"
    )
    for name, hx in rows:
        cur.execute(
            f"INSERT INTO {table}(id, name) VALUES (?, ?)",
            bytes.fromhex(hx),
            name,
        )


def load_ids(cur, table: str, ids: list[str]) -> None:
    cur.execute(f"IF OBJECT_ID('tempdb..{table}') IS NOT NULL DROP TABLE {table}")
    cur.execute(f"CREATE TABLE {table} (id binary(16) PRIMARY KEY)")
    for hx in ids:
        cur.execute(f"INSERT INTO {table}(id) VALUES (?)", bytes.fromhex(hx))


def amount_expr() -> str:
    # Сторно (_Fld140878 = 0x01) → минус
    return """
        CASE WHEN r._Fld140878 = 0x01
             THEN -CAST(r._Fld2010 AS float)
             ELSE CAST(r._Fld2010 AS float) END
    """


def calc_fot_by_dept(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    load_temp(cur, "#depts", FOT_DEPTS)
    load_ids(cur, "#accs", [hx for _, hx in ACCOUNTS])
    load_ids(cur, "#arts", [hx for _, hx in ARTICLES])
    cur.execute(
        f"""
        SELECT d.name, SUM({amount_expr()}) AS Amt
        FROM _AccRg2005 r WITH (NOLOCK)
        INNER JOIN #depts d ON d.id = r._Fld2008DtRRef
        INNER JOIN #accs a ON a.id = r._AccountDtRRef
        INNER JOIN #arts t ON t.id = r._ValueDt1_RRRef
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
        GROUP BY d.name
        """,
        p0,
        p_next,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_fot_for_dept(
    cur, p0: datetime, p_next: datetime, dept: bytes
) -> float:
    load_ids(cur, "#accs", [hx for _, hx in ACCOUNTS])
    load_ids(cur, "#arts", [hx for _, hx in ARTICLES])
    cur.execute(
        f"""
        SELECT SUM({amount_expr()}) AS Amt
        FROM _AccRg2005 r WITH (NOLOCK)
        INNER JOIN #accs a ON a.id = r._AccountDtRRef
        INNER JOIN #arts t ON t.id = r._ValueDt1_RRRef
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
          AND r._Fld2008DtRRef = ?
        """,
        p0,
        p_next,
        dept,
    )
    row = cur.fetchone()
    return float(row[0] or 0)


def dept_name(cur, dept_id: bytes) -> str:
    cur.execute(
        "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef = ?",
        dept_id,
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else dept_id.hex()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Факт ФОТ коммерческой службы за месяц")
    ap.add_argument("month", help="Месяц YYYY-MM (например 2026-07)")
    ap.add_argument(
        "--dept",
        default=None,
        help="GUID подразделения (32 hex) — только этот отдел",
    )
    args = ap.parse_args(argv)

    y, m = parse_month(args.month)
    p0 = to_1c_dt(date(y, m, 1))
    p_next = to_1c_dt(date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    lines: list[str] = [
        f"Факт ФОТ за {y}-{m:02d}",
        "Источник: Хозрасчётный (_AccRg2005), дебет 44/44.01/44.02",
        "Статьи: оплата труда и страховые взносы (возмещение НПО / ТД НПО)",
        "",
    ]

    print(f"Факт ФОТ за {y}-{m:02d}")
    print("БД: erp_pm @ localhost\n")

    if args.dept:
        dept = bytes.fromhex(args.dept)
        name = dept_name(cur, dept)
        amt = calc_fot_for_dept(cur, p0, p_next, dept)
        print(f"Отдел: {name}")
        print(f"Подразделение_Key: {args.dept}")
        print(f"ФОТ факт: {fmt(amt)}")
        lines += [
            f"Отдел: {name}",
            f"Подразделение_Key: {args.dept}",
            f"ФОТ факт: {fmt(amt)}",
        ]
        out = OUT_DIR / f"plan_fact_fot_{y}_{m:02d}_{args.dept[:8]}.txt"
        out.write_text("\n".join(lines), encoding="utf-8")
        print(f"\nОтчёт: {out}")
        cn.close()
        return 0

    by_dept = calc_fot_by_dept(cur, p0, p_next)
    total = sum(by_dept.get(n, 0.0) for n in FACT_ORDER)

    print("=" * 78)
    print(f"{'Отдел':<55} {'ФОТ факт':>18}")
    print("-" * 78)
    lines.append(f"{'Отдел':<55} {'ФОТ факт':>18}")
    lines.append("-" * 78)

    for name in FACT_ORDER:
        val = by_dept.get(name, 0.0)
        print(f"{name:<55} {fmt(val):>18}")
        lines.append(f"{name:<55} {fmt(val):>18}")

    print("-" * 78)
    print(f"{'ИТОГО коммерческий директор':<55} {fmt(total):>18}")
    lines += [
        "-" * 78,
        f"{'ИТОГО коммерческий директор':<55} {fmt(total):>18}",
        "",
        f"ФОТ факт итого: {total}",
    ]

    out = OUT_DIR / f"plan_fact_fot_{y}_{m:02d}.txt"
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
