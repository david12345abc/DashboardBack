# -*- coding: utf-8 -*-
"""
Цена фактическая и цена расчётная по КП для коммерческого директора.

Источник: Документ.КоммерческоеПредложениеКлиенту (_Document770)
Подразделение: Справочник.Пользователи (_Reference366).Подразделение
               ← КП.Менеджер

Отбор КП:
  • Date в месяце
  • DeletionMark = false
  • статус Действует / Исполнено,
    либо Согласовано и СогласованоСКлиентом = true

Формулы:
  Цена фактическая = СуммаДокумента; если 0 → СуммаДокументаТКП
  Цена расчётная   = СуммаДокументаТКП + СуммаСкидкиТКП × (−1)

Запуск:
  python calc_plan_fact_cena.py 2026-07
  python calc_plan_fact_cena.py 2026-07 --dept 96f96cb31113810e11f092f67587c178
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

import pyodbc

YEAR_OFFSET = 2000
EMPTY16 = bytes(16)

# СтатусыКоммерческихПредложенийКлиентам (_Enum1651)
ST_AGREED = bytes.fromhex("8d29db1c3ffa703d4cb89ffc9bc266f1")  # Согласовано
ST_ACTIVE = bytes.fromhex("81f06ae643084e4f4703dbfdf33634ed")  # Действует
ST_DONE = bytes.fromhex("9508a6ca490b5e884233984bcae99fdf")  # Исполнено

# _Document770
KP_STATUS = "_Fld25044RRef"
KP_MANAGER = "_Fld25039RRef"
KP_SUM = "_Fld25035"  # СуммаДокумента
KP_SUM_TKP = "_Fld100364"  # СуммаДокументаТКП
KP_DISC_TKP = "_Fld86875"  # СуммаСкидкиТКП
KP_AGREED_CLIENT = "_Fld86887"  # СогласованоСКлиентом

# _Reference366 Пользователи
USER_DEPT = "_Fld10996RRef"  # Подразделение

COMMERCIAL_DEPTS: list[tuple[str, str]] = [
    ("Отдел по работе с ПАО Газпром", "80da001e6711250911e49f9cbd7b5184"),
    ("Отдел дилерских продаж", "96f96cb31113810e11f092f67587c178"),
    ("Отдел по работе с ключевыми клиентами", "8523ac1f6b05524d11eb67b6639ec87b"),
    ("Отдел продаж эталонного оборудования и услуг", "80d6001e6711250911e4810f34497ef7"),
    ("Отдел внешнеэкономической деятельности", "8283ac1f6b05524d11e8e40149480c10"),
    ("Отдел продаж БМИ", "93d36cb31113810e11ee37a59edaa7d4"),
]

LIQUIDATED_DEPTS: list[tuple[str, str]] = [
    ("(ликв.) Отдел дилерских продаж бытового оборудования", "80da001e6711250911e49f994edcf3a0"),
    ("(ликв.) Отдел дилерских продаж промышленного оборудования", "8127001e6711250911e6d71eff740269"),
]

FACT_ORDER: list[str] = [
    "(ликв.) Отдел дилерских продаж бытового оборудования",
    "(ликв.) Отдел дилерских продаж промышленного оборудования",
    "Отдел внешнеэкономической деятельности",
    "Отдел дилерских продаж",
    "Отдел по работе с ключевыми клиентами",
    "Отдел по работе с ПАО Газпром",
    "Отдел продаж БМИ",
    "Отдел продаж эталонного оборудования и услуг",
]

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
    y, m = s.split("-")
    return int(y), int(m)


def fact_expr() -> str:
    return f"""
    CASE WHEN ISNULL(kp.[{KP_SUM}], 0) = 0
         THEN ISNULL(kp.[{KP_SUM_TKP}], 0)
         ELSE kp.[{KP_SUM}]
    END
    """


def calc_expr() -> str:
    return f"ISNULL(kp.[{KP_SUM_TKP}], 0) + ISNULL(kp.[{KP_DISC_TKP}], 0) * (-1)"


def status_filter_sql() -> str:
    return f"""
      AND (
            kp.[{KP_STATUS}] IN (?, ?)
            OR (kp.[{KP_STATUS}] = ? AND ISNULL(kp.[{KP_AGREED_CLIENT}], 0x00) = 0x01)
          )
    """


def status_params() -> list[bytes]:
    return [ST_ACTIVE, ST_DONE, ST_AGREED]


def calc_by_dept(cur, p0: datetime, p_next: datetime) -> dict[bytes, tuple[float, float, int]]:
    cur.execute(
        f"""
        SELECT u.[{USER_DEPT}] AS Dept,
               SUM({fact_expr()}) AS FactPrice,
               SUM({calc_expr()}) AS CalcPrice,
               COUNT(*) AS N
        FROM _Document770 kp WITH (NOLOCK)
        INNER JOIN _Reference366 u WITH (NOLOCK)
          ON u._IDRRef = kp.[{KP_MANAGER}]
        WHERE kp._Date_Time >= ? AND kp._Date_Time < ?
          AND kp._Marked = 0x00
          {status_filter_sql()}
          AND kp.[{KP_MANAGER}] <> ?
          AND u.[{USER_DEPT}] <> ?
        GROUP BY u.[{USER_DEPT}]
        """,
        p0,
        p_next,
        *status_params(),
        EMPTY16,
        EMPTY16,
    )
    out: dict[bytes, tuple[float, float, int]] = {}
    for dept, fact, calc, n in cur.fetchall():
        if dept:
            out[bytes(dept)] = (float(fact or 0), float(calc or 0), int(n or 0))
    return out


def calc_for_dept(
    cur, p0: datetime, p_next: datetime, dept: bytes
) -> tuple[float, float, int]:
    cur.execute(
        f"""
        SELECT SUM({fact_expr()}) AS FactPrice,
               SUM({calc_expr()}) AS CalcPrice,
               COUNT(*) AS N
        FROM _Document770 kp WITH (NOLOCK)
        INNER JOIN _Reference366 u WITH (NOLOCK)
          ON u._IDRRef = kp.[{KP_MANAGER}]
        WHERE kp._Date_Time >= ? AND kp._Date_Time < ?
          AND kp._Marked = 0x00
          {status_filter_sql()}
          AND u.[{USER_DEPT}] = ?
        """,
        p0,
        p_next,
        *status_params(),
        dept,
    )
    row = cur.fetchone()
    return float(row[0] or 0), float(row[1] or 0), int(row[2] or 0)


def dept_name(cur, dept_id: bytes) -> str:
    cur.execute(
        "SELECT _Description FROM _Reference513 WITH (NOLOCK) WHERE _IDRRef = ?",
        dept_id,
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else dept_id.hex()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Цена фактическая / расчётная по КП")
    ap.add_argument("month", help="Месяц YYYY-MM (например 2026-07)")
    ap.add_argument(
        "--dept",
        default=None,
        help="Подразделение_Key (32 hex) — только этот отдел",
    )
    args = ap.parse_args(argv)

    y, m = parse_month(args.month)
    p0 = to_1c_dt(date(y, m, 1))
    p_next = to_1c_dt(date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    lines: list[str] = [
        f"Цена фактическая / расчётная по КП за {y}-{m:02d}",
        "Источник: КоммерческоеПредложениеКлиенту; подразделение = Пользователи.Подразделение (менеджер КП)",
        "Отбор: Date в месяце, DeletionMark=false,",
        "       статус Действует/Исполнено либо Согласовано+СогласованоСКлиентом",
        "Цена факт = СуммаДокумента (если 0 → СуммаДокументаТКП)",
        "Цена расч = СуммаДокументаТКП + СуммаСкидкиТКП×(−1)",
        "",
    ]

    print(f"Цена факт/расч по КП за {y}-{m:02d}")
    print("БД: erp_pm @ localhost\n")

    if args.dept:
        dept = bytes.fromhex(args.dept)
        name = dept_name(cur, dept)
        fact, calc, n = calc_for_dept(cur, p0, p_next, dept)
        block = [
            f"Отдел: {name}",
            f"Подразделение_Key: {args.dept}",
            f"КП: {n}",
            f"Цена фактическая: {fmt(fact)}",
            f"Цена расчётная:   {fmt(calc)}",
        ]
        for s in block:
            print(s)
        lines += block
        out = OUT_DIR / f"plan_fact_cena_{y}_{m:02d}_{args.dept[:8]}.txt"
        out.write_text("\n".join(lines), encoding="utf-8")
        print(f"\nОтчёт: {out}")
        cn.close()
        return 0

    by_dept = calc_by_dept(cur, p0, p_next)
    total_fact = sum(v[0] for v in by_dept.values())
    total_calc = sum(v[1] for v in by_dept.values())
    total_n = sum(v[2] for v in by_dept.values())

    header = f"{'Отдел':<55} {'КП':>6} {'Цена факт':>16} {'Цена расч':>16}"
    print("=" * 100)
    print(header)
    print("-" * 100)
    lines += [header, "-" * 100]

    known = {bytes.fromhex(hx): name for name, hx in COMMERCIAL_DEPTS + LIQUIDATED_DEPTS}
    shown_fact = shown_calc = shown_n = 0.0
    shown_n = 0

    for name in FACT_ORDER:
        hx = dict(COMMERCIAL_DEPTS + LIQUIDATED_DEPTS)[name]
        fact, calc, n = by_dept.get(bytes.fromhex(hx), (0.0, 0.0, 0))
        shown_fact += fact
        shown_calc += calc
        shown_n += n
        line = f"{name:<55} {n:>6} {fmt(fact):>16} {fmt(calc):>16}"
        print(line)
        lines.append(line)

    other_ids = [d for d in by_dept if d not in known]
    other_fact = sum(by_dept[d][0] for d in other_ids)
    other_calc = sum(by_dept[d][1] for d in other_ids)
    other_n = sum(by_dept[d][2] for d in other_ids)
    line = f"{'Прочие подразделения':<55} {other_n:>6} {fmt(other_fact):>16} {fmt(other_calc):>16}"
    print(line)
    lines.append(line)

    tot = (
        f"{'ИТОГО коммерческий директор (все отделы)':<55} "
        f"{total_n:>6} {fmt(total_fact):>16} {fmt(total_calc):>16}"
    )
    print("-" * 100)
    print(tot)
    print()
    print(f"КП всего:           {total_n}")
    print(f"Цена фактическая:   {fmt(total_fact)}")
    print(f"Цена расчётная:     {fmt(total_calc)}")

    lines += [
        "-" * 100,
        tot,
        "",
        f"КП всего: {total_n}",
        f"Цена фактическая итого: {total_fact}",
        f"Цена расчётная итого: {total_calc}",
    ]

    if other_ids:
        lines.append("")
        lines.append("Прочие подразделения (деталь):")
        print("\nПрочие подразделения (деталь):")
        detail = sorted(
            ((dept_name(cur, d), by_dept[d]) for d in other_ids),
            key=lambda x: -x[1][0],
        )
        for name, (fact, calc, n) in detail:
            line = f"  {name:<53} {n:>6} {fmt(fact):>16} {fmt(calc):>16}"
            print(line)
            lines.append(line)

    out = OUT_DIR / f"plan_fact_cena_{y}_{m:02d}.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nОтчёт: {out}")
    cn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
