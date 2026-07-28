# -*- coding: utf-8 -*-
"""
Факт ВП (валовая прибыль) за месяц.

Источник: РН ВыручкаИСебестоимостьПродаж (_AccumRg51076)

ВП = СуммаВыручки − (Стоимость + ДопРасходы + Трудозатраты
                   + ПостатейныеПостоянныеСНДС + ПостатейныеПеременныеСНДС
                   + РасходыНаПродажуСНДС)

Исключения:
  • ТипЗапасов = КомиссионныйТовар
  • аналитика учёта по партнёрам с Партнёр = «Наше предприятие»

Если Стоимость = 0 и организация из списка «предварительных» —
себестоимость (и связанные статьи) подставляются из РС СтоимостьТоваров
(_InfoRg49613) × Количество.

Комдир: все строки за месяц (все подразделения).
Отдел: фильтр по Подразделение (_Fld51080RRef).

Запуск:
  python calc_plan_fact_vp.py 2026-07
  python calc_plan_fact_vp.py 2026-07 --dept 96f96cb31113810e11f092f67587c178
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pyodbc

YEAR_OFFSET = 2000

# Перечисление.ТипыЗапасов.КомиссионныйТовар
COMMISSION = bytes.fromhex("8c8d4fb9e9a4c800423c97037b7eee65")

# Справочник.Партнеры «Наше предприятие»
OUR_ENTERPRISE = bytes.fromhex("812d001e6711250911e762515fc811cd")

# Организации с предварительной себестоимостью (группа компаний)
PRELIM_ORGS: list[tuple[str, str]] = [
    ("Турбулентность-Дон ООО", "812d001e6711250911e76cfdfbca2143"),
    ("ИП Корниенко М.В.", "812d001e6711250911e76cfdfbca2144"),
    ("Метрогазсервис ООО", "812d001e6711250911e76cfdfbca2145"),
    ("АЛМАЗ ООО", "812d001e6711250911e76cfdfbca2146"),
    ("СКТБ Турбо-Дон ООО", "812d001e6711250911e76cfdfbca2147"),
    ("ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО", "812d001e6711250911e76cfdfbca2148"),
]

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
    parts = s.strip().split("-")
    if len(parts) != 2:
        raise ValueError("Ожидается месяц в формате YYYY-MM")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError("Месяц должен быть 01..12")
    return y, m


def load_prelim(cur) -> None:
    cur.execute("IF OBJECT_ID('tempdb..#prelim') IS NOT NULL DROP TABLE #prelim")
    cur.execute("CREATE TABLE #prelim (id binary(16) PRIMARY KEY)")
    for _, hx in PRELIM_ORGS:
        cur.execute("INSERT INTO #prelim(id) VALUES (?)", bytes.fromhex(hx))


def vp_expr() -> str:
    """SQL expression for VP with cost substitution from СтоимостьТоваров."""
    cost = """
        CASE WHEN r._Fld51094 = 0 AND p.id IS NOT NULL
             THEN ISNULL(c.st, 0) * CAST(r._Fld51091 AS float)
             ELSE CAST(ISNULL(r._Fld51094, 0) AS float) END
    """
    dop = """
        CASE WHEN r._Fld51094 = 0 AND p.id IS NOT NULL
             THEN ISNULL(c.dop, 0) * CAST(r._Fld51091 AS float)
             ELSE CAST(ISNULL(r._Fld51096, 0) AS float) END
    """
    trud = """
        CASE WHEN r._Fld51094 = 0 AND p.id IS NOT NULL
             THEN ISNULL(c.trud, 0) * CAST(r._Fld51091 AS float)
             ELSE CAST(ISNULL(r._Fld69180, 0) AS float) END
    """
    post = """
        CASE WHEN r._Fld51094 = 0 AND p.id IS NOT NULL
             THEN ISNULL(c.post, 0) * CAST(r._Fld51091 AS float)
             ELSE CAST(ISNULL(r._Fld69176, 0) AS float) END
    """
    perem = """
        CASE WHEN r._Fld51094 = 0 AND p.id IS NOT NULL
             THEN ISNULL(c.perem, 0) * CAST(r._Fld51091 AS float)
             ELSE CAST(ISNULL(r._Fld69183, 0) AS float) END
    """
    rask = "CAST(ISNULL(r._Fld140207, 0) AS float)"
    rev = "CAST(ISNULL(r._Fld51092, 0) AS float)"
    return f"({rev}) - (({cost}) + ({dop}) + ({trud}) + ({post}) + ({perem}) + ({rask}))"


def cost_cte_sql() -> str:
    """Unit costs from РС СтоимостьТоваров for the month (1 row per key)."""
    return """
        ;WITH cost AS (
          SELECT
            _Fld49614RRef AS anal,
            _Fld49615RRef AS vid,
            _Fld49616RRef AS org,
            _Fld49617RRef AS razdel,
            AVG(CAST(_Fld49618 AS float)) AS st,
            AVG(CAST(_Fld49619 AS float)) AS dop,
            AVG(CAST(_Fld49623 AS float)) AS trud,
            AVG(CAST(_Fld49624 AS float)) AS post,
            AVG(CAST(_Fld69146 AS float)) AS perem
          FROM _InfoRg49613 WITH (NOLOCK)
          WHERE _Period >= ? AND _Period < ? AND _Active = 0x01
          GROUP BY _Fld49614RRef, _Fld49615RRef, _Fld49616RRef, _Fld49617RRef
        )
    """


def base_from_joins() -> str:
    return """
        FROM _AccumRg51076 r WITH (NOLOCK)
        INNER JOIN _Reference225 a WITH (NOLOCK)
            ON a._IDRRef = r._Fld51079RRef
        LEFT JOIN #prelim p
            ON p.id = a._Fld6923RRef
        LEFT JOIN cost c
            ON c.anal = r._Fld51077RRef
           AND c.vid = r._Fld51082RRef
           AND c.org = a._Fld6923RRef
           AND c.razdel = r._Fld69166RRef
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
          AND r._Fld51081RRef <> ?
          AND a._Fld6922RRef <> ?
    """


def calc_vp_by_dept(cur, p0: datetime, p_next: datetime) -> dict[bytes, float]:
    load_prelim(cur)
    sql = (
        cost_cte_sql()
        + f"""
        SELECT r._Fld51080RRef AS Dept, SUM({vp_expr()}) AS VP
        {base_from_joins()}
        GROUP BY r._Fld51080RRef
        """
    )
    cur.execute(sql, p0, p_next, p0, p_next, COMMISSION, OUR_ENTERPRISE)
    return {bytes(r[0]): float(r[1] or 0) for r in cur.fetchall() if r[0]}


def calc_vp_for_dept(
    cur, p0: datetime, p_next: datetime, dept: bytes
) -> float:
    load_prelim(cur)
    sql = (
        cost_cte_sql()
        + f"""
        SELECT SUM({vp_expr()}) AS VP
        {base_from_joins()}
          AND r._Fld51080RRef = ?
        """
    )
    cur.execute(sql, p0, p_next, p0, p_next, COMMISSION, OUR_ENTERPRISE, dept)
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
    ap = argparse.ArgumentParser(description="Факт ВП за месяц")
    ap.add_argument(
        "month",
        help="Месяц YYYY-MM (например 2026-07)",
    )
    ap.add_argument(
        "--dept",
        dest="dept",
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

    lines: list[str] = []
    lines.append(f"Факт ВП за {y}-{m:02d}")
    lines.append("Источник: ВыручкаИСебестоимостьПродаж + СтоимостьТоваров (подстановка)")
    lines.append(
        "Исключено: ТипЗапасов=КомиссионныйТовар, партнёр «Наше предприятие»"
    )
    lines.append(
        "Предварительные организации: "
        + ", ".join(n for n, _ in PRELIM_ORGS)
    )
    lines.append("")

    print(f"Факт ВП за {y}-{m:02d}")
    print(f"БД: erp_pm @ localhost\n")

    if args.dept:
        dept = bytes.fromhex(args.dept)
        name = dept_name(cur, dept)
        vp = calc_vp_for_dept(cur, p0, p_next, dept)
        print(f"Отдел: {name}")
        print(f"Подразделение_Key: {args.dept}")
        print(f"ВП факт: {fmt(vp)}")
        lines += [
            f"Отдел: {name}",
            f"Подразделение_Key: {args.dept}",
            f"ВП факт: {fmt(vp)}",
        ]
        out = OUT_DIR / f"plan_fact_vp_{y}_{m:02d}_{args.dept[:8]}.txt"
        out.write_text("\n".join(lines), encoding="utf-8")
        print(f"\nОтчёт: {out}")
        cn.close()
        return 0

    by_dept = calc_vp_by_dept(cur, p0, p_next)
    total = sum(by_dept.values())

    print("=" * 78)
    print(f"{'Отдел':<55} {'ВП факт':>18}")
    print("-" * 78)
    lines.append(f"{'Отдел':<55} {'ВП факт':>18}")
    lines.append("-" * 78)

    known = {bytes.fromhex(hx): name for name, hx in COMMERCIAL_DEPTS + LIQUIDATED_DEPTS}
    shown = 0.0
    for name in FACT_ORDER:
        hx = dict(COMMERCIAL_DEPTS + LIQUIDATED_DEPTS)[name]
        val = by_dept.get(bytes.fromhex(hx), 0.0)
        shown += val
        print(f"{name:<55} {fmt(val):>18}")
        lines.append(f"{name:<55} {fmt(val):>18}")

    other = total - shown
    print(f"{'Прочие подразделения':<55} {fmt(other):>18}")
    lines.append(f"{'Прочие подразделения':<55} {fmt(other):>18}")

    print("-" * 78)
    print(f"{'ИТОГО коммерческий директор (все отделы)':<55} {fmt(total):>18}")
    lines += [
        "-" * 78,
        f"{'ИТОГО коммерческий директор (все отделы)':<55} {fmt(total):>18}",
        "",
        f"ВП факт итого: {total}",
    ]

    # top other depts for transparency
    extras = [
        (dept_name(cur, did), val)
        for did, val in by_dept.items()
        if did not in known and abs(val) > 0.005
    ]
    extras.sort(key=lambda x: abs(x[1]), reverse=True)
    if extras:
        lines.append("")
        lines.append("Прочие подразделения (деталь):")
        for name, val in extras[:25]:
            lines.append(f"  {name:<53} {fmt(val):>18}")

    out = OUT_DIR / f"plan_fact_vp_{y}_{m:02d}.txt"
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
