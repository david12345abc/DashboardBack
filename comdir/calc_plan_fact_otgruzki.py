# -*- coding: utf-8 -*-
"""
План / факт / ожидаемые по отгрузкам для коммерческой службы.

Маркетинговый план (МП_СуммаОтгрузки)
  — регистр ТД_ПланированиеДоговоровОтгрузокДС, вид «Отгрузки», 6 коммерческих отделов.

Факт (Отгрузки произведенные)
  — РН РаспоряженияНаОтгрузку (_AccumRg169757), вид движения Расход
  — сумма = -Сумма (в SQL расход хранится со знаком минус)
  — заказ: соглашение заполнено, не ТД_НеУчитыватьВПланФакте*

Ожидаемые (Заказы ожидаемые к отгрузке)
  — тот же регистр: Приход + Расход (знаковая сумма) по заказам
    с ДатаОтгрузки < конец месяца, остаток > 0
  — ветка возвратов комиссионеров (СебестоимостьТоваров) не включена

Итог коммерческого директора = сумма по отделам отчёта.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from pathlib import Path

import pyodbc

YEAR_OFFSET = 2000

PLAN_SHIPS = bytes.fromhex("a78fb8d2bce0c3ee491eb1eaa9925bc4")  # вид «Отгрузки»
EMPTY16 = bytes(16)
ORDER_TREF = bytes.fromhex("000002c0")  # Документ.ЗаказКлиента

# ВидыДвиженияНакопления (_Enum167554) в измерении _Fld169764RRef
KIND_RECEIPT = bytes.fromhex("b4b5c6b0366e5eac4492529af0e7f236")  # Приход
KIND_EXPENSE = bytes.fromhex("85662942ac5e614b4aca8d30654dd705")  # Расход

# Ликвидированные холдинги → ключевые клиенты
HOLDINGS_DEPTS: list[tuple[str, str]] = [
    ("(ликв.) Отдел по работе с холдингами 1", "95e86cb31113810e11efcf32c6810cc3"),
    ("(ликв.) Отдел по работе с холдингами 2", "95e86cb31113810e11efcf38ebd2d511"),
    ("(ликв.) Отдел по работе с холдингами 3", "95e86cb31113810e11efcf39ad83f8bd"),
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
    from comdir.common import connect as _connect

    return _connect()


def to_1c_dt(d: date) -> datetime:
    return datetime(d.year + YEAR_OFFSET, d.month, d.day)


def fmt(x) -> str:
    return f"{float(x or 0):,.2f}".replace(",", " ").replace(".", ",")


def pct(fact: float, plan: float) -> str:
    if not plan:
        return "—"
    return f"{fact / plan * 100:.1f}%"


def load_depts(cur, rows: list[tuple[str, str]], table: str = "#depts") -> None:
    cur.execute(f"IF OBJECT_ID('tempdb..{table}') IS NOT NULL DROP TABLE {table}")
    cur.execute(f"CREATE TABLE {table} (id binary(16) PRIMARY KEY, name nvarchar(255))")
    for name, hx in rows:
        cur.execute(f"INSERT INTO {table}(id, name) VALUES (?, ?)", bytes.fromhex(hx), name)


def load_resale(cur) -> None:
    from comdir.resale import load_resale_temp

    load_resale_temp(cur)


def calc_mp_plan(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    load_depts(cur, COMMERCIAL_DEPTS, "#plan_depts")
    cur.execute(
        """
        SELECT d.name,
               SUM(CASE WHEN p._Active = 0x01 THEN p._Fld96971 ELSE 0 END) AS PlanSum
        FROM _AccumRg96963 p WITH (NOLOCK)
        INNER JOIN #plan_depts d ON d.id = p._Fld96965RRef
        WHERE p._Fld122525RRef = ?
          AND p._Period >= ? AND p._Period < ?
        GROUP BY d.name
        """,
        PLAN_SHIPS,
        p0,
        p_next,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_fact(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Отгрузки произведённые: расход по РаспоряженияНаОтгрузку.

    Перепродажа: для ОДП/ликв. — список без МГС; для прочих — полный список,
    но партнёр перепродажи допускается при ТД_СопровождениеПродажи (как в OData).
    """
    from comdir.resale import ORDER_SOPR_FIELD

    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#fact_depts")
    load_resale(cur)
    cur.execute(
        f"""
        SELECT d.name, SUM(-s._Fld169768) AS FactSum
        FROM _AccumRg169757 s WITH (NOLOCK)
        INNER JOIN _Document704 o WITH (NOLOCK)
          ON o._IDRRef = s._Fld169758_RRRef
        INNER JOIN #fact_depts d ON d.id = o._Fld21220RRef
        WHERE s._Period >= ? AND s._Period < ?
          AND s._Active = 0x01
          AND ISNULL(s._Fld169770, 0x00) = 0x00
          AND s._Fld169758_RTRef = ?
          AND s._Fld169764RRef = ?
          AND o._Fld21183RRef <> ?
          AND ISNULL(o._Fld184301, 0x00) = 0x00
          AND ISNULL(o._Fld185211, 0x00) = 0x00
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = o._Fld21220RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = o._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = o._Fld21180RRef
                    ) AND ISNULL(o.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                    THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        p0,
        p_next,
        ORDER_TREF,
        KIND_EXPENSE,
        EMPTY16,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_expected(cur, p_month_end: datetime) -> dict[str, float]:
    """Заказы ожидаемые к отгрузке: остаток > 0, ДатаОтгрузки < конец месяца."""
    from comdir.resale import ORDER_SOPR_FIELD

    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#exp_depts")
    load_resale(cur)
    empty_date = datetime(4001, 1, 2)  # «пустая» дата 1С с запасом
    cur.execute(
        f"""
        SELECT d.name, SUM(x.net) AS ExpSum
        FROM (
          SELECT o._Fld21220RRef AS dept,
                 s._Fld169758_RRRef AS ord,
                 SUM(s._Fld169768) AS net
          FROM _AccumRg169757 s WITH (NOLOCK)
          INNER JOIN _Document704 o WITH (NOLOCK)
            ON o._IDRRef = s._Fld169758_RRRef
          WHERE s._Period < ?
            AND s._Active = 0x01
            AND ISNULL(s._Fld169770, 0x00) = 0x00
            AND s._Fld169758_RTRef = ?
            AND o._Fld21183RRef <> ?
            AND o._Fld21205 < ?
            AND o._Fld21205 > ?
            AND ISNULL(o._Fld184301, 0x00) = 0x00
            AND ISNULL(o._Fld185211, 0x00) = 0x00
            AND (
                  CASE
                    WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = o._Fld21220RRef) THEN
                      CASE WHEN EXISTS (
                        SELECT 1 FROM #resale_nomgs r WHERE r.id = o._Fld21180RRef
                      ) THEN 0 ELSE 1 END
                    ELSE
                      CASE WHEN EXISTS (
                        SELECT 1 FROM #resale r WHERE r.id = o._Fld21180RRef
                      ) AND ISNULL(o.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                      THEN 0 ELSE 1 END
                  END
                ) = 1
          GROUP BY o._Fld21220RRef, s._Fld169758_RRRef
          HAVING SUM(s._Fld169768) > 0
        ) x
        INNER JOIN #exp_depts d ON d.id = x.dept
        GROUP BY d.name
        """,
        p_month_end,
        ORDER_TREF,
        EMPTY16,
        p_month_end,
        empty_date,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def main(as_of: date | None = None) -> None:
    as_of = as_of or date.today()
    y, m = as_of.year, as_of.month
    days_in_month = monthrange(y, m)[1]
    elapsed = min(as_of.day, days_in_month)
    p0 = to_1c_dt(date(y, m, 1))
    if m == 12:
        p_next = to_1c_dt(date(y + 1, 1, 1))
    else:
        p_next = to_1c_dt(date(y, m + 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    print(f"Период: {y}-{m:02d}, на дату {as_of.isoformat()} ({elapsed}/{days_in_month} дн.)")
    print("БД: erp_pm @ localhost")
    print("Метрика: отгрузки (МП / факт / ожидаемые)\n")

    mp = calc_mp_plan(cur, p0, p_next)
    fact = calc_fact(cur, p0, p_next)
    expected = calc_expected(cur, p_next)

    commercial = dict(COMMERCIAL_DEPTS)
    header = (
        f"{'Отдел':<55} {'МП план':>14} {'МП проп.':>14} "
        f"{'Факт':>14} {'% проп.':>8} {'Ожидаемые':>14}"
    )
    print("=" * 130)
    print(header)
    print("-" * 130)

    report_lines = [
        f"Период: {y}-{m:02d}, на дату {as_of.isoformat()} ({elapsed}/{days_in_month})",
        "Колонки: Маркетинговый план по отгрузкам | Отгрузки произведенные (факт) | "
        "Заказы ожидаемые к отгрузке (план)",
        "",
        header,
        "-" * 130,
    ]

    mp_full_total = 0.0
    mp_pr_total = 0.0
    fact_total = 0.0
    exp_total = 0.0

    for name in FACT_ORDER:
        mp_full = mp.get(name, 0.0)
        mp_pr = mp_full * elapsed / days_in_month if name in commercial else 0.0
        fact_v = fact.get(name, 0.0)
        exp_v = expected.get(name, 0.0)

        if name in commercial:
            mp_full_total += mp_full
            mp_pr_total += mp_pr
        fact_total += fact_v
        exp_total += exp_v

        mp_cell = fmt(mp_full) if name in commercial else "—"
        mp_pr_cell = fmt(mp_pr) if name in commercial else "—"
        line = (
            f"{name:<55} {mp_cell:>14} {mp_pr_cell:>14} "
            f"{fmt(fact_v):>14} {pct(fact_v, mp_pr):>8} {fmt(exp_v):>14}"
        )
        print(line)
        report_lines.append(line)

    tot = (
        f"{'ИТОГО коммерческий директор':<55} "
        f"{fmt(mp_full_total):>14} {fmt(mp_pr_total):>14} "
        f"{fmt(fact_total):>14} {pct(fact_total, mp_pr_total):>8} {fmt(exp_total):>14}"
    )
    print("-" * 130)
    print(tot)
    print()
    print(f"МП план (полный месяц):     {fmt(mp_full_total)}")
    print(f"МП план (пропорц. {elapsed}/{days_in_month}): {fmt(mp_pr_total)}")
    print(f"Факт отгрузки:              {fmt(fact_total)}")
    print(f"Ожидаемые к отгрузке:       {fmt(exp_total)}")
    print(f"% факта к пропорц. МП:      {pct(fact_total, mp_pr_total)}")
    print(f"% факта к полному МП:       {pct(fact_total, mp_full_total)}")

    report_lines += [
        "-" * 130,
        tot,
        "",
        f"МП полный месяц: {mp_full_total}",
        f"МП пропорционально: {mp_pr_total}",
        f"Факт итого: {fact_total}",
        f"Ожидаемые итого: {exp_total}",
        "",
        "Примечания:",
        "- МП: _AccumRg96963, вид Отгрузки (a78fb8d2…)",
        "- Факт/ожидаемые: _AccumRg169757 (РаспоряженияНаОтгрузку), сумма _Fld169768",
        "- Вид движения: _Fld169764RRef / _Enum167554 (Расход=85662942…, Приход=b4b5c6b0…)",
        "- Заказ: отдел _Fld21220RRef, соглашение _Fld21183RRef, ДатаОтгрузки _Fld21205",
        "- Возвраты комиссионеров (СебестоимостьТоваров) не включены",
        "- Фильтр партнёров перепродажи и курсы валют не применены",
    ]
    out = OUT_DIR / f"plan_fact_otgruzki_{y}_{m:02d}.txt"
    out.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nОтчёт сохранён: {out}")
    cn.close()


if __name__ == "__main__":
    main(as_of=date(2026, 7, 27))
