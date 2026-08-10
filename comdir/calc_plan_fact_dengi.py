# -*- coding: utf-8 -*-
"""
План / факт / ожидаемые по деньгам для коммерческой службы.

План  — регистр ТД_ПланированиеДоговоровОтгрузокДС, вид «Деньги», 6 коммерческих отделов.
        Полный месяц и пропорционально (дней прошло / дней в месяце).

Факт  — «Платежи полученные» по логике отчёта 1С:
        • СуммаОплатыРегл (возврат оплаты клиенту → минус)
        • СуммаПостоплатыРегл по ОтчетКомиссионера + ПоступлениеБезнал
        • взаимозачёты ДвиженияКонтрагентКонтрагент
        Отделы факта: 6 коммерческих + 2 ликвидированных (как в отчёте).

Ожидаемые (Сумма планируемых платежей / dengi_expected)
  — РН РасчетыСКлиентами (_AccumRg53885), ресурс КОплате (_Fld53890)
  — остаток > 0; ОбъектРасчетов типа ЗаказКлиента
  — этап оплаты с ДатаПлатежа в выбранном месяце
  — фильтры заказа: соглашение, не ТД_НеУчитыватьВПланФакте/ДС,
    не ТД_СопровождениеПродажи, партнёр не из списка перепродажи
  — валюта заказа × фиксированные курсы (как в отчёте 1С)

Итог коммерческого директора = сумма по всем отделам факта / плану.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from pathlib import Path

import pyodbc

# ---------------------------------------------------------------------------
# Константы маппинга 1С → SQL (erp_pm)
# ---------------------------------------------------------------------------
YEAR_OFFSET = 2000
PLAN_MONEY = bytes.fromhex("a83bb28fc79697ac4a05bbb0b1271987")  # ВидПланирования «Деньги»
ORDER_TREF = bytes.fromhex("000002c0")  # Документ.ЗаказКлиента
BANK_TREF = bytes.fromhex("00000386")  # ПоступлениеБезналичныхДенежныхСредств
COMM_TREF = bytes.fromhex("00000346")  # ОтчетКомиссионера
EMPTY16 = bytes(16)
# ХозяйственнаяОперация.ВозвратОплатыКлиенту (_Enum1919)
RET_OP = bytes.fromhex("b4af52c1b39555e54eeac8d5724dc975")

# Валюты (_Reference53) — курсы как в getkpi/calc_plan.EXCHANGE_RATES
CUR_USD = bytes.fromhex("963e001cc4d0438811dfe1b60a7c6f22")
CUR_EUR = bytes.fromhex("81cd001583b3d75c11e07405d328a18d")
CUR_BYN = bytes.fromhex("8756ac1f6b05524d11ec45dc095e2c36")
CUR_KZT = bytes.fromhex("95fc6cb31113810e11efde2ee2bc7bc0")

ODP_DEPT = bytes.fromhex("96f96cb31113810e11f092f67587c178")  # Отдел дилерских продаж
OPBO_DEPT = bytes.fromhex("80da001e6711250911e49f994edcf3a0")  # ликв. бытовое
OPPO_DEPT = bytes.fromhex("8127001e6711250911e6d71eff740269")  # ликв. пром.
DEPTS_RESALE_NO_MGS = (ODP_DEPT, OPBO_DEPT, OPPO_DEPT)

# Ликвидированные «холдинги» → ключевые клиенты (как OData aliases)
HOLDINGS_DEPTS: list[tuple[str, str]] = [
    ("(ликв.) Отдел по работе с холдингами 1", "95e86cb31113810e11efcf32c6810cc3"),
    ("(ликв.) Отдел по работе с холдингами 2", "95e86cb31113810e11efcf38ebd2d511"),
    ("(ликв.) Отдел по работе с холдингами 3", "95e86cb31113810e11efcf39ad83f8bd"),
]

# 6 коммерческих отделов (план + факт)
COMMERCIAL_DEPTS: list[tuple[str, str]] = [
    ("Отдел по работе с ПАО Газпром", "80da001e6711250911e49f9cbd7b5184"),
    ("Отдел дилерских продаж", "96f96cb31113810e11f092f67587c178"),
    ("Отдел по работе с ключевыми клиентами", "8523ac1f6b05524d11eb67b6639ec87b"),
    ("Отдел продаж эталонного оборудования и услуг", "80d6001e6711250911e4810f34497ef7"),
    ("Отдел внешнеэкономической деятельности", "8283ac1f6b05524d11e8e40149480c10"),
    ("Отдел продаж БМИ", "93d36cb31113810e11ee37a59edaa7d4"),
]

# + ликвидированные — в факте только бытовое (как в OData/отчёте: пром. не даёт платежей в итоге)
LIQUIDATED_DEPTS: list[tuple[str, str]] = [
    ("(ликв.) Отдел дилерских продаж бытового оборудования", "80da001e6711250911e49f994edcf3a0"),
]
# пром. ликвидированный — только для отображения строки в CLI (факт = 0)
LIQUIDATED_DEPTS_DISPLAY: list[tuple[str, str]] = [
    ("(ликв.) Отдел дилерских продаж бытового оборудования", "80da001e6711250911e49f994edcf3a0"),
    ("(ликв.) Отдел дилерских продаж промышленного оборудования", "8127001e6711250911e6d71eff740269"),
]

# Порядок вывода как в отчёте
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


def calc_plan(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """План денег по 6 коммерческим отделам (полный месяц)."""
    load_depts(cur, COMMERCIAL_DEPTS, "#plan_depts")
    cur.execute(
        """
        SELECT d.name,
               SUM(CASE WHEN p._Active = 0x01 THEN p._Fld96971 ELSE 0 END) AS PlanSum
        FROM _AccumRg96963 p WITH (NOLOCK)
        INNER JOIN #plan_depts d ON d.id = p._Fld96965RRef
        LEFT JOIN _Reference112236 plan_obj WITH (NOLOCK)
          ON plan_obj._IDRRef = p._Fld96964_RRRef
        WHERE p._Fld122525RRef = ?
          AND p._Period >= ? AND p._Period < ?
          AND (
                plan_obj._IDRRef IS NULL
                OR plan_obj._Fld122423 <= ?
                OR plan_obj._Fld122423 >= ?
              )
        GROUP BY d.name
        """,
        PLAN_MONEY,
        p0,
        p_next,
        datetime(2001, 1, 1),
        p_next,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_fact(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Факт «Платежи полученные» как в отчёте 1С «План-фактный анализ продаж».

    Ветка 1: СуммаОплатыРегл, отдел = Подразделение объекта расчётов
      (как ОбъектРасчетов.Подразделение в запросе 1С).
      • есть заказ → фильтры: соглашение объекта, не ТД_НеУчитывать*, не сопровождение;
        при провале строку не берём;
      • нет заказа (договор и др.) → берём, если отдел объекта в списке отчёта.
    Ветка 2: комиссия (СуммаПостоплатыРегл), отдел регистра.
    Ветка 3: взаимозачёты по объекту→заказу.
    Перепродажа: ТД_ПредопределенныеЗначения через comdir.resale (без хардкода списка).
    """
    from comdir.resale import ORDER_SOPR_FIELD

    all_depts = COMMERCIAL_DEPTS
    load_depts(cur, all_depts, "#fact_depts")
    load_resale(cur)
    cur.execute(
        f"""
        SELECT d.name, SUM(x.amt) AS FactSum
        FROM (
          -- оплаты: отдел объекта расчётов
          SELECT o._Fld138169RRef AS dept,
                 CASE WHEN c._Fld51417RRef = ? THEN -c._Fld51434 ELSE c._Fld51434 END AS amt,
                 COALESCE(ord._Fld21180RRef, c._Fld51418RRef) AS partner
          FROM _AccumRg51416 c WITH (NOLOCK)
          INNER JOIN _Reference134945 o WITH (NOLOCK)
            ON o._IDRRef = c._Fld140225_RRRef
          LEFT JOIN _Document704 ord WITH (NOLOCK)
            ON ord._IDRRef = o._Fld138162_RRRef AND o._Fld138162_RTRef = ?
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01
            AND ISNULL(c._Fld140228, 0x00) = 0x00
            AND c._Fld51434 <> 0
            AND o._Fld138169RRef IN (SELECT id FROM #fact_depts)
            AND o._Fld138169RRef <> ?
            AND (
                  ord._IDRRef IS NULL
                  OR (
                       o._Fld138193_RRRef <> ?
                       AND ISNULL(ord._Fld184301, 0x00) = 0x00
                       AND ISNULL(ord._Fld185210, 0x00) = 0x00
                       AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                     )
                )

          UNION ALL

          -- комиссия: постоплата, отдел = измерение регистра
          SELECT c._Fld51419RRef,
                 c._Fld51437,
                 c._Fld51418RRef
          FROM _AccumRg51416 c WITH (NOLOCK)
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01
            AND ISNULL(c._Fld140228, 0x00) = 0x00
            AND c._Fld51419RRef IN (SELECT id FROM #fact_depts)
            AND c._Fld51432_RTRef = ?
            AND c._RecorderTRef = ?
            AND c._Fld51437 <> 0

          UNION ALL

          -- взаимозачёты
          SELECT o._Fld138169RRef,
                 c._Fld51626,
                 ord._Fld21180RRef
          FROM _AccumRg51608 c WITH (NOLOCK)
          INNER JOIN _Reference134945 o WITH (NOLOCK)
            ON o._IDRRef = c._Fld140249_RRRef
          INNER JOIN _Document704 ord WITH (NOLOCK)
            ON ord._IDRRef = o._Fld138162_RRRef AND o._Fld138162_RTRef = ?
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01
            AND o._Fld138169RRef IN (SELECT id FROM #fact_depts)
            AND o._Fld138193_RRRef <> ?
            AND ISNULL(ord._Fld184301, 0x00) = 0x00
            AND ISNULL(ord._Fld185210, 0x00) = 0x00
            AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
        ) x
        INNER JOIN #fact_depts d ON d.id = x.dept
        WHERE (
          CASE
            WHEN EXISTS (SELECT 1 FROM #resale r WHERE r.id = x.partner)
            THEN 0 ELSE 1
          END
        ) = 1
        GROUP BY d.name
        """,
        RET_OP,
        ORDER_TREF,
        p0,
        p_next,
        EMPTY16,
        EMPTY16,
        p0,
        p_next,
        COMM_TREF,
        BANK_TREF,
        ORDER_TREF,
        p0,
        p_next,
        EMPTY16,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_expected(cur, p_period_start: datetime, p_month_end: datetime) -> dict[str, float]:
    """Ожидаемые деньги (Сумма планируемых платежей) — как OData/отчёт 1С.

    • остаток КОплате > 0 на конец месяца по ОбъектРасчетов;
    • объект расчётов типа ЗаказКлиента (_Fld138162_RTRef);
    • заказ с этапом оплаты ДатаПлатежа в [начало выбранного месяца .. конец месяца);
    • соглашение заполнено; не ТД_НеУчитыватьВПланФакте / …ДС;
    • партнёр заказа не из списка перепродажи.
    """
    from comdir.resale import ORDER_SOPR_FIELD

    exp_depts = COMMERCIAL_DEPTS
    load_depts(cur, exp_depts, "#exp_depts")
    load_resale(cur)

    fx = """
      CASE
        WHEN ord._Fld21185RRef = ? THEN 90.0
        WHEN ord._Fld21185RRef = ? THEN 98.0
        WHEN ord._Fld21185RRef = ? THEN 28.0
        WHEN ord._Fld21185RRef = ? THEN 0.19
        ELSE 1.0
      END
    """
    cur.execute(
        f"""
        SELECT d.name, SUM(bal.bal * {fx}) AS ExpSum
        FROM (
          SELECT s._Fld140429RRef AS obj,
                 SUM(
                   CASE WHEN s._RecordKind = 1 THEN -s._Fld53890 ELSE s._Fld53890 END
                 ) AS bal
          FROM _AccumRg53885 s WITH (NOLOCK)
          WHERE s._Period < ?
            AND s._Active = 0x01
            AND ISNULL(s._Fld140434, 0x00) = 0x00
            AND s._Fld53890 <> 0
          GROUP BY s._Fld140429RRef
          HAVING SUM(
            CASE WHEN s._RecordKind = 1 THEN -s._Fld53890 ELSE s._Fld53890 END
          ) > 0
        ) bal
        INNER JOIN _Reference134945 co WITH (NOLOCK)
          ON co._IDRRef = bal.obj
         AND co._Fld138162_RTRef = ?
        INNER JOIN (
          SELECT ord._Fld138973RRef AS obj, MIN(ord._IDRRef) AS ord_id
          FROM _Document704 ord WITH (NOLOCK)
          INNER JOIN #exp_depts d0 ON d0.id = ord._Fld21220RRef
          WHERE ord._Fld138973RRef <> ?
            AND ord._Fld21183RRef <> ?
            AND ISNULL(ord._Fld184301, 0x00) = 0x00
            AND ISNULL(ord._Fld185210, 0x00) = 0x00
            AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
            AND EXISTS (
              SELECT 1 FROM _Document704_VT21278 st WITH (NOLOCK)
              WHERE st._Document704_IDRRef = ord._IDRRef
                AND st._Fld21281 >= ?
                AND st._Fld21281 < ?
            )
            AND NOT EXISTS (SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef)
          GROUP BY ord._Fld138973RRef
        ) pick ON pick.obj = bal.obj
        INNER JOIN _Document704 ord WITH (NOLOCK)
          ON ord._IDRRef = pick.ord_id
        INNER JOIN #exp_depts d ON d.id = ord._Fld21220RRef
        GROUP BY d.name
        """,
        CUR_USD,
        CUR_EUR,
        CUR_BYN,
        CUR_KZT,
        p_month_end,
        ORDER_TREF,
        EMPTY16,
        EMPTY16,
        p_period_start,
        p_month_end,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def main(as_of: date | None = None) -> None:
    as_of = as_of or date.today()
    y, m = as_of.year, as_of.month
    days_in_month = monthrange(y, m)[1]
    elapsed = min(as_of.day, days_in_month)
    p0 = to_1c_dt(date(y, m, 1))
    p_year = to_1c_dt(date(y, 1, 1))
    if m == 12:
        p_next = to_1c_dt(date(y + 1, 1, 1))
    else:
        p_next = to_1c_dt(date(y, m + 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    print(f"Период: {y}-{m:02d}, на дату {as_of.isoformat()} ({elapsed}/{days_in_month} дн.)")
    print(f"БД: erp_pm @ localhost\n")

    plan = calc_plan(cur, p0, p_next)
    fact = calc_fact(cur, p0, p_next)
    expected = calc_expected(cur, p_year, p_next)

    # --- по отделам ---
    print("=" * 110)
    print(f"{'Отдел':<55} {'План':>12} {'Факт':>14} {'%':>8} {'Ожидаемо':>14}")
    print("-" * 110)

    report_lines = [
        f"Период: {y}-{m:02d}, на дату {as_of.isoformat()} ({elapsed}/{days_in_month})",
        "",
        f"{'Отдел':<55} {'План':>14} {'План∝':>14} {'Факт':>14} {'% к ∝':>8} {'Ожидаемо':>14}",
        "-" * 125,
    ]

    plan_full_total = 0.0
    plan_prorata_total = 0.0
    fact_total = 0.0
    expected_total = 0.0
    commercial = dict(COMMERCIAL_DEPTS)

    for name in FACT_ORDER:
        plan_full = plan.get(name, 0.0)
        plan_pr = plan_full * elapsed / days_in_month if name in plan else 0.0
        fact_v = fact.get(name, 0.0)
        exp_v = expected.get(name, 0.0)

        # в итог коммерческого директора по плану — только 6 коммерческих
        if name in commercial:
            plan_full_total += plan_full
            plan_prorata_total += plan_pr
            expected_total += exp_v
        fact_total += fact_v

        plan_cell = fmt(plan_full) if name in commercial else "—"
        exp_cell = fmt(exp_v) if name in commercial else "—"
        print(
            f"{name:<55} {plan_cell:>12} {fmt(fact_v):>14} "
            f"{pct(fact_v, plan_pr):>8} {exp_cell:>14}"
        )
        report_lines.append(
            f"{name:<55} {plan_cell:>14} "
            f"{fmt(plan_pr) if name in commercial else '—':>14} "
            f"{fmt(fact_v):>14} {pct(fact_v, plan_pr):>8} {exp_cell:>14}"
        )

    print("-" * 110)
    print(
        f"{'ИТОГО коммерческий директор':<55} "
        f"{fmt(plan_full_total):>12} {fmt(fact_total):>14} "
        f"{pct(fact_total, plan_prorata_total):>8} {fmt(expected_total):>14}"
    )
    print()
    print(f"План (полный месяц):     {fmt(plan_full_total)}")
    print(f"План (пропорц. {elapsed}/{days_in_month}):  {fmt(plan_prorata_total)}")
    print(f"Факт (всего):            {fmt(fact_total)}")
    print(f"Ожидаемо (всего):        {fmt(expected_total)}")
    print(f"% к пропорц. плану:      {pct(fact_total, plan_prorata_total)}")
    print(f"% к полному плану:       {pct(fact_total, plan_full_total)}")

    report_lines += [
        "-" * 125,
        f"{'ИТОГО коммерческий директор':<55} "
        f"{fmt(plan_full_total):>14} {fmt(plan_prorata_total):>14} "
        f"{fmt(fact_total):>14} {pct(fact_total, plan_prorata_total):>8} "
        f"{fmt(expected_total):>14}",
        "",
        f"План полный месяц: {plan_full_total}",
        f"План пропорционально: {plan_prorata_total}",
        f"Факт итого: {fact_total}",
        f"Ожидаемо итого: {expected_total}",
    ]
    out = OUT_DIR / f"plan_fact_{y}_{m:02d}.txt"
    out.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nОтчёт сохранён: {out}")

    cn.close()


if __name__ == "__main__":
    # Фиксируем дату отчёта (как в сверке); для «сегодня» — main()
    main(as_of=date(2026, 7, 27))
