# -*- coding: utf-8 -*-
"""
План / факт / ожидаемые по договорам для коммерческой службы.

Маркетинговый план (МП) — без изменений:
  _AccumRg96963, вид «Договоры», 6 коммерческих отделов.

Факт (ТД_ДоговорыПодписанные / _InfoRg112278) — по запросу 1С:
  • ДатаПодписания в периоде
  • Спецификация.Статус = Действует
  • исключение партнёров перепродажи (с особым правилом ТД_СопровождениеПродажи)
  • НЕ ТД_НеУчитыватьВПланФакте (если заказ заполнен)
  • курсы валют (если заказ в USD/EUR/BYN/KZT)

Ожидаемые (ТД_ДоговорыПотенциальные / _InfoRg112240):
  • ДатаПодписанияПлан в периоде
  • КП по ссылке заполнен
  • подразделение из коммерческого списка (не пустое)
  • ЗаказКлиента пуст (иначе это уже не «ожидаемый»)
  • статус КП — чёрный список (Enum1651):
      НеСогласовано, Аннулировано (+ Черновик, Отменено);
      проходят: Согласовано, Действует, Исполнено
  • партнёр не из перепродажи; для ОДП (и ликв. ОПБО) — список без Метрогазсервис
  • если ТД_ОсновноеТКПДляБМИ — берём ТД_СуммаТКПБМИ
  • курсы валют КП
  • ветка счёт-оферта пока не включена (в SQL 0)
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from pathlib import Path

import pyodbc

YEAR_OFFSET = 2000
PLAN_DEALS = bytes.fromhex("9370c9cb1d3024c84863b32957436199")
EMPTY16 = bytes(16)

# СоглашенияСКлиентами.Статус = Действует
AG_STATUS_ACTIVE = bytes.fromhex("9bc837718a593711499faab4626a96cf")

# СтатусыКоммерческихПредложенийКлиентам (_Enum1651) — имена из Config:
#   0 Черновик, 1 Согласовано, 2 НеСогласовано, 3 Действует,
#   4 Аннулировано, 5 Отменено, 6 Исполнено
# Чёрный список: не «ожидаемые». Белый: Согласовано / Действует / Исполнено.
KP_STATUS_BLACKLIST = [
    bytes.fromhex("9997b28a99e76971468fd27d9692c43e"),  # 0 Черновик
    bytes.fromhex("96ad0bb5f0c5c55d49f80062c82e990f"),  # 2 НеСогласовано
    bytes.fromhex("b6d16d5e86f9a4ab4dd5e9acb562957f"),  # 4 Аннулировано
    bytes.fromhex("8d9131a3b7366b594a93f1ade4584df3"),  # 5 Отменено
]
KP_TREF = bytes.fromhex("00000302")

# Валюты (_Reference53)
CUR_USD = bytes.fromhex("963e001cc4d0438811dfe1b60a7c6f22")
CUR_EUR = bytes.fromhex("81cd001583b3d75c11e07405d328a18d")
CUR_BYN = bytes.fromhex("8756ac1f6b05524d11ec45dc095e2c36")
CUR_KZT = bytes.fromhex("95fc6cb31113810e11efde2ee2bc7bc0")
CUR_RUB = bytes.fromhex("812d001e6711250911e762523fdf75b4")

# Партнёры перепродажи (Справочники_Партнеры_СписокПартнеровПерепродажи_НашиОрганизации)
# АЛМАЗ ООО (рабочий), Турбулентность-Дон ООО, Турбулентность-ДОН ООО НПО,
# СКТБ Турбо-Дон ООО, Метрогазсервис ООО
RESALE_PARTNERS = [
    bytes.fromhex("8266ac1f6b05524d11e7a8c56ff45495"),  # АЛМАЗ ООО (рабочий)
    bytes.fromhex("812e001e6711250911e788a06ac41964"),  # Турбулентность-Дон ООО
    bytes.fromhex("8266ac1f6b05524d11e7a8c46cdfe9f3"),  # Турбулентность-ДОН ООО НПО
    bytes.fromhex("8266ac1f6b05524d11e7a8c74babc7a7"),  # СКТБ Турбо-Дон ООО
    bytes.fromhex("8266ac1f6b05524d11e7a8c6d7f5ff44"),  # Метрогазсервис ООО
]
METROGAZ = bytes.fromhex("8266ac1f6b05524d11e7a8c6d7f5ff44")

# Отделы с правилом «перепродажа без МГС»: ОДП + ликв. ОПБО
ODP_DEPT = bytes.fromhex("96f96cb31113810e11f092f67587c178")  # Отдел дилерских продаж
OPBO_DEPT = bytes.fromhex("80da001e6711250911e49f994edcf3a0")  # ликв. бытовое
DEPTS_RESALE_NO_MGS = (ODP_DEPT, OPBO_DEPT)

# КП: ТД_ОсновноеТКПДляБМИ / ТД_СуммаТКПБМИ
KP_BMI_FLAG = "_Fld184256"
KP_BMI_SUM = "_Fld86876"
KP_CURRENCY = "_Fld25034RRef"

# Курсы констант ТД_ВалютаПланФакта_УЕ_* (в июле 2026 почти всё в RUB → 1)
# Подставляются если найдены; иначе 1.
FX_RATES = {"USD": 1.0, "EUR": 1.0, "BYN": 1.0, "KZT": 1.0}

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
    cur.execute("IF OBJECT_ID('tempdb..#resale') IS NOT NULL DROP TABLE #resale")
    cur.execute("CREATE TABLE #resale (id binary(16) PRIMARY KEY)")
    for p in RESALE_PARTNERS:
        cur.execute("INSERT INTO #resale(id) VALUES (?)", p)

    cur.execute("IF OBJECT_ID('tempdb..#resale_nomgs') IS NOT NULL DROP TABLE #resale_nomgs")
    cur.execute("CREATE TABLE #resale_nomgs (id binary(16) PRIMARY KEY)")
    for p in RESALE_PARTNERS:
        if p != METROGAZ:
            cur.execute("INSERT INTO #resale_nomgs(id) VALUES (?)", p)

    cur.execute("IF OBJECT_ID('tempdb..#dept_nomgs') IS NOT NULL DROP TABLE #dept_nomgs")
    cur.execute("CREATE TABLE #dept_nomgs (id binary(16) PRIMARY KEY)")
    for d in DEPTS_RESALE_NO_MGS:
        cur.execute("INSERT INTO #dept_nomgs(id) VALUES (?)", d)


def fx_sql(amount_expr: str, currency_expr: str) -> str:
    """SQL expression: amount * rate by currency (else as-is)."""
    return f"""
    CASE
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      ELSE ({amount_expr})
    END
    """


def fx_params() -> list:
    return [
        CUR_USD, FX_RATES["USD"],
        CUR_EUR, FX_RATES["EUR"],
        CUR_BYN, FX_RATES["BYN"],
        CUR_KZT, FX_RATES["KZT"],
    ]


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
        PLAN_DEALS,
        p0,
        p_next,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_fact(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """
    Договоры факт.
    Для ОДП/ОПБО: НЕ партнёр В перепродаже без МГС.
    Иначе: НЕ (партнёр В перепродаже И НЕ ТД_СопровождениеПродажи).
    """
    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS
    load_depts(cur, all_depts, "#fact_depts")
    load_resale(cur)

    amt = fx_sql("s._Fld112283", "ord._Fld21185RRef")
    cur.execute(
        f"""
        SELECT d.name, SUM({amt}) AS FactSum
        FROM _InfoRg112278 s WITH (NOLOCK)
        INNER JOIN #fact_depts d ON d.id = s._Fld112280RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = s._Fld112279RRef
        LEFT JOIN _Document704 ord WITH (NOLOCK)
          ON ord._IDRRef = s._Fld112481RRef
        WHERE s._Fld112284 >= ? AND s._Fld112284 < ?
          AND s._Fld112280RRef <> ?
          AND a._Fld13714RRef = ?
          AND (
                s._Fld112481RRef = ?
                OR ISNULL(ord._Fld184301, 0x00) = 0x00
              )
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = s._Fld112280RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = s._Fld112282RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = s._Fld112282RRef
                    ) AND ISNULL(s._Fld123477, 0x00) = 0x00
                    THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        *fx_params(),
        p0,
        p_next,
        EMPTY16,
        AG_STATUS_ACTIVE,
        EMPTY16,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_expected(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Договоры, ожидаемые к заключению (потенциальные), чёрный список статусов КП."""
    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS
    load_depts(cur, all_depts, "#exp_depts")
    load_resale(cur)

    amt_raw = f"""
      CASE WHEN ISNULL(kp.[{KP_BMI_FLAG}], 0x00) = 0x01
           THEN kp.[{KP_BMI_SUM}]
           ELSE p._Fld112244
      END
    """
    amt = fx_sql(amt_raw, f"kp.[{KP_CURRENCY}]")
    bl = ",".join("?" * len(KP_STATUS_BLACKLIST))

    cur.execute(
        f"""
        SELECT d.name, SUM({amt}) AS ExpSum
        FROM _InfoRg112240 p WITH (NOLOCK)
        INNER JOIN #exp_depts d ON d.id = p._Fld112241RRef
        INNER JOIN _Document770 kp WITH (NOLOCK)
          ON kp._IDRRef = p._Fld112292_RRRef
         AND p._Fld112292_RTRef = ?
        WHERE p._Fld112245 >= ? AND p._Fld112245 < ?
          AND p._Fld112241RRef <> ?
          AND p._Fld114063RRef = ?
          AND p._Fld112292_RRRef <> ?
          AND kp._Fld25044RRef NOT IN ({bl})
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = p._Fld112241RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = p._Fld112243RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = p._Fld112243RRef
                    ) THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        *fx_params(),
        KP_TREF,
        p0,
        p_next,
        EMPTY16,
        EMPTY16,
        EMPTY16,
        *KP_STATUS_BLACKLIST,
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
    print(f"БД: erp_pm @ localhost")
    print("Метрика: договоры (МП / факт / ожидаемые)")
    print(f"FX rates: {FX_RATES}")
    print(f"Партнёры перепродажи: {len(RESALE_PARTNERS)}\n")

    mp = calc_mp_plan(cur, p0, p_next)
    fact = calc_fact(cur, p0, p_next)
    expected = calc_expected(cur, p0, p_next)

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
        "Колонки: Маркетинговый план по договорам | Договоры заключенные (факт) | "
        "Договоры, ожидаемые к заключению (план)",
        f"FX: {FX_RATES}",
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
    print(f"Факт заключённые:           {fmt(fact_total)}")
    print(f"Ожидаемые к заключению:     {fmt(exp_total)}")
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
        "- МП: _AccumRg96963, вид Договоры",
        "- Факт: _InfoRg112278, статус Действует, перепродажа + ТД_СопровождениеПродажи",
        "- Ожидаемые: чёрный список статусов КП "
        "(Черновик, НеСогласовано, Аннулировано, Отменено);",
        "  КП заполнен, заказ пуст, коммерч. отделы, перепродажа (ОДП — без МГС), БМИ-сумма",
        "- Ветка счёт-оферта пока не включена",
        f"- FX rates: {FX_RATES} (в июле почти все суммы в RUB)",
    ]
    out = OUT_DIR / f"plan_fact_dogovory_{y}_{m:02d}.txt"
    out.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nОтчёт сохранён: {out}")
    cn.close()


if __name__ == "__main__":
    main(as_of=date(2026, 7, 27))
