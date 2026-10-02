# -*- coding: utf-8 -*-
"""
Факт расходов для коммерческого директора за месяц.

Источник: регистр бухгалтерии Хозрасчётный (_AccRg2005).

Формула (по каждому отделу):
  Расходы = сумма проведённых «Списание безналичных денежных средств»
            по статьям ДДС группы «КС»,
            подразделение — из заявки на расходование.
  Бухгалтерия и подразделения вне 8 коммерческих отделов не входят.

Где:
  • 44 / 44.01 / 44.02 — дебетовый оборот по 8 статьям затрат (ValueDt1 → _Chrc1945)
  • 44 без ВРТ: интернет, командировки, представительские, таможня
  • 44-ВРТ: выставки, рекламная продукция, тендер-инфо, транспорт на выставки
  • СДС: кредит 51 по 3 статьям ДДС (ValueCt2 → _Reference503), подразделение на Dt
  • Дт 51: дебет 51 по 2 статьям ДДС (ValueDt2), подразделение на Ct
  • 71.01: накопленное дебетовое сальдо на конец месяца (только > 0)
  • Сторно (_Fld140878=0x01) → сумма с минусом

Комдир = сумма по 8 отделам (ВЭД, ОПЭОиУ, БМИ, ОРКК, ОДП, Газпром, PR, тендерный).

Запуск:
  python calc_plan_fact_rashody.py 2026-07
"""
from __future__ import annotations

import argparse
import calendar
import sys
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import pyodbc
import requests
from requests.auth import HTTPBasicAuth

YEAR_OFFSET = 2000

ACCOUNTS_44 = [
    ("44", "812d001e6711250911e76250fb2bde54"),
    ("44.01", "812d001e6711250911e76250fb2bde55"),
    ("44.02", "812d001e6711250911e76250fb2bde56"),
]
ACC_51 = "812d001e6711250911e76250fb2bde64"
ACC_7101 = "812d001e6711250911e76250fb2bded3"

# Статьи затрат 44 (_Chrc1945) — 8 шт.
ARTS_44_NON_VRT = [
    ("Интернет-продвижение (44 сч)!", "8271ac1f6b05524d11e83418f6b5f6a7"),
    ("Командировка (44 сч)!", "b81600804842857511e056cb19919da8"),
    ("Представительские расходы (44.01) ТД!", "826dac1f6b05524d11e7ff4ad211f6cb"),
    ("Таможенные расходы (44.01)!", "826fac1f6b05524d11e81b839900a8d0"),
]
ARTS_44_VRT = [
    ("Расходы на выставки, конференцию (44 сч)!", "8271ac1f6b05524d11e8341a3cba9c50"),
    ("Рекламная продукция (44)!", "8128001e6711250911e6ed4acdc34aed"),
    (
        "Тендер ТОЛЬКО НПО- Информационные услуги-электронной площадке/сайт (44 сч)!",
        "adf9001e6711250911e3ae99eee66843",
    ),
    (
        "Транспортные расходы по доставке ТМЦ на выставки (44 сч)!",
        "828dac1f6b05524d11e95d1df33409f4",
    ),
]

# СДС: кредит 51, 3 статьи ДДС (_Reference503) — списание ДС под ВРТ
ARTS_SDS = [
    (
        "Выставки, конференции, затраты на рекламу_2_КС_ОС_3.3.",
        "810b001e6711250911e5c8a95ec4fd64",
    ),
    (
        "Печатная продукция (визитки, буклеты, листовки, плакаты)_2_КС_ОМ_3.3.3.",
        "8127001e6711250911e6ec45badd95fb",
    ),
    (
        "Тендеры затраты по подпискам и участию_2_КС_ОТП_3.4.",
        "84f3ac1f6b05524d11eb533f55eeaa60",
    ),
]

# Дт 51: 2 статьи ДДС (_Reference503, ValueDt2; подразделение на Ct).
# TODO: подставить точные 2 статьи — в ТЗ названия не указаны.
# Пока пусто (0), чтобы не искажать итог; СДС / 44 / 71.01 считаются.
ARTS_DT51: list[tuple[str, str]] = [
    ("Статья ДДС 1 (Дт 51)", "810b001e6711250911e5d476f6f0d74d"),
    ("Статья ДДС 2 (Дт 51)", "8930ac1f6b05524d11ed081233a983cc"),
]

DEPTS: list[tuple[str, str]] = [
    ("Отдел внешнеэкономической деятельности", "8283ac1f6b05524d11e8e40149480c10"),
    ("Отдел продаж эталонного оборудования и услуг", "80d6001e6711250911e4810f34497ef7"),
    ("Отдел продаж БМИ", "93d36cb31113810e11ee37a59edaa7d4"),
    ("Отдел по работе с ключевыми клиентами", "8523ac1f6b05524d11eb67b6639ec87b"),
    ("Отдел дилерских продаж", "96f96cb31113810e11f092f67587c178"),
    ("Отдел по работе с ПАО Газпром", "80da001e6711250911e49f9cbd7b5184"),
    ("Сектор рекламы и PR", "93d36cb31113810e11ee37a495dfd1c6"),
    ("Тендерный офис", "8129cd2988c3db2d11e0d91b1c9f9419"),
]
FACT_ORDER = [n for n, _ in DEPTS]

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


def amount_expr(alias: str = "r") -> str:
    return f"""
        CASE WHEN {alias}._Fld140878 = 0x01
             THEN -CAST({alias}._Fld2010 AS float)
             ELSE CAST({alias}._Fld2010 AS float) END
    """


def zero_by_dept() -> dict[str, float]:
    return {n: 0.0 for n in FACT_ORDER}


def calc_44_by_dept(
    cur, p0: datetime, p1: datetime, articles: list[tuple[str, str]]
) -> dict[str, float]:
    load_temp(cur, "#depts", DEPTS)
    load_ids(cur, "#accs", [hx for _, hx in ACCOUNTS_44])
    load_ids(cur, "#arts", [hx for _, hx in articles])
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
        p1,
    )
    out = zero_by_dept()
    for name, amt in cur.fetchall():
        out[name] = float(amt or 0)
    return out


def calc_sds_by_dept(cur, p0: datetime, p1: datetime) -> dict[str, float]:
    """Кредит 51 по 3 статьям СДС; подразделение — сторона Dt."""
    load_temp(cur, "#depts", DEPTS)
    load_ids(cur, "#arts", [hx for _, hx in ARTS_SDS])
    acc = bytes.fromhex(ACC_51)
    cur.execute(
        f"""
        SELECT d.name, SUM({amount_expr()}) AS Amt
        FROM _AccRg2005 r WITH (NOLOCK)
        INNER JOIN #depts d ON d.id = r._Fld2008DtRRef
        INNER JOIN #arts t ON t.id = r._ValueCt2_RRRef
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
          AND r._AccountCtRRef = ?
        GROUP BY d.name
        """,
        p0,
        p1,
        acc,
    )
    out = zero_by_dept()
    for name, amt in cur.fetchall():
        out[name] = float(amt or 0)
    return out


def calc_dt51_by_dept(cur, p0: datetime, p1: datetime) -> dict[str, float]:
    """Дебет 51 по 2 статьям ДДС; подразделение — сторона Ct (корреспонденция)."""
    load_temp(cur, "#depts", DEPTS)
    load_ids(cur, "#arts", [hx for _, hx in ARTS_DT51])
    acc = bytes.fromhex(ACC_51)
    cur.execute(
        f"""
        SELECT d.name, SUM({amount_expr()}) AS Amt
        FROM _AccRg2005 r WITH (NOLOCK)
        INNER JOIN #depts d ON d.id = r._Fld2008CtRRef
        INNER JOIN #arts t ON t.id = r._ValueDt2_RRRef
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
          AND r._AccountDtRRef = ?
        GROUP BY d.name
        """,
        p0,
        p1,
        acc,
    )
    out = zero_by_dept()
    for name, amt in cur.fetchall():
        out[name] = float(amt or 0)
    return out


# Группа статей ДДС «КС» (_Reference503). Факт плитки — списания безналичных
# по этим статьям, подразделение берётся из заявки на расходование.
KS_DDS_GROUP = "84F3AC1F6B05524D11EB533E680B2130"
WRITEOFF_DOC = "_Document980"
WRITEOFF_LINES = "_Document980_VT37251"
REQUEST_DOC = "_Document726"
DEPT_CATALOG = "_Reference513"


def calc_ks_writeoff_by_dept(cur, p0: datetime, p1: datetime) -> dict[str, float]:
    """Сумма проведённых списаний безналичных ДС по статьям группы «КС».

    Сумма строки — _Fld37256 (колонка «Сумма» отчёта «Списания ДС по статьям ДДС»).
    Подразделение — заявка на расходование (_Document726._Fld22796RRef).
    В словарь попадают только 8 коммерческих отделов; бухгалтерия и прочие нет.
    """
    ks = bytes.fromhex(KS_DDS_GROUP)
    cur.execute(
        f"""
        WITH tree AS (
            SELECT _IDRRef
            FROM _Reference503 WITH (NOLOCK)
            WHERE _IDRRef = ?
            UNION ALL
            SELECT c._IDRRef
            FROM _Reference503 c WITH (NOLOCK)
            INNER JOIN tree t ON c._ParentIDRRef = t._IDRRef
            WHERE c._Marked = 0x00
        )
        SELECT CAST(dep._Description AS nvarchar(200)) AS dept,
               SUM(CAST(v._Fld37256 AS float)) AS amt
        FROM {WRITEOFF_LINES} v WITH (NOLOCK)
        INNER JOIN {WRITEOFF_DOC} d WITH (NOLOCK)
            ON d._IDRRef = v._Document980_IDRRef
        INNER JOIN tree a ON a._IDRRef = v._Fld37254RRef
        INNER JOIN {REQUEST_DOC} z WITH (NOLOCK)
            ON z._IDRRef = v._Fld37264_RRRef
           AND v._Fld37264_RTRef = 726
        INNER JOIN {DEPT_CATALOG} dep WITH (NOLOCK)
            ON dep._IDRRef = z._Fld22796RRef
        WHERE d._Date_Time >= ? AND d._Date_Time < ?
          AND d._Posted = 0x01
          AND d._Marked = 0x00
          AND a._IDRRef <> ?
        GROUP BY dep._Description
        """,
        ks,
        p0,
        p1,
        ks,
    )
    out = zero_by_dept()
    for name, amt in cur.fetchall():
        name = str(name or "").strip()
        if name in out:
            out[name] = float(amt or 0)
    return out


ODATA_BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
ODATA_AUTH = HTTPBasicAuth("odata.user", "npo852456")
ORG_NPO = "fbca2148-6cfd-11e7-812d-001e67112509"
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


def _hex_to_odata(hx: str) -> str:
    h = hx.lower()
    return f"{h[24:32]}-{h[20:24]}-{h[16:20]}-{h[0:4]}-{h[4:16]}"


def dump_covers_through(cur, p_next: datetime) -> bool:
    """Локальная копия закрывает месяц, если в ней есть проводки не раньше его конца."""
    cur.execute(
        """
        SELECT MAX(_Date_Time)
        FROM _Document980 WITH (NOLOCK)
        WHERE _Posted = 0x01 AND _Marked = 0x00
        """
    )
    row = cur.fetchone()
    mx = row[0] if row else None
    return mx is not None and mx >= p_next


def _ks_article_guids(cur) -> set[str]:
    ks = bytes.fromhex(KS_DDS_GROUP)
    cur.execute(
        """
        WITH tree AS (
            SELECT _IDRRef
            FROM _Reference503 WITH (NOLOCK)
            WHERE _IDRRef = ?
            UNION ALL
            SELECT c._IDRRef
            FROM _Reference503 c WITH (NOLOCK)
            INNER JOIN tree t ON c._ParentIDRRef = t._IDRRef
            WHERE c._Marked = 0x00
        )
        SELECT CONVERT(varchar(32), _IDRRef, 2) FROM tree
        """,
        ks,
    )
    group = _hex_to_odata(KS_DDS_GROUP)
    return {_hex_to_odata(r[0]) for r in cur.fetchall()} - {group}


def calc_ks_writeoff_live(cur, p0: datetime, p1: datetime) -> dict[str, float]:
    """Тот же отбор, что у calc_ks_writeoff_by_dept, по живой 1С (OData).

    Локальный erp_pm — копия. Пока она отстаёт от конца месяца, сентябрьские
    списания после даты копии в неё не попадают.
    """
    articles = _ks_article_guids(cur)
    name_by_guid = {_hex_to_odata(hx): name for name, hx in DEPTS}
    start = f"{p0.year - YEAR_OFFSET:04d}-{p0.month:02d}-{p0.day:02d}T00:00:00"
    end = f"{p1.year - YEAR_OFFSET:04d}-{p1.month:02d}-{p1.day:02d}T00:00:00"
    flt = (
        f"Date ge datetime'{start}' and Date lt datetime'{end}'"
        f" and Posted eq true and DeletionMark eq false"
        f" and Организация_Key eq guid'{ORG_NPO}'"
    )
    session = requests.Session()
    session.auth = ODATA_AUTH
    docs: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{ODATA_BASE}/Document_СписаниеБезналичныхДенежныхСредств"
            f"?$format=json&$top=80&$skip={skip}"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote('Ref_Key,РасшифровкаПлатежа', safe=',')}"
        )
        resp = None
        for attempt in range(4):
            resp = session.get(url, timeout=180)
            if resp.status_code < 500:
                break
            time.sleep(1.5 * (attempt + 1))
        resp.raise_for_status()
        batch = resp.json().get("value") or []
        docs.extend(batch)
        if len(batch) < 80:
            break
        skip += 80

    by_req: dict[str, float] = {}
    for doc in docs:
        for row in doc.get("РасшифровкаПлатежа") or []:
            art = (row.get("СтатьяДвиженияДенежныхСредств_Key") or "").lower()
            if art not in articles:
                continue
            raw = row.get("ЗаявкаНаРасходованиеДенежныхСредств")
            if not isinstance(raw, str) or not raw or raw.lower() == EMPTY_GUID:
                continue
            by_req[raw.lower()] = by_req.get(raw.lower(), 0.0) + float(row.get("Сумма") or 0)

    dept_of: dict[str, str] = {}
    keys = list(by_req)
    for i in range(0, len(keys), 15):
        batch = keys[i:i + 15]
        req_flt = " or ".join(f"Ref_Key eq guid'{g}'" for g in batch)
        url = (
            f"{ODATA_BASE}/Document_ЗаявкаНаРасходованиеДенежныхСредств"
            f"?$format=json&$top=15"
            f"&$filter={quote(req_flt, safe='')}"
            f"&$select={quote('Ref_Key,Подразделение_Key', safe=',')}"
        )
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        for item in resp.json().get("value") or []:
            dept_of[item["Ref_Key"].lower()] = (item.get("Подразделение_Key") or "").lower()

    out = zero_by_dept()
    for req, amt in by_req.items():
        name = name_by_guid.get(dept_of.get(req, ""))
        if name in out:
            out[name] += amt
    return {name: round(amt, 2) for name, amt in out.items()}


def calc_71_debit_balance(cur, asof_exclusive: datetime) -> dict[str, float]:
    """Дебетовое сальдо 71.01 на конец месяца (= начало следующего); кредит/≤0 → 0."""
    load_temp(cur, "#depts", DEPTS)
    acc = bytes.fromhex(ACC_7101)
    cur.execute(
        f"""
        SELECT d.name,
          SUM(CASE WHEN r._AccountDtRRef = ? AND r._Fld2008DtRRef = d.id
                   THEN {amount_expr()} ELSE 0 END) AS DtAmt,
          SUM(CASE WHEN r._AccountCtRRef = ? AND r._Fld2008CtRRef = d.id
                   THEN {amount_expr()} ELSE 0 END) AS CtAmt
        FROM _AccRg2005 r WITH (NOLOCK)
        CROSS JOIN #depts d
        WHERE r._Period < ?
          AND r._Active = 0x01
          AND (r._AccountDtRRef = ? OR r._AccountCtRRef = ?)
          AND (
                r._Fld2008DtRRef IN (SELECT id FROM #depts)
             OR r._Fld2008CtRRef IN (SELECT id FROM #depts)
          )
        GROUP BY d.name
        """,
        acc,
        acc,
        asof_exclusive,
        acc,
        acc,
    )
    out = zero_by_dept()
    for name, dt, ct in cur.fetchall():
        bal = float(dt or 0) - float(ct or 0)
        out[name] = bal if bal > 0 else 0.0
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Факт расходов коммерческой службы за месяц"
    )
    ap.add_argument("month", help="Месяц YYYY-MM (например 2026-07)")
    args = ap.parse_args(argv)

    y, m = parse_month(args.month)
    p0 = to_1c_dt(date(y, m, 1))
    if m == 12:
        p1 = to_1c_dt(date(y + 1, 1, 1))
        month_end = date(y, 12, 31)
    else:
        p1 = to_1c_dt(date(y, m + 1, 1))
        month_end = date(y, m, calendar.monthrange(y, m)[1])

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    print(f"Факт расходов за {y}-{m:02d}")
    print("БД: erp_pm @ localhost")
    print("Считаю обороты 44 / СДС / Дт51 ...")

    non_vrt = calc_44_by_dept(cur, p0, p1, ARTS_44_NON_VRT)
    vrt = calc_44_by_dept(cur, p0, p1, ARTS_44_VRT)
    sds = calc_sds_by_dept(cur, p0, p1)
    dt51 = calc_dt51_by_dept(cur, p0, p1)

    print("Считаю сальдо 71.01 (накопительно, может занять ~1 мин) ...")
    bal71 = calc_71_debit_balance(cur, p1)

    rows: list[dict] = []
    for name in FACT_ORDER:
        a = non_vrt.get(name, 0.0)
        b = vrt.get(name, 0.0)
        c = sds.get(name, 0.0)
        d = dt51.get(name, 0.0)
        e = bal71.get(name, 0.0)
        chosen_vrt = max(b, c)
        total = a + chosen_vrt + d + e
        rows.append(
            {
                "name": name,
                "44_bez": a,
                "44_vrt": b,
                "sds": c,
                "max_vrt_sds": chosen_vrt,
                "dt51": d,
                "saldo71": e,
                "total": total,
            }
        )

    grand = sum(r["total"] for r in rows)

    header = (
        f"{'Отдел':<48} {'44 без ВРТ':>12} {'44-ВРТ':>12} {'СДС':>12} "
        f"{'max(ВРТ,СДС)':>12} {'Дт51':>12} {'71.01':>12} {'Расходы':>14}"
    )
    sep = "-" * len(header)

    lines: list[str] = [
        f"Факт расходов за {y}-{m:02d} (на {month_end.isoformat()})",
        "Источник: Хозрасчётный (_AccRg2005)",
        "Формула: (44 без ВРТ) + max(44-ВРТ, СДС) + Дт51(2 статьи) + max(сальдо 71.01, 0)",
        "",
        "44 без ВРТ: интернет, командировки, представительские, таможня",
        "44-ВРТ: выставки, рекламная продукция, тендер-инфо, транспорт на выставки",
        "СДС (Kt 51): "
        + "; ".join(n for n, _ in ARTS_SDS),
        "Дт 51: "
        + (
            "; ".join(n for n, _ in ARTS_DT51)
            if ARTS_DT51
            else "(статьи не заданы — нужно уточнить 2 статьи ДДС)"
        ),
        "",
        header,
        sep,
    ]

    print()
    print(header)
    print(sep)
    for r in rows:
        line = (
            f"{r['name']:<48} {fmt(r['44_bez']):>12} {fmt(r['44_vrt']):>12} "
            f"{fmt(r['sds']):>12} {fmt(r['max_vrt_sds']):>12} "
            f"{fmt(r['dt51']):>12} {fmt(r['saldo71']):>12} {fmt(r['total']):>14}"
        )
        print(line)
        lines.append(line)

    total_line = f"{'ИТОГО коммерческий директор':<48} {'':>12} {'':>12} {'':>12} {'':>12} {'':>12} {'':>12} {fmt(grand):>14}"
    print(sep)
    print(total_line)
    lines += [
        sep,
        total_line,
        "",
        f"Расходы факт итого: {grand}",
        "",
        "Суммы компонентов (итого по 8 отделам):",
        f"  44 без ВРТ:     {sum(r['44_bez'] for r in rows)}",
        f"  44-ВРТ:         {sum(r['44_vrt'] for r in rows)}",
        f"  СДС:            {sum(r['sds'] for r in rows)}",
        f"  max(ВРТ,СДС):   {sum(r['max_vrt_sds'] for r in rows)}",
        f"  Дт 51:          {sum(r['dt51'] for r in rows)}",
        f"  71.01 дебет:    {sum(r['saldo71'] for r in rows)}",
    ]

    out = OUT_DIR / f"plan_fact_rashody_{y}_{m:02d}.txt"
    out.write_text("\n".join(lines), encoding="utf-8")

    # TSV breakdown
    tsv_path = OUT_DIR / f"plan_fact_rashody_{y}_{m:02d}.tsv"
    tsv_lines = [
        "Отдел\t44_без_ВРТ\t44_ВРТ\tСДС\tmax_ВРТ_СДС\tДт51\tСальдо_71_01\tРасходы"
    ]
    for r in rows:
        tsv_lines.append(
            f"{r['name']}\t{r['44_bez']:.2f}\t{r['44_vrt']:.2f}\t{r['sds']:.2f}\t"
            f"{r['max_vrt_sds']:.2f}\t{r['dt51']:.2f}\t{r['saldo71']:.2f}\t{r['total']:.2f}"
        )
    tsv_lines.append(
        f"ИТОГО\t{sum(r['44_bez'] for r in rows):.2f}\t{sum(r['44_vrt'] for r in rows):.2f}\t"
        f"{sum(r['sds'] for r in rows):.2f}\t{sum(r['max_vrt_sds'] for r in rows):.2f}\t"
        f"{sum(r['dt51'] for r in rows):.2f}\t{sum(r['saldo71'] for r in rows):.2f}\t{grand:.2f}"
    )
    tsv_path.write_text("\n".join(tsv_lines), encoding="utf-8")

    print(f"\nОтчёт: {out}")
    print(f"TSV:   {tsv_path}")
    cn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        raise
