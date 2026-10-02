# -*- coding: utf-8 -*-
"""
Цена фактическая и цена расчётная по КП для коммерческого директора.

Источник: Документ.КоммерческоеПредложениеКлиенту (_Document770)
Подразделение: Справочник.Пользователи (_Reference366).Подразделение
               ← КП.Менеджер

Отбор КП:
  • Date в месяце
  • DeletionMark = false
  • статус Действует или Исполнено
    (как в запросе 1С; поле SQL _Fld25044 для части документов расходится
     со статусом, который видит запрос, поэтому отбор идёт через OData)

Формулы:
  Цена фактическая = СуммаДокумента
  Цена расчётная   = СуммаДокумента + |СуммаСкидкиТКП|

Запуск:
  python calc_plan_fact_cena.py 2026-07
  python calc_plan_fact_cena.py 2026-07 --dept 96f96cb31113810e11f092f67587c178
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import pyodbc
import requests
from requests.auth import HTTPBasicAuth

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


ODATA_BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
ODATA_ENTITY = "Document_КоммерческоеПредложениеКлиенту"
ODATA_AUTH = HTTPBasicAuth("odata.user", "npo852456")
ACTIVE_STATUSES = ("Действует", "Исполнено")


def fact_expr() -> str:
    return f"ISNULL(kp.[{KP_SUM}], 0)"


def calc_expr() -> str:
    return f"ISNULL(kp.[{KP_SUM}], 0) + ABS(ISNULL(kp.[{KP_DISC_TKP}], 0))"


def status_filter_sql() -> str:
    return f"AND kp.[{KP_STATUS}] IN (?, ?)"


def status_params() -> list[bytes]:
    return [ST_ACTIVE, ST_DONE]


def _odata_guid_to_sql(guid: str) -> bytes:
    g = guid.replace("-", "")
    return bytes.fromhex(g[16:20] + g[20:32] + g[12:16] + g[8:12] + g[0:8])


def _sql_to_odata_guid(raw: bytes) -> str:
    h = raw.hex()
    return f"{h[24:32]}-{h[20:24]}-{h[16:20]}-{h[0:4]}-{h[4:16]}"


def _calendar_dt(dt: datetime) -> datetime:
    return dt.replace(year=dt.year - YEAR_OFFSET)


def _fetch_kp(start: datetime, end: datetime) -> list[dict]:
    """КП за календарный период со статусом Действует или Исполнено."""
    flt = (
        f"Date ge datetime'{start:%Y-%m-%dT%H:%M:%S}'"
        f" and Date lt datetime'{end:%Y-%m-%dT%H:%M:%S}'"
        f" and DeletionMark eq false"
        f" and (Статус eq 'Действует' or Статус eq 'Исполнено')"
    )
    sel = "Ref_Key,СуммаДокумента,СуммаСкидкиТКП,Менеджер_Key"
    session = requests.Session()
    session.auth = ODATA_AUTH
    docs: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{ODATA_BASE}/{quote(ODATA_ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',')}"
            f"&$top=300&$skip={skip}&$orderby=Ref_Key"
        )
        response = session.get(url, timeout=120)
        response.raise_for_status()
        batch = response.json().get("value") or []
        docs.extend(batch)
        if len(batch) < 300:
            return docs
        skip += len(batch)


def _manager_depts(cur, manager_guids: set[str]) -> dict[str, bytes]:
    ids: list[bytes] = []
    for guid in manager_guids:
        if not guid or guid.startswith("00000000"):
            continue
        try:
            ids.append(_odata_guid_to_sql(guid))
        except ValueError:
            continue
    out: dict[str, bytes] = {}
    for offset in range(0, len(ids), 80):
        chunk = ids[offset:offset + 80]
        marks = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT _IDRRef, [{USER_DEPT}]
            FROM _Reference366 WITH (NOLOCK)
            WHERE _IDRRef IN ({marks})
            """,
            *chunk,
        )
        for user_id, dept in cur.fetchall():
            if not dept or bytes(dept) == EMPTY16:
                continue
            out[_sql_to_odata_guid(bytes(user_id))] = bytes(dept)
    return out


def _accumulate(docs: list[dict], dept_of: dict[str, bytes]) -> dict[bytes, tuple[float, float, int]]:
    acc: dict[bytes, list[float]] = {}
    for doc in docs:
        dept = dept_of.get(doc.get("Менеджер_Key") or "")
        if not dept:
            continue
        fact = float(doc.get("СуммаДокумента") or 0)
        calc = fact + abs(float(doc.get("СуммаСкидкиТКП") or 0))
        bucket = acc.setdefault(dept, [0.0, 0.0, 0.0])
        bucket[0] += fact
        bucket[1] += calc
        bucket[2] += 1
    return {dept: (fact, calc, int(count)) for dept, (fact, calc, count) in acc.items()}


def calc_by_dept(cur, p0: datetime, p_next: datetime) -> dict[bytes, tuple[float, float, int]]:
    start, end = _calendar_dt(p0), _calendar_dt(p_next)
    docs = _fetch_kp(start, end)
    depts = _manager_depts(cur, {d.get("Менеджер_Key") or "" for d in docs})
    return _accumulate(docs, depts)


def _calc_by_dept_sql(cur, p0: datetime, p_next: datetime) -> dict[bytes, tuple[float, float, int]]:
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
    return calc_by_dept(cur, p0, p_next).get(dept, (0.0, 0.0, 0))


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
        "Отбор: Date в месяце, DeletionMark=false, статус Действует или Исполнено",
        "Цена факт = СуммаДокумента",
        "Цена расч = СуммаДокумента + |СуммаСкидкиТКП|",
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
