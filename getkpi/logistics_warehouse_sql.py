"""Отгрузки складов НПО и Алмаз из MSSQL erp_pm."""
from __future__ import annotations

from typing import Any

from comdir.calc_plan_fact_otgruzki import (
    COMMERCIAL_DEPTS,
    KIND_EXPENSE,
    KIND_RECEIPT,
    calc_mp_plan,
)
from comdir.common import period_bounds
from sql_connection import SqlConnection

# Справочник.Склады
WAREHOUSE_NPO_FG = bytes.fromhex("8267AC1F6B05524D11E7CAB8023B3C10")  # №1
WAREHOUSE_ALMAZ_FG = bytes.fromhex("8267AC1F6B05524D11E7D055C7513EFC")  # №2
WAREHOUSE_ALMAZ_REPAIR_FG = bytes.fromhex("8276AC1F6B05524D11E8640F4FE2C8B3")
WAREHOUSE_ALMAZ_REPAIR_IN = bytes.fromhex("840AAC1F6B05524D11EAF805DB866457")

# Организация ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО (как в calc_plan_fact_vp)
ORG_NPO = bytes.fromhex("812d001e6711250911e76cfdfbca2148")
COMMISSION = bytes.fromhex("8c8d4fb9e9a4c800423c97037b7eee65")
OUR_ENTERPRISE = bytes.fromhex("812d001e6711250911e762515fc811cd")

NPO_PLAN_DEPTS: list[tuple[str, str]] = [
    name_hex
    for name_hex in COMMERCIAL_DEPTS
    if name_hex[0]
    not in {
        "Отдел продаж эталонного оборудования и услуг",
        "Отдел продаж БМИ",
    }
]

# Вид номенклатуры: счётчики / датчики давления / СПУ (без серий, ремонта, услуг)
ALMAZ_FG_KIND_SQL = """
(
    (
        kind._Description LIKE N'%Счетчик%'
        OR kind._Description LIKE N'%счетчик%'
        OR kind._Description LIKE N'%Датчик%давлен%'
        OR kind._Description LIKE N'%Датчики давления%'
        OR kind._Description LIKE N'%Преобразователь давления%'
        OR kind._Description LIKE N'%СПУ%'
    )
    AND kind._Description NOT LIKE N'%Для серий%'
    AND kind._Description NOT LIKE N'%Для характеристик%'
    AND kind._Description NOT LIKE N'Ремонт%'
    AND kind._Description NOT LIKE N'Замена%'
    AND kind._Description NOT LIKE N'Услуги%'
    AND kind._Description NOT LIKE N'Эталонные%'
)
"""


def _round_money(value: float | None) -> float:
    return round(float(value or 0), 2)


def _round_qty(value: float | None) -> float:
    return round(float(value or 0), 3)


def calc_npo_plan(cursor, year: int, month: int) -> float:
    p0, p_next = period_bounds(year, month)
    by_name = calc_mp_plan(cursor, p0, p_next)
    allowed = {name for name, _ in NPO_PLAN_DEPTS}
    return _round_money(sum(amt for name, amt in by_name.items() if name in allowed))


def calc_npo_fact(cursor, year: int, month: int) -> float:
    """Выручка отчёта «Валовая прибыль предприятия — для зп склада».

    РН ВыручкаИСебестоимостьПродаж, организация НПО, без отделов БМИ и услуг.
    """
    p0, p_next = period_bounds(year, month)
    cursor.execute("IF OBJECT_ID('tempdb..#npo_fact_depts') IS NOT NULL DROP TABLE #npo_fact_depts")
    cursor.execute("CREATE TABLE #npo_fact_depts (id binary(16) PRIMARY KEY)")
    for _, hx in NPO_PLAN_DEPTS:
        cursor.execute("INSERT INTO #npo_fact_depts(id) VALUES (?)", bytes.fromhex(hx))
    cursor.execute(
        """
        SELECT SUM(CAST(r._Fld51092 AS float))
        FROM _AccumRg51076 r WITH (NOLOCK)
        INNER JOIN _Reference225 a WITH (NOLOCK)
            ON a._IDRRef = r._Fld51079RRef
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
          AND r._Fld51081RRef <> ?
          AND a._Fld6922RRef <> ?
          AND a._Fld6923RRef = ?
          AND r._Fld51080RRef IN (SELECT id FROM #npo_fact_depts)
        """,
        p0,
        p_next,
        COMMISSION,
        OUR_ENTERPRISE,
        ORG_NPO,
    )
    row = cursor.fetchone()
    return _round_money(row[0] if row else 0)


def _ship_qty(
    cursor,
    *,
    year: int,
    month: int,
    warehouse: bytes,
    kind: bytes,
    analytics: bool,
) -> float:
    p0, p_next = period_bounds(year, month)
    sign = "-s._Fld169765" if kind == KIND_EXPENSE else "s._Fld169765"
    analytics_join = ""
    analytics_where = ""
    if analytics:
        analytics_join = """
        INNER JOIN _Reference269 nom WITH (NOLOCK)
          ON nom._IDRRef = s._Fld169759RRef
        INNER JOIN _Reference87 kind WITH (NOLOCK)
          ON kind._IDRRef = nom._Fld8008RRef
        """
        analytics_where = f"AND {ALMAZ_FG_KIND_SQL}"
    cursor.execute(
        f"""
        SELECT SUM(CAST({sign} AS float))
        FROM _AccumRg169757 s WITH (NOLOCK)
        {analytics_join}
        WHERE s._Period >= ? AND s._Period < ?
          AND s._Active = 0x01
          AND ISNULL(s._Fld169770, 0x00) = 0x00
          AND s._Fld169762RRef = ?
          AND s._Fld169764RRef = ?
          {analytics_where}
        """,
        p0,
        p_next,
        warehouse,
        kind,
    )
    row = cursor.fetchone()
    return _round_qty(row[0] if row else 0)


def calc_almaz_fact(cursor, year: int, month: int) -> dict[str, float]:
    fg = _ship_qty(
        cursor,
        year=year,
        month=month,
        warehouse=WAREHOUSE_ALMAZ_FG,
        kind=KIND_EXPENSE,
        analytics=True,
    )
    repair_out = _ship_qty(
        cursor,
        year=year,
        month=month,
        warehouse=WAREHOUSE_ALMAZ_REPAIR_FG,
        kind=KIND_EXPENSE,
        analytics=False,
    )
    repair_in = _ship_qty(
        cursor,
        year=year,
        month=month,
        warehouse=WAREHOUSE_ALMAZ_REPAIR_IN,
        kind=KIND_RECEIPT,
        analytics=False,
    )
    return {
        "fg": fg,
        "repair_out": repair_out,
        "repair_in": repair_in,
        "total": _round_qty(fg + repair_out + repair_in),
    }


def calculate_npo_month(
    year: int,
    month: int,
    *,
    sql: SqlConnection | None = None,
    cursor=None,
) -> dict[str, Any]:
    if cursor is not None:
        return {
            "plan": calc_npo_plan(cursor, year, month),
            "fact": calc_npo_fact(cursor, year, month),
        }
    sql = sql or SqlConnection()
    with sql.connect_ctx() as connection:
        return calculate_npo_month(year, month, cursor=connection.cursor())


def calculate_almaz_month(
    year: int,
    month: int,
    *,
    sql: SqlConnection | None = None,
    cursor=None,
) -> dict[str, Any]:
    if cursor is not None:
        parts = calc_almaz_fact(cursor, year, month)
        return {
            "plan": None,
            "fact": parts["total"],
            "fg": parts["fg"],
            "repair_out": parts["repair_out"],
            "repair_in": parts["repair_in"],
        }
    sql = sql or SqlConnection()
    with sql.connect_ctx() as connection:
        return calculate_almaz_month(year, month, cursor=connection.cursor())
