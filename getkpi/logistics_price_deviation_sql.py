"""LOG-M2: закупочная и проектная стоимость из MSSQL-копии 1С."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sql_connection import SqlConnection

YEAR_OFFSET = 2000
EMPTY_HEX = "0x" + "00" * 16
PROJECT_PRICE_TYPE_HEX = "0x812D001E6711250911E76D2325B38F16"
RUB_HEX = "0x812D001E6711250911E762523FDF75B4"
SERVICE_TYPE_HEX = "0xB5AE541216C91B074CF8BAABD257BE95"
WORK_TYPE_HEX = "0xB1066855158DCB944EA9CADFE654EB8F"


def _month_bounds_sql(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year + YEAR_OFFSET, month, 1)
    if month == 12:
        end = datetime(year + YEAR_OFFSET + 1, 1, 1)
    else:
        end = datetime(year + YEAR_OFFSET, month + 1, 1)
    return start, end


def _row_dicts(cursor) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def calculate_month(
    year: int,
    month: int,
    *,
    sql: SqlConnection | None = None,
    sample_limit: int = 50,
) -> dict[str, Any]:
    """Стоимость сопоставимых строк поступлений за календарный месяц."""
    sql = sql or SqlConnection()
    start, end = _month_bounds_sql(year, month)
    query = f"""
        SELECT
            receipt._Number AS document_number,
            receipt._Date_Time AS document_date,
            receipt_row._LineNo33229 AS line_number,
            receipt_row._Fld33235 AS quantity,
            receipt_row._Fld33234 AS package_quantity,
            receipt_row._Fld33240 AS amount,
            receipt_row._Fld33242 AS vat_amount,
            receipt_row._Fld33255 AS settlement_amount,
            receipt._Fld33160RRef AS actual_currency_ref,
            project_price._Period AS project_price_period,
            project_price._Fld50795 AS project_unit_price,
            project_price._Fld50797RRef AS project_currency_ref,
            actual_rate._Fld45369 AS actual_rate,
            actual_rate._Fld45370 AS actual_multiplicity,
            project_rate._Fld45369 AS project_rate,
            project_rate._Fld45370 AS project_multiplicity
        FROM dbo._Document907 AS receipt WITH (NOLOCK)
        INNER JOIN dbo._Document907_VT33228 AS receipt_row WITH (NOLOCK)
            ON receipt_row._Document907_IDRRef = receipt._IDRRef
        INNER JOIN dbo._Reference269 AS nomenclature WITH (NOLOCK)
            ON nomenclature._IDRRef = receipt_row._Fld33230RRef
        OUTER APPLY (
            SELECT TOP (1) price.*
            FROM dbo._InfoRg50791 AS price WITH (NOLOCK)
            WHERE price._Active = 0x01
              AND price._Fld50794RRef = {PROJECT_PRICE_TYPE_HEX}
              AND price._Fld50792RRef = receipt_row._Fld33230RRef
              AND price._Period <= receipt._Date_Time
              AND price._Fld50793RRef IN (
                    receipt_row._Fld33232RRef,
                    {EMPTY_HEX}
              )
              AND price._Fld50796RRef IN (
                    receipt_row._Fld33233RRef,
                    {EMPTY_HEX}
              )
            ORDER BY
                CASE
                    WHEN price._Fld50793RRef = receipt_row._Fld33232RRef
                     AND price._Fld50796RRef = receipt_row._Fld33233RRef THEN 0
                    WHEN price._Fld50793RRef = receipt_row._Fld33232RRef
                     AND price._Fld50796RRef = {EMPTY_HEX} THEN 1
                    WHEN price._Fld50793RRef = {EMPTY_HEX}
                     AND price._Fld50796RRef = receipt_row._Fld33233RRef THEN 2
                    ELSE 3
                END,
                price._Period DESC,
                price._LineNo DESC
        ) AS project_price
        OUTER APPLY (
            SELECT TOP (1) rate._Fld45369, rate._Fld45370
            FROM dbo._InfoRg45367 AS rate WITH (NOLOCK)
            WHERE rate._Fld45368RRef = receipt._Fld33160RRef
              AND rate._Period <= receipt._Date_Time
            ORDER BY rate._Period DESC
        ) AS actual_rate
        OUTER APPLY (
            SELECT TOP (1) rate._Fld45369, rate._Fld45370
            FROM dbo._InfoRg45367 AS rate WITH (NOLOCK)
            WHERE rate._Fld45368RRef = project_price._Fld50797RRef
              AND rate._Period <= receipt._Date_Time
            ORDER BY rate._Period DESC
        ) AS project_rate
        WHERE receipt._Posted = 0x01
          AND receipt._Marked = 0x00
          AND receipt._Date_Time >= ?
          AND receipt._Date_Time < ?
          AND receipt_row._Fld33230RRef <> {EMPTY_HEX}
          AND COALESCE(NULLIF(receipt_row._Fld33235, 0), receipt_row._Fld33234) > 0
          AND nomenclature._Fld8039RRef NOT IN (
                {SERVICE_TYPE_HEX},
                {WORK_TYPE_HEX}
          )
        ORDER BY receipt._Date_Time, receipt._Number, receipt_row._LineNo33229
    """
    with sql.connect_ctx() as connection:
        cursor = connection.cursor()
        cursor.execute(query, start, end)
        raw_rows = _row_dicts(cursor)

    total_rows = len(raw_rows)
    compared_rows = 0
    missing_project_price = 0
    zero_project_price = 0
    missing_currency_rate = 0
    actual_amount_rub = 0.0
    project_amount_rub = 0.0
    samples: list[dict[str, Any]] = []

    for row in raw_rows:
        quantity = float(row.get("quantity") or row.get("package_quantity") or 0)
        project_unit_price = float(row.get("project_unit_price") or 0)
        if project_unit_price <= 0:
            if row.get("project_price_period") is None:
                missing_project_price += 1
            else:
                zero_project_price += 1
            continue

        amount = float(row.get("amount") or 0)
        vat_amount = float(row.get("vat_amount") or 0)
        settlement_amount = float(row.get("settlement_amount") or 0)
        actual_amount = amount - vat_amount if amount else settlement_amount
        if actual_amount <= 0:
            continue

        actual_currency = bytes(row.get("actual_currency_ref") or bytes(16))
        project_currency = bytes(row.get("project_currency_ref") or bytes(16))
        rub_bytes = bytes.fromhex(RUB_HEX[2:])

        if actual_currency in (bytes(16), rub_bytes):
            actual_factor = 1.0
        else:
            actual_rate = float(row.get("actual_rate") or 0)
            actual_multiplicity = float(row.get("actual_multiplicity") or 0)
            actual_factor = actual_rate / actual_multiplicity if actual_multiplicity else 0.0

        if project_currency in (bytes(16), rub_bytes):
            project_factor = 1.0
        else:
            project_rate = float(row.get("project_rate") or 0)
            project_multiplicity = float(row.get("project_multiplicity") or 0)
            project_factor = project_rate / project_multiplicity if project_multiplicity else 0.0

        if actual_factor <= 0 or project_factor <= 0:
            missing_currency_rate += 1
            continue

        row_actual_rub = actual_amount * actual_factor
        row_project_rub = project_unit_price * quantity * project_factor
        if row_project_rub <= 0:
            zero_project_price += 1
            continue

        actual_amount_rub += row_actual_rub
        project_amount_rub += row_project_rub
        compared_rows += 1
        if len(samples) < sample_limit:
            samples.append({
                "document": str(row.get("document_number") or "").strip(),
                "line": int(row.get("line_number") or 0),
                "quantity": round(quantity, 6),
                "actual_amount_rub": round(row_actual_rub, 2),
                "project_amount_rub": round(row_project_rub, 2),
                "deviation_pct": round((row_actual_rub / row_project_rub - 1) * 100, 2),
            })

    deviation_amount = actual_amount_rub - project_amount_rub
    deviation_pct = (
        round(deviation_amount / project_amount_rub * 100, 2)
        if project_amount_rub > 0
        else None
    )
    return {
        "year": year,
        "month": month,
        "plan": round(project_amount_rub, 2) if project_amount_rub > 0 else None,
        "fact": round(actual_amount_rub, 2) if project_amount_rub > 0 else None,
        "kpi_pct": deviation_pct,
        "weighted_delta_amount": round(deviation_amount, 2),
        "project_amount": round(project_amount_rub, 2),
        "actual_amount": round(actual_amount_rub, 2),
        "total_rows": total_rows,
        "compared_rows": compared_rows,
        "missing_project_price": missing_project_price,
        "zero_project_price": zero_project_price,
        "missing_currency_rate": missing_currency_rate,
        "sample_rows": samples,
    }


__all__ = ["calculate_month"]
