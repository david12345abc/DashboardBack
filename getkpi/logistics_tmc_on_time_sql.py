"""LOG-M1: сроки поставок ТМЦ из локальной копии 1С в MSSQL.

SQL (erp_pm):
  Document_ПриобретениеТоваровУслуг → dbo._Document907
  ТЧ Товары                         → dbo._Document907_VT33228
  Document_ЗаказПоставщику          → dbo._Document713X1
  ТЧ Товары                         → dbo._Document713_VT22136X1
  Catalog_Номенклатура              → dbo._Reference269

Даты 1С в SQL хранятся со смещением +2000 лет.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sql_connection import SqlConnection

YEAR_OFFSET = 2000
EMPTY_HEX = "0x" + "00" * 16


def _month_bounds_sql(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year + YEAR_OFFSET, month, 1)
    if month == 12:
        end = datetime(year + YEAR_OFFSET + 1, 1, 1)
    else:
        end = datetime(year + YEAR_OFFSET, month + 1, 1)
    return start, end


def _from_sql_date(value: Any) -> date | None:
    if not isinstance(value, (date, datetime)) or value.year <= YEAR_OFFSET + 1:
        return None
    try:
        return date(value.year - YEAR_OFFSET, value.month, value.day)
    except ValueError:
        return None


def _bin_to_guid(value: Any) -> str:
    if value is None:
        return ""
    raw = bytes(value)
    if len(raw) != 16 or raw == bytes(16):
        return ""
    guid_bytes = raw[12:16] + raw[10:12] + raw[8:10] + raw[0:8]
    return str(uuid.UUID(bytes=guid_bytes))


def _row_dicts(cursor) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def load_delivery_rows(
    year: int,
    month: int,
    *,
    sql: SqlConnection | None = None,
) -> list[dict[str, Any]]:
    """Все строки фактических поступлений и соответствующие плановые даты."""
    sql = sql or SqlConnection()
    start, end = _month_bounds_sql(year, month)
    query = f"""
        SELECT
            receipt._IDRRef AS receipt_ref,
            receipt._Number AS receipt_number,
            receipt._Date_Time AS receipt_date,
            receipt_row._LineNo33229 AS receipt_line,
            receipt_row._Fld33230RRef AS nomenclature_ref,
            receipt_row._Fld33232RRef AS characteristic_ref,
            COALESCE(NULLIF(receipt_row._Fld33235, 0), receipt_row._Fld33234) AS quantity,
            nomenclature._Description AS nomenclature_name,
            order_doc._IDRRef AS order_ref,
            order_doc._Number AS order_number,
            order_row._LineNo22137 AS order_line,
            order_row._Fld22144 AS order_line_plan_date,
            order_doc._Fld22095 AS order_header_plan_date,
            order_doc._Fld22110 AS order_header_plan_date_2,
            order_doc._Fld143509 AS order_header_plan_date_3
        FROM dbo._Document907 AS receipt WITH (NOLOCK)
        INNER JOIN dbo._Document907_VT33228 AS receipt_row WITH (NOLOCK)
            ON receipt_row._Document907_IDRRef = receipt._IDRRef
        LEFT JOIN dbo._Reference269 AS nomenclature WITH (NOLOCK)
            ON nomenclature._IDRRef = receipt_row._Fld33230RRef
        LEFT JOIN dbo._Document713X1 AS order_doc WITH (NOLOCK)
            ON order_doc._IDRRef = CASE
                WHEN receipt_row._Fld33249RRef <> {EMPTY_HEX}
                    THEN receipt_row._Fld33249RRef
                WHEN receipt._Fld33169RRef <> {EMPTY_HEX}
                    THEN receipt._Fld33169RRef
                ELSE NULL
            END
            AND order_doc._Posted = 0x01
            AND order_doc._Marked = 0x00
        OUTER APPLY (
            SELECT TOP (1)
                candidate._LineNo22137,
                candidate._Fld22144
            FROM dbo._Document713_VT22136X1 AS candidate WITH (NOLOCK)
            WHERE candidate._Document713_IDRRef = order_doc._IDRRef
              AND candidate._Fld22139RRef = receipt_row._Fld33230RRef
              AND (
                    receipt_row._Fld33232RRef = {EMPTY_HEX}
                    OR candidate._Fld22138RRef = receipt_row._Fld33232RRef
              )
            ORDER BY candidate._LineNo22137
        ) AS order_row
        WHERE receipt._Posted = 0x01
          AND receipt._Marked = 0x00
          AND receipt._Date_Time >= ?
          AND receipt._Date_Time < ?
          AND receipt_row._Fld33230RRef <> {EMPTY_HEX}
          AND COALESCE(NULLIF(receipt_row._Fld33235, 0), receipt_row._Fld33234) > 0
        ORDER BY receipt._Date_Time, receipt._Number, receipt_row._LineNo33229
    """
    with sql.connect_ctx() as connection:
        cursor = connection.cursor()
        cursor.execute(query, start, end)
        raw_rows = _row_dicts(cursor)

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        fact_date = _from_sql_date(raw.get("receipt_date"))
        plan_date = (
            _from_sql_date(raw.get("order_line_plan_date"))
            or _from_sql_date(raw.get("order_header_plan_date"))
            or _from_sql_date(raw.get("order_header_plan_date_2"))
            or _from_sql_date(raw.get("order_header_plan_date_3"))
        )
        has_order = bool(_bin_to_guid(raw.get("order_ref")))
        if not has_order or plan_date is None:
            status = "without_order"
            on_time: bool | None = None
        elif fact_date is not None and fact_date <= plan_date:
            status = "on_time"
            on_time = True
        else:
            status = "overdue"
            on_time = False

        quantity = float(raw.get("quantity") or 0)
        rows.append({
            "receipt_ref": _bin_to_guid(raw.get("receipt_ref")),
            "receipt_number": str(raw.get("receipt_number") or "").strip(),
            "receipt_line": int(raw.get("receipt_line") or 0),
            "receipt_date": fact_date.isoformat() if fact_date else None,
            "order_ref": _bin_to_guid(raw.get("order_ref")),
            "order_number": str(raw.get("order_number") or "").strip(),
            "order_line": int(raw.get("order_line") or 0) or None,
            "plan_date": plan_date.isoformat() if plan_date else None,
            "nomenclature_ref": _bin_to_guid(raw.get("nomenclature_ref")),
            "nomenclature": str(raw.get("nomenclature_name") or "").strip(),
            "characteristic_ref": _bin_to_guid(raw.get("characteristic_ref")),
            "quantity": round(quantity, 6),
            "on_time": on_time,
            "status": status,
        })
    return rows


def calculate_month(
    year: int,
    month: int,
    *,
    sql: SqlConnection | None = None,
    include_rows: bool = False,
) -> dict[str, Any]:
    rows = load_delivery_rows(year, month, sql=sql)
    eligible = [row for row in rows if row["status"] != "without_order"]
    on_time = sum(1 for row in eligible if row["status"] == "on_time")
    overdue = sum(1 for row in eligible if row["status"] == "overdue")
    without_order = sum(1 for row in rows if row["status"] == "without_order")
    total = len(eligible)
    receipt_dates = [row["receipt_date"] for row in rows if row.get("receipt_date")]
    payload: dict[str, Any] = {
        "year": year,
        "month": month,
        "total_deliveries": total,
        "on_time": on_time,
        "overdue": overdue,
        "without_order": without_order,
        "source_rows": len(rows),
        "data_through": max(receipt_dates) if receipt_dates else None,
    }
    if include_rows:
        payload["rows"] = rows
    return payload


__all__ = ["calculate_month", "load_delivery_rows"]
