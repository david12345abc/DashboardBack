# -*- coding: utf-8 -*-
"""Клиентская ДЗ НПО+АЛМАЗ для дашборда начальника логистики.

Источник: erp_pm SQL, регистр РасчетыСКлиентамиПоСрокам (_AccumRg107662).

Просроченной для этой логистической плитки считаем стабильную ДЗ:
остаток на начало 60-дневного окна равен остатку на дату среза, а движений
по объекту расчетов за окно нет.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from comdir.common import connect_ctx, to_1c_dt, uuid_to_1c_bytes
from getkpi import cache_manager

ACCUM = "_AccumRg107662"
OBJ_TABLE = "_Reference134945"
PARTNER_TABLE = "_Reference328"
ORG_TABLE = "_Reference288"

OBJ_COL = "_Fld140445RRef"
PLAN_COL = "_Fld107667"
DOLG_COL = "_Fld107672"
OBJ_ORG_COL = "_Fld138178RRef"
OBJ_PARTNER_COL = "_Fld138177RRef"
OBJ_NUMBER_COL = "_Fld138170"
OBJ_DATE_COL = "_Fld138172"

ORG_NPO = "fbca2148-6cfd-11e7-812d-001e67112509"
ORG_ALMAZ = "fbca2146-6cfd-11e7-812d-001e67112509"
ORG_KEYS = (ORG_NPO, ORG_ALMAZ)
ORG_BINS = tuple(uuid_to_1c_bytes(g) for g in ORG_KEYS)

SOURCE_TAG = "logistics_client_dz_sql_v1"
TOLERANCE = 0.01
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


def _cache_path(na_datu: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"logistics_client_dz_{na_datu.isoformat()}.json"


def cache_path(na_datu: date) -> Path:
    return _cache_path(na_datu)


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        import json

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _save_json(path: Path, data: dict) -> None:
    try:
        import json

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _bin_to_hex(value: bytes | bytearray | None) -> str:
    return bytes(value).hex() if value else ""


def _date_from_1c(value: Any) -> str:
    if value is None:
        return ""
    try:
        if getattr(value, "year", 0) > 2000:
            return value.replace(year=value.year - 2000).date().isoformat()
        return value.date().isoformat()
    except Exception:
        return str(value)[:10]


def _as_float(value: Any) -> float:
    return float(value or 0)


def _window_start(na_datu: date) -> date:
    return na_datu - timedelta(days=60)


def _fetch_rows(na_datu: date) -> list[dict]:
    window_start = _window_start(na_datu)
    p_open = to_1c_dt(window_start)
    p_close = to_1c_dt(na_datu + timedelta(days=1))

    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        org_placeholders = ",".join("?" for _ in ORG_BINS)
        cur.execute(
            f"""
            WITH opening AS (
              SELECT s.[{OBJ_COL}] AS obj,
                     SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{DOLG_COL}] ELSE s.[{DOLG_COL}] END) AS balance
              FROM [{ACCUM}] s WITH (NOLOCK)
              INNER JOIN [{OBJ_TABLE}] o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ_COL}]
              WHERE s._Period < ?
                AND s._Active = 0x01
                AND o.[{OBJ_ORG_COL}] IN ({org_placeholders})
              GROUP BY s.[{OBJ_COL}]
            ),
            closing AS (
              SELECT s.[{OBJ_COL}] AS obj,
                     SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{DOLG_COL}] ELSE s.[{DOLG_COL}] END) AS balance
              FROM [{ACCUM}] s WITH (NOLOCK)
              INNER JOIN [{OBJ_TABLE}] o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ_COL}]
              WHERE s._Period < ?
                AND s._Active = 0x01
                AND o.[{OBJ_ORG_COL}] IN ({org_placeholders})
              GROUP BY s.[{OBJ_COL}]
            ),
            movement AS (
              SELECT s.[{OBJ_COL}] AS obj,
                     SUM(ABS(s.[{DOLG_COL}])) AS turnover_abs,
                     COUNT_BIG(*) AS records,
                     MIN(s._Period) AS first_period,
                     MAX(s._Period) AS last_period
              FROM [{ACCUM}] s WITH (NOLOCK)
              INNER JOIN [{OBJ_TABLE}] o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ_COL}]
              WHERE s._Period >= ?
                AND s._Period < ?
                AND s._Active = 0x01
                AND ABS(s.[{DOLG_COL}]) > ?
                AND o.[{OBJ_ORG_COL}] IN ({org_placeholders})
              GROUP BY s.[{OBJ_COL}]
            )
            SELECT
              o._IDRRef AS object_key,
              o._Description AS object_name,
              o.[{OBJ_NUMBER_COL}] AS object_number,
              o.[{OBJ_DATE_COL}] AS object_date,
              o.[{OBJ_ORG_COL}] AS org_key,
              org._Description AS org_name,
              o.[{OBJ_PARTNER_COL}] AS partner_key,
              partner._Description AS partner_name,
              COALESCE(opening.balance, 0) AS opening_balance,
              closing.balance AS closing_balance,
              COALESCE(movement.turnover_abs, 0) AS turnover_abs,
              COALESCE(movement.records, 0) AS movement_records,
              movement.first_period,
              movement.last_period
            FROM closing
            INNER JOIN [{OBJ_TABLE}] o WITH (NOLOCK) ON o._IDRRef = closing.obj
            LEFT JOIN opening ON opening.obj = closing.obj
            LEFT JOIN movement ON movement.obj = closing.obj
            LEFT JOIN [{ORG_TABLE}] org WITH (NOLOCK) ON org._IDRRef = o.[{OBJ_ORG_COL}]
            LEFT JOIN [{PARTNER_TABLE}] partner WITH (NOLOCK) ON partner._IDRRef = o.[{OBJ_PARTNER_COL}]
            WHERE closing.balance > ?
            ORDER BY closing.balance DESC, partner._Description, o._Description
            """,
            p_open,
            *ORG_BINS,
            p_close,
            *ORG_BINS,
            p_open,
            p_close,
            TOLERANCE,
            *ORG_BINS,
            TOLERANCE,
        )
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def calculate(na_datu: date | None = None, *, use_cache: bool = True) -> dict:
    na_datu = na_datu or date.today()
    cache_file = _cache_path(na_datu)
    if use_cache and not cache_manager.is_force_compute_context():
        cached = _load_json(cache_file)
        if cached is not None and cached.get("source") == SOURCE_TAG:
            return cached

    window_start = _window_start(na_datu)
    rows_raw = _fetch_rows(na_datu)
    rows: list[dict] = []
    total_dz = 0.0
    total_overdue = 0.0

    for item in rows_raw:
        opening_balance = round(_as_float(item.get("opening_balance")), 2)
        closing_balance = round(_as_float(item.get("closing_balance")), 2)
        turnover_abs = round(_as_float(item.get("turnover_abs")), 2)
        movement_records = int(item.get("movement_records") or 0)
        balance_unchanged = abs(opening_balance - closing_balance) <= TOLERANCE
        no_movements = turnover_abs <= TOLERANCE and movement_records == 0
        is_overdue = closing_balance > TOLERANCE and balance_unchanged and no_movements
        if closing_balance <= TOLERANCE:
            continue

        total_dz += closing_balance
        if is_overdue:
            total_overdue += closing_balance

        rows.append({
            "object_key": _bin_to_hex(item.get("object_key")),
            "object_number": (item.get("object_number") or "").strip(),
            "object_date": _date_from_1c(item.get("object_date")),
            "object_name": (item.get("object_name") or "").strip(),
            "organization_key": _bin_to_hex(item.get("org_key")),
            "organization": (item.get("org_name") or "").strip(),
            "partner_key": _bin_to_hex(item.get("partner_key")),
            "partner": (item.get("partner_name") or "").strip(),
            "amount": closing_balance,
            "opening_balance": opening_balance,
            "closing_balance": closing_balance,
            "turnover_60_days": turnover_abs,
            "movement_records": movement_records,
            "is_overdue": is_overdue,
            "days_without_movement": 60 if is_overdue else None,
            "window_start": window_start.isoformat(),
            "window_end": na_datu.isoformat(),
            "first_movement_date": _date_from_1c(item.get("first_period")),
            "last_movement_date": _date_from_1c(item.get("last_period")),
        })

    total_dz = round(total_dz, 2)
    total_overdue = round(total_overdue, 2)
    overdue_pct = round(total_overdue / total_dz * 100, 2) if total_dz else None
    payload = {
        "source": SOURCE_TAG,
        "source_register": ACCUM,
        "na_datu": na_datu.isoformat(),
        "window": {
            "start": window_start.isoformat(),
            "end": na_datu.isoformat(),
            "days": 60,
        },
        "organizations": {
            ORG_NPO: "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
            ORG_ALMAZ: "АЛМАЗ ООО",
        },
        "total_dz": total_dz,
        "total_overdue": total_overdue,
        "overdue_pct": overdue_pct,
        "rows": rows,
        "verification": {
            "positive_objects": len(rows),
            "overdue_objects": sum(1 for row in rows if row.get("is_overdue")),
            "rules": [
                "closing balance ДолгРегл > 0 на дату среза",
                "организация объекта расчетов НПО или АЛМАЗ",
                "просрочка = остаток на начало 60-дневного окна равен остатку на дату среза",
                "просрочка = движений ДолгРегл по объекту за 60-дневное окно нет",
            ],
        },
        "debug": {
            "tables": {
                "register": ACCUM,
                "objects": OBJ_TABLE,
                "partners": PARTNER_TABLE,
                "organizations": ORG_TABLE,
            },
            "object_columns": {
                "organization": OBJ_ORG_COL,
                "partner": OBJ_PARTNER_COL,
                "number": OBJ_NUMBER_COL,
                "date": OBJ_DATE_COL,
            },
            "amount_field": DOLG_COL,
        },
    }
    _save_json(cache_file, payload)
    return payload


if __name__ == "__main__":
    import json
    import sys

    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    print(json.dumps(calculate(target, use_cache=False), ensure_ascii=False, indent=2))
