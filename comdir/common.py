"""Общие хелперы comdir: SQL-коннект, GUID 1С ↔ OData, словари отделов."""
from __future__ import annotations

import sys
from calendar import monthrange
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

import pyodbc

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sql_connection import SqlConnection  # noqa: E402

YEAR_OFFSET = 2000
EMPTY16 = bytes(16)

# Имя (как в SQL-эталонах) → OData UUID
DEPT_NAME_TO_ODATA: dict[str, str] = {
    "Отдел по работе с ПАО Газпром": "bd7b5184-9f9c-11e4-80da-001e67112509",
    "Отдел дилерских продаж": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "Отдел по работе с ключевыми клиентами": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "Отдел продаж эталонного оборудования и услуг": "34497ef7-810f-11e4-80d6-001e67112509",
    "Отдел внешнеэкономической деятельности": "49480c10-e401-11e8-8283-ac1f6b05524d",
    "Отдел продаж БМИ": "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "Сектор рекламы и PR": "95dfd1c6-37a4-11ee-93d3-6cb31113810e",
    "Отдел тендерных продаж": "1c9f9419-d91b-11e0-8129-cd2988c3db2d",
    "Тендерный офис": "1c9f9419-d91b-11e0-8129-cd2988c3db2d",
    "Тендерный отдел": "1c9f9419-d91b-11e0-8129-cd2988c3db2d",
    # ликвидированные → действующие
    "(ликв.) Отдел дилерских продаж бытового оборудования": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "(ликв.) Отдел дилерских продаж промышленного оборудования": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "(ликв.) Отдел по работе с холдингами 1": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "(ликв.) Отдел по работе с холдингами 2": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "(ликв.) Отдел по работе с холдингами 3": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
}

# 1C binary hex → OData UUID (для VP/cena keyed by bytes)
HEX_TO_ODATA: dict[str, str] = {
    "80da001e6711250911e49f9cbd7b5184": "bd7b5184-9f9c-11e4-80da-001e67112509",
    "96f96cb31113810e11f092f67587c178": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "8523ac1f6b05524d11eb67b6639ec87b": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "80d6001e6711250911e4810f34497ef7": "34497ef7-810f-11e4-80d6-001e67112509",
    "8283ac1f6b05524d11e8e40149480c10": "49480c10-e401-11e8-8283-ac1f6b05524d",
    "93d36cb31113810e11ee37a59edaa7d4": "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "93d36cb31113810e11ee37a495dfd1c6": "95dfd1c6-37a4-11ee-93d3-6cb31113810e",
    "8129cd2988c3db2d11e0d91b1c9f9419": "1c9f9419-d91b-11e0-8129-cd2988c3db2d",
    "80da001e6711250911e49f994edcf3a0": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "8127001e6711250911e6d71eff740269": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "95e86cb31113810e11efcf32c6810cc3": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "95e86cb31113810e11efcf38ebd2d511": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "95e86cb31113810e11efcf39ad83f8bd": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
}

ODATA_TO_HEX = {v: k for k, v in HEX_TO_ODATA.items() if k[:8] != "80da001e" or "bd7b" in v or "4edc" in k}
# Prefer primary (non-liquidated) hex for each OData GUID
_PRIMARY_HEX = {
    "bd7b5184-9f9c-11e4-80da-001e67112509": "80da001e6711250911e49f9cbd7b5184",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "96f96cb31113810e11f092f67587c178",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "8523ac1f6b05524d11eb67b6639ec87b",
    "34497ef7-810f-11e4-80d6-001e67112509": "80d6001e6711250911e4810f34497ef7",
    "49480c10-e401-11e8-8283-ac1f6b05524d": "8283ac1f6b05524d11e8e40149480c10",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "93d36cb31113810e11ee37a59edaa7d4",
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": "93d36cb31113810e11ee37a495dfd1c6",
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": "8129cd2988c3db2d11e0d91b1c9f9419",
}

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def uuid_to_1c_hex(uuid_str: str) -> str:
    """OData UUID → 1C binary hex (16 bytes)."""
    if uuid_str in _PRIMARY_HEX:
        return _PRIMARY_HEX[uuid_str]
    u = uuid_str.replace("-", "").lower()
    if len(u) != 32:
        raise ValueError(f"bad uuid: {uuid_str}")
    return u[16:20] + u[20:32] + u[12:16] + u[8:12] + u[0:8]


def uuid_to_1c_bytes(uuid_str: str) -> bytes:
    return bytes.fromhex(uuid_to_1c_hex(uuid_str))


def hex_to_odata(hx: str | bytes) -> str | None:
    if isinstance(hx, bytes):
        hx = hx.hex()
    return HEX_TO_ODATA.get(hx.lower())


def name_to_odata(name: str) -> str | None:
    return DEPT_NAME_TO_ODATA.get(name)


def to_1c_dt(d: date) -> datetime:
    return datetime(d.year + YEAR_OFFSET, d.month, d.day)


def period_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    p0 = to_1c_dt(date(year, month, 1))
    if month == 12:
        p_next = to_1c_dt(date(year + 1, 1, 1))
    else:
        p_next = to_1c_dt(date(year, month + 1, 1))
    return p0, p_next


def connect() -> pyodbc.Connection:
    """Подключение через sql_connection (.env) с fallback на localhost Trusted."""
    sql = SqlConnection()
    # Локальный инстанс / уже под нужной учёткой — Trusted без impersonation.
    server = (sql.server or "").strip()
    if server in {".", "(local)", "localhost", "127.0.0.1", "localhost,1433", "127.0.0.1,1433"}:
        for driver in (
            sql.driver,
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ):
            try:
                cn = pyodbc.connect(
                    f"DRIVER={{{driver}}};SERVER={server or '.'};DATABASE={sql.database};"
                    "Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;",
                    autocommit=True,
                )
                cn.timeout = 0
                cur = cn.cursor()
                cur.execute("SET LOCK_TIMEOUT 600000")
                cur.close()
                return cn
            except Exception:
                continue
    # Удалённый сервер — impersonation + Trusted
    try:
        with sql.connect_ctx() as cn:
            # connect_ctx закрывает conn при выходе — нужен отдельный connect
            pass
    except Exception:
        pass
    if not sql.password:
        # fallback localhost
        for driver in (
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server",
            "SQL Server",
        ):
            try:
                cn = pyodbc.connect(
                    f"Driver={{{driver}}};Server=.;Database=erp_pm;"
                    "Trusted_Connection=yes;TrustServerCertificate=yes;",
                    autocommit=True,
                )
                cn.timeout = 0
                return cn
            except Exception:
                continue
        raise RuntimeError("SQL_PASSWORD не задан и localhost недоступен")
    from sql_connection import windows_impersonation

    with windows_impersonation(sql.user, sql.password):
        cn = pyodbc.connect(sql.connection_string(), autocommit=True)
    cn.timeout = 0
    return cn


@contextmanager
def connect_ctx() -> Iterator[pyodbc.Connection]:
    cn = connect()
    try:
        yield cn
    finally:
        cn.close()


def aggregate_by_odata_name(
    by_name: dict[str, float],
    *,
    include_unknown: bool = False,
) -> dict[str, float]:
    """Суммы по имени отдела → {odata_guid: sum}, ликвидированные в действующие."""
    out: dict[str, float] = {}
    for name, val in by_name.items():
        guid = name_to_odata(name)
        if not guid:
            if include_unknown:
                out[name] = out.get(name, 0.0) + float(val or 0)
            continue
        out[guid] = out.get(guid, 0.0) + float(val or 0)
    return {k: round(v, 2) for k, v in out.items()}


def aggregate_by_odata_bytes(
    by_bytes: dict[bytes, Any],
    value_fn=None,
) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, raw in by_bytes.items():
        guid = hex_to_odata(key if isinstance(key, (bytes, str)) else bytes(key))
        if not guid:
            continue
        val = value_fn(raw) if value_fn else float(raw or 0)
        out[guid] = out.get(guid, 0.0) + float(val or 0)
    return {k: round(v, 2) for k, v in out.items()}


def empty_error_payload(ref_y: int, ref_m: int, kpi_id: str, exc: Exception) -> dict[str, Any]:
    return {
        "year": ref_y,
        "ref_month": ref_m,
        "months": [],
        "data_granularity": "monthly",
        "monthly_data": [],
        "last_full_month_row": None,
        "ytd": {
            "total_plan": None,
            "total_fact": None,
            "kpi_pct": None,
            "months_with_data": 0,
            "months_total": 0,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_RU.get(ref_m, str(ref_m)),
            "data_complete": False,
        },
        "debug": {"status": "error", "kpi_id": kpi_id, "error": str(exc)},
    }


def slice_months_by_dept(months: list[dict], dept_guid: str | None) -> list[dict]:
    if not dept_guid:
        return months
    guid = str(dept_guid).lower()
    sliced = []
    for row in months:
        bd = row.get("by_dept") or {}
        dept_row = bd.get(guid) or bd.get(dept_guid) or {}
        if isinstance(dept_row, dict):
            fact = dept_row.get("fact", 0)
            plan = dept_row.get("plan", row.get("plan"))
            expected = dept_row.get("expected", row.get("expected", 0))
            calc = dept_row.get("calc", row.get("calc"))
        else:
            fact = float(dept_row or 0)
            plan = row.get("plan")
            expected = row.get("expected", 0)
            calc = row.get("calc")
        item = {
            "year": row["year"],
            "month": row["month"],
            "fact": fact,
            "plan": plan if plan is not None else 0,
        }
        if "expected" in row or expected is not None:
            item["expected"] = expected or 0
        if calc is not None:
            item["calc"] = calc
        sliced.append(item)
    return sliced


def days_elapsed(year: int, month: int, as_of: date | None = None) -> tuple[int, int]:
    as_of = as_of or date.today()
    dim = monthrange(year, month)[1]
    if (year, month) == (as_of.year, as_of.month):
        return min(as_of.day, dim), dim
    if (year, month) < (as_of.year, as_of.month):
        return dim, dim
    return 0, dim
