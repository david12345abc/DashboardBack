"""Дата «Обновлено» на плитке.

Живые источники (OData, TurboProject, Excel) не трогаем: остаётся время файла кэша.
Копия SQL — дата последней записи в таблице, из которой считается плитка.
Будущие плановые даты не берём: иначе в регистре бухгалтерии побеждает август следующего года.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_SQL_STAMP_TTL_SECONDS = 300
_YEAR_OFFSET = 2000

# (таблица, колонка даты записи)
_Row = tuple[str, str]

_MILESTONES: tuple[_Row, ...] = (("_AccumRg127619", "_Period"),)
_ACC_RG: tuple[_Row, ...] = (("_AccRg2005", "_Period"),)
_HR: tuple[_Row, ...] = (("_InfoRg45157", "_Period"),)
_BUDGET_DOCS: tuple[_Row, ...] = (
    ("_Document726", "_Date_Time"),
    ("_Document980", "_Date_Time"),
)
_IT_BUDGET: tuple[_Row, ...] = _BUDGET_DOCS + (("_AccumRg51416", "_Period"),)
_CLAIMS: tuple[_Row, ...] = (("_Reference389", "_Fld11617"),)  # ДатаРегистрации
_SUPPORT_REQUEST: tuple[_Row, ...] = (("_Document76754X1", "_Date_Time"),)

_SQL_SOURCES: dict[str, tuple[_Row, ...]] = {
    "GSP-M1": _MILESTONES,
    "GSPP-M1": _MILESTONES,
    "GSP-M2": _MILESTONES,
    "GSPP-M2": _MILESTONES,
    "GSP-M3": _ACC_RG,
    "GSPP-M3": _ACC_RG,
    "GSP-Q5": _HR,
    "GSPP-Q5": _HR,
    "QD-M1": (("_Document100537X1", "_Date_Time"),),
    "QD-M1W": (("_Document100537X1", "_Date_Time"),),
    "QD-M3": _BUDGET_DOCS,
    "QD-M4": _ACC_RG,
    "QD-M5": (("_Document148564X1", "_Date_Time"),),
    "QD-M5W": (("_Document148564X1", "_Date_Time"),),
    "QD-M6": (("_Document122024", "_Date_Time"),),
    "QD-M7": (("_Document140962", "_Date_Time"),),
    "QD-M8": (("_Document148563X1", "_Date_Time"),),
    "QD-M8W": (("_Document148563X1", "_Date_Time"),),
    "QD-M9": (("_Document122024", "_Date_Time"),),
    "QD-M10": (("_Document122024", "_Date_Time"),),
    "QD-Q2": _HR,
    "HRD-M2": _ACC_RG,
    "HRD-M3": _BUDGET_DOCS,
    "HRD-M7": _HR,
    "HRD-M9": _HR,
    "RD-M1": (("_Document76753", "_Date_Time"),),
    "RD-M3": (("_Document980", "_Date_Time"),),
    "RD-M4": _ACC_RG,
    "RD-Q2": _HR,
    "SH-M1": _CLAIMS,
    "SH-M2": _CLAIMS,
    "SH-M3": _CLAIMS,
    "SH-M4": _CLAIMS,
    "SH-M5": _CLAIMS,
    "IT-M1": _SUPPORT_REQUEST,
    "IT-M1-1": _SUPPORT_REQUEST,
    "IT-M1-2": _SUPPORT_REQUEST,
    "1C-M1": _SUPPORT_REQUEST,
    "IT-M3": _IT_BUDGET,
    "1C-M3": _IT_BUDGET,
    "IT-M4": _ACC_RG,
    "1C-M4": _ACC_RG,
    "IT-Q2": _HR,
    "IT-Q5": _HR,
    "1C-Q5": _HR,
    "TD-M3": _BUDGET_DOCS,
    "TD-M4": _ACC_RG,
}

_CYR_TO_LAT = str.maketrans({
    "А": "A", "В": "B", "Д": "D", "Е": "E", "К": "K", "М": "M",
    "Н": "H", "О": "O", "Р": "R", "Т": "T", "У": "Y", "Х": "X",
})
_PREFIXES = (
    ("ГСПП", "GSPP"),
    ("ГCПП", "GSPP"),
    ("ГСП", "GSP"),
    ("ГCП", "GSP"),
    ("1С", "1C"),
    ("ИТ", "IT"),
)

_stamp_lock = threading.Lock()
_stamp_cache: tuple[float, dict[tuple[str, str], str]] | None = None


def normalize_kpi_id(kpi_id: str | None) -> str:
    text = str(kpi_id or "").strip().upper()
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u2212"):
        text = text.replace(dash, "-")
    for src, dst in _PREFIXES:
        text = text.replace(src, dst)
    return text.translate(_CYR_TO_LAT)


def is_sql_copy_tile(kpi_id: str | None) -> bool:
    return normalize_kpi_id(kpi_id) in _SQL_SOURCES


def _from_sql_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    year = value.year - _YEAR_OFFSET if value.year >= 3000 else value.year
    if year < 2000 or year > 2100:
        return None
    return value.replace(year=year, microsecond=0)


def _load_table_stamps() -> dict[tuple[str, str], str]:
    rows = sorted({row for sources in _SQL_SOURCES.values() for row in sources})
    if not rows:
        return {}
    upper = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    upper_sql = upper.replace(year=upper.year + _YEAR_OFFSET)
    lower_sql = datetime(2001, 1, 2)
    parts = []
    params: list[Any] = []
    for table, column in rows:
        parts.append(
            f"SELECT ? AS src_table, ? AS src_column, MAX([{column}]) AS last_at "
            f"FROM [{table}] WITH (NOLOCK) "
            f"WHERE [{column}] > ? AND [{column}] < ?"
        )
        params.extend([table, column, lower_sql, upper_sql])
    sql_text = " UNION ALL ".join(parts)
    found: dict[tuple[str, str], str] = {}
    from sql_connection import SqlConnection

    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 60
        cur = conn.cursor()
        cur.execute(sql_text, params)
        for table, column, last_at in cur.fetchall():
            stamped = _from_sql_datetime(last_at)
            if stamped is not None:
                found[(str(table), str(column))] = stamped.isoformat()
    return found


def _table_stamps() -> dict[tuple[str, str], str]:
    global _stamp_cache
    now = time.monotonic()
    with _stamp_lock:
        if _stamp_cache is not None and now < _stamp_cache[0]:
            return _stamp_cache[1]
    try:
        found = _load_table_stamps()
    except Exception:
        logger.exception("tile_freshness: не удалось прочитать даты последних записей")
        with _stamp_lock:
            if _stamp_cache is not None:
                return _stamp_cache[1]
        return {}
    with _stamp_lock:
        _stamp_cache = (time.monotonic() + _SQL_STAMP_TTL_SECONDS, found)
    return found


def tile_updated_at(kpi_id: str | None) -> str | None:
    """Дата последней записи для плитки из копии. Для живого источника — None."""
    sources = _SQL_SOURCES.get(normalize_kpi_id(kpi_id))
    if not sources:
        return None
    stamps = _table_stamps()
    latest: str | None = None
    for row in sources:
        stamp = stamps.get(row)
        if stamp and (latest is None or stamp > latest):
            latest = stamp
    return latest


def apply_tile_freshness(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """У плиток из копии заменить дату на последнюю запись таблицы. Живые не менять."""
    if not isinstance(payload, dict):
        return payload
    items = (payload.get("Плитки") or {}).get("items")
    if not isinstance(items, list):
        return payload
    for tile in items:
        if not isinstance(tile, dict) or not is_sql_copy_tile(tile.get("kpi_id")):
            continue
        stamp = tile_updated_at(tile.get("kpi_id"))
        if stamp:
            tile["cache_updated_at"] = stamp
    return payload
