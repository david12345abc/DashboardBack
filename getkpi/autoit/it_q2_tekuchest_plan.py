"""План текучести (%) для KPI IT-Q2 из документа 1С.

Источник: Document_ТД_ТекучестьПерсонала (SQL erp_pm, OData-имя не опубликовано).

  dbo._Document185058
    _Fld185059     ВидДокумента: 0 = план, 1 = факт
    _Fld185060RRef Подразделение (Catalog_СтруктураПредприятия → _Reference513)
    _Marked        пометка удаления
  dbo._Document185058_VT185063  табличная часть «Текучесть»
    _Fld185065     Месяц
    _Fld185066     План, %
    _Fld185067     Факт, % (на плитку не берётся — факт IT-Q2 считается из HR)

Подразделение плитки — «Отдел информационных технологий», код 00-000057.
Если на месяц нет строки плана, возвращается None.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

from hr_turnover_sql import YEAR_OFFSET
from sql_connection import SqlConnection

logger = logging.getLogger(__name__)

STRUCTURE_TABLE = "_Reference513"
STRUCTURE_CODE = "00-000057"

DOC_TABLE = "_Document185058"
VT_TABLE = "_Document185058_VT185063"
COL_DOC_TYPE = "_Fld185059"
COL_DEPT = "_Fld185060RRef"
COL_MONTH = "_Fld185065"
COL_PLAN = "_Fld185066"

_LOCK = threading.Lock()
_CACHE_KEY: tuple[int, str] | None = None
_CACHE_VALUE: dict[int, float] | None = None
_FAIL_UNTIL = 0.0
_FAIL_YEAR: int | None = None


def _load_plans(year: int) -> dict[int, float]:
    """План по месяцам: max(План) по документам вида «план» без пометки удаления."""
    start = datetime(year + YEAR_OFFSET, 1, 1)
    end = datetime(year + YEAR_OFFSET + 1, 1, 1)
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 60
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT TOP 1 _IDRRef
            FROM dbo.[{STRUCTURE_TABLE}] WITH (NOLOCK)
            WHERE _Code = ? AND _Marked = 0x00
            """,
            [STRUCTURE_CODE],
        )
        row = cur.fetchone()
        if row is None:
            logger.error(
                "IT-Q2 plan: подразделение %s не найдено в %s",
                STRUCTURE_CODE,
                STRUCTURE_TABLE,
            )
            return {}
        dept_bin = bytes(row[0])
        cur.execute(
            f"""
            SELECT MONTH(vt.[{COL_MONTH}]) AS month_no,
                   MAX(vt.[{COL_PLAN}]) AS plan_pct
            FROM dbo.[{DOC_TABLE}] d WITH (NOLOCK)
            JOIN dbo.[{VT_TABLE}] vt WITH (NOLOCK)
              ON vt.[{DOC_TABLE}_IDRRef] = d._IDRRef
            WHERE d._Marked = 0x00
              AND d.[{COL_DOC_TYPE}] = 0
              AND d.[{COL_DEPT}] = ?
              AND vt.[{COL_MONTH}] >= ?
              AND vt.[{COL_MONTH}] < ?
            GROUP BY MONTH(vt.[{COL_MONTH}])
            """,
            [dept_bin, start, end],
        )
        plans: dict[int, float] = {}
        for month_no, plan_pct in cur.fetchall():
            if month_no is None or plan_pct is None:
                continue
            month = int(month_no)
            if 1 <= month <= 12:
                plans[month] = round(float(plan_pct), 1)
        return plans


def plans_for_year(year: int) -> dict[int, float]:
    """Помесячный план за год. Пустой словарь — документа нет или SQL недоступен."""
    global _CACHE_KEY, _CACHE_VALUE, _FAIL_UNTIL, _FAIL_YEAR
    today = datetime.now().date().isoformat()
    with _LOCK:
        if _CACHE_KEY == (year, today) and _CACHE_VALUE is not None:
            return dict(_CACHE_VALUE)
        if _FAIL_YEAR == year and time.monotonic() < _FAIL_UNTIL:
            return {}
    try:
        loaded = _load_plans(year)
    except Exception:
        logger.exception("IT-Q2 plan: не удалось прочитать Document_ТД_ТекучестьПерсонала за %s", year)
        with _LOCK:
            _FAIL_YEAR = year
            _FAIL_UNTIL = time.monotonic() + 60
        return {}
    with _LOCK:
        _CACHE_KEY = (year, today)
        _CACHE_VALUE = loaded
        _FAIL_YEAR = None
    return dict(loaded)


def plan_for_month(year: int, month: int) -> float | None:
    if not 1 <= int(month) <= 12:
        return None
    return plans_for_year(int(year)).get(int(month))
