"""Служебные записки «Заключение договора ГПХ» для дашборда юриста.

Источник — SQL erp_pm, документ «Служебная записка (ТД)»:

  dbo._Document76733
    _Date_Time         дата документа (+2000 лет)
    _Marked            пометка удаления
    _Posted            проведена
    _Number            номер
    _Fld76735RRef      организация
    _Fld76737RRef      ответственный → _Reference366
    _Fld76738          текст служебной записки
    _Fld76743_RRRef    тема → _Reference76795
    _Fld186355         СуммаДоговораГПХ

Тема «Заключение договора ГПХ (регл.)» — предопределённый элемент
«ЗаключениеДоговораГПХ» справочника «ТД_ТемыСлужебныхЗаписок».
"""
from __future__ import annotations

import uuid
from datetime import datetime

from jurist.companies import COMPANIES
from sql_connection import SqlConnection

YEAR_OFFSET = 2000
THEME_GUID = "c9abcd4d-dc28-11e8-8283-ac1f6b05524d"
GOAL_CODE = "ЗаключениеДоговораГПХ"
GOAL_LABEL = "Заключение договора ГПХ"
MEMO_KPI_ID = "YR-SZ-Q"

SQL = """
SELECT
    d._Fld76735RRef,
    d._Number,
    d._Date_Time,
    d._Fld76738,
    d._Fld186355,
    d._Posted,
    lw._Description
FROM dbo._Document76733 AS d WITH (NOLOCK)
LEFT JOIN dbo._Reference366 AS lw WITH (NOLOCK)
    ON lw._IDRRef = d._Fld76737RRef
WHERE d._Marked = 0x00
  AND d._Fld76743_RRRef = ?
  AND d._Date_Time >= ?
  AND d._Date_Time < ?
  AND d._Fld76735RRef IN (?, ?, ?, ?)
"""


def _guid_to_1c(guid_str: str) -> bytes:
    raw = uuid.UUID(guid_str).bytes
    return raw[8:16] + raw[6:8] + raw[4:6] + raw[0:4]


def _sql_day(value):
    if not isinstance(value, datetime) or value.year < 3000:
        return None
    return value.replace(year=value.year - YEAR_OFFSET).date()


def _month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year + YEAR_OFFSET, month, 1)
    if month == 12:
        end = datetime(year + YEAR_OFFSET + 1, 1, 1)
    else:
        end = datetime(year + YEAR_OFFSET, month + 1, 1)
    return start, end


def _posted(value) -> bool:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)[:1] == b"\x01"
    return value == 1


def fetch_gph_memos(year: int, month: int) -> list[dict]:
    """Неудалённые служебные записки темы ГПХ четырёх организаций за месяц."""
    start, end = _month_bounds(year, month)
    org_bins = [_guid_to_1c(company["org_guid"]) for company in COMPANIES]
    by_org = {org_bins[index]: COMPANIES[index]["code"] for index in range(len(COMPANIES))}
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(SQL, _guid_to_1c(THEME_GUID), start, end, *org_bins)
        fetched = cur.fetchall()

    rows = []
    for org_ref, number, doc_date, text, amount_raw, posted, responsible in fetched:
        code = by_org.get(bytes(org_ref) if org_ref is not None else b"")
        if not code:
            continue
        started = _sql_day(doc_date)
        if started is None or started.year != year or started.month != month:
            continue
        amount = round(float(amount_raw or 0), 2)
        body = " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split())
        rows.append({
            "company_code": code,
            "number": str(number or "").strip(),
            "name": body or "Служебная записка",
            "goal": GOAL_CODE,
            "status": "Проведена" if _posted(posted) else "Не проведена",
            "sum": amount,
            "start_date": started.isoformat(),
            "end_date": "",
            "counterparty": str(responsible or "").strip() or "—",
        })
    return rows
