"""Суды для дашборда юриста.

Источник — SQL erp_pm, документ
«Претензии, судебные споры и исковая работа (ТД)»:

  dbo._Document185083
    _Date_Time         дата документа (+2000 лет)
    _Marked            пометка удаления
    _Number            номер
    _Fld185098RRef     статус → _Enum185084
    _Fld185101RRef     инициатор → _Reference366
    _Fld185102         крайний срок по SLA
    _Fld185104RRef     тип документа → _Enum185086
    _Fld185105RRef     роль ГК → _Enum185087
    _Fld185107RRef     контрагент → _Reference233
    _Fld185109         предмет спора
    _Fld185111         сумма требований
    _Fld185118_RRRef   площадка / юрлицо ГК → _Reference288
    _Fld185119         краткое описание ситуации

Подразделение инициатора — _Reference366._Fld10996RRef → _Reference513.
Месяц карточки — дата документа. Берутся все типы документа, не только
исполнительное производство.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime

from devdir import ytd_json_cache
from sql_connection import SqlConnection

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
CACHE_PREFIX = "jurist_lawsuits"
CACHE_SOURCE = "jurist_lawsuits_v1"
CACHE_VERSION = 1
EMPTY_BIN = bytes(16)

STATUS_BY_ORDER = {
    0: "Черновик карточки",
    1: "Зарегистрирована",
    2: "На первичном анализе",
    3: "Ожидание документов/пояснений",
    4: "Проект ответа/претензии подготовлен",
    5: "На согласовании",
    6: "Направлена контрагенту",
    7: "Получен ответ контрагента",
    8: "Урегулирована без суда",
    9: "Отказ/спор не урегулирован",
    10: "Принято решение о подаче иска/подготовке отзыва",
    11: "В суде первой инстанции",
    12: "Апелляция/кассация",
    13: "Исполнительное производство",
    14: "Закрыта",
}

DOC_TYPE_BY_ORDER = {
    0: "Входящая претензия",
    1: "Исходящая претензия",
    2: "Ответ на претензию",
    3: "Исковое заявление",
    4: "Отзыв",
    5: "Апелляция",
    6: "Исполнительное производство",
}

ROLE_BY_ORDER = {
    0: "Истец",
    1: "Ответчик",
    2: "Заявитель",
    3: "Взыскатель",
    4: "Должник",
    5: "Третье лицо",
}

SQL = """
SELECT
    d._Number,
    d._Date_Time,
    st._EnumOrder,
    tp._EnumOrder,
    ct._Description,
    d._Fld185109,
    d._Fld185111,
    rl._EnumOrder,
    org._Description,
    dept._Description,
    dept._IDRRef,
    d._Fld185102,
    d._Fld185119
FROM dbo._Document185083 AS d WITH (NOLOCK)
LEFT JOIN dbo._Enum185084 AS st WITH (NOLOCK)
    ON st._IDRRef = d._Fld185098RRef
LEFT JOIN dbo._Enum185086 AS tp WITH (NOLOCK)
    ON tp._IDRRef = d._Fld185104RRef
LEFT JOIN dbo._Enum185087 AS rl WITH (NOLOCK)
    ON rl._IDRRef = d._Fld185105RRef
LEFT JOIN dbo._Reference233 AS ct WITH (NOLOCK)
    ON ct._IDRRef = d._Fld185107RRef
LEFT JOIN dbo._Reference288 AS org WITH (NOLOCK)
    ON org._IDRRef = d._Fld185118_RRRef
LEFT JOIN dbo._Reference366 AS usr WITH (NOLOCK)
    ON usr._IDRRef = d._Fld185101RRef
LEFT JOIN dbo._Reference513 AS dept WITH (NOLOCK)
    ON dept._IDRRef = usr._Fld10996RRef
WHERE d._Marked = 0x00
  AND d._Date_Time >= ?
  AND d._Date_Time < ?
ORDER BY d._Date_Time DESC, d._Number DESC
"""


def _bin_to_guid(blob) -> str:
    if not isinstance(blob, (bytes, bytearray)):
        return ""
    raw = bytes(blob)
    if len(raw) != 16 or raw == EMPTY_BIN:
        return ""
    orig = raw[12:16] + raw[10:12] + raw[8:10] + raw[0:8]
    return str(uuid.UUID(bytes=orig))


def _sql_date(value) -> str:
    if not isinstance(value, datetime) or value.year < 3000:
        return ""
    return value.replace(year=value.year - YEAR_OFFSET).date().isoformat()


def _text(value) -> str:
    return (value or "").replace("\r\n", " ").replace("\n", " ").strip()


def _label(mapping: dict[int, str], order) -> str:
    if order is None:
        return ""
    return mapping.get(int(order), "")


def _money(value) -> float:
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _fetch(year: int, month: int) -> list[dict]:
    start = datetime(year + YEAR_OFFSET, month, 1)
    end = datetime(year + YEAR_OFFSET + (month == 12), 1 if month == 12 else month + 1, 1)
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(SQL, start, end)
        rows = []
        for (
            number,
            doc_date,
            status_order,
            type_order,
            counterparty,
            subject,
            amount,
            role_order,
            org_name,
            dept_name,
            dept_ref,
            sla_date,
            situation,
        ) in cur.fetchall():
            rows.append({
                "number": _text(number),
                "date": _sql_date(doc_date),
                "status": _label(STATUS_BY_ORDER, status_order),
                "doc_type": _label(DOC_TYPE_BY_ORDER, type_order),
                "counterparty": _text(counterparty),
                "subject": _text(subject),
                "claim_amount": _money(amount),
                "gc_role": _label(ROLE_BY_ORDER, role_order),
                "gc_entity": _text(org_name),
                "initiator_dept": _text(dept_name),
                "initiator_dept_key": _bin_to_guid(dept_ref),
                "sla_date": _sql_date(sla_date),
                "situation": _text(situation),
            })
    return rows


def lawsuits_table_rows(*, year: int, month: int) -> list[dict]:
    ref_y = int(year)
    ref_m = max(1, min(12, int(month)))
    path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def compute():
        try:
            rows = _fetch(ref_y, ref_m)
        except Exception:
            logger.exception("jurist lawsuits SQL %s-%02d", ref_y, ref_m)
            rows = []
        payload = {"rows": rows}
        ytd_json_cache.save_payload(
            path,
            payload,
            source_tag=CACHE_SOURCE,
            version=CACHE_VERSION,
        )
        return payload

    payload = ytd_json_cache.resolve_payload(
        path,
        source_tag=CACHE_SOURCE,
        version=CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"jurist_lawsuits_{ref_y}_{ref_m:02d}",
        compute_fn=compute,
    )
    rows = (payload or {}).get("rows")
    return rows if isinstance(rows, list) else []
