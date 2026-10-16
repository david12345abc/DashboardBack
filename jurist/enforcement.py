"""Исполнительное производство для дашборда юриста.

Источник — SQL erp_pm, документ
«Претензии, судебные споры и исковая работа (ТД)»:

  dbo._Document185083
    _Date_Time            дата документа (+2000 лет)
    _Marked               пометка удаления
    _Fld185098RRef        статус → _Enum185084
    _Fld185100RRef        ответственный юрист → _Reference366
    _Fld185104RRef        тип документа → _Enum185086
    _Fld185105RRef        роль ГК → _Enum185087
    _Fld185107RRef        контрагент → _Reference233
    _Fld185109            предмет спора
    _Fld185111            сумма требований
    _Fld185125            номер дела
    _Fld185138            итоговая взысканная сумма
    _Fld185182            исполнительный лист получен
    _Fld185183            исполнительный лист подан в ФССП
    _Fld185184            дата возбуждения исполнительного производства

Тип «Исполнительное производство» — _EnumOrder 6
перечисления ТД_ТипДокументаСпораПретензии.
Месяц карточки — дата документа.
"""
from __future__ import annotations

import logging
from datetime import datetime

from devdir import ytd_json_cache
from sql_connection import SqlConnection

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
CACHE_PREFIX = "jurist_enforcement"
CACHE_SOURCE = "jurist_enforcement_v1"
CACHE_VERSION = 1
DOC_TYPE_ORDER = 6

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
    d._Fld185109,
    d._Fld185111,
    d._Fld185125,
    d._Fld185138,
    d._Fld185182,
    d._Fld185183,
    d._Fld185184,
    st._EnumOrder,
    rl._EnumOrder,
    ct._Description,
    lw._Description
FROM dbo._Document185083 AS d WITH (NOLOCK)
LEFT JOIN dbo._Enum185084 AS st WITH (NOLOCK)
    ON st._IDRRef = d._Fld185098RRef
LEFT JOIN dbo._Enum185087 AS rl WITH (NOLOCK)
    ON rl._IDRRef = d._Fld185105RRef
LEFT JOIN dbo._Reference233 AS ct WITH (NOLOCK)
    ON ct._IDRRef = d._Fld185107RRef
LEFT JOIN dbo._Reference366 AS lw WITH (NOLOCK)
    ON lw._IDRRef = d._Fld185100RRef
WHERE d._Marked = 0x00
  AND d._Fld185104RRef = (
        SELECT _IDRRef FROM dbo._Enum185086 WHERE _EnumOrder = ?
  )
  AND d._Date_Time >= ?
  AND d._Date_Time < ?
ORDER BY d._Date_Time DESC, d._Number DESC
"""


def _sql_date(value) -> str:
    if not isinstance(value, datetime) or value.year < 3000:
        return ""
    shifted = value.replace(year=value.year - YEAR_OFFSET)
    return shifted.date().isoformat()


def _sql_bool(value) -> bool:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)[:1] == b"\x01"
    return value == 1


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
        cur.execute(SQL, DOC_TYPE_ORDER, start, end)
        rows = []
        for (
            number,
            doc_date,
            subject,
            amount,
            case_number,
            collected,
            writ_received,
            filed_fssp,
            opened_on,
            status_order,
            role_order,
            counterparty,
            lawyer,
        ) in cur.fetchall():
            rows.append({
                "number": (number or "").strip(),
                "date": _sql_date(doc_date),
                "counterparty": (counterparty or "").strip(),
                "lawyer": (lawyer or "").strip(),
                "subject": (subject or "").replace("\r\n", " ").replace("\n", " ").strip(),
                "status": STATUS_BY_ORDER.get(int(status_order), "") if status_order is not None else "",
                "role": ROLE_BY_ORDER.get(int(role_order), "") if role_order is not None else "",
                "case_number": (case_number or "").strip(),
                "amount": _money(amount),
                "collected": _money(collected),
                "opened_on": _sql_date(opened_on),
                "writ_received": _sql_bool(writ_received),
                "filed_fssp": _sql_bool(filed_fssp),
            })
    return rows


def enforcement_table_rows(*, year: int, month: int) -> list[dict]:
    ref_y = int(year)
    ref_m = max(1, min(12, int(month)))
    path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def compute():
        try:
            rows = _fetch(ref_y, ref_m)
        except Exception:
            logger.exception("jurist enforcement SQL %s-%02d", ref_y, ref_m)
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
        lock_key=f"jurist_enforcement_{ref_y}_{ref_m:02d}",
        compute_fn=compute,
    )
    rows = (payload or {}).get("rows")
    return rows if isinstance(rows, list) else []
