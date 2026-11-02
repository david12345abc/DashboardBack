"""Претензии клиентов для дашборда юриста.

Источник — SQL erp_pm, справочник «Претензии» (Catalog_Претензии):

  dbo._Reference389
    _Code            код
    _Description     наименование
    _Fld11617        дата регистрации (+2000 лет)
    _Fld11618        дата окончания
    _Fld11622        описание претензии
    _Fld11624RRef    клиент → _Reference328
    _Fld11625RRef    статус → _Enum1688
    _Fld132055       дата окончания (план)
    _Fld132070RRef   номенклатура → _Reference269
    _Fld132071RRef   характеристика → _Reference600
    _Fld132077RRef   заказ клиента → _Document704

Заказ:
  _Number          номер
  _Fld21186        сумма документа
  _Fld21220RRef    подразделение → _Reference513

Месяц — дата регистрации. В таблицу не попадают претензии со статусом
«Удовлетворена» и без заказа клиента. Пометка удаления не отсекается:
так же была устроена прежняя выборка.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime

from devdir import ytd_json_cache
from sql_connection import SqlConnection

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
CACHE_PREFIX = "jurist_claims"
CACHE_SOURCE = "jurist_claims_v1"
CACHE_VERSION = 1
EMPTY_BIN = bytes(16)
STATUS_SATISFIED = 3

STATUS_BY_ORDER = {
    0: "Зарегистрирована",
    1: "Обрабатывается",
    2: "На контроле",
    3: "Удовлетворена",
    4: "НеУдовлетворена",
}

SQL = """
SELECT
    c._Code,
    c._Description,
    p._Description,
    c._Fld11617,
    c._Fld132055,
    c._Fld11618,
    o._Number,
    dept._Description,
    dept._IDRRef,
    nom._Description,
    nom._Code,
    ch._Description,
    o._Fld21186,
    c._Fld11622,
    st._EnumOrder
FROM dbo._Reference389 AS c WITH (NOLOCK)
INNER JOIN dbo._Document704 AS o WITH (NOLOCK)
    ON o._IDRRef = c._Fld132077RRef
LEFT JOIN dbo._Reference328 AS p WITH (NOLOCK)
    ON p._IDRRef = c._Fld11624RRef
LEFT JOIN dbo._Enum1688 AS st WITH (NOLOCK)
    ON st._IDRRef = c._Fld11625RRef
LEFT JOIN dbo._Reference513 AS dept WITH (NOLOCK)
    ON dept._IDRRef = o._Fld21220RRef
LEFT JOIN dbo._Reference269 AS nom WITH (NOLOCK)
    ON nom._IDRRef = c._Fld132070RRef
LEFT JOIN dbo._Reference600 AS ch WITH (NOLOCK)
    ON ch._IDRRef = c._Fld132071RRef
WHERE c._Fld11617 >= ?
  AND c._Fld11617 < ?
  AND c._Fld132077RRef <> 0x00000000000000000000000000000000
  AND (st._EnumOrder IS NULL OR st._EnumOrder <> ?)
ORDER BY c._Fld11617, c._Code
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


def _nomenclature(description, code) -> str:
    title = _text(description)
    number = _text(code)
    if title and number:
        return f"{title} ({number})"
    return title or number


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
        cur.execute(SQL, start, end, STATUS_SATISFIED)
        rows = []
        for (
            code,
            name,
            partner,
            date_reg,
            date_plan,
            date_end,
            order_num,
            order_dept,
            order_dept_ref,
            nom_name,
            nom_code,
            characteristic,
            order_sum,
            description,
            status_order,
        ) in cur.fetchall():
            rows.append({
                "code": _text(code),
                "name": _text(name),
                "partner": _text(partner),
                "date_reg": _sql_date(date_reg),
                "date_plan": _sql_date(date_plan),
                "date_end": _sql_date(date_end),
                "order_num": _text(order_num),
                "order_dept": _text(order_dept),
                "order_dept_key": _bin_to_guid(order_dept_ref),
                "nomenclature": _nomenclature(nom_name, nom_code),
                "characteristic": _text(characteristic),
                "order_sum": _money(order_sum),
                "description": _text(description),
                "status": STATUS_BY_ORDER.get(int(status_order), "") if status_order is not None else "",
            })
    return rows


def claims_table_rows(*, year: int, month: int) -> list[dict]:
    ref_y = int(year)
    ref_m = max(1, min(12, int(month)))
    path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def compute():
        try:
            rows = _fetch(ref_y, ref_m)
        except Exception:
            logger.exception("jurist claims SQL %s-%02d", ref_y, ref_m)
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
        lock_key=f"jurist_claims_{ref_y}_{ref_m:02d}",
        compute_fn=compute,
    )
    rows = (payload or {}).get("rows")
    return rows if isinstance(rows, list) else []
