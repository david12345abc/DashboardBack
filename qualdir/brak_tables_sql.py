"""SQL-загрузчик строк таблиц брака (QD-T-M1 / QD-T-M5 / QD-T-M8).

Замена OData-логики ``qualdir.brak_report.load_brak_table_rows`` на прямые
запросы к SQL-бэкенду 1С (erp_pm), в том же стиле, что эталонные плитки
``qualdir.qd_m1`` / ``qualdir.qd_m5`` / ``qualdir.qd_m8``.

Справочники SQL (erp_pm):
  Формы (документы):
    ТД_Форма0319 (внешний брак)   → dbo._Document100537X1
    ТД_Форма0318 (внутренний)     → dbo._Document148564X1
    ТД_Форма0317 (проц. несоотв.) → dbo._Document148563X1
  Поля документа (одинаковый смысл в трёх формах, разные _Fld):
    _Date_Time      — Date (+2000 лет)
    _Marked         — DeletionMark (0x00 = нет)
    _Number         — Номер документа
    <status>RRef    — Статус                 → _Enum100559 (STATUS_BY_ORDER)
    <significant>   — ФормаЯвляетсяЗначимой   (0x01 = да)
    <dept>RRef      — ПодразделениеПоставщика → _Reference513._Description
    <product>RRef   — НаименованиеИзделия     → _Enum87024 (PRODUCT_NAME_BY_ORDER)
  ТЧ «Несоответствия» → dbo._Document<...>_VT<...>X1
    <kind>RRef      — ВидНесоответствия       → _Reference100536._Description
    <desc>          — ОписаниеНесоответствия  (fallback, если вид пуст)
  Справочники:
    Catalog_СтруктураПредприятия          → _Reference513
    Catalog_ТД_ВидыНесоответствияПроцессовСТО → _Reference100536

Перечисления 1С не хранят имена значений в БД (как и статусы в эталоне),
поэтому order→имя задаётся статически (STATUS_BY_ORDER / PRODUCT_NAME_BY_ORDER).
PRODUCT_NAME_BY_ORDER построен по всем документам 2021–2026 (OData сверен с SQL).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from sql_connection import SqlConnection

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000

STATUS_ENUM_TABLE = "_Enum100559"
PRODUCT_ENUM_TABLE = "_Enum87024"
DEPT_TABLE = "_Reference513"
KIND_TABLE = "_Reference100536"

# _Enum100559._EnumOrder → имя статуса формы (как в qualdir.qd_m1/qd_m5/qd_m8)
STATUS_BY_ORDER: dict[int, str] = {
    0: "Подготовлен",
    1: "НаСогласовании",
    2: "НеСогласовано",
    3: "РазработкаКМ",
    4: "ИсполнениеКМ",
    5: "Выполнено",
    6: "Отменена",
}

# _Enum87024._EnumOrder → НаименованиеИзделия (перечисление «Изделие»).
# Построено по документам форм 0317/0318/0319 за 2021–2026 (OData ↔ SQL).
PRODUCT_NAME_BY_ORDER: dict[int, str] = {
    0: "РасходомерUFG",
    1: "РасходомерTFG",
    2: "РасходомерCFM",
    3: "РасходомерUDM",
    4: "РасходомерUFGH",
    5: "РасходомерUFL",
    6: "РасходомерGFG",
    7: "РасходомерРС2М",
    8: "РасходомерРГА100_300",
    9: "УстановкаПоверочнаяСПУ3",
    10: "УстановкаПоверочнаяСПУ5",
    12: "ДатчикДавления",
    13: "Телеметрия",
    14: "ВыноснойТерминал",
    15: "ПрямойУчасток",
    16: "ИзделияМехЦеха",
    17: "СчетчикГрандSPI",
    18: "СчетчикГранд",
    19: "Поверка_Калибровка",
    20: "СМР_ПНР",
    21: "СИРГ",
    22: "ПокупныеТМЦ",
    23: "Прочее",
}

# Заявка попадает в таблицу (plan), если статус не из исключённых.
PLAN_EXCLUDED_STATUSES = frozenset(
    {
        "НеСогласовано",
        "Отменена",
        "Подготовлен",
        "Подготовлено",
    }
)

BRAK_TABLE_COLUMNS = [
    "Документ",
    "Объект несоответствия",
    "Вид несоответствия",
    "Подразделение",
    "Статус",
    "Значимая форма",
]


def _normalize_text(value: str | None) -> str:
    text = (value or "").strip().lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in text).split())


PLAN_EXCLUDED_NORM = frozenset(_normalize_text(s) for s in PLAN_EXCLUDED_STATUSES)


@dataclass(frozen=True)
class SqlFormConfig:
    table_kind: str
    doc_table: str
    col_status: str
    col_significant: str
    col_dept: str
    col_product: str
    vt_table: str
    vt_doc_col: str
    vt_kind_col: str
    vt_desc_col: str
    vt_lineno_col: str
    col_number: str = "_Number"
    col_date: str = "_Date_Time"
    col_marked: str = "_Marked"


FORMS: dict[str, SqlFormConfig] = {
    "external": SqlFormConfig(
        table_kind="external",
        doc_table="_Document100537X1",
        col_status="_Fld100562RRef",
        col_significant="_Fld185472",
        col_dept="_Fld100569RRef",
        col_product="_Fld100564RRef",
        vt_table="_Document100537_VT147092X1",
        vt_doc_col="_Document100537_IDRRef",
        vt_kind_col="_Fld147095RRef",
        vt_desc_col="_Fld147094",
        vt_lineno_col="_LineNo147093",
    ),
    "internal": SqlFormConfig(
        table_kind="internal",
        doc_table="_Document148564X1",
        col_status="_Fld148654RRef",
        col_significant="_Fld185471",
        col_dept="_Fld148649RRef",
        col_product="_Fld148644RRef",
        vt_table="_Document148564_VT148659X1",
        vt_doc_col="_Document148564_IDRRef",
        vt_kind_col="_Fld148662RRef",
        vt_desc_col="_Fld148661",
        vt_lineno_col="_LineNo148660",
    ),
    "forma0317": SqlFormConfig(
        table_kind="forma0317",
        doc_table="_Document148563X1",
        col_status="_Fld148623RRef",
        col_significant="_Fld185470",
        col_dept="_Fld148618RRef",
        col_product="_Fld148613RRef",
        vt_table="_Document148563_VT148628X1",
        vt_doc_col="_Document148563_IDRRef",
        vt_kind_col="_Fld148631RRef",
        vt_desc_col="_Fld148630",
        vt_lineno_col="_LineNo148629",
    ),
}


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    month = max(1, min(12, int(month)))
    start = date(year, month, 1)
    if month == 12:
        end_exclusive = date(year + 1, 1, 1)
    else:
        end_exclusive = date(year, month + 1, 1)
    return start, end_exclusive


def _to_sql_dt(value: date) -> datetime:
    return datetime(value.year + YEAR_OFFSET, value.month, value.day)


def _is_plan_status(status_name: str | None) -> bool:
    if not status_name:
        return False
    return _normalize_text(status_name) not in PLAN_EXCLUDED_NORM


def _fmt_date(date_raw: datetime | None) -> str:
    if date_raw is None:
        return ""
    year = date_raw.year - YEAR_OFFSET
    if year < 1:
        return ""
    return f"{date_raw.day:02d}.{date_raw.month:02d}.{year:04d}"


def _fmt_doc_number(number: str | None) -> str:
    text = (number or "").strip()
    if not text:
        return "—"
    return text if text.startswith("№") else f"№{text}"


def _load_status_bin_to_name(cur) -> dict[bytes, str]:
    cur.execute(
        f"SELECT _IDRRef, _EnumOrder FROM [{STATUS_ENUM_TABLE}] WITH (NOLOCK)"
    )
    result: dict[bytes, str] = {}
    for idr, order in cur.fetchall():
        name = STATUS_BY_ORDER.get(int(order))
        if name:
            result[bytes(idr)] = name
    return result


def _load_product_bin_to_name(cur) -> dict[bytes, str]:
    cur.execute(
        f"SELECT _IDRRef, _EnumOrder FROM [{PRODUCT_ENUM_TABLE}] WITH (NOLOCK)"
    )
    result: dict[bytes, str] = {}
    for idr, order in cur.fetchall():
        name = PRODUCT_NAME_BY_ORDER.get(int(order))
        if name:
            result[bytes(idr)] = name
    return result


def _load_kinds_by_doc(
    cur,
    cfg: SqlFormConfig,
    doc_ids: list[bytes],
) -> dict[bytes, list[str]]:
    """ТЧ «Несоответствия»: doc_id → упорядоченный список видов (с дедупликацией)."""
    kinds: dict[bytes, list[str]] = {}
    seen: dict[bytes, set[str]] = {}
    chunk_size = 400
    for offset in range(0, len(doc_ids), chunk_size):
        chunk = doc_ids[offset : offset + chunk_size]
        placeholders = ",".join("?" * len(chunk))
        cur.execute(
            f"""
            SELECT
                vt.[{cfg.vt_doc_col}],
                k._Description,
                vt.[{cfg.vt_desc_col}]
            FROM [{cfg.vt_table}] vt WITH (NOLOCK)
            LEFT JOIN [{KIND_TABLE}] k WITH (NOLOCK)
                ON k._IDRRef = vt.[{cfg.vt_kind_col}]
            WHERE vt.[{cfg.vt_doc_col}] IN ({placeholders})
            ORDER BY vt.[{cfg.vt_doc_col}], vt.[{cfg.vt_lineno_col}]
            """,
            *chunk,
        )
        for doc_id_raw, kind_name, fallback in cur.fetchall():
            doc_id = bytes(doc_id_raw)
            name = (kind_name or "").strip() or (fallback or "").strip()
            if not name:
                continue
            bucket_seen = seen.setdefault(doc_id, set())
            if name in bucket_seen:
                continue
            bucket_seen.add(name)
            kinds.setdefault(doc_id, []).append(name)
    return kinds


def _document_row(
    number: str | None,
    date_raw: datetime | None,
    product: str | None,
    kinds: list[str],
    dept_name: str | None,
    status_name: str | None,
    is_significant: bool,
) -> dict[str, str]:
    doc_label = _fmt_doc_number(number)
    date_str = _fmt_date(date_raw)
    if date_str:
        doc_label = f"{doc_label} от {date_str}"
    return {
        "Документ": doc_label,
        "Объект несоответствия": (product or "").strip() or "—",
        "Вид несоответствия": "; ".join(kinds) if kinds else "—",
        "Подразделение": (dept_name or "").strip() or "—",
        "Статус": (status_name or "").strip() or "—",
        "Значимая форма": "да" if is_significant else "нет",
    }


def load_brak_table_rows_sql(
    table_kind: str,
    year: int,
    month: int,
    *,
    connection: Any | None = None,
) -> list[dict[str, str]]:
    """Строки таблицы брака за календарный месяц (аналог OData load_brak_table_rows)."""
    cfg = FORMS[table_kind]
    start, end_exclusive = _month_bounds(year, month)
    sql_start = _to_sql_dt(start)
    sql_end = _to_sql_dt(end_exclusive)

    def _run(cur) -> list[dict[str, str]]:
        status_names = _load_status_bin_to_name(cur)
        product_names = _load_product_bin_to_name(cur)

        cur.execute(
            f"""
            SELECT
                doc._IDRRef,
                doc.[{cfg.col_number}],
                doc.[{cfg.col_date}],
                doc.[{cfg.col_status}],
                doc.[{cfg.col_significant}],
                doc.[{cfg.col_product}],
                dept._Description
            FROM [{cfg.doc_table}] doc WITH (NOLOCK)
            LEFT JOIN [{DEPT_TABLE}] dept WITH (NOLOCK)
                ON dept._IDRRef = doc.[{cfg.col_dept}]
            WHERE doc.[{cfg.col_marked}] = 0x00
              AND doc.[{cfg.col_date}] >= ?
              AND doc.[{cfg.col_date}] < ?
            ORDER BY doc.[{cfg.col_date}] DESC, doc.[{cfg.col_number}] DESC
            """,
            sql_start,
            sql_end,
        )

        raw_docs: list[tuple] = []
        doc_ids: list[bytes] = []
        for idr, number, date_raw, status_bin, sig_raw, product_bin, dept_name in cur.fetchall():
            if idr is None or status_bin is None:
                continue
            status_name = status_names.get(bytes(status_bin))
            if not _is_plan_status(status_name):
                continue
            doc_id = bytes(idr)
            doc_ids.append(doc_id)
            product = product_names.get(bytes(product_bin)) if product_bin is not None else None
            is_significant = bytes(sig_raw) != b"\x00" if sig_raw is not None else False
            raw_docs.append(
                (doc_id, number, date_raw, product, dept_name, status_name, is_significant)
            )

        kinds_by_doc = _load_kinds_by_doc(cur, cfg, doc_ids) if doc_ids else {}

        rows: list[dict[str, str]] = []
        for doc_id, number, date_raw, product, dept_name, status_name, is_significant in raw_docs:
            rows.append(
                _document_row(
                    number,
                    date_raw,
                    product,
                    kinds_by_doc.get(doc_id, []),
                    dept_name,
                    status_name,
                    is_significant,
                )
            )
        return rows

    if connection is not None:
        return _run(connection.cursor())

    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        return _run(conn.cursor())
