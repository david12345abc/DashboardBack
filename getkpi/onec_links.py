"""Навигационные ссылки 1С на документы из строк дашборда.

Ссылка вида ``e1cib/data/Документ.ЗаказКлиента?ref=<32 hex>``: ref — это
``_IDRRef`` объекта (байты GUID в порядке 1С), тот же, что в SQL-копии.
Префикс базы (``e1c://server/srv1/erp_pm#``) добавляет клиент.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import quote

logger = logging.getLogger(__name__)

EMPTY_GUID = "00000000-0000-0000-0000-000000000000"
ORDER_TYPE = "Документ.ЗаказКлиента"
KP_TYPE = "Документ.КоммерческоеПредложениеКлиенту"
AGREEMENT_TYPE = "Справочник.СоглашенияСКлиентами"

_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_ORDER_RE = re.compile(r"заказ(?:\s+клиента)?\s+([^\s,;]+)", re.IGNORECASE)
_KP_RE = re.compile(r"коммерческое\s+предложение\s+([^\s,;]+)", re.IGNORECASE)
# Подписи строк без конкретного документа — искать по ним в 1С нечего.
_GENERIC_DOCUMENTS = {
    "", "—", "платёж", "комиссия", "взаимозачёт", "отгрузка", "счёт-оферта",
    "договор", "дозаказ", "потенциальный договор", "плановый платёж", "заказ к отгрузке",
}


def metadata_name(type_name: str) -> str:
    """``StandardODATA.Document_X`` / ``Document_X`` / ``Документ.X`` → ``Документ.X``."""
    text = str(type_name or "").strip()
    if text.startswith("StandardODATA."):
        text = text[len("StandardODATA."):]
    for prefix, meta in (("Document_", "Документ."), ("Catalog_", "Справочник.")):
        if text.startswith(prefix):
            return meta + text[len(prefix):]
    if text.startswith(("Документ.", "Справочник.")):
        return text
    return ""


def guid_to_ref_hex(guid: str) -> str:
    text = str(guid or "").strip().lower()
    if not _GUID_RE.match(text) or text == EMPTY_GUID:
        return ""
    u = text.replace("-", "")
    return u[16:20] + u[20:32] + u[12:16] + u[8:12] + u[0:8]


def nav_link(type_name: str, guid: str = "", ref_hex: str = "") -> str:
    meta = metadata_name(type_name)
    ref = (ref_hex or "").strip().lower() or guid_to_ref_hex(guid)
    if not meta or len(ref) != 32 or ref == "0" * 32:
        return ""
    return f"e1cib/data/{meta}?ref={ref}"


def nav_from_line(row: dict) -> str:
    """Ссылка для строки расшифровки: явный документ строки, иначе заказ."""
    link = nav_link(row.get("onec_type") or "", row.get("onec_ref") or "", row.get("onec_hex") or "")
    if link:
        return link
    return nav_link(ORDER_TYPE, row.get("order_key") or "")


def _odata_value(session, base: str, entity: str, flt: str, select: str) -> list[dict]:
    url = (
        f"{base}/{quote(entity)}?$format=json&$filter={quote(flt, safe='')}"
        f"&$select={quote(select, safe=',_')}&$top=20"
    )
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json().get("value") or []


def _literal(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _pick(rows: list[dict], session, base: str, partner: str) -> dict | None:
    """Номер в 1С уникален в пределах года: при нескольких — по партнёру, иначе самый новый."""
    rows = [row for row in rows if not row.get("DeletionMark")]
    if not rows:
        return None
    if len(rows) > 1 and partner:
        keys = {row.get("Партнер_Key") or "" for row in rows} - {"", EMPTY_GUID}
        if keys:
            flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in keys)
            names = {
                item.get("Ref_Key"): (item.get("Description") or "").strip().lower()
                for item in _odata_value(session, base, "Catalog_Партнеры", flt, "Ref_Key,Description")
            }
            wanted = partner.strip().lower()
            same = [row for row in rows if names.get(row.get("Партнер_Key")) == wanted]
            if same:
                rows = same
    rows.sort(key=lambda row: str(row.get("Date") or row.get("Дата") or ""), reverse=True)
    return rows[0]


def _by_number(session, base: str, entity: str, meta: str, number: str, partner: str) -> str:
    rows = _odata_value(
        session, base, entity,
        f"Number eq {_literal(number)}",
        "Ref_Key,Date,Партнер_Key,DeletionMark",
    )
    found = _pick(rows, session, base, partner)
    return nav_link(meta, found.get("Ref_Key") or "") if found else ""


def resolve(
    *,
    settlement: str = "",
    order_num: str = "",
    document: str = "",
    partner: str = "",
) -> str:
    """Найти документ в 1С и вернуть навигационную ссылку ('' — не нашли)."""
    from comdir.resale import _base, _session

    session = _session()
    base = _base()
    settlement = (settlement or "").strip().lower()
    if _GUID_RE.match(settlement) and settlement != EMPTY_GUID:
        url = (
            f"{base}/{quote('Catalog_ОбъектыРасчетов')}(guid'{settlement}')"
            f"?$format=json&$select={quote('Объект,Объект_Type', safe=',_')}"
        )
        response = session.get(url, timeout=30)
        if response.ok:
            item = response.json()
            link = nav_link(item.get("Объект_Type") or "", item.get("Объект") or "")
            if link:
                return link
    order_num = (order_num or "").strip()
    if order_num and order_num != "?":
        return _by_number(session, base, "Document_ЗаказКлиента", ORDER_TYPE, order_num, partner)

    text = (document or "").strip()
    if text.lower() in _GENERIC_DOCUMENTS:
        return ""
    match = _KP_RE.search(text)
    if match:
        return _by_number(
            session, base, "Document_КоммерческоеПредложениеКлиенту", KP_TYPE, match.group(1), partner,
        )
    match = _ORDER_RE.search(text)
    if match:
        return _by_number(session, base, "Document_ЗаказКлиента", ORDER_TYPE, match.group(1), partner)
    # Факт договоров подписан названием спецификации (соглашения с клиентом).
    rows = _odata_value(
        session, base, "Catalog_СоглашенияСКлиентами",
        f"Description eq {_literal(text)}",
        "Ref_Key,Дата,Партнер_Key,DeletionMark",
    )
    found = _pick(rows, session, base, partner)
    return nav_link(AGREEMENT_TYPE, found.get("Ref_Key") or "") if found else ""
