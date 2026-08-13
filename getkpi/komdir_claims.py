"""
Получение активных претензий (Catalog_Претензии) из 1С OData.

Логика идентична export_claims2.py, но возвращает список dict (для JSON API),
а не записывает CSV. Результат кэшируется на день в JSON-файл.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from .commercial_department_aliases import normalize_commercial_dept_guid
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ALLOWED_DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
}
VED_DEPT_KEY = "49480c10-e401-11e8-8283-ac1f6b05524d"
VED_DEPT_NAME = "Отдел ВЭД"
UZTRANSGAZ_PARTNER_MARKER = "узтрансгаз"

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'
CACHE_VERSION = 10
ALLOWED_CLAIM_STATUSES = frozenset({
    "Зарегистрирована",
    "Обрабатывается",
    "НаКонтроле",
})
CLAIM_STATUS_LABELS = {
    "НаКонтроле": "На контроле",
}
CLAIM_REASON_PRETENSION = "7a4719be-3e1b-11ec-8742-ac1f6b05524d"
CLAIM_REASON_LABELS = {
    CLAIM_REASON_PRETENSION: "Претензия",
}

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

CLAIMS_SQL_TABLE = "_Reference389"
CLAIMS_SQL_PARTNER_TABLE = "_Reference328"
CLAIMS_SQL_REASON_TABLE = "_Reference396"
CLAIMS_SQL_NOMENCLATURE_TABLE = "_Reference269"
CLAIMS_SQL_CHARACTERISTIC_TABLE = "_Reference600"
CLAIMS_SQL_YEAR_OFFSET = 2000
CLAIMS_SQL_EMPTY_DATE = datetime(2001, 1, 2)
CLAIMS_SQL_EMPTY_REF = b"\x00" * 16


def _cache_path(year: int, month: int, include_all: bool = False) -> Path:
    suffix = "_all" if include_all else ""
    return CACHE_DIR / f"claims{suffix}_{year}_{month:02d}.json"


def _load_cache(year: int, month: int, include_all: bool = False) -> list[dict] | None:
    p = _cache_path(year, month, include_all=include_all)
    if not p.exists():
        return None
    try:
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
        rows = data.get('rows')
        if (
            data.get('date') == date.today().isoformat()
            and data.get('cache_version') == CACHE_VERSION
            and isinstance(rows, list)
            and rows
        ):
            return rows
    except (json.JSONDecodeError, OSError):
        pass
    return None


def _load_stale_nonempty_cache(year: int, month: int, include_all: bool = False) -> list[dict] | None:
    p = _cache_path(year, month, include_all=include_all)
    if not p.exists():
        return None
    try:
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
        rows = data.get('rows')
        if data.get('cache_version') == CACHE_VERSION and isinstance(rows, list) and rows:
            return rows
    except (json.JSONDecodeError, OSError):
        pass
    return None


def _save_cache(year: int, month: int, rows: list[dict], include_all: bool = False) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month, include_all=include_all), 'w', encoding='utf-8') as f:
            json.dump(
                {
                    'date': date.today().isoformat(),
                    'cache_version': CACHE_VERSION,
                    'rows': rows,
                },
                f,
                ensure_ascii=False,
            )
    except OSError:
        pass


def _sql_ref_to_guid(value) -> str:
    if not isinstance(value, (bytes, bytearray)) or len(value) != 16 or bytes(value) == CLAIMS_SQL_EMPTY_REF:
        return ""
    h = bytes(value).hex()
    return f"{h[24:32]}-{h[20:24]}-{h[16:20]}-{h[0:4]}-{h[4:16]}"


def _sql_1c_date(value) -> str:
    if not isinstance(value, datetime) or value.year < CLAIMS_SQL_YEAR_OFFSET + 2:
        return ""
    try:
        return value.replace(year=value.year - CLAIMS_SQL_YEAR_OFFSET).date().isoformat()
    except ValueError:
        return ""


def _sql_text(value) -> str:
    return str(value or "").strip()


def _load_sql_ref_map(cur, table: str, refs: set[bytes], *, with_code: bool = False) -> dict[bytes, str]:
    refs = {bytes(r) for r in refs if isinstance(r, (bytes, bytearray)) and bytes(r) != CLAIMS_SQL_EMPTY_REF}
    if not refs:
        return {}

    result: dict[bytes, str] = {}
    items = list(refs)
    for start in range(0, len(items), 500):
        chunk = items[start:start + 500]
        placeholders = ",".join("?" for _ in chunk)
        cols = "_IDRRef,_Description" + (",_Code" if with_code else "")
        cur.execute(
            f"SELECT {cols} FROM dbo.[{table}] WHERE _IDRRef IN ({placeholders})",
            chunk,
        )
        for row in cur.fetchall():
            desc = _sql_text(row._Description)
            if with_code:
                code = _sql_text(row._Code)
                result[bytes(row._IDRRef)] = f"{desc} ({code})" if desc and code else desc or code
            else:
                result[bytes(row._IDRRef)] = desc
    return result


def _fetch_from_sql(year: int, month: int, include_all: bool = False) -> list[dict]:
    """Загружает активные претензии напрямую из SQL-снимка erp_pm."""
    from sql_connection import SqlConnection

    if not 1 <= int(month) <= 12:
        return []
    date_to = datetime(year + CLAIMS_SQL_YEAR_OFFSET, month, 28)
    if month == 12:
        date_next = datetime(year + CLAIMS_SQL_YEAR_OFFSET + 1, 1, 1)
    else:
        date_next = datetime(year + CLAIMS_SQL_YEAR_OFFSET, month + 1, 1)

    with SqlConnection().connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                _Code,
                _Description,
                _Fld11617 AS date_reg,
                _Fld11618 AS date_end,
                _Fld132055 AS date_plan,
                _Fld11622 AS claim_description,
                _Fld11623RRef AS reason_ref,
                _Fld11624RRef AS partner_ref,
                _Fld132070RRef AS nomenclature_ref,
                _Fld132071RRef AS characteristic_ref
            FROM dbo.[{CLAIMS_SQL_TABLE}]
            WHERE _Marked = 0x00
              AND _Fld11617 < ?
              AND (_Fld11618 < ? OR _Fld11618 >= ?)
            ORDER BY _Fld11617 DESC, _Code
            """,
            date_next,
            CLAIMS_SQL_EMPTY_DATE,
            date_next,
        )
        raw_rows = cur.fetchall()

        partner_refs = {bytes(r.partner_ref) for r in raw_rows if isinstance(r.partner_ref, bytes)}
        reason_refs = {bytes(r.reason_ref) for r in raw_rows if isinstance(r.reason_ref, bytes)}
        nom_refs = {bytes(r.nomenclature_ref) for r in raw_rows if isinstance(r.nomenclature_ref, bytes)}
        char_refs = {bytes(r.characteristic_ref) for r in raw_rows if isinstance(r.characteristic_ref, bytes)}

        partners = _load_sql_ref_map(cur, CLAIMS_SQL_PARTNER_TABLE, partner_refs)
        reasons = _load_sql_ref_map(cur, CLAIMS_SQL_REASON_TABLE, reason_refs)
        nom_display = _load_sql_ref_map(cur, CLAIMS_SQL_NOMENCLATURE_TABLE, nom_refs, with_code=True)
        char_names = _load_sql_ref_map(cur, CLAIMS_SQL_CHARACTERISTIC_TABLE, char_refs)

    result_rows: list[dict] = []
    for row in raw_rows:
        partner_ref = bytes(row.partner_ref) if isinstance(row.partner_ref, bytes) else CLAIMS_SQL_EMPTY_REF
        partner = partners.get(partner_ref, _sql_ref_to_guid(partner_ref))
        is_uztransgaz = _is_uztransgaz_partner(partner)
        normalized_dept_key = VED_DEPT_KEY if is_uztransgaz else ""
        if not include_all and normalized_dept_key not in ALLOWED_DEPARTMENTS:
            # В SQL-снимке прямой связи с отделом заказа для этого справочника нет.
            # Для дочерних отделов лучше ничего не подмешивать, чем показать чужую претензию.
            continue

        reason_ref = bytes(row.reason_ref) if isinstance(row.reason_ref, bytes) else CLAIMS_SQL_EMPTY_REF
        reason_key = _sql_ref_to_guid(reason_ref)
        reason = CLAIM_REASON_LABELS.get(reason_key) or reasons.get(reason_ref) or reason_key

        nom_ref = bytes(row.nomenclature_ref) if isinstance(row.nomenclature_ref, bytes) else CLAIMS_SQL_EMPTY_REF
        char_ref = bytes(row.characteristic_ref) if isinstance(row.characteristic_ref, bytes) else CLAIMS_SQL_EMPTY_REF
        desc = _sql_text(row.claim_description).replace("\r\n", " ").replace("\n", " ")

        result_rows.append({
            "code": _sql_text(row._Code),
            "name": _sql_text(row._Description),
            "partner": partner,
            "date_reg": _sql_1c_date(row.date_reg),
            "date_plan": _sql_1c_date(row.date_plan),
            "date_end": _sql_1c_date(row.date_end),
            "order_num": "",
            "order_dept": VED_DEPT_NAME if is_uztransgaz else "",
            "order_dept_key": VED_DEPT_KEY if is_uztransgaz else "",
            "normalized_order_dept_key": normalized_dept_key,
            "nomenclature": nom_display.get(nom_ref, ""),
            "characteristic": char_names.get(char_ref, ""),
            "order_sum": 0,
            "description": desc,
            "reason_key": reason_key,
            "reason": reason,
            "status": "Обрабатывается",
            "source": "sql_erp_pm",
        })

    return result_rows


def _load_catalog_full(session: requests.Session,
                       entity: str, select_fields: str) -> dict[str, dict]:
    """Загрузка справочника целиком с пагинацией и $orderby."""
    result: dict[str, dict] = {}
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$top={PAGE}&$skip={skip}"
            f"&$select={quote(select_fields, safe=',_')}"
            f"&$orderby=Ref_Key"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label=f"Claims/{entity}")
        if r is None:
            logger.error("Claims/%s: request dropped after retries", entity)
            break
        if not r.ok:
            logger.error("%s HTTP %d", entity, r.status_code)
            break
        rows = r.json().get("value", [])
        if not rows:
            break
        for item in rows:
            result[item["Ref_Key"]] = item
        if len(rows) < PAGE:
            break
        skip += len(rows)
    return result


def _fetch_single(session: requests.Session,
                  entity: str, guid: str, select_fields: str) -> dict | None:
    """Точечная загрузка одной записи по GUID."""
    url = (
        f"{BASE}/{quote(entity)}(guid'{guid}')"
        f"?$format=json&$select={quote(select_fields, safe=',_')}"
    )
    try:
        r = session.get(url, timeout=15)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None


def _is_uztransgaz_partner(partner_name: str) -> bool:
    return UZTRANSGAZ_PARTNER_MARKER in str(partner_name or "").strip().lower().replace("ё", "е")


def _fetch_from_odata(year: int, month: int, include_all: bool = False) -> list[dict]:
    """Загружает все претензии из 1С OData с разрешенными статусами."""

    session = requests.Session()
    session.auth = AUTH

    select_claims = (
        "Ref_Key,Code,Description,Партнер_Key,ДатаРегистрации,ДатаОкончания,"
        "ТД_ДатаОкончанияПлан,ТД_ЗаказКлиента_Key,ТД_Номенклатура_Key,"
        "ТД_Характеристика_Key,ОписаниеПретензии,DeletionMark,Статус,"
        "ПричинаВозникновения_Key"
    )

    claims = []
    skip = 0
    status_filter = " or ".join(
        f"Статус eq '{status}'" for status in sorted(ALLOWED_CLAIM_STATUSES)
    )
    while True:
        odata_filter = (
            f"({status_filter})"
        )
        url = (
            f"{BASE}/Catalog_Претензии?$format=json"
            f"&$select={select_claims}&$top=5000&$skip={skip}"
            f"&$filter={odata_filter}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="Claims")
        if r is None:
            logger.error("Claims: request dropped after retries")
            raise RuntimeError("Claims request dropped after retries")
        if not r.ok:
            logger.error("Claims HTTP %d: %s", r.status_code, r.text[:300])
            raise RuntimeError(f"Claims HTTP {r.status_code}")
        rows = r.json().get("value", [])
        claims.extend(rows)
        if len(rows) < 5000:
            break
        skip += 5000

    # ── Партнёры (bulk + дозагрузка поштучно) ──
    raw_partners = _load_catalog_full(session, "Catalog_Партнеры", "Ref_Key,Description")
    partners = {k: v.get("Description", "").strip() for k, v in raw_partners.items()}

    needed_p = {
        c.get("Партнер_Key")
        for c in claims if c.get("Партнер_Key") and c.get("Партнер_Key") != EMPTY
    }
    for pk in needed_p - set(partners.keys()):
        item = _fetch_single(session, "Catalog_Партнеры", pk, "Ref_Key,Description")
        if item:
            partners[pk] = (item.get("Description") or "").strip()

    # ── Подразделения ──
    raw_depts = _load_catalog_full(session, "Catalog_СтруктураПредприятия", "Ref_Key,Description")
    depts = {k: v.get("Description", "").strip() for k, v in raw_depts.items()}

    # ── Заказы клиентов ──
    order_keys_needed = set()
    for c in claims:
        ok = c.get("ТД_ЗаказКлиента_Key")
        if ok and ok != EMPTY:
            order_keys_needed.add(ok)

    orders_info: dict[str, dict] = {}
    o_skip = 0
    o_select = "Ref_Key,Number,Date,Подразделение_Key,СуммаДокумента"
    while True:
        url = (
            f"{BASE}/{quote('Document_ЗаказКлиента')}?$format=json"
            f"&$top=500&$skip={o_skip}&$select={o_select}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="Claims/Orders")
        if r is None or not r.ok:
            break
        chunk = r.json().get("value", [])
        if not chunk:
            break
        for item in chunk:
            rk = item["Ref_Key"]
            if rk in order_keys_needed:
                orders_info[rk] = item
        o_skip += len(chunk)
        if len(orders_info) >= len(order_keys_needed):
            break

    # ── Номенклатура: Description + Code (bulk + дозагрузка) ──
    raw_nom = _load_catalog_full(session, "Catalog_Номенклатура", "Ref_Key,Description,Code")
    nom_display: dict[str, str] = {}
    for k, v in raw_nom.items():
        desc = (v.get("Description") or "").strip()
        code = (v.get("Code") or "").strip()
        nom_display[k] = f"{desc} ({code})" if desc and code else desc or code

    needed_n = {
        c.get("ТД_Номенклатура_Key")
        for c in claims if c.get("ТД_Номенклатура_Key") and c.get("ТД_Номенклатура_Key") != EMPTY
    }
    for nk in needed_n - set(nom_display.keys()):
        item = _fetch_single(session, "Catalog_Номенклатура", nk, "Ref_Key,Description,Code")
        if item:
            desc = (item.get("Description") or "").strip()
            code = (item.get("Code") or "").strip()
            nom_display[nk] = f"{desc} ({code})" if desc and code else desc or code

    # ── Характеристики номенклатуры (bulk + дозагрузка) ──
    raw_char = _load_catalog_full(
        session, "Catalog_ХарактеристикиНоменклатуры", "Ref_Key,Description",
    )
    char_names = {k: v.get("Description", "").strip() for k, v in raw_char.items()}

    needed_c = {
        c.get("ТД_Характеристика_Key")
        for c in claims if c.get("ТД_Характеристика_Key") and c.get("ТД_Характеристика_Key") != EMPTY
    }
    for ck in needed_c - set(char_names.keys()):
        item = _fetch_single(
            session, "Catalog_ХарактеристикиНоменклатуры", ck, "Ref_Key,Description",
        )
        if item:
            char_names[ck] = (item.get("Description") or "").strip()

    # ── Сборка результата ──
    result_rows = []
    for c in claims:
        order_key = c.get("ТД_ЗаказКлиента_Key", "")
        order = orders_info.get(order_key)
        partner = partners.get(c.get("Партнер_Key", ""), c.get("Партнер_Key", ""))
        is_uztransgaz = _is_uztransgaz_partner(partner)
        if not order and not include_all and not is_uztransgaz:
            continue

        order_dept_key = order.get("Подразделение_Key", "") if order else ""
        normalized_dept_key = normalize_commercial_dept_guid(order_dept_key)
        if is_uztransgaz:
            normalized_dept_key = VED_DEPT_KEY
            if not order_dept_key:
                order_dept_key = VED_DEPT_KEY
        if not include_all and normalized_dept_key not in ALLOWED_DEPARTMENTS:
            continue
        date_reg = (c.get("ДатаРегистрации") or "")[:10]
        date_plan = (c.get("ТД_ДатаОкончанияПлан") or "")[:10]
        date_end = (c.get("ДатаОкончания") or "")[:10]

        order_num = order.get("Number", "").strip() if order else ""
        order_dept = depts.get(order_dept_key, order_dept_key)
        if is_uztransgaz:
            order_dept = VED_DEPT_NAME
        order_sum = order.get("СуммаДокумента", 0) if order else 0

        nom_key = c.get("ТД_Номенклатура_Key", "")
        nom = nom_display.get(nom_key, nom_key if nom_key and nom_key != EMPTY else "")

        char_key = c.get("ТД_Характеристика_Key", "")
        char = char_names.get(char_key, char_key if char_key and char_key != EMPTY else "")

        desc = (c.get("ОписаниеПретензии") or "").replace("\r\n", " ").replace("\n", " ")
        raw_status = c.get("Статус", "")
        if raw_status not in ALLOWED_CLAIM_STATUSES:
            continue
        reason_key = c.get("ПричинаВозникновения_Key") or ""
        reason = CLAIM_REASON_LABELS.get(reason_key, reason_key)
        status = CLAIM_STATUS_LABELS.get(raw_status, raw_status)

        result_rows.append({
            "code": c.get("Code", ""),
            "name": c.get("Description", ""),
            "partner": partner,
            "date_reg": date_reg,
            "date_plan": date_plan,
            "date_end": date_end,
            "order_num": order_num,
            "order_dept": order_dept,
            "order_dept_key": order_dept_key,
            "normalized_order_dept_key": normalized_dept_key,
            "nomenclature": nom,
            "characteristic": char,
            "order_sum": order_sum,
            "description": desc,
            "reason_key": reason_key,
            "reason": reason,
            "status": status,
        })

    return result_rows


def fetch_claims_for_month(year: int, month: int, include_all: bool = False) -> list[dict]:
    """Возвращает список активных претензий (с кэшированием на день)."""
    cached = _load_cache(year, month, include_all=include_all)
    if cached is not None:
        return cached

    try:
        rows = _fetch_from_sql(year, month, include_all=include_all)
    except Exception as e:
        logger.error("Failed to fetch claims from SQL: %s", e)
        try:
            rows = _fetch_from_odata(year, month, include_all=include_all)
        except Exception as e:
            logger.error("Failed to fetch claims: %s", e)
            stale = _load_stale_nonempty_cache(year, month, include_all=include_all)
            return stale or []

    if rows:
        _save_cache(year, month, rows, include_all=include_all)
    return rows
