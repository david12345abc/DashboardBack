from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
DOC_ENTITY = "Document_ТД_АктОНесоответствиеПриборовИКомплектующих"
ROWS_ENTITY = "Document_ТД_АктОНесоответствиеПриборовИКомплектующих_Несоответствия"
NOMENCLATURE_ENTITY = "Catalog_Номенклатура"
DEPARTMENT_ENTITY = "Catalog_СтруктураПредприятия"
PARTNER_ENTITY = "Catalog_Партнеры"
COUNTERPARTY_ENTITY = "Catalog_Контрагенты"
CLAIMS_ENTITY = "Catalog_Претензии"
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"

TARGET_REASON_CATEGORY = "Поставщик"
TARGET_RESOLUTION = "Окончательный"
PRODUCTION_CLAIM_REASON_PRETENSION = "7a4719be-3e1b-11ec-8742-ac1f6b05524d"
PRODUCTION_CULPRIT_DEPARTMENTS: dict[str, str] = {
    "f12f2fca-d5d2-11e7-8267-ac1f6b05524d": "ТурбулентностьДОНПроизводство1",
    "3a9ac2f2-214f-11e0-b91c-00248c26ee57": "Алмаз",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_VERSION = 6


def _normalize_odata_base(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    if base.endswith("/odata/standard.odata"):
        return base
    return f"{base}/odata/standard.odata"


BASE = _normalize_odata_base(os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL))
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)


def _cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"logistics_claims_{year}_{month:02d}.json"


def _production_cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"prod_deputy_claims_{year}_{month:02d}.json"


def _load_cache_from_path(path: Path) -> list[dict] | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if data.get("date") == date.today().isoformat() and data.get("cache_version") == CACHE_VERSION:
            rows = data.get("rows")
            return rows if isinstance(rows, list) else None
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _load_cache(year: int, month: int) -> list[dict] | None:
    return _load_cache_from_path(_cache_path(year, month))


def _load_production_cache(year: int, month: int) -> list[dict] | None:
    return _load_cache_from_path(_production_cache_path(year, month))


def _save_cache_to_path(path: Path, rows: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "date": date.today().isoformat(),
                    "cache_version": CACHE_VERSION,
                    "rows": rows,
                },
                fh,
                ensure_ascii=False,
            )
    except OSError:
        logger.exception("LOG claims: failed to save cache %s", path)


def _save_cache(year: int, month: int, rows: list[dict]) -> None:
    _save_cache_to_path(_cache_path(year, month), rows)


def _save_production_cache(year: int, month: int, rows: list[dict]) -> None:
    _save_cache_to_path(_production_cache_path(year, month), rows)


def _month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(int(year), int(month), 1)
    if month == 12:
        end = datetime(int(year) + 1, 1, 1)
    else:
        end = datetime(int(year), int(month) + 1, 1)
    return start, end


def _parse_odata_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


def _odata_get(session: requests.Session, entity: str, query: str = "", *, timeout: int = 120) -> list[dict]:
    url = f"{BASE}/{quote(entity)}"
    if query:
        url = f"{url}?{query}"
    response = request_with_retry(session, url, timeout=timeout, retries=4, label=f"LOG claims/{entity}")
    if response is None:
        logger.error("LOG claims/%s: request dropped after retries", entity)
        return []
    if not response.ok:
        logger.error("LOG claims/%s HTTP %s: %s", entity, response.status_code, response.text[:500])
        return []
    data = response.json()
    rows = data.get("value", [])
    return rows if isinstance(rows, list) else []


def _fetch_single(session: requests.Session, entity: str, guid: str, select_fields: str) -> dict | None:
    clean_guid = str(guid or "").strip().strip("{}")
    if not clean_guid or clean_guid == EMPTY_GUID:
        return None
    url = (
        f"{BASE}/{quote(entity)}(guid'{clean_guid}')"
        f"?$format=json&$select={quote(select_fields, safe=',_')}"
    )
    response = request_with_retry(session, url, timeout=45, retries=3, label=f"LOG claims/{entity}/single")
    if response is None or not response.ok:
        return None
    data = response.json()
    return data if isinstance(data, dict) else None


def _fetch_entity_object(session: requests.Session, entity: str, guid: str) -> dict | None:
    clean_guid = str(guid or "").strip().strip("{}")
    if not clean_guid or clean_guid == EMPTY_GUID:
        return None
    url = f"{BASE}/{quote(entity)}(guid'{clean_guid}')?$format=json"
    response = request_with_retry(session, url, timeout=45, retries=2, label=f"LOG claims/{entity}/object")
    if response is None or not response.ok:
        return None
    data = response.json()
    return data if isinstance(data, dict) else None


def _format_ref_name(item: dict | None, fallback: str = "") -> str:
    if not item:
        return fallback
    desc = str(item.get("Description") or "").strip()
    code = str(item.get("Code") or "").strip()
    if desc and code:
        return f"{desc} ({code})"
    return desc or code or fallback


def _clean_text(value: object) -> str:
    return str(value or "").replace("\r\n", " ").replace("\n", " ").strip()


def _odata_entity_from_type(type_name: object) -> str:
    raw = _clean_text(type_name)
    prefix = "StandardODATA."
    if raw.startswith(prefix):
        raw = raw[len(prefix):]
    return raw


def _display_number(item: dict | None) -> str:
    if not item:
        return ""
    return _clean_text(item.get("Number")) or _clean_text(item.get("Code"))


def _first_guid(item: dict | None, candidates: tuple[str, ...]) -> str:
    if not item:
        return ""
    for key in candidates:
        value = _clean_text(item.get(key))
        if value and value != EMPTY_GUID:
            return value
    return ""


def _resolve_ref_name(
    session: requests.Session,
    cache: dict[str, str],
    entity: str,
    key: str,
) -> str:
    clean_key = _clean_text(key)
    if not clean_key or clean_key == EMPTY_GUID:
        return ""
    if clean_key not in cache:
        cache[clean_key] = _format_ref_name(
            _fetch_single(session, entity, clean_key, "Ref_Key,Description,Code"),
            clean_key,
        )
    return cache.get(clean_key, "")


def _resolve_supplier_order(
    session: requests.Session,
    doc: dict,
    supplier_name_cache: dict[str, str],
    counterparty_name_cache: dict[str, str],
    order_cache: dict[tuple[str, str], dict | None],
) -> tuple[str, str]:
    basis_key = _clean_text(doc.get("ДокументОснование"))
    if not basis_key or basis_key == EMPTY_GUID:
        return "", ""

    basis_entity = _odata_entity_from_type(doc.get("ДокументОснование_Type"))
    candidates = [basis_entity] if basis_entity else []
    for entity in ("Document_ЗаказПоставщику", "Document_ТД_ЗаявкаПоставщику"):
        if entity not in candidates:
            candidates.append(entity)

    basis_doc = None
    for entity in candidates:
        if not entity:
            continue
        cache_key = (entity, basis_key)
        if cache_key not in order_cache:
            order_cache[cache_key] = _fetch_entity_object(session, entity, basis_key)
        basis_doc = order_cache.get(cache_key)
        if basis_doc:
            break

    if not basis_doc:
        return basis_key, ""

    supplier_key = _first_guid(
        basis_doc,
        ("Поставщик_Key", "Партнер_Key", "Контрагент_Key"),
    )
    counterparty_key = _first_guid(basis_doc, ("Контрагент_Key",))
    supplier_name = ""
    if supplier_key:
        supplier_name = _resolve_ref_name(session, supplier_name_cache, PARTNER_ENTITY, supplier_key)
    if (not supplier_name or supplier_name == supplier_key) and (counterparty_key or supplier_key):
        supplier_name = _resolve_ref_name(
            session,
            counterparty_name_cache,
            COUNTERPARTY_ENTITY,
            counterparty_key or supplier_key,
        )

    return _display_number(basis_doc) or basis_key, supplier_name


def _load_target_rows(session: requests.Session) -> list[dict]:
    select_fields = (
        "Ref_Key,LineNumber,Поступило,Проверено,СоответствуетНТД,НеСоответствуетНТД,"
        "РасчетноеКоличествоБракаВПартии,ФактическийБрак,"
        "ВозможностьУстраненияНесоответствия,КатегорияПоПричинеВозникновения,"
        "ПодразделениеВиновник_Key,СотрудникДопустившийБрак_Key"
    )
    row_filter = (
        f"КатегорияПоПричинеВозникновения eq '{TARGET_REASON_CATEGORY}' "
        f"and ВозможностьУстраненияНесоответствия eq '{TARGET_RESOLUTION}'"
    )

    result: list[dict] = []
    skip = 0
    while True:
        query = (
            "$format=json"
            f"&$top=5000&$skip={skip}"
            f"&$select={quote(select_fields, safe=',_')}"
            f"&$filter={quote(row_filter, safe='')}"
        )
        rows = _odata_get(session, ROWS_ENTITY, query)
        if not rows:
            break
        result.extend(rows)
        if len(rows) < 5000:
            break
        skip += len(rows)
    return result


def _load_production_target_rows(session: requests.Session) -> list[dict]:
    select_fields = (
        "Ref_Key,LineNumber,Поступило,Проверено,СоответствуетНТД,НеСоответствуетНТД,"
        "РасчетноеКоличествоБракаВПартии,ФактическийБрак,"
        "ВозможностьУстраненияНесоответствия,КатегорияПоПричинеВозникновения,"
        "ПодразделениеВиновник_Key,СотрудникДопустившийБрак_Key"
    )
    culprit_filter = " or ".join(
        f"ПодразделениеВиновник_Key eq guid'{key}'"
        for key in PRODUCTION_CULPRIT_DEPARTMENTS
    )

    result: list[dict] = []
    skip = 0
    while True:
        query = (
            "$format=json"
            f"&$top=5000&$skip={skip}"
            f"&$select={quote(select_fields, safe=',_')}"
            f"&$filter={quote(culprit_filter, safe='')}"
        )
        rows = _odata_get(session, ROWS_ENTITY, query)
        if not rows:
            break
        result.extend(rows)
        if len(rows) < 5000:
            break
        skip += len(rows)
    return result


def _build_rows_from_target_rows(
    session: requests.Session,
    target_rows: list[dict],
    period_start: datetime,
    period_end: datetime,
) -> list[dict]:
    if not target_rows:
        return []

    doc_keys = sorted({str(row.get("Ref_Key") or "").strip() for row in target_rows if row.get("Ref_Key")})
    docs: dict[str, dict] = {}
    doc_select = (
        "Ref_Key,Number,Date,DeletionMark,Posted,Номенклатура_Key,ДокументОснование,ДокументОснование_Type,"
        "Состояние,СрокИсполнения,ВыявленныеНесоответствия,Комментарий,"
        "ОписаниеНесоответствия,Тип,Статус"
    )
    for key in doc_keys:
        doc = _fetch_single(session, DOC_ENTITY, key, doc_select)
        if doc:
            docs[key] = doc

    nomenclature_cache: dict[str, str] = {}
    department_cache: dict[str, str] = {}
    supplier_name_cache: dict[str, str] = {}
    counterparty_name_cache: dict[str, str] = {}
    order_cache: dict[tuple[str, str], dict | None] = {}

    result: list[dict] = []
    for row in target_rows:
        doc_key = str(row.get("Ref_Key") or "").strip()
        doc = docs.get(doc_key) or {}
        if doc.get("DeletionMark") is True:
            continue

        doc_date = _parse_odata_datetime(doc.get("Date"))
        if doc_date is None or doc_date < period_start or doc_date >= period_end:
            continue

        nomenclature_key = str(doc.get("Номенклатура_Key") or "").strip()
        if nomenclature_key and nomenclature_key != EMPTY_GUID and nomenclature_key not in nomenclature_cache:
            nomenclature_cache[nomenclature_key] = _format_ref_name(
                _fetch_single(session, NOMENCLATURE_ENTITY, nomenclature_key, "Ref_Key,Description,Code"),
                nomenclature_key,
            )

        dept_key = str(row.get("ПодразделениеВиновник_Key") or "").strip()
        if dept_key and dept_key != EMPTY_GUID and dept_key not in department_cache:
            department_cache[dept_key] = _format_ref_name(
                _fetch_single(session, DEPARTMENT_ENTITY, dept_key, "Ref_Key,Description"),
                dept_key,
            )

        description = (
            _clean_text(doc.get("ОписаниеНесоответствия"))
            or _clean_text(doc.get("ВыявленныеНесоответствия"))
            or _clean_text(doc.get("Комментарий"))
        )
        status = _clean_text(doc.get("Статус")) or _clean_text(doc.get("Состояние"))
        supplier_order_number, supplier = _resolve_supplier_order(
            session,
            doc,
            supplier_name_cache,
            counterparty_name_cache,
            order_cache,
        )

        result.append({
            "code": _clean_text(doc.get("Number")),
            "name": "Акт о несоответствии приборов и комплектующих",
            "date_reg": str(doc.get("Date") or "")[:10],
            "date_plan": str(doc.get("СрокИсполнения") or "")[:10],
            "order_num": _clean_text(doc.get("ДокументОснование")),
            "supplier_order_number": supplier_order_number,
            "supplier": supplier,
            "order_dept": department_cache.get(dept_key, "" if dept_key in {"", EMPTY_GUID} else dept_key),
            "culprit_dept_key": dept_key,
            "nomenclature": nomenclature_cache.get(
                nomenclature_key,
                "" if nomenclature_key in {"", EMPTY_GUID} else nomenclature_key,
            ),
            "description": description,
            "status": status,
            "posted": bool(doc.get("Posted")),
            "line_number": row.get("LineNumber"),
            "reason_category": _clean_text(row.get("КатегорияПоПричинеВозникновения")),
            "resolution": _clean_text(row.get("ВозможностьУстраненияНесоответствия")),
            "received_qty": row.get("Поступило"),
            "checked_qty": row.get("Проверено"),
            "not_match_qty": row.get("НеСоответствуетНТД"),
            "calculated_defect_qty": row.get("РасчетноеКоличествоБракаВПартии"),
            "actual_defect_pct": row.get("ФактическийБрак"),
            "source_document": DOC_ENTITY,
            "source_rows_entity": ROWS_ENTITY,
        })

    return sorted(result, key=lambda item: (item.get("date_reg") or "", item.get("code") or ""))


def _fetch_from_odata(year: int, month: int) -> list[dict]:
    period_start, period_end = _month_bounds(year, month)
    session = requests.Session()
    session.auth = AUTH
    return _build_rows_from_target_rows(session, _load_target_rows(session), period_start, period_end)


def _fetch_production_from_odata(year: int, month: int) -> list[dict]:
    period_start, period_end = _month_bounds(year, month)
    session = requests.Session()
    session.auth = AUTH
    rows = _build_rows_from_target_rows(
        session,
        _load_production_target_rows(session),
        period_start,
        period_end,
    )
    return [
        {
            **row,
            "production_claim": True,
            "name": "Претензия на стороне производства",
            "order_dept": PRODUCTION_CULPRIT_DEPARTMENTS.get(str(row.get("culprit_dept_key") or "").strip(), row.get("order_dept")),
            "source": "Document_ТД_АктОНесоответствиеПриборовИКомплектующих",
        }
        for row in rows
        if row.get("culprit_dept_key") in PRODUCTION_CULPRIT_DEPARTMENTS
    ]


def _infer_production_culprit_from_claim(row: dict) -> tuple[str, str]:
    dept_key = str(row.get("ВиновноеПодразделение_Key") or "").strip()
    if dept_key in PRODUCTION_CULPRIT_DEPARTMENTS:
        return dept_key, PRODUCTION_CULPRIT_DEPARTMENTS[dept_key]

    direction = _clean_text(row.get("ТД_ПлательщикНаправление")).lower()
    compact = direction.replace(" ", "").replace("-", "")
    if "производство1" in compact or "цех1" in compact or "пц1" in compact:
        return "f12f2fca-d5d2-11e7-8267-ac1f6b05524d", "ТурбулентностьДОНПроизводство1"
    if (
        "производство2" in compact
        or "цех2" in compact
        or "пц2" in compact
        or "алмаз" in compact
    ):
        return "3a9ac2f2-214f-11e0-b91c-00248c26ee57", "Алмаз"

    return "", ""


def _fetch_production_catalog_claims_from_odata(year: int, month: int) -> list[dict]:
    period_start, period_end = _month_bounds(year, month)
    session = requests.Session()
    session.auth = AUTH
    select_fields = (
        "Ref_Key,Code,Description,ДатаРегистрации,ДатаОкончания,ТД_ДатаОкончанияПлан,"
        "Статус,ВиновноеПодразделение_Key,ТД_Номенклатура_Key,ОписаниеПретензии,"
        "ПричинаВозникновения_Key,ТД_ПлательщикНаправление,DeletionMark"
    )
    period_filter = (
        f"ДатаРегистрации ge datetime'{period_start.strftime('%Y-%m-%dT%H:%M:%S')}' "
        f"and ДатаРегистрации lt datetime'{period_end.strftime('%Y-%m-%dT%H:%M:%S')}'"
    )

    rows: list[dict] = []
    skip = 0
    while True:
        query = (
            "$format=json"
            f"&$top=5000&$skip={skip}"
            f"&$select={quote(select_fields, safe=',_')}"
            f"&$filter={quote(period_filter, safe='')}"
        )
        chunk = _odata_get(session, CLAIMS_ENTITY, query)
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < 5000:
            break
        skip += len(chunk)

    rows = [
        row
        for row in rows
        if row.get("DeletionMark") is not True
        and _clean_text(row.get("ПричинаВозникновения_Key")) == PRODUCTION_CLAIM_REASON_PRETENSION
    ]

    nomenclature_cache: dict[str, str] = {}
    result: list[dict] = []
    for row in rows:
        dept_key, dept_name = _infer_production_culprit_from_claim(row)
        if not dept_name:
            continue

        nomenclature_key = str(row.get("ТД_Номенклатура_Key") or "").strip()
        if nomenclature_key and nomenclature_key != EMPTY_GUID and nomenclature_key not in nomenclature_cache:
            nomenclature_cache[nomenclature_key] = _format_ref_name(
                _fetch_single(session, NOMENCLATURE_ENTITY, nomenclature_key, "Ref_Key,Description,Code"),
                nomenclature_key,
            )

        result.append({
            "code": _clean_text(row.get("Code")),
            "name": _clean_text(row.get("Description")) or "Претензия",
            "date_reg": str(row.get("ДатаРегистрации") or "")[:10],
            "date_plan": str(row.get("ТД_ДатаОкончанияПлан") or "")[:10],
            "date_end": str(row.get("ДатаОкончания") or "")[:10],
            "order_dept": dept_name,
            "culprit_dept_key": dept_key,
            "nomenclature": nomenclature_cache.get(
                nomenclature_key,
                "" if nomenclature_key in {"", EMPTY_GUID} else nomenclature_key,
            ),
            "description": _clean_text(row.get("ОписаниеПретензии")),
            "status": _clean_text(row.get("Статус")),
            "reason_category": _clean_text(row.get("ПричинаВозникновения_Key")),
            "calculated_defect_qty": None,
            "production_claim": True,
            "source": "Catalog_Претензии",
        })

    return sorted(result, key=lambda item: (item.get("date_reg") or "", item.get("code") or ""))


def fetch_logistics_claims_for_month(year: int, month: int) -> list[dict]:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached

    try:
        rows = _fetch_from_odata(year, month)
    except Exception:
        logger.exception("LOG claims: failed to fetch rows for %s-%02d", year, month)
        rows = []

    _save_cache(year, month, rows)
    return rows


def fetch_production_claims_for_month(year: int, month: int) -> list[dict]:
    cached = _load_production_cache(year, month)
    if cached is not None:
        return cached

    try:
        rows = _fetch_production_catalog_claims_from_odata(year, month)
    except Exception:
        logger.exception("Production claims: failed to fetch rows for %s-%02d", year, month)
        rows = []

    _save_production_cache(year, month, rows)
    return rows


def fetch_production_claims_for_period(year: int, months: list[int]) -> list[dict]:
    out: list[dict] = []
    for month in sorted({int(m) for m in months if 1 <= int(m) <= 12}):
        out.extend(fetch_production_claims_for_month(year, month))
    return sorted(out, key=lambda item: (item.get("date_reg") or "", item.get("code") or ""))
