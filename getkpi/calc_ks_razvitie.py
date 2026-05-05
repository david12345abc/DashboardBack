"""
calc_ks_razvitie.py — показатели блока «КС развитие» (ТД).

Источник: Document_ТД_КСРазвитие с табличной частью «Показатели».
Каждый документ = один показатель × одно подразделение × 12 месяцев.
Единица измерения берётся из реквизита документа «ЕдИзмерения».

По ТЗ: для каждой пары (подразделение × показатель) выдаём помесячный ряд
{ plan, fact }. Большинство фактов в 1С не ведётся, но для диаграммы
«Новые заказчики по услугам» факт рассчитывается по реализациям.

Фильтрация по подразделению выполняется уже на уровне дашборда
(см. `by_dept_guid` + поле `charts` на срезе отдела). Модуль отдаёт:

{
  "year": 2026,
  "indicators": ["Развитие имеющихся дилеров", "Новые дилеры", ...],
  "dept_indicators": {
    "Отдел дилерских продаж": ["Развитие имеющихся дилеров", "Новые дилеры"],
    ...
  },
  # Агрегат (сумма) по всем подразделениям — для КД / ПСД коммерческого блока.
  "months": {
    "1":  {"Развитие имеющихся дилеров": {"plan": 0, "fact": 0}, ...},
    ...
    "12": {...}
  },
  # Плоский список круговых диаграмм (по паре отдел × показатель) — удобен
  # фронту: один элемент = одна диаграмма с 12 месячными точками.
  "charts": [
    {
      "dept_name": "Отдел дилерских продаж",
      "dept_guid": "7587...",
      "indicator": "Развитие имеющихся дилеров",
      "unit": "шт.",
      "months": [
        {"month": 1, "month_name": "январь", "plan": 0, "fact": 0, "unit": "шт."},
        ...
      ]
    },
    ...
  ],
  # Детализация по подразделению (имя → {indicators, months, charts}).
  "by_dept": {
    "Отдел дилерских продаж": {
      "dept_name": "Отдел дилерских продаж",
      "dept_guid": "7587...",
      "indicators": [...],
      "months": {"1": {...}, ..., "12": {...}},
      "charts": [ {...}, ... ]
    },
    ...
  },
  # Тот же срез, но ключ — GUID подразделения (для точной фильтрации).
  "by_dept_guid": {
    "7587...": {...}
  }
}

Кэшируется на день: dashboard/ks_razvitie_<year>.json.
"""
from __future__ import annotations

import json
import logging
from datetime import date
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

# Подразделения, считающиеся «коммерческим блоком» (дети коммерческого директора).
ALLOWED_DEPARTMENTS: dict[str, str] = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборудования и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО «Газпром»",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_VERSION = "ks_razvitie_units_v4_project_bookmarks_fact"
TARGET_SERVICE_CUSTOMERS_DEPT_KEY = "34497ef7-810f-11e4-80d6-001e67112509"
TARGET_SERVICE_CUSTOMERS_DEPT_NAME = "Отдел продаж эталонного оборудования и услуг"
TARGET_SERVICE_CUSTOMERS_INDICATOR = "Новые заказчики по услугам"
PROJECT_BOOKMARKS_INDICATOR = "Закладки в проекты"
PROJECT_BOOKMARKS_DOC = "Document_КоммерческоеПредложениеКлиенту"
PROJECT_BOOKMARK_DEPARTMENTS: dict[str, str] = {
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО «Газпром»",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
}
SERVICE_WORK_TYPES = {"услуга", "работа"}
ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"
REALIZATION_DOC = "Document_РеализацияТоваровУслуг"
FACT_HISTORY_START = "2000-01-01T00:00:00"

# Возможные варианты имени EntitySet в OData и имени таб.части.
DOC_ENTITY_CANDIDATES = [
    "Document_ТД_КСРазвитие",
    "Document_ТД_КСРазвитие_Показатели",  # табличная часть как отдельный набор
]

TAB_ENTITY_CANDIDATES = [
    "Document_ТД_КСРазвитие_Показатели",
    "Document_ТД_КСРазвитие_ПоказателиКСРазвития",
]

_DISCOVERED_DOC: str | None = None
_DISCOVERED_TAB: str | None = None


def _cache_path(year: int) -> Path:
    return CACHE_DIR / f"ks_razvitie_{year}.json"


def _load_cache(year: int) -> dict | None:
    path = _cache_path(year)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("source_tag") != CACHE_VERSION:
        return None
    # Текущий год — перепроверяем на дневной кэш, прошлые годы — кэшируем без TTL.
    today = date.today()
    if int(data.get("year") or 0) == today.year and data.get("cached_at") != today.isoformat():
        return None
    return data


def _save_cache(year: int, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {**payload, "cached_at": date.today().isoformat(), "source_tag": CACHE_VERSION}
    try:
        with open(_cache_path(year), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _discover_entities(session: requests.Session) -> tuple[str, str]:
    """Находит реальные имена EntitySet для документа и его таб.части."""
    global _DISCOVERED_DOC, _DISCOVERED_TAB
    if _DISCOVERED_DOC and _DISCOVERED_TAB:
        return _DISCOVERED_DOC, _DISCOVERED_TAB

    try:
        r = session.get(f"{BASE}/$metadata", timeout=30)
        if r.ok and r.text:
            text = r.text
            import re
            pat_any = re.compile(
                r'EntitySet\s+Name="(Document_[^"]*\u041a\u0421\u0420\u0430\u0437\u0432\u0438\u0442\u0438\u0435[^"]*)"',
                re.IGNORECASE,
            )
            names = pat_any.findall(text)
            # Разделяем: без «_Показатели»/«_…» — это сам документ, с ним — таб.часть.
            doc = None
            tab = None
            for name in names:
                if "_Показатели" in name or name.endswith("_ПоказателиКСРазвития"):
                    tab = tab or name
                else:
                    doc = doc or name
            if doc and tab:
                _DISCOVERED_DOC, _DISCOVERED_TAB = doc, tab
                return doc, tab
            if doc and not tab:
                _DISCOVERED_DOC = doc
                tab = doc + "_Показатели"
                _DISCOVERED_TAB = tab
                return doc, tab
    except Exception as exc:
        logger.warning("ks_razvitie: metadata discover error: %s", exc)

    # Фоллбэк — пробуем перебор.
    for name in DOC_ENTITY_CANDIDATES:
        try:
            r = session.get(f"{BASE}/{quote(name)}?$top=1&$format=json", timeout=15)
            if r.ok:
                _DISCOVERED_DOC = name
                break
        except Exception:
            continue
    for name in TAB_ENTITY_CANDIDATES:
        try:
            r = session.get(f"{BASE}/{quote(name)}?$top=1&$format=json", timeout=15)
            if r.ok:
                _DISCOVERED_TAB = name
                break
        except Exception:
            continue
    return _DISCOVERED_DOC or DOC_ENTITY_CANDIDATES[0], _DISCOVERED_TAB or TAB_ENTITY_CANDIDATES[0]


def _fetch_documents(session: requests.Session, doc_entity: str) -> list[dict]:
    """Загружает шапки документов Document_ТД_КСРазвитие.

    401/403/404/5xx/timeout обрабатываются через request_with_retry
    (экспоненциальный backoff, несколько попыток). Если после всех попыток
    не получилось — возвращаем пустой список и пишем warning, чтобы
    дашборд собрался без этого блока.
    """
    select = "Ref_Key,Number,Date,Posted,DeletionMark,Подразделение_Key,ЕдИзмерения"
    docs: list[dict] = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(doc_entity)}?$format=json"
            f"&$select={quote(select, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=5, label="ks_razvitie/docs")
        if r is None:
            logger.warning(
                "ks_razvitie: docs request dropped after retries for %s — "
                "блок «КС развитие» не будет подтянут.", doc_entity,
            )
            return []
        if r.status_code in (401, 403):
            logger.warning(
                "ks_razvitie: access denied to %s (HTTP %d) даже после повторов. "
                "Если права в 1С есть — скорее всего 1С под пиковой нагрузкой. "
                "Повторный запрос чуть позже должен помочь.",
                doc_entity, r.status_code,
            )
            return []
        if r.status_code == 404:
            logger.warning(
                "ks_razvitie: entity %s not visible (HTTP 404) — в 1С OData так "
                "иногда маскируется access denied.", doc_entity,
            )
            return []
        if not r.ok:
            logger.error("ks_razvitie: docs HTTP %d: %s", r.status_code, r.text[:300])
            return []
        rows = r.json().get("value", [])
        docs.extend(rows)
        if len(rows) < PAGE:
            break
        skip += PAGE
    return [d for d in docs if not d.get("DeletionMark") and d.get("Posted")]


def _fetch_tab_rows(session: requests.Session, tab_entity: str) -> list[dict]:
    """Табличная часть «Показатели» у документов Document_ТД_КСРазвитие.

    `Показатель` — ссылка на Catalog_ТД_ПоказателиРазвитияКС; используем $expand,
    чтобы сразу получить `Description` (название показателя).
    """
    rows: list[dict] = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(tab_entity)}?$format=json"
            f"&$expand={quote('Показатель', safe='')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=5, label="ks_razvitie/tab")
        if r is None:
            logger.warning("ks_razvitie: tab request dropped after retries for %s", tab_entity)
            return []
        if r.status_code in (401, 403, 404):
            logger.warning(
                "ks_razvitie: tab %s not accessible (HTTP %d) даже после повторов.",
                tab_entity, r.status_code,
            )
            return []
        if not r.ok:
            logger.error("ks_razvitie: tab HTTP %d: %s", r.status_code, r.text[:300])
            return []
        chunk = r.json().get("value", [])
        rows.extend(chunk)
        if len(chunk) < PAGE:
            break
        skip += PAGE
    return rows


def _fetch_all_json(session: requests.Session, url: str, *, label: str, page: int = 5000) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    sep = "&" if "?" in url else "?"
    while True:
        r = request_with_retry(
            session,
            f"{url}{sep}$top={page}&$skip={skip}",
            timeout=120,
            retries=4,
            label=label,
        )
        if r is None or not r.ok:
            if r is not None:
                logger.warning("%s HTTP %s: %s", label, r.status_code, r.text[:250])
            return rows
        batch = r.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        skip += page


def _fetch_by_refs(
    session: requests.Session,
    entity: str,
    refs: set[str],
    select: str,
    *,
    label: str,
    ref_field: str = "Ref_Key",
    batch_size: int = 25,
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    ref_list = sorted(ref for ref in refs if ref and ref != EMPTY)
    for i in range(0, len(ref_list), batch_size):
        batch = ref_list[i:i + batch_size]
        flt = " or ".join(f"{ref_field} eq guid'{ref}'" for ref in batch)
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        for row in _fetch_all_json(session, url, label=label, page=batch_size):
            key = row.get(ref_field)
            if key:
                out[key] = row
    return out


def _fetch_table_by_refs(
    session: requests.Session,
    entity: str,
    refs: set[str],
    select: str,
    *,
    label: str,
    batch_size: int = 25,
) -> list[dict]:
    rows: list[dict] = []
    ref_list = sorted(ref for ref in refs if ref and ref != EMPTY)
    for i in range(0, len(ref_list), batch_size):
        batch = ref_list[i:i + batch_size]
        flt = " or ".join(f"Ref_Key eq guid'{ref}'" for ref in batch)
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        rows.extend(_fetch_all_json(session, url, label=label))
    return rows


def _normalize_text(value: object) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in raw).split())


def _load_service_work_order_keys(session: requests.Session, order_keys: set[str]) -> set[str]:
    lines = _fetch_table_by_refs(
        session,
        "Document_ЗаказКлиента_Товары",
        order_keys,
        "Ref_Key,Номенклатура_Key,Отменено",
        label="ks_razvitie/order_lines",
    )
    nomenclature_keys = {
        row.get("Номенклатура_Key")
        for row in lines
        if not row.get("Отменено") and row.get("Номенклатура_Key")
    }
    nomenclature = _fetch_by_refs(
        session,
        "Catalog_Номенклатура",
        set(nomenclature_keys),
        "Ref_Key,ТипНоменклатуры",
        label="ks_razvitie/nomenclature",
    )
    service_nomenclature = {
        key for key, row in nomenclature.items()
        if _normalize_text(row.get("ТипНоменклатуры")) in SERVICE_WORK_TYPES
    }
    service_orders: set[str] = set()
    for row in lines:
        if row.get("Отменено"):
            continue
        if row.get("Номенклатура_Key") in service_nomenclature:
            service_orders.add(row.get("Ref_Key"))
    return service_orders


def _load_realizations(
    session: requests.Session,
    date_from: str,
    date_to: str,
    *,
    partner_keys: set[str] | None = None,
) -> list[dict]:
    select = (
        "Ref_Key,Date,Number,Подразделение_Key,Партнер_Key,"
        "ЗаказКлиента,ЗаказКлиента_Type"
    )
    base_filter = (
        f"Date ge datetime'{date_from}' and Date lt datetime'{date_to}' "
        f"and Posted eq true and DeletionMark eq false "
        f"and Подразделение_Key eq guid'{TARGET_SERVICE_CUSTOMERS_DEPT_KEY}'"
    )
    if not partner_keys:
        url = (
            f"{BASE}/{REALIZATION_DOC}?$format=json"
            f"&$filter={quote(base_filter, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        return _fetch_all_json(session, url, label="ks_razvitie/service_realizations")

    rows: list[dict] = []
    keys = sorted(k for k in partner_keys if k and k != EMPTY)
    for i in range(0, len(keys), 25):
        batch = keys[i:i + 25]
        partner_filter = " or ".join(f"Партнер_Key eq guid'{key}'" for key in batch)
        flt = f"{base_filter} and ({partner_filter})"
        url = (
            f"{BASE}/{REALIZATION_DOC}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        rows.extend(_fetch_all_json(session, url, label="ks_razvitie/service_realizations_by_partner"))
    return rows


def _service_realization_events(session: requests.Session, shipments: list[dict]) -> list[tuple[str, int, int]]:
    order_keys = {
        row["ЗаказКлиента"]
        for row in shipments
        if row.get("ЗаказКлиента_Type") == ORDER_TYPE and row.get("ЗаказКлиента")
    }
    if not order_keys:
        return []

    orders = _fetch_by_refs(
        session,
        "Document_ЗаказКлиента",
        order_keys,
        "Ref_Key,Подразделение_Key,Партнер_Key,ТД_НеУчитыватьВПланФакте",
        label="ks_razvitie/orders",
    )
    target_order_keys = {
        key for key, order in orders.items()
        if (
            normalize_commercial_dept_guid(order.get("Подразделение_Key") or "")
            == TARGET_SERVICE_CUSTOMERS_DEPT_KEY
            and not order.get("ТД_НеУчитыватьВПланФакте")
            and order.get("Партнер_Key") not in ("", EMPTY, None)
        )
    }
    service_orders = _load_service_work_order_keys(session, target_order_keys)

    events: list[tuple[str, int, int]] = []
    for shipment in shipments:
        order_key = shipment.get("ЗаказКлиента")
        if order_key not in service_orders:
            continue
        order = orders.get(order_key)
        if not order:
            continue
        partner = order.get("Партнер_Key") or shipment.get("Партнер_Key") or ""
        if not partner or partner == EMPTY:
            continue
        period = str(shipment.get("Date") or "")
        if len(period) < 7:
            continue
        try:
            y = int(period[:4])
            m = int(period[5:7])
        except ValueError:
            continue
        if y >= 1900 and 1 <= m <= 12:
            events.append((partner, y, m))
    return events


def _load_new_service_customers_fact(session: requests.Session, year: int) -> dict:
    """Факт для «Новые заказчики по услугам».

    Заказчик: Document_ЗаказКлиента.Партнер_Key.
    Отгрузка/реализация: проведённый Document_РеализацияТоваровУслуг
    по целевому подразделению. Период берём по Date документа реализации.
    Услуга/Работа: Catalog_Номенклатура.ТипНоменклатуры в строках заказа.
    """
    year_start = f"{int(year)}-01-01T00:00:00"
    year_end = f"{int(year) + 1}-01-01T00:00:00"
    current_shipments = _load_realizations(session, year_start, year_end)
    current_events = _service_realization_events(session, current_shipments)

    candidate_partners = {partner for partner, _y, _m in current_events}
    if candidate_partners:
        history_shipments = _load_realizations(
            session,
            FACT_HISTORY_START,
            year_end,
            partner_keys=candidate_partners,
        )
        history_events = _service_realization_events(session, history_shipments)
    else:
        history_shipments = []
        history_events = []

    first_month_by_partner: dict[str, tuple[int, int]] = {}
    for partner, y, m in history_events:
        current = first_month_by_partner.get(partner)
        if current is None or (y, m) < current:
            first_month_by_partner[partner] = (y, m)

    facts = {m: 0 for m in range(1, 13)}
    for y, m in first_month_by_partner.values():
        if y == int(year):
            facts[m] += 1
    return {
        "facts_by_month": facts,
        "partners_total": len(first_month_by_partner),
        "current_shipment_rows": len(current_shipments),
        "current_service_events": len(current_events),
        "candidate_partners": len(candidate_partners),
        "history_shipment_rows": len(history_shipments),
        "history_service_events": len(history_events),
    }


def _load_manager_departments(session: requests.Session, manager_keys: set[str]) -> dict[str, str]:
    managers = _fetch_by_refs(
        session,
        "Catalog_Пользователи",
        manager_keys,
        "Ref_Key,Подразделение_Key",
        label="ks_razvitie/managers",
    )
    return {
        key: normalize_commercial_dept_guid(row.get("Подразделение_Key") or "")
        for key, row in managers.items()
    }


def _load_project_bookmarks_fact(session: requests.Session, year: int) -> dict:
    """Факт для «Закладки в проекты».

    Документ: проведённый Document_КоммерческоеПредложениеКлиенту.
    Период: Date документа КП.
    Отбор: ТД_ПроектнаяЗакладка = true.
    Подразделение: Catalog_Пользователи.Подразделение_Key по Менеджер_Key.
    """
    year_start = f"{int(year)}-01-01T00:00:00"
    year_end = f"{int(year) + 1}-01-01T00:00:00"
    flt = (
        f"Date ge datetime'{year_start}' and Date lt datetime'{year_end}' "
        f"and Posted eq true and DeletionMark eq false "
        f"and ТД_ПроектнаяЗакладка eq true"
    )
    select = "Ref_Key,Date,Number,Менеджер_Key,ТД_ПроектнаяЗакладка"
    url = (
        f"{BASE}/{PROJECT_BOOKMARKS_DOC}?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(select, safe=',_')}"
    )
    docs = _fetch_all_json(session, url, label="ks_razvitie/project_bookmarks")
    manager_keys = {
        row.get("Менеджер_Key")
        for row in docs
        if row.get("Менеджер_Key") and row.get("Менеджер_Key") != EMPTY
    }
    manager_depts = _load_manager_departments(session, set(manager_keys))

    facts_by_dept = {
        dept_key: {m: 0 for m in range(1, 13)}
        for dept_key in PROJECT_BOOKMARK_DEPARTMENTS
    }
    docs_by_dept = {dept_key: 0 for dept_key in PROJECT_BOOKMARK_DEPARTMENTS}
    skipped_no_manager_dept = 0
    for row in docs:
        manager = row.get("Менеджер_Key") or ""
        dept_key = manager_depts.get(manager)
        if not dept_key:
            skipped_no_manager_dept += 1
            continue
        if dept_key not in facts_by_dept:
            continue
        period = str(row.get("Date") or "")
        if len(period) < 7:
            continue
        try:
            month = int(period[5:7])
        except ValueError:
            continue
        if not (1 <= month <= 12):
            continue
        facts_by_dept[dept_key][month] += 1
        docs_by_dept[dept_key] += 1

    return {
        "facts_by_dept": facts_by_dept,
        "docs_total": len(docs),
        "managers_loaded": len(manager_depts),
        "docs_by_dept": docs_by_dept,
        "skipped_no_manager_dept": skipped_no_manager_dept,
    }


def _find_or_create_indicator_key(
    by_dept_agg: dict[str, dict[str, dict[str, float]]],
    indicator_labels: dict[str, str],
    unit_by_dept_indicator: dict[tuple[str, str], str],
    *,
    dept_name: str = TARGET_SERVICE_CUSTOMERS_DEPT_NAME,
    indicator: str = TARGET_SERVICE_CUSTOMERS_INDICATOR,
    unit: str = "шт.",
) -> str:
    target_norm = _normalize_text(indicator)
    for ind_key, label in indicator_labels.items():
        if _normalize_text(label) != target_norm:
            continue
        dept_months = by_dept_agg.get(dept_name) or {}
        if any(ind_key in month_map for month_map in dept_months.values()):
            return ind_key
    ind_key = f"{indicator}@@unit:{unit}"
    indicator_labels[ind_key] = indicator
    unit_by_dept_indicator[(dept_name, ind_key)] = unit
    return ind_key


def _parse_month(value) -> int | None:
    """Месяц из поля «Месяц» (может быть строкой-датой или datetime-строкой 1С)."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Форматы 1С: '2026-01-01T00:00:00' или '0001-01-01T00:00:00' или '2026-01'
    try:
        if "T" in s:
            return int(s[5:7])
        if "-" in s:
            return int(s[5:7])
    except (ValueError, IndexError):
        pass
    return None


def _parse_plan(value) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _unit_from_1c(value) -> str:
    """Enum документа ТД_КСРазвитие: 0=руб., 1=шт., 2=%."""
    try:
        code = int(value)
    except (TypeError, ValueError):
        return ""
    return {0: "руб.", 1: "шт.", 2: "%"}.get(code, "")


def _merge_units(left: str | None, right: str | None) -> str:
    units = []
    for value in (left, right):
        for part in str(value or "").split("/"):
            part = part.strip()
            if part and part not in units:
                units.append(part)
    return "/".join(units)


MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _pf(plan: float, fact: float = 0.0, unit: str = "") -> dict:
    """Ячейка круговой диаграммы: две цифры — план и факт (%факт пока всегда 0)."""
    return {"plan": round(float(plan), 4), "fact": round(float(fact), 4), "unit": unit or ""}


def _build_empty_months(indicators: list[str]) -> dict[str, dict[str, dict[str, float]]]:
    """12 месяцев × все показатели = пары {plan: 0, fact: 0}."""
    return {
        str(m): {ind: _pf(0.0) for ind in indicators}
        for m in range(1, 13)
    }


def _months_to_chart(indicator: str,
                     months_map: dict,
                     unit: str = "",
                     indicator_label: str | None = None) -> dict:
    """Массив {month, month_name, plan, fact} для одной круговой диаграммы."""
    series = []
    for m in range(1, 13):
        cell = (months_map or {}).get(str(m), {}).get(indicator) or {}
        plan = float(cell.get("plan") or 0.0)
        fact = float(cell.get("fact") or 0.0)
        point_unit = cell.get("unit") or unit or ""
        series.append({
            "month": m,
            "month_name": MONTH_NAMES_RU[m],
            "plan": round(plan, 4),
            "fact": round(fact, 4),
            "unit": point_unit,
        })
    return {"indicator": indicator_label or indicator, "unit": unit or "", "months": series}


def _fetch_from_odata(year: int) -> dict:
    """Загружает и агрегирует планы КС развитие за указанный год."""
    session = requests.Session()
    session.auth = AUTH

    doc_entity, tab_entity = _discover_entities(session)
    logger.info("ks_razvitie: doc=%s tab=%s year=%s", doc_entity, tab_entity, year)

    docs = _fetch_documents(session, doc_entity)
    if not docs:
        return {
            "year": int(year),
            "months": {},
            "by_dept": {},
            "indicators": [],
        }

    # Весь документ «КС развитие» предназначен для круговых диаграмм ПСД
    # коммерческого блока, поэтому ничего не фильтруем по подразделению —
    # берём все проведённые и не помеченные на удаление документы.
    # Подразделение резолвим через Catalog_СтруктураПредприятия, чтобы в by_dept
    # отображались читабельные имена, а не GUID.
    dept_keys_needed = {
        str(d.get("Подразделение_Key") or "").lower()
        for d in docs
        if str(d.get("Подразделение_Key") or "").lower() not in ("", EMPTY)
    }
    dept_names = _resolve_department_names(session, dept_keys_needed)

    # Запомним GUID → имя подразделения, чтобы потом собрать отдельный срез
    # by_dept_guid (для точной фильтрации по GUID в dashboard-слое).
    dept_name_by_key: dict[str, str] = {}

    docs_by_ref: dict[str, dict] = {}
    for d in docs:
        dept_key = str(d.get("Подразделение_Key") or "").lower()
        dept_name = (
            dept_names.get(dept_key)
            or ALLOWED_DEPARTMENTS.get(dept_key)
            or (dept_key[:8] if dept_key else "Без подразделения")
        )
        if dept_key:
            dept_name_by_key[dept_key] = dept_name
        docs_by_ref[str(d.get("Ref_Key") or "").lower()] = {
            "ref": str(d.get("Ref_Key") or "").lower(),
            "number": str(d.get("Number") or "").strip(),
            "date": str(d.get("Date") or "")[:10],
            "dept_key": dept_key,
            "dept_name": dept_name,
            "unit": _unit_from_1c(d.get("ЕдИзмерения")),
            "unit_code": d.get("ЕдИзмерения"),
        }

    tab_rows = _fetch_tab_rows(session, tab_entity)

    # indicators — множество всех диаграмм, собранных по документам коммерческого блока.
    # Если один и тот же показатель заведен с разными единицами измерения,
    # это разные диаграммы: складывать рубли, штуки и проценты нельзя.
    indicators_set: set[str] = set()
    indicator_labels: dict[str, str] = {}

    # Структура агрегатов: by_dept[dept_name][month][indicator] = sum(plan)
    by_dept_agg: dict[str, dict[str, dict[str, float]]] = {}
    unit_by_dept_indicator: dict[tuple[str, str], str] = {}

    for row in tab_rows:
        ref = str(row.get("Ref_Key") or "").lower()
        info = docs_by_ref.get(ref)
        if not info:
            continue
        month = _parse_month(row.get("Месяц"))
        if not month or not (1 <= month <= 12):
            continue

        # Год: значение «Месяц» (дата первого числа месяца) должно быть в выбранном году.
        month_raw = str(row.get("Месяц") or "")
        row_year = None
        if month_raw and "-" in month_raw:
            try:
                row_year = int(month_raw[:4])
            except ValueError:
                row_year = None
        if row_year is not None and row_year != int(year):
            continue

        # `Показатель` приходит развёрнутым: {Ref_Key, Description, ...}.
        # Если вдруг пришёл как строка (старые публикации) — тоже поддержим.
        indicator = ""
        ind_field = row.get("Показатель")
        if isinstance(ind_field, dict):
            indicator = str(ind_field.get("Description") or "").strip()
        elif isinstance(ind_field, str):
            indicator = ind_field.strip()
        if not indicator:
            continue
        dept_name = info["dept_name"]
        unit = str(info.get("unit") or "")
        chart_key = f"{indicator}@@unit:{unit}"
        indicator_labels[chart_key] = indicator
        plan = _parse_plan(row.get("План"))
        indicators_set.add(chart_key)

        unit_key = (dept_name, chart_key)
        prev_unit = unit_by_dept_indicator.get(unit_key)
        if prev_unit is None:
            unit_by_dept_indicator[unit_key] = unit
        elif prev_unit != unit:
            unit_by_dept_indicator[unit_key] = _merge_units(prev_unit, unit)
        dept_bucket = by_dept_agg.setdefault(dept_name, {})
        month_bucket = dept_bucket.setdefault(str(month), {})
        # Суммируем план, если в рамках отдела на один месяц/показатель
        # несколько документов; факт пока всегда 0.
        prev = month_bucket.get(chart_key) or {"plan": 0.0, "fact": 0.0}
        month_bucket[chart_key] = {
            "plan": round(prev.get("plan", 0.0) + plan, 4),
            "fact": round(prev.get("fact", 0.0), 4),
            "unit": unit_by_dept_indicator.get(unit_key, unit),
        }

    service_customers_debug: dict = {}
    try:
        service_fact = _load_new_service_customers_fact(session, int(year))
        service_customers_debug = {
            key: value
            for key, value in service_fact.items()
            if key != "facts_by_month"
        }
        indicator_key = _find_or_create_indicator_key(
            by_dept_agg,
            indicator_labels,
            unit_by_dept_indicator,
        )
        indicators_set.add(indicator_key)
        dept_bucket = by_dept_agg.setdefault(TARGET_SERVICE_CUSTOMERS_DEPT_NAME, {})
        unit_by_dept_indicator[(TARGET_SERVICE_CUSTOMERS_DEPT_NAME, indicator_key)] = "шт."
        for m, fact in (service_fact.get("facts_by_month") or {}).items():
            month_bucket = dept_bucket.setdefault(str(m), {})
            prev = month_bucket.get(indicator_key) or {"plan": 0.0, "fact": 0.0, "unit": "шт."}
            month_bucket[indicator_key] = {
                "plan": round(float(prev.get("plan") or 0.0), 4),
                "fact": round(float(fact or 0.0), 4),
                "unit": "шт.",
            }
    except Exception:
        logger.exception("ks_razvitie: failed to calculate new service customers fact")

    project_bookmarks_debug: dict = {}
    try:
        project_fact = _load_project_bookmarks_fact(session, int(year))
        project_bookmarks_debug = {
            key: value
            for key, value in project_fact.items()
            if key != "facts_by_dept"
        }
        for dept_key, dept_name in PROJECT_BOOKMARK_DEPARTMENTS.items():
            dept_name_by_key[dept_key] = dept_name
            indicator_key = _find_or_create_indicator_key(
                by_dept_agg,
                indicator_labels,
                unit_by_dept_indicator,
                dept_name=dept_name,
                indicator=PROJECT_BOOKMARKS_INDICATOR,
                unit="шт.",
            )
            indicators_set.add(indicator_key)
            dept_bucket = by_dept_agg.setdefault(dept_name, {})
            unit_by_dept_indicator[(dept_name, indicator_key)] = "шт."
            facts_by_month = (project_fact.get("facts_by_dept") or {}).get(dept_key) or {}
            for m, fact in facts_by_month.items():
                month_bucket = dept_bucket.setdefault(str(m), {})
                prev = month_bucket.get(indicator_key) or {"plan": 0.0, "fact": 0.0, "unit": "шт."}
                month_bucket[indicator_key] = {
                    "plan": round(float(prev.get("plan") or 0.0), 4),
                    "fact": round(float(fact or 0.0), 4),
                    "unit": "шт.",
                }
    except Exception:
        logger.exception("ks_razvitie: failed to calculate project bookmarks fact")

    indicators = sorted(indicators_set)

    # Индикаторы per-dept: только те, что реально есть в документах
    # конкретного подразделения (хотя бы один раз за год).
    dept_indicators: dict[str, list[str]] = {}
    indicator_units: dict[str, str] = {}
    for dept_name, month_map in by_dept_agg.items():
        present: set[str] = set()
        for ind_map in month_map.values():
            for ind_key in ind_map.keys():
                if ind_key:
                    present.add(ind_key)
                    unit = unit_by_dept_indicator.get((dept_name, ind_key), "")
                    prev_unit = indicator_units.get(ind_key)
                    if prev_unit is None:
                        indicator_units[ind_key] = unit
                    elif prev_unit != unit:
                        indicator_units[ind_key] = _merge_units(prev_unit, unit)
        dept_indicators[dept_name] = sorted(present)

    # by_dept: для каждого подразделения 12 месяцев × только его показатели.
    # Отсутствующие у подразделения показатели НЕ выводятся вовсе (это и есть
    # «нужный» перечень круговых диаграмм для этого подразделения).
    by_dept_full: dict[str, dict] = {}
    name_to_key = {name: key for key, name in dept_name_by_key.items()}
    for dept_name in sorted(by_dept_agg.keys()):
        dept_inds = dept_indicators.get(dept_name) or []
        base = {
            str(m): {
                ind_key: _pf(0.0, unit=unit_by_dept_indicator.get((dept_name, ind_key), ""))
                for ind_key in dept_inds
            }
            for m in range(1, 13)
        }
        dept_present = by_dept_agg.get(dept_name) or {}
        for m in range(1, 13):
            key = str(m)
            if key in dept_present:
                for ind_key in dept_inds:
                    cell = dept_present[key].get(ind_key)
                    if isinstance(cell, dict):
                        base[key][ind_key] = _pf(
                            cell.get("plan", 0.0),
                            cell.get("fact", 0.0),
                            cell.get("unit") or unit_by_dept_indicator.get((dept_name, ind_key), ""),
                        )
        dept_charts = [
            _months_to_chart(
                ind_key,
                base,
                unit_by_dept_indicator.get((dept_name, ind_key), ""),
                indicator_labels.get(ind_key, ind_key),
            )
            for ind_key in dept_inds
        ]
        # Обогащаем чарт контекстом отдела, чтобы было удобно рендерить.
        for ch in dept_charts:
            ch["dept_name"] = dept_name
            ch["dept_guid"] = name_to_key.get(dept_name, "")
        by_dept_full[dept_name] = {
            "dept_name": dept_name,
            "dept_guid": name_to_key.get(dept_name, ""),
            "indicators": dept_inds,
            "months": base,
            "charts": dept_charts,
        }

    # Общий агрегат (сумма по всем подразделениям) — для коммерческого директора
    # и ПСД коммерческого блока. Здесь — все показатели из всех документов.
    total: dict[str, dict[str, dict[str, float]]] = _build_empty_months(indicators)
    for dept_entry in by_dept_full.values():
        for m_key, ind_map in (dept_entry.get("months") or {}).items():
            for ind, pf in ind_map.items():
                prev = total[m_key].get(ind) or {"plan": 0.0, "fact": 0.0}
                total[m_key][ind] = {
                    "plan": round(prev.get("plan", 0.0) + float(pf.get("plan", 0.0)), 4),
                    "fact": round(prev.get("fact", 0.0) + float(pf.get("fact", 0.0)), 4),
                    "unit": indicator_units.get(ind, ""),
                }

    # GUID → отдельный срез (для точной фильтрации по dept_guid).
    by_dept_guid: dict[str, dict] = {}
    for dept_name, dept_entry in by_dept_full.items():
        key = dept_entry.get("dept_guid")
        if not key:
            continue
        by_dept_guid[key] = dept_entry

    # Плоский список всех диаграмм «отдел × показатель» — удобен фронту
    # для коммерческого директора / ПСД коммерческого блока, где нужно
    # показать сразу документы всех подразделений.
    flat_charts: list[dict] = []
    for dept_name in sorted(by_dept_full.keys()):
        for ch in (by_dept_full[dept_name].get("charts") or []):
            flat_charts.append(ch)

    display_indicator_units = {
        indicator_labels.get(key, key): unit
        for key, unit in indicator_units.items()
    }

    return {
        "year": int(year),
        "indicators": sorted({indicator_labels.get(key, key) for key in indicators}),
        "indicator_units": display_indicator_units,
        "months": total,                    # помесячный агрегат {plan, fact} по всем отделам
        "by_dept": by_dept_full,            # детализация по подразделению (имя → срез)
        "by_dept_guid": by_dept_guid,       # то же, ключ — GUID подразделения
        "dept_indicators": dept_indicators, # какие показатели есть у каждого отдела
        "charts": flat_charts,              # плоский список диаграмм (КД / ПСД ком.блока)
        "fact_debug": {
            "new_service_customers": service_customers_debug,
            "project_bookmarks": project_bookmarks_debug,
        },
    }


def _resolve_department_names(session: requests.Session, keys: set[str]) -> dict[str, str]:
    """Резолвит GUID подразделения → Description через Catalog_СтруктураПредприятия."""
    if not keys:
        return {}
    names: dict[str, str] = {}
    BATCH = 40
    keys_list = [k for k in keys if k]
    for i in range(0, len(keys_list), BATCH):
        chunk = keys_list[i:i + BATCH]
        flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in chunk)
        url = (
            f"{BASE}/{quote('Catalog_СтруктураПредприятия')}?$format=json"
            f"&$select=Ref_Key,Description"
            f"&$filter={quote(flt, safe='')}&$top=5000"
        )
        r = request_with_retry(session, url, timeout=60, retries=4, label="ks_razvitie/depts")
        if r is None or not r.ok:
            continue
        try:
            for item in r.json().get("value", []):
                k = str(item.get("Ref_Key") or "").lower()
                desc = (item.get("Description") or "").strip()
                if k and desc:
                    names[k] = desc
        except Exception:
            continue
    return names


def get_ks_razvitie_plans(year: int | None = None) -> dict:
    """Кэшируемая обёртка: помесячные планы блока «КС развитие» за год.

    Для текущего года кэш живёт до конца календарного дня.
    Для прошлых лет — бесконечно.
    """
    if year is None:
        year = date.today().year

    cached = _load_cache(int(year))
    if cached is not None:
        return cached

    try:
        payload = _fetch_from_odata(int(year))
    except Exception as exc:
        logger.error("ks_razvitie: fetch failed: %s", exc)
        payload = {
            "year": int(year),
            "indicators": [],
            "months": {},
            "by_dept": {},
        }

    _save_cache(int(year), payload)
    return payload
