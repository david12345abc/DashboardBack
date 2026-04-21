"""
calc_ks_razvitie.py — Плановые показатели блока «КС развитие» (ТД).

Источник: Document_ТД_КСРазвитие с табличной частью «Показатели».
Каждый документ = один показатель × одно подразделение × 12 месяцев.

Структура возвращаемого JSON (`get_ks_razvitie_plans`):
{
  "year": 2026,
  "months": {  # агрегат по всем подразделениям из ALLOWED_DEPARTMENTS
    "1":  {"Развитие имеющихся дилеров": 0, "Новые дилеры": 0, ...},
    ...
    "12": {...}
  },
  "by_dept": {  # детализация по подразделению
    "Отдел дилерских продаж": {
      "1":  {"Развитие имеющихся дилеров": 0, "Новые дилеры": 0},
      ...
      "12": {...}
    },
    ...
  },
  "indicators": ["Развитие имеющихся дилеров", "Новые дилеры", ...]
}

Фильтр: в выдаче — только документы с Подразделением из ALLOWED_DEPARTMENTS
(дочерние подразделения коммерческого директора).

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
    # Текущий год — перепроверяем на дневной кэш, прошлые годы — кэшируем без TTL.
    today = date.today()
    if int(data.get("year") or 0) == today.year and data.get("cached_at") != today.isoformat():
        return None
    return data


def _save_cache(year: int, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {**payload, "cached_at": date.today().isoformat()}
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
    """Загружает шапки документов Document_ТД_КСРазвитие (Ref_Key, Number, Date, Подразделение_Key)."""
    select = "Ref_Key,Number,Date,Posted,DeletionMark,Подразделение_Key"
    docs: list[dict] = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(doc_entity)}?$format=json"
            f"&$select={quote(select, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as exc:
            logger.error("ks_razvitie: docs HTTP error: %s", exc)
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
    """Табличная часть «Показатели» у документов Document_ТД_КСРазвитие (Ref_Key, Месяц, Показатель, План)."""
    # LineNumber — стандартное поле табличной части
    select = "Ref_Key,LineNumber,Месяц,Показатель,План"
    rows: list[dict] = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(tab_entity)}?$format=json"
            f"&$select={quote(select, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as exc:
            logger.error("ks_razvitie: tab HTTP error: %s", exc)
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


def _build_empty_months(indicators: list[str]) -> dict[str, dict[str, float]]:
    """12 месяцев × все показатели = нули."""
    return {
        str(m): {ind: 0.0 for ind in indicators}
        for m in range(1, 13)
    }


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

    # Отфильтруем только документы с нужными подразделениями.
    allowed_lower = {k.lower(): v for k, v in ALLOWED_DEPARTMENTS.items()}
    docs_by_ref: dict[str, dict] = {}
    for d in docs:
        dept_key = str(d.get("Подразделение_Key") or "").lower()
        if dept_key not in allowed_lower:
            continue
        docs_by_ref[str(d.get("Ref_Key") or "").lower()] = {
            "ref": str(d.get("Ref_Key") or "").lower(),
            "number": str(d.get("Number") or "").strip(),
            "date": str(d.get("Date") or "")[:10],
            "dept_key": dept_key,
            "dept_name": allowed_lower[dept_key],
        }

    if not docs_by_ref:
        return {
            "year": int(year),
            "months": {},
            "by_dept": {},
            "indicators": [],
        }

    tab_rows = _fetch_tab_rows(session, tab_entity)

    # indicators — множество всех показателей, собранных по документам коммерческого блока.
    indicators_set: set[str] = set()

    # Структура агрегатов: by_dept[dept_name][month][indicator] = sum(plan)
    by_dept_agg: dict[str, dict[str, dict[str, float]]] = {}

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

        indicator = (row.get("Показатель") or "").strip()
        if not indicator:
            continue
        plan = _parse_plan(row.get("План"))
        indicators_set.add(indicator)

        dept_name = info["dept_name"]
        dept_bucket = by_dept_agg.setdefault(dept_name, {})
        month_bucket = dept_bucket.setdefault(str(month), {})
        month_bucket[indicator] = round(month_bucket.get(indicator, 0.0) + plan, 4)

    indicators = sorted(indicators_set)

    # Заполняем нулями отсутствующие месяцы/показатели в каждом подразделении.
    by_dept_full: dict[str, dict[str, dict[str, float]]] = {}
    for dept_name in ALLOWED_DEPARTMENTS.values():
        base = _build_empty_months(indicators)
        dept_present = by_dept_agg.get(dept_name) or {}
        for m in range(1, 13):
            key = str(m)
            if key in dept_present:
                for ind in indicators:
                    base[key][ind] = float(dept_present[key].get(ind, 0.0))
        by_dept_full[dept_name] = base

    # Общий (по коммерческому блоку) агрегат.
    total: dict[str, dict[str, float]] = _build_empty_months(indicators)
    for dept_map in by_dept_full.values():
        for m_key, ind_map in dept_map.items():
            for ind, v in ind_map.items():
                total[m_key][ind] = round(total[m_key].get(ind, 0.0) + float(v), 4)

    return {
        "year": int(year),
        "indicators": indicators,
        "months": total,
        "by_dept": by_dept_full,
    }


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
