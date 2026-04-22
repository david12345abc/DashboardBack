"""
calc_dz_limits.py — Лимиты просроченной дебиторской задолженности.

Источник: документ «Лимиты просроченной дебиторской задолженности (ТД)» в 1С.
В OData он называется `Document_ТД_ЛимитыПросроченнойДЗ` (табличная часть —
`Document_ТД_ЛимитыПросроченнойДЗ_Лимиты`). Берётся **последний по дате
проведённый (и не помеченный на удаление) документ** — он задаёт лимиты
для всех подразделений на весь год (один и тот же лимит во всех месяцах).

Суммы в документе указаны в **миллионах** рублей (колонка «Лимит»).

Структура кэша (`dashboard/dz_limits_latest.json`):
{
  "cache_date": "2026-04-22",
  "doc": {"ref": "...", "number": "000000001", "date": "2026-04-14"},
  "limits": {
    "<dept_guid>": 185000000,         # руб., для быстрой фильтрации по GUID
    ...
  },
  "limits_by_name": {"Отдел ПАО Газпром": 185000000, ...}
}

Запуск (standalone):
  python calc_dz_limits.py

API (из Django):
  from getkpi.calc_dz_limits import get_overdue_limits, get_dept_overdue_limit
  limits = get_overdue_limits()           # dict с limits / limits_by_name / doc
  by_name = limits["limits_by_name"]      # {"Отдел ПАО Газпром": 185_000_000, ...}
  by_guid = limits["limits_by_guid"]      # {"<guid>": 185_000_000, ...}
  total   = get_dept_overdue_limit(None)  # сумма всех лимитов
  газпром = get_dept_overdue_limit("bd7b5184-9f9c-11e4-80da-001e67112509")
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
_CACHE_FILE = CACHE_DIR / "dz_limits_latest.json"

# Основное имя сущности в OData (определено эмпирически по $metadata).
DOC_ENTITY_DEFAULT = "Document_ТД_ЛимитыПросроченнойДЗ"
TAB_ENTITY_DEFAULT = "Document_ТД_ЛимитыПросроченнойДЗ_Лимиты"

# Резервные кандидаты имени документа/таб.части на случай переименования.
DOC_ENTITY_CANDIDATES = [
    DOC_ENTITY_DEFAULT,
    "Document_ТД_ЛимитыПросроченнойДебиторскойЗадолженности",
    "Document_ЛимитыПросроченнойДебиторскойЗадолженностиТД",
    "Document_ЛимитыПросроченнойДебиторскойЗадолженности",
]

# Известные сокращённые названия отделов (для подписей в дашборде).
DEPT_GUID_TO_SHORT: dict[str, str] = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел эталонного оборудования",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел ключевых клиентов",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел ПАО Газпром",
}

_DISCOVERED_DOC: str | None = None
_DISCOVERED_TAB: str | None = None


def _discover_entities(session: requests.Session) -> tuple[str, str]:
    """Находит реальные имена EntitySet для документа и таб.части через $metadata.

    Всегда возвращает пару (document_entity, tab_entity). Если $metadata
    недоступен — отдаём дефолтные имена; если отдаёт другие варианты с
    «Лимит» — подбираем самый подходящий (документ без суффикса + табличная
    часть с суффиксом).
    """
    global _DISCOVERED_DOC, _DISCOVERED_TAB
    if _DISCOVERED_DOC and _DISCOVERED_TAB:
        return _DISCOVERED_DOC, _DISCOVERED_TAB

    try:
        r = request_with_retry(session, f"{BASE}/$metadata", timeout=30, retries=3,
                               label="dz_limits/metadata")
        if r is not None and r.ok:
            text = r.text
            pat = re.compile(
                r'EntitySet\s+Name="(Document_[^"]*\u041b\u0438\u043c\u0438\u0442[^"]*)"'
            )
            names = sorted(set(pat.findall(text)))
            # Сортируем: берём имена с «ДЗ» / «Дебитор» / «Просроч».
            relevant = [
                n for n in names
                if "\u0414\u0417" in n
                or "\u0414\u0435\u0431\u0438\u0442\u043e\u0440" in n
                or "\u041f\u0440\u043e\u0441\u0440\u043e\u0447" in n
            ]
            candidates = relevant or names
            doc = None
            tab = None
            for name in candidates:
                if name.endswith("_\u041b\u0438\u043c\u0438\u0442\u044b") \
                        or name.endswith("_\u041f\u043e\u0434\u0440\u0430\u0437\u0434\u0435\u043b\u0435\u043d\u0438\u044f"):
                    tab = tab or name
                else:
                    doc = doc or name
            if doc:
                _DISCOVERED_DOC = doc
                _DISCOVERED_TAB = tab or (doc + "_\u041b\u0438\u043c\u0438\u0442\u044b")
                return _DISCOVERED_DOC, _DISCOVERED_TAB
    except Exception as exc:
        logger.warning("dz_limits: metadata discover error: %s", exc)

    _DISCOVERED_DOC = DOC_ENTITY_DEFAULT
    _DISCOVERED_TAB = TAB_ENTITY_DEFAULT
    return _DISCOVERED_DOC, _DISCOVERED_TAB


def _resolve_dept_names(session: requests.Session, keys: set[str]) -> dict[str, str]:
    """GUID → название подразделения из Catalog_СтруктураПредприятия."""
    if not keys:
        return {}
    names: dict[str, str] = {}
    keys_list = [k for k in keys if k and k != EMPTY]
    BATCH = 40
    for i in range(0, len(keys_list), BATCH):
        chunk = keys_list[i:i + BATCH]
        flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in chunk)
        url = (
            f"{BASE}/{quote('Catalog_СтруктураПредприятия')}?$format=json"
            f"&$select=Ref_Key,Description&$top=5000"
            f"&$filter={quote(flt, safe='')}"
        )
        r = request_with_retry(session, url, timeout=60, retries=3, label="dz_limits/depts")
        if r is None or not r.ok:
            continue
        try:
            for row in r.json().get("value", []):
                k = str(row.get("Ref_Key") or "").lower()
                desc = (row.get("Description") or "").strip()
                if k and desc:
                    names[k] = desc
        except (ValueError, KeyError):
            continue
    return names


def _fetch_latest_doc(session: requests.Session,
                     doc_entity: str) -> dict | None:
    """Последний проведённый документ `Document_ТД_ЛимитыПросроченнойДЗ`.

    Проходит по страницам $orderby=Date desc и возвращает первый, у которого
    Posted=true и DeletionMark=false. Обычно находим на первой странице.
    """
    url = (
        f"{BASE}/{quote(doc_entity)}?$format=json"
        f"&$orderby=Date desc&$top=20"
        f"&$select=Ref_Key,Number,Date,Posted,DeletionMark"
    )
    r = request_with_retry(session, url, timeout=60, retries=4, label="dz_limits/doc")
    if r is None or not r.ok:
        if r is not None:
            logger.error("dz_limits: doc HTTP %d: %s", r.status_code, r.text[:200])
        return None
    for d in r.json().get("value", []) or []:
        if d.get("Posted") and not d.get("DeletionMark"):
            return d
    return None


def _fetch_tab_rows(session: requests.Session,
                   tab_entity: str,
                   doc_ref: str) -> list[dict]:
    """Строки таб.части документа лимитов."""
    url = (
        f"{BASE}/{quote(tab_entity)}?$format=json"
        f"&$filter=Ref_Key eq guid'{doc_ref}'"
        f"&$top=5000"
    )
    r = request_with_retry(session, url, timeout=60, retries=4, label="dz_limits/tab")
    if r is None or not r.ok:
        if r is not None:
            logger.error("dz_limits: tab HTTP %d: %s", r.status_code, r.text[:200])
        return []
    return r.json().get("value", []) or []


def _try_fetch_from_odata(session: requests.Session) -> dict | None:
    """
    Основной сборщик: находит последний документ лимитов, парсит таб.часть,
    резолвит названия подразделений. Возвращает None, если что-то пошло не
    так (обычно access denied / entity not found).
    """
    doc_entity, tab_entity = _discover_entities(session)
    logger.info("dz_limits: using doc=%s tab=%s", doc_entity, tab_entity)

    doc = _fetch_latest_doc(session, doc_entity)
    if doc is None:
        return None

    doc_ref = str(doc.get("Ref_Key") or "").lower()
    doc_num = str(doc.get("Number") or "").strip()
    doc_date = str(doc.get("Date") or "")[:10]
    logger.info("dz_limits: latest doc=%s date=%s ref=%s", doc_num, doc_date, doc_ref)

    rows = _fetch_tab_rows(session, tab_entity, doc_ref)
    if not rows:
        return None

    dept_keys = {str(row.get("Подразделение_Key") or "").lower() for row in rows}
    dept_keys.discard("")
    dept_keys.discard(EMPTY)
    dept_names = _resolve_dept_names(session, dept_keys)

    limits_by_guid: dict[str, float] = {}
    limits_by_name: dict[str, float] = {}

    for row in rows:
        dept_key = str(row.get("Подразделение_Key") or "").lower()
        if not dept_key or dept_key == EMPTY:
            continue
        limit_val = row.get("Лимит")
        if limit_val is None:
            limit_val = row.get("ЛимитПросрДЗ") or 0
        try:
            limit_mln = float(limit_val)
        except (TypeError, ValueError):
            limit_mln = 0.0
        limit_rub = round(limit_mln * 1_000_000, 2)

        limits_by_guid[dept_key] = limit_rub

        # Подпись: сокращённое имя → название из 1С → «Отдел <guid8>».
        short = (
            DEPT_GUID_TO_SHORT.get(dept_key)
            or dept_names.get(dept_key)
            or f"Отдел {dept_key[:8]}"
        )
        limits_by_name[short] = limit_rub

    if not limits_by_guid:
        return None

    return {
        "doc": {"ref": doc_ref, "number": doc_num, "date": doc_date},
        "limits_by_guid": limits_by_guid,
        "limits_by_name": limits_by_name,
    }


def _load_cache() -> dict | None:
    """Возвращает полный payload (с doc/limits_by_guid/limits_by_name) или None."""
    if not _CACHE_FILE.exists():
        return None
    try:
        with open(_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    # Проверим, что кэш — в новом формате (есть limits_by_guid).
    if "limits_by_guid" not in data:
        return None
    return data


def _save_cache(payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"cache_date": date.today().isoformat(), **payload},
                f, ensure_ascii=False, indent=2,
            )
    except OSError:
        pass


def _fetch_limits_payload() -> dict:
    """
    Собирает payload лимитов, кэширует на день. Если OData недоступен —
    возвращает payload с пустыми лимитами (никаких магических fallback-значений).
    """
    session = requests.Session()
    session.auth = AUTH

    result = _try_fetch_from_odata(session)
    if result:
        return result

    logger.warning(
        "dz_limits: не удалось получить данные из OData — лимиты будут пустыми."
    )
    return {
        "doc": None,
        "limits_by_guid": {},
        "limits_by_name": {},
    }


def get_overdue_limits() -> dict:
    """Полный payload лимитов просроченной ДЗ (из 1С, с суточным кэшем).

    Возвращает:
      {
        "doc": {"ref": "...", "number": "000000001", "date": "2026-04-14"} | None,
        "limits_by_guid": {"<guid>": 43_500_000.0, ...},   # руб.
        "limits_by_name": {"Отдел ПАО Газпром": 43_500_000, ...},
      }
    """
    cached = _load_cache()
    if cached is not None:
        return {
            "doc": cached.get("doc"),
            "limits_by_guid": cached.get("limits_by_guid") or {},
            "limits_by_name": cached.get("limits_by_name") or {},
        }

    payload = _fetch_limits_payload()
    _save_cache(payload)
    return payload


def get_total_overdue_limit() -> float:
    """Суммарный лимит по всем подразделениям (руб.)."""
    return float(sum((get_overdue_limits().get("limits_by_guid") or {}).values()))


def get_dept_overdue_limit(dept_guid: str | None) -> float:
    """Лимит просроченной ДЗ для конкретного подразделения (по GUID).

    dept_guid=None → суммарный лимит всех отделов из последнего документа.
    """
    payload = get_overdue_limits()
    by_guid = payload.get("limits_by_guid") or {}

    if dept_guid is None:
        return float(sum(by_guid.values()))

    key = str(dept_guid).lower()
    if key in by_guid:
        return float(by_guid[key])
    return 0.0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 60)
    print("  ЛИМИТЫ ПРОСРОЧЕННОЙ ДЕБИТОРСКОЙ ЗАДОЛЖЕННОСТИ")
    print("=" * 60)

    t0 = time.time()
    payload = _fetch_limits_payload()

    doc = payload.get("doc") or {}
    print(f"\nДокумент: {doc.get('number')} от {doc.get('date')}  (ref={doc.get('ref')})")

    by_name = payload.get("limits_by_name") or {}
    total = sum(by_name.values())
    print(f"\n{'Подразделение':<45s} {'Лимит, руб.':>18s}")
    print("─" * 65)
    for dept, val in sorted(by_name.items()):
        print(f"  {dept:<43s} {val:>18,.2f}")
    print("─" * 65)
    print(f"  {'ИТОГО':<43s} {total:>18,.2f}")
    print(f"\n  Время: {time.time() - t0:.1f}с")

    _save_cache(payload)
    print(f"  Кэш: {_CACHE_FILE}")
