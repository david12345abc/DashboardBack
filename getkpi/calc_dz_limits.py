"""
calc_dz_limits.py — Лимиты просроченной дебиторской задолженности.

Источник: документ «Лимиты просроченной дебиторской задолженности (ТД)» в 1С.
Берётся последний по дате документ, из его табличной части извлекаются
лимиты по подразделениям. Суммы в документе указаны в **миллионах** рублей.

Запуск (standalone):
  python calc_dz_limits.py

API (из Django):
  from getkpi.calc_dz_limits import get_overdue_limits
  limits = get_overdue_limits()
  # {"Отдел ПАО Газпром": 185_000_000, ...}
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

DOC_ENTITY_CANDIDATES = [
    "Document_ТД_ЛимитыПросроченнойДебиторскойЗадолженности",
    "Document_ЛимитыПросроченнойДебиторскойЗадолженностиТД",
    "Document_ЛимитыПросроченнойДебиторскойЗадолженности",
]

TABULAR_PART_CANDIDATES = [
    "Подразделения",
    "Лимиты",
    "ЛимитыПоПодразделениям",
    "ТабличнаяЧасть",
]

DEPT_GUID_TO_SHORT: dict[str, str] = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел эталонного оборудования",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел ключевых клиентов",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел ПАО Газпром",
}

FALLBACK_LIMITS: dict[str, float] = {
    "Отдел ПАО Газпром":           185_000_000,
    "Отдел дилерских продаж":       30_000_000,
    "Отдел ВЭД":                    65_000_000,
    "Отдел эталонного оборудования": 10_000_000,
    "Отдел ключевых клиентов":      35_000_000,
    "Отдел БМИ":                   180_000_000,
}

_CACHE_FILE = CACHE_DIR / "dz_limits_latest.json"


def _load_cache() -> dict | None:
    if not _CACHE_FILE.exists():
        return None
    try:
        with open(_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_date") == date.today().isoformat():
            return data.get("limits")
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(limits: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"cache_date": date.today().isoformat(), "limits": limits},
                f, ensure_ascii=False, indent=2,
            )
    except OSError:
        pass


def _resolve_dept_names(session: requests.Session) -> dict[str, str]:
    """GUID → название подразделения из Catalog_СтруктураПредприятия."""
    names: dict[str, str] = {}
    skip = 0
    while True:
        url = (
            f"{BASE}/Catalog_СтруктураПредприятия"
            f"?$format=json&$top=5000&$skip={skip}"
            f"&$select=Ref_Key,Description"
        )
        try:
            r = session.get(url, timeout=30)
        except Exception:
            break
        if not r.ok:
            break
        rows = r.json().get("value", [])
        for row in rows:
            names[row["Ref_Key"].lower()] = row.get("Description", "").strip()
        if len(rows) < 5000:
            break
        skip += 5000
    return names


def _try_fetch_from_odata(session: requests.Session) -> dict[str, float] | None:
    """
    Попытка найти документ лимитов через OData.
    Перебирает варианты имён сущности и табличной части.
    """
    doc_entity = None
    for candidate in DOC_ENTITY_CANDIDATES:
        url = f"{BASE}/{candidate}?$format=json&$top=1&$orderby=Date desc"
        try:
            r = session.get(url, timeout=15)
            if r.ok and r.json().get("value"):
                doc_entity = candidate
                break
        except Exception:
            continue

    if doc_entity is None:
        return None

    url = f"{BASE}/{doc_entity}?$format=json&$top=1&$orderby=Date desc"
    try:
        r = session.get(url, timeout=15)
    except Exception:
        return None
    if not r.ok:
        return None

    docs = r.json().get("value", [])
    if not docs:
        return None

    doc = docs[0]
    doc_key = doc.get("Ref_Key", "")
    doc_date = (doc.get("Date") or "")[:10]
    logger.info("Found DZ limits doc: key=%s date=%s", doc_key, doc_date)

    for tp_name in TABULAR_PART_CANDIDATES:
        tp_entity = f"{doc_entity}_{tp_name}"
        url = (
            f"{BASE}/{tp_entity}"
            f"?$format=json&$top=5000"
            f"&$filter=Ref_Key eq guid'{doc_key}'"
        )
        try:
            r = session.get(url, timeout=15)
        except Exception:
            continue
        if not r.ok:
            continue
        rows = r.json().get("value", [])
        if not rows:
            continue

        dept_names = _resolve_dept_names(session)
        limits: dict[str, float] = {}

        for row in rows:
            dept_key = (row.get("Подразделение_Key") or "").lower()
            limit_val = float(row.get("Лимит") or row.get("ЛимитПросрДЗ") or 0)

            limit_rub = limit_val * 1_000_000

            short = DEPT_GUID_TO_SHORT.get(dept_key)
            if short is None:
                full_name = dept_names.get(dept_key, "")
                if full_name:
                    short = full_name
                else:
                    continue

            limits[short] = limit_rub

        if limits:
            return limits

    return None


def _fetch_limits() -> dict[str, float]:
    session = requests.Session()
    session.auth = AUTH

    result = _try_fetch_from_odata(session)
    if result:
        return result

    logger.warning("Could not fetch DZ limits from OData, using fallback values")
    return dict(FALLBACK_LIMITS)


def get_overdue_limits() -> dict[str, float]:
    """
    Лимиты просроченной ДЗ по подразделениям (руб.).
    Кэшируется на день.
    """
    cached = _load_cache()
    if cached is not None:
        return cached

    limits = _fetch_limits()
    _save_cache(limits)
    return limits


def get_total_overdue_limit() -> float:
    """Суммарный лимит по всем подразделениям (руб.)."""
    return sum(get_overdue_limits().values())


def get_dept_overdue_limit(dept_guid: str | None) -> float:
    """Лимит просроченной ДЗ для конкретного подразделения (по GUID).
    dept_guid=None → суммарный лимит всех отделов.
    """
    if dept_guid is None:
        return get_total_overdue_limit()
    limits = get_overdue_limits()
    dept_name = DEPT_GUID_TO_SHORT.get(dept_guid.lower())
    if dept_name and dept_name in limits:
        return limits[dept_name]
    dept_name_upper = DEPT_GUID_TO_SHORT.get(dept_guid)
    if dept_name_upper and dept_name_upper in limits:
        return limits[dept_name_upper]
    return get_total_overdue_limit()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    import functools
    print = functools.partial(print, flush=True)

    print("=" * 60)
    print("  ЛИМИТЫ ПРОСРОЧЕННОЙ ДЕБИТОРСКОЙ ЗАДОЛЖЕННОСТИ")
    print("=" * 60)

    t0 = time.time()
    limits = _fetch_limits()

    total = sum(limits.values())
    print(f"\n{'Подразделение':<45s} {'Лимит, руб.':>18s}")
    print("─" * 65)
    for dept, val in sorted(limits.items()):
        print(f"  {dept:<43s} {val:>18,.2f}")
    print("─" * 65)
    print(f"  {'ИТОГО':<43s} {total:>18,.2f}")
    print(f"\n  Время: {time.time() - t0:.1f}с")

    _save_cache(limits)
    print(f"  Кэш: {_CACHE_FILE}")
