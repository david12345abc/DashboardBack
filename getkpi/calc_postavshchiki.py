"""
calc_postavshchiki.py — задолженность с поставщиками для FND-T3.

Используется для плитки FND-T3 «Соотношение ДЗ и КЗ» (ПСД, «Мой дашборд»).

Источник истины — тот же, что и в `calc_psd_kz_dz.py`:
  AccumulationRegister_РасчетыСПоставщикамиПоСрокам/Balance

Что считаем:
  - КЗ (наш долг поставщикам)         = Σ ДолгРеглBalance
  - ДЗ (авансы, выданные поставщикам) = Σ ПредоплатаРеглBalance

Фильтр:
  - только объекты расчётов организации ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО

Важно:
  - `get_supplier_snapshot()` возвращает корректный остаток на конец даты;
  - `get_supplier_monthly()` отдаёт не накопительный остаток с января,
    а дельту относительно предыдущего месяца, чтобы фронт мог суммировать
    месяцы за произвольный период.

Запуск (CLI):
  python calc_postavshchiki.py [ГГГГ-ММ-ДД]        # остаток на конкретную дату
  python calc_postavshchiki.py                     # остаток на сегодня
  python calc_postavshchiki.py --monthly 2026     # помесячные дельты за год
"""
from __future__ import annotations

import calendar
import functools
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

REGISTER = "AccumulationRegister_РасчетыСПоставщикамиПоСрокам"
EMPTY = "00000000-0000-0000-0000-000000000000"
ORG_GUID_NPO = "fbca2148-6cfd-11e7-812d-001e67112509"

TOLERANCE = 0.01
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

# Маркер версии кэша. Меняй при изменении формулы расчёта, чтобы
# пересобрать кэш на сервере при первом запросе.
SOURCE_TAG = "supplier_balance_month_delta_v2"
DETAIL_SOURCE_TAG = "supplier_debt_stable_2_full_months_v3"


# ──────────────────────────────────────────────────────────────────────
#  Работа с регистром
# ──────────────────────────────────────────────────────────────────────
def _fetch_all(
    session: requests.Session,
    base_url: str,
    *,
    page: int = 5000,
    label: str,
) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    sep = "&" if "?" in base_url else "?"

    while True:
        url = f"{base_url}{sep}$top={page}&$skip={skip}"
        r = request_with_retry(session, url, timeout=180, retries=4, label=label)
        if r is None or not r.ok:
            logger.warning(
                "Postavshchiki HTTP %s on skip=%d for %s",
                r.status_code if r else "no-response",
                skip,
                label,
            )
            break
        chunk = r.json().get("value", [])
        rows.extend(chunk)
        if len(chunk) < page:
            break
        skip += page

    return rows


def _load_supplier_obj_keys(session: requests.Session) -> set[str]:
    """Все Ref_Key из Catalog_ОбъектыРасчетов для организации НПО."""
    flt = quote(f"Организация_Key eq guid'{ORG_GUID_NPO}'", safe="")
    url = (
        f"{BASE}/Catalog_ОбъектыРасчетов"
        f"?$format=json"
        f"&$select=Ref_Key"
        f"&$filter={flt}"
    )
    rows = _fetch_all(session, url, label="KZ/ObjCatalog")
    return {
        str(r.get("Ref_Key") or EMPTY).lower()
        for r in rows
        if str(r.get("Ref_Key") or EMPTY).lower() != EMPTY
    }


def _load_supplier_obj_map(session: requests.Session) -> dict[str, dict]:
    """Объекты расчётов НПО с привязкой к партнёру-поставщику."""
    flt = quote(f"Организация_Key eq guid'{ORG_GUID_NPO}'", safe="")
    url = (
        f"{BASE}/Catalog_ОбъектыРасчетов"
        f"?$format=json"
        f"&$select=Ref_Key,Партнер_Key,Description,Номер,Дата"
        f"&$filter={flt}"
    )
    rows = _fetch_all(session, url, label="KZ/ObjCatalogDetail")
    out: dict[str, dict] = {}
    for row in rows:
        key = str(row.get("Ref_Key") or EMPTY).lower()
        if key == EMPTY:
            continue
        out[key] = {
            "partner_key": str(row.get("Партнер_Key") or EMPTY).lower(),
            "description": (row.get("Description") or "").strip(),
            "number": (row.get("Номер") or "").strip(),
            "date": str(row.get("Дата") or "")[:10],
        }
    return out


def _resolve_partner_names(session: requests.Session, partner_keys: set[str]) -> dict[str, str]:
    keys = sorted(
        key for key in partner_keys
        if key and key != EMPTY
    )
    if not keys:
        return {}

    names: dict[str, str] = {}
    batch_size = 40
    for i in range(0, len(keys), batch_size):
        batch = keys[i:i + batch_size]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        url = (
            f"{BASE}/Catalog_Партнеры?$format=json"
            f"&$select=Ref_Key,Description"
            f"&$filter={quote(flt, safe='')}&$top=5000"
        )
        response = request_with_retry(session, url, timeout=60, retries=4, label="KZ/Partners")
        if response is None or not response.ok:
            continue
        for item in response.json().get("value", []):
            key = str(item.get("Ref_Key") or "").lower()
            name = (item.get("Description") or "").strip()
            if key:
                names[key] = name or key[:8]
    return names


def _load_supplier_balance(session: requests.Session, na_datu: date) -> list[dict]:
    """
    Balance регистра на начало следующего дня, как в отчёте 1С.
    """
    period_iso = f"{(na_datu + timedelta(days=1)).isoformat()}T00:00:00"
    url = (
        f"{BASE}/{REGISTER}/Balance"
        f"?$format=json"
        f"&Period=datetime'{period_iso}'"
        f"&$select=ОбъектРасчетов_Key,ДолгРеглBalance,ПредоплатаРеглBalance"
    )
    return _fetch_all(session, url, label="KZ/Balance")


def _load_supplier_document_balance(session: requests.Session, na_datu: date) -> list[dict]:
    """Balance регистра РасчетыСПоставщикамиПоДокументам на конец даты."""
    period_iso = f"{(na_datu + timedelta(days=1)).isoformat()}T00:00:00"
    url = (
        f"{BASE}/AccumulationRegister_РасчетыСПоставщикамиПоДокументам/Balance"
        f"?$format=json"
        f"&Period=datetime'{period_iso}'"
        f"&$select=АналитикаУчетаПоПартнерам_Key,ЗаказПоставщику_Key,"
        f"РасчетныйДокумент,РасчетныйДокумент_Type,Валюта_Key,ДолгРеглBalance,ПредоплатаРеглBalance"
    )
    return _fetch_all(session, url, label="KZ/DocumentBalance")


def _two_full_month_window(na_datu: date) -> tuple[date, date]:
    """Два полных календарных месяца перед месяцем даты среза: [start, end)."""
    end = date(na_datu.year, na_datu.month, 1)
    month = end.month - 2
    year = end.year
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1), end


def _balance_by_object(
    rows: list[dict],
    allowed_obj_keys: set[str],
    *,
    resource: str = "ДолгРеглBalance",
) -> dict[str, float]:
    result: dict[str, float] = {}
    for row in rows:
        obj_key = str(row.get("ОбъектРасчетов_Key") or EMPTY).lower()
        if obj_key not in allowed_obj_keys:
            continue
        value = float(row.get(resource) or 0)
        if abs(value) <= TOLERANCE:
            continue
        result[obj_key] = round(result.get(obj_key, 0.0) + value, 2)
    return result


def _load_supplier_debt_movements_by_object(
    session: requests.Session,
    start: date,
    end: date,
    allowed_obj_keys: set[str],
) -> dict[str, dict]:
    """Обороты ДолгРегл по объектам расчётов за период [start, end)."""
    flt = (
        "Active eq true"
        f" and Period ge datetime'{start.isoformat()}T00:00:00'"
        f" and Period lt datetime'{end.isoformat()}T00:00:00'"
    )
    url = (
        f"{BASE}/{REGISTER}_RecordType?$format=json"
        f"&$select=Period,RecordType,Recorder,Recorder_Type,ОбъектРасчетов_Key,РасчетныйДокумент,"
        f"РасчетныйДокумент_Type,ДолгРегл,ПредоплатаРегл"
        f"&$filter={quote(flt, safe='')}"
    )
    rows = _fetch_all(session, url, label="KZ/ObjectMovements")
    result: dict[str, dict] = {}
    for row in rows:
        obj_key = str(row.get("ОбъектРасчетов_Key") or EMPTY).lower()
        if obj_key not in allowed_obj_keys:
            continue
        dolg = float(row.get("ДолгРегл") or 0)
        if abs(dolg) <= TOLERANCE:
            continue
        item = result.setdefault(obj_key, {
            "turnover_abs": 0.0,
            "turnover_signed": 0.0,
            "records": 0,
            "sample": [],
        })
        item["turnover_abs"] += abs(dolg)
        item["turnover_signed"] += dolg
        item["records"] += 1
        if len(item["sample"]) < 3:
            item["sample"].append({
                "period": str(row.get("Period") or "")[:10],
                "record_type": row.get("RecordType"),
                "recorder": row.get("Recorder"),
                "recorder_type": row.get("Recorder_Type"),
                "dolg_regl": round(dolg, 2),
            })
    for item in result.values():
        item["turnover_abs"] = round(float(item.get("turnover_abs") or 0), 2)
        item["turnover_signed"] = round(float(item.get("turnover_signed") or 0), 2)
    return result


def _load_supplier_debt_movements_by_document(
    session: requests.Session,
    start: date,
    end: date,
) -> dict[str, dict]:
    """Обороты ДолгРегл по расчетным документам за период [start, end)."""
    flt = (
        "Active eq true"
        f" and Period ge datetime'{start.isoformat()}T00:00:00'"
        f" and Period lt datetime'{end.isoformat()}T00:00:00'"
    )
    url = (
        f"{BASE}/AccumulationRegister_РасчетыСПоставщикамиПоДокументам_RecordType?$format=json"
        f"&$select=Period,RecordType,Recorder,Recorder_Type,ЗаказПоставщику_Key,"
        f"РасчетныйДокумент,РасчетныйДокумент_Type,ДолгРегл,ПредоплатаРегл"
        f"&$filter={quote(flt, safe='')}"
    )
    rows = _fetch_all(session, url, label="KZ/DocumentMovements")
    result: dict[str, dict] = {}
    for row in rows:
        doc_key = str(row.get("РасчетныйДокумент") or row.get("ЗаказПоставщику_Key") or EMPTY).lower()
        if doc_key == EMPTY:
            continue
        dolg = float(row.get("ДолгРегл") or 0)
        if abs(dolg) <= TOLERANCE:
            continue
        item = result.setdefault(doc_key, {
            "turnover_abs": 0.0,
            "turnover_signed": 0.0,
            "records": 0,
            "sample": [],
        })
        item["turnover_abs"] += abs(dolg)
        item["turnover_signed"] += dolg
        item["records"] += 1
        if len(item["sample"]) < 3:
            item["sample"].append({
                "period": str(row.get("Period") or "")[:10],
                "record_type": row.get("RecordType"),
                "recorder": row.get("Recorder"),
                "recorder_type": row.get("Recorder_Type"),
                "dolg_regl": round(dolg, 2),
            })
    for item in result.values():
        item["turnover_abs"] = round(float(item.get("turnover_abs") or 0), 2)
        item["turnover_signed"] = round(float(item.get("turnover_signed") or 0), 2)
    return result


def _aggregate_balance_rows(rows: list[dict], allowed_obj_keys: set[str]) -> tuple[float, float]:
    total_kz = 0.0
    total_dz = 0.0

    for row in rows:
        obj_key = str(row.get("ОбъектРасчетов_Key") or EMPTY).lower()
        if obj_key not in allowed_obj_keys:
            continue

        dolg = float(row.get("ДолгРеглBalance") or 0)
        avans = float(row.get("ПредоплатаРеглBalance") or 0)
        if dolg > TOLERANCE:
            total_kz += dolg
        if avans > TOLERANCE:
            total_dz += avans

    return round(total_kz, 2), round(total_dz, 2)


def _build_snapshot(
    session: requests.Session,
    na_datu: date,
    allowed_obj_keys: set[str],
) -> dict:
    rows = _load_supplier_balance(session, na_datu)
    total_kz, total_dz = _aggregate_balance_rows(rows, allowed_obj_keys)
    return {
        "na_datu": na_datu.isoformat(),
        "source": SOURCE_TAG,
        "total_dolg_regl": total_kz,           # КЗ поставщикам (мы должны)
        "total_predoplata_regl": total_dz,     # ДЗ поставщиков (наши авансы)
    }


# ──────────────────────────────────────────────────────────────────────
#  Кэш
# ──────────────────────────────────────────────────────────────────────
def _cache_path_snapshot(na_datu: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"postavshchiki_{na_datu.isoformat()}.json"


def _cache_path_monthly(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"postavshchiki_monthly_{year}_{month:02d}.json"


def _cache_path_dz_detail(na_datu: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"postavshchiki_dz_detail_{na_datu.isoformat()}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _save_json(path: Path, data: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


# ──────────────────────────────────────────────────────────────────────
#  Публичный API
# ──────────────────────────────────────────────────────────────────────
def get_supplier_snapshot(na_datu: date) -> dict:
    """Остатки на одну дату. Кэшируется в файл."""
    cached = _load_json(_cache_path_snapshot(na_datu))
    if cached is not None and cached.get("source") == SOURCE_TAG:
        return cached

    session = requests.Session()
    session.auth = AUTH
    logger.info("calc_postavshchiki: snapshot for %s", na_datu.isoformat())
    allowed_obj_keys = _load_supplier_obj_keys(session)
    payload = _build_snapshot(session, na_datu, allowed_obj_keys)
    _save_json(_cache_path_snapshot(na_datu), payload)
    return payload


def get_supplier_dz_detail(na_datu: date) -> dict:
    """Наш долг поставщикам на дату: долг > 0 без оборота за 2 полных месяца."""
    cache_path = _cache_path_dz_detail(na_datu)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == DETAIL_SOURCE_TAG:
        return cached

    session = requests.Session()
    session.auth = AUTH
    logger.info("calc_postavshchiki: supplier DZ detail for %s", na_datu.isoformat())

    obj_map = _load_supplier_obj_map(session)
    allowed_obj_keys = set(obj_map)
    window_start, window_end = _two_full_month_window(na_datu)
    balance_rows = _load_supplier_balance(session, na_datu)
    start_balance_rows = _load_supplier_balance(session, window_start - timedelta(days=1))
    end_balance_rows = _load_supplier_balance(session, window_end - timedelta(days=1))
    document_balance_rows = _load_supplier_document_balance(session, na_datu)
    object_movements = _load_supplier_debt_movements_by_object(
        session,
        window_start,
        window_end,
        allowed_obj_keys,
    )
    document_movements = _load_supplier_debt_movements_by_document(session, window_start, window_end)

    current_debt = _balance_by_object(balance_rows, allowed_obj_keys, resource="ДолгРеглBalance")
    start_debt = _balance_by_object(start_balance_rows, allowed_obj_keys, resource="ДолгРеглBalance")
    end_debt = _balance_by_object(end_balance_rows, allowed_obj_keys, resource="ДолгРеглBalance")

    by_object: dict[str, dict] = {}
    partner_keys: set[str] = set()
    excluded_positive = 0
    for obj_key, amount in current_debt.items():
        obj = obj_map.get(obj_key)
        if not obj:
            continue
        if amount <= TOLERANCE:
            continue
        start_amount = start_debt.get(obj_key, 0.0)
        end_amount = end_debt.get(obj_key, 0.0)
        movement = object_movements.get(obj_key) or {}
        turnover_abs = float(movement.get("turnover_abs") or 0)
        balance_unchanged = (
            abs(start_amount - end_amount) <= TOLERANCE
            and abs(end_amount - amount) <= TOLERANCE
        )
        no_turnover = turnover_abs <= TOLERANCE
        if not (balance_unchanged and no_turnover):
            excluded_positive += 1
            continue
        partner_key = obj.get("partner_key") or EMPTY
        item = by_object.setdefault(obj_key, {
            "order_key": obj_key,
            "order_num": obj.get("number") or "",
            "order_date": obj.get("date") or "",
            "object_name": obj.get("description") or "",
            "supplier_key": partner_key if partner_key != EMPTY else "",
            "supplier": obj.get("description") or partner_key[:8] or obj_key[:8],
            "amount": 0.0,
            "balance_start": start_amount,
            "balance_window_end": end_amount,
            "turnover_2_full_months": turnover_abs,
            "movement_records": int(movement.get("records") or 0),
        })
        item["amount"] += amount
        if partner_key and partner_key != EMPTY:
            partner_keys.add(partner_key)

    names = _resolve_partner_names(session, partner_keys)
    rows = []
    for item in by_object.values():
        supplier_key = item.get("supplier_key") or ""
        supplier_name = names.get(supplier_key) or item.get("supplier") or supplier_key[:8]
        rows.append({
            "order_key": item.get("order_key") or "",
            "order_num": item.get("order_num") or "",
            "order_date": item.get("order_date") or "",
            "object_name": item.get("object_name") or "",
            "supplier_key": supplier_key,
            "supplier": supplier_name,
            "amount": round(float(item.get("amount") or 0), 2),
            "balance_start": round(float(item.get("balance_start") or 0), 2),
            "balance_window_end": round(float(item.get("balance_window_end") or 0), 2),
            "turnover_2_full_months": round(float(item.get("turnover_2_full_months") or 0), 2),
            "movement_records": int(item.get("movement_records") or 0),
            "stability_window_start": window_start.isoformat(),
            "stability_window_end": (window_end - timedelta(days=1)).isoformat(),
        })
    rows.sort(key=lambda entry: (-float(entry.get("amount") or 0), str(entry.get("supplier") or "")))

    register_total_debt = round(sum(current_debt.values()), 2)
    document_total_debt = round(sum(
        float(row.get("ДолгРеглBalance") or 0)
        for row in document_balance_rows
        if float(row.get("ДолгРеглBalance") or 0) > TOLERANCE
    ), 2)
    included_total = round(sum(float(row.get("amount") or 0) for row in rows), 2)

    payload = {
        "na_datu": na_datu.isoformat(),
        "source": DETAIL_SOURCE_TAG,
        "source_register": f"{REGISTER}/Balance",
        "source_document_register": "AccumulationRegister_РасчетыСПоставщикамиПоДокументам/Balance",
        "resource": "ДолгРегл",
        "stability_window": {
            "start": window_start.isoformat(),
            "end_exclusive": window_end.isoformat(),
            "label": f"{window_start.isoformat()}..{(window_end - timedelta(days=1)).isoformat()}",
        },
        "total_dolg_regl": included_total,
        # Оставляем старый ключ как alias для совместимости фронта/кэшей.
        "total_predoplata_regl": included_total,
        "verification": {
            "report_like_register": f"{REGISTER}/Balance",
            "document_register": "AccumulationRegister_РасчетыСПоставщикамиПоДокументам",
            "movement_register": f"{REGISTER}_RecordType",
            "resource": "ДолгРегл",
            "current_positive_objects": len(current_debt),
            "included_stable_objects": len(rows),
            "excluded_positive_objects": excluded_positive,
            "register_total_dolg_regl_positive": register_total_debt,
            "document_register_total_dolg_regl_positive": document_total_debt,
            "included_total_dolg_regl": included_total,
            "object_movement_keys_with_turnover": len(object_movements),
            "document_movement_keys_with_turnover": len(document_movements),
            "rules": [
                "ДолгРеглBalance > 0 на дату среза",
                "ПредоплатаРеглBalance не включается",
                "по объекту расчётов за два полных календарных месяца до даты среза оборот ДолгРегл = 0",
                "остаток ДолгРегл на начало окна, конец окна и дату среза не менялся",
            ],
        },
        "query_protocol": {
            "balance": (
                f"{REGISTER}/Balance Period={(na_datu + timedelta(days=1)).isoformat()}T00:00:00; "
                "select ОбъектРасчетов_Key, ДолгРеглBalance, ПредоплатаРеглBalance"
            ),
            "movements": (
                f"{REGISTER}_RecordType Period >= {window_start.isoformat()} and Period < {window_end.isoformat()}; "
                "select ОбъектРасчетов_Key, РасчетныйДокумент, ДолгРегл, ПредоплатаРегл"
            ),
            "document_movements": (
                "AccumulationRegister_РасчетыСПоставщикамиПоДокументам_RecordType "
                f"Period >= {window_start.isoformat()} and Period < {window_end.isoformat()}; "
                "select РасчетныйДокумент, ЗаказПоставщику_Key, ДолгРегл, ПредоплатаРегл"
            ),
        },
        "rows": rows,
    }
    _save_json(cache_path, payload)
    return payload


def get_supplier_monthly(year: int, ref_month: int) -> dict:
    """
    Помесячные ДЗ/КЗ как дельта между соседними месячными остатками.

    То есть в `dolg_regl` / `predoplata_regl` лежат значения именно
    за календарный месяц, а не накопительный остаток с января.
    Закрывающий остаток месяца тоже сохраняем отдельными полями.
    """
    cache_path = _cache_path_monthly(year, ref_month)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        rows = cached.get("months") or []
        if rows and all(
            "dolg_regl" in r
            and "predoplata_regl" in r
            and "closing_dolg_regl" in r
            and "closing_predoplata_regl" in r
            for r in rows
        ):
            return cached

    today = date.today()

    snap_dates: list[tuple[int, date]] = []
    for mm in range(1, ref_month + 1):
        d = _month_end(year, mm)
        if d > today:
            d = today
        snap_dates.append((mm, d))

    if not snap_dates:
        payload = {
            "year": year,
            "ref_month": ref_month,
            "source": SOURCE_TAG,
            "months": [],
        }
        _save_json(cache_path, payload)
        return payload

    session = requests.Session()
    session.auth = AUTH
    allowed_obj_keys = _load_supplier_obj_keys(session)
    logger.info(
        "calc_postavshchiki: monthly for %s, ref_month=%d",
        year,
        ref_month,
    )

    rows_out: list[dict] = []
    prev_closing_kz = 0.0
    prev_closing_dz = 0.0
    for mm, na_datu in snap_dates:
        snapshot = _build_snapshot(session, na_datu, allowed_obj_keys)
        closing_kz = float(snapshot.get("total_dolg_regl") or 0)
        closing_dz = float(snapshot.get("total_predoplata_regl") or 0)
        month_kz = round(closing_kz - prev_closing_kz, 2)
        month_dz = round(closing_dz - prev_closing_dz, 2)

        _save_json(_cache_path_snapshot(na_datu), snapshot)
        rows_out.append({
            "year": year,
            "month": mm,
            "na_datu": na_datu.isoformat(),
            "dolg_regl": month_kz,                  # КЗ именно за месяц
            "predoplata_regl": month_dz,            # ДЗ именно за месяц
            "closing_dolg_regl": round(closing_kz, 2),
            "closing_predoplata_regl": round(closing_dz, 2),
        })
        prev_closing_kz = closing_kz
        prev_closing_dz = closing_dz

    payload = {
        "year": year,
        "ref_month": ref_month,
        "source": SOURCE_TAG,
        "months": rows_out,
    }
    _save_json(cache_path, payload)
    return payload


# ──────────────────────────────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────────────────────────────
def _main_cli() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    _print = functools.partial(print, flush=True)

    args = sys.argv[1:]
    if "--monthly" in args:
        idx = args.index("--monthly")
        year = int(args[idx + 1]) if idx + 1 < len(args) else date.today().year
        today = date.today()
        ref_m = 12 if year < today.year else today.month
        data = get_supplier_monthly(year, ref_m)
        _print("=" * 60)
        _print(f"  ПОСТАВЩИКИ ПО МЕСЯЦАМ: {year} (ref_month={ref_m})")
        _print(f"  источник: РасчетыСПоставщикамиПоСрокам/Balance (tag={data.get('source')})")
        _print("=" * 60)
        _print(f"  {'Мес.':<6s} {'На дату':<12s} {'КЗ за мес.':>18s} {'ДЗ за мес.':>18s}")
        for r in data["months"]:
            _print(
                f"  {r['month']:<6d} {r['na_datu']:<12s} "
                f"{r['dolg_regl']:>18,.2f} {r['predoplata_regl']:>18,.2f}"
            )
        return

    na_datu_str = args[0] if args else date.today().isoformat()
    na_datu = date.fromisoformat(na_datu_str)
    data = get_supplier_snapshot(na_datu)
    _print("=" * 60)
    _print(f"  ПОСТАВЩИКИ на {data['na_datu']}")
    _print(f"  источник: РасчетыСПоставщикамиПоСрокам/Balance (tag={data.get('source')})")
    _print("=" * 60)
    _print(f"  КЗ (наш долг поставщикам):   {data['total_dolg_regl']:>18,.2f}")
    _print(f"  ДЗ (наши авансы поставщикам): {data['total_predoplata_regl']:>18,.2f}")


if __name__ == "__main__":
    _main_cli()
