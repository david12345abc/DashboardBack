"""
Действующие дилеры за скользящие 12 месяцев (коммерческий блок).

Период: [as_of − 12 месяцев (тот же календарный день), as_of], время — 00:00 … 23:59:59.

Признак «дилер»: партнёры из актуального (последнего по дате) проведённого документа
Document_ТД_СоставСегмента для каждого сегмента Catalog_СегментыПартнеров,
у которого в наименовании есть «дилер» (без учёта регистра).

Активность:
  • оплаты — движения AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType
    (ветка как calc_dengi_fact: объект расчётов → ЗаказКлиента), сумма СуммаОплатыРегл;
  • отгрузки — AccumulationRegister_РаспоряженияНаОтгрузку_RecordType (расход),
    сумма как calc_otgruzki_fact: abs(Сумма) * курс валюты заказа.

Фильтры заказа: проведён, не помечен на удаление, не ТД_НеУчитыватьВПланФакте,
подразделение ∈ коммерческие отделы (как calc_dengi_fact / calc_otgruzki_fact),
исключения партнёров-перепродаж как в calc_otgruzki_fact.

Кэш:
  • JSON-файл на дату as_of (см. active_dealers_cache_path) — основной кэш между запросами;
  • короткий in-process кэш (TTL) — снижает повторные чтения 1С при частых запросах.
"""
from __future__ import annotations

import calendar
import json
import logging
import threading
import time as time_wall
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from . import cache_manager
from .commercial_department_aliases import normalize_commercial_dept_guid
from .calc_dengi_fact import (
    BATCH,
    DEPT_SET,
    EMPTY,
    OPBO_DEPT,
    ORDER_TYPE_MARKER,
    REG_DS,
    _batch_load_catalog,
    _is_empty_ref,
)
from .calc_otgruzki_fact import (
    CURRENCY_KEYS,
    EXCHANGE_RATES,
    EXCLUDE_PARTNER_KEYS,
    EXCLUDE_PARTNER_KEYS_NO_MGS,
    EXCLUDE_PARTNER_NAMES,
    EXCLUDE_PARTNER_NAMES_NO_MGS,
    KEEPER_TRANSFER_PARTNER_KEYS,
    KEEPER_TRANSFER_TYPE,
    ORDER_TYPE,
    REG_OTGRUZKA,
)
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_VERSION = 2

# In-process: один ключ на дату as_of (ISO), значение (monotonic_ts, payload).
_mem_lock = threading.Lock()
_mem_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_MEM_TTL_SEC = 180.0

SEGMENT_CATALOG = "Catalog_СегментыПартнеров"
COMPOSITION_DOC = "Document_ТД_СоставСегмента"
COMPOSITION_TAB = "Document_ТД_СоставСегмента_Партнеры"


def _minus_12_months(d: date) -> date:
    """Тот же день месяца год назад (с усечением дня для февраля)."""
    y, m = d.year, d.month - 12
    while m <= 0:
        m += 12
        y -= 1
    last = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, last))


def _period_bounds(as_of: date) -> tuple[datetime, datetime]:
    start_d = _minus_12_months(as_of)
    d0 = datetime.combine(start_d, time.min)
    d1 = datetime.combine(as_of, time.max)
    return d0, d1


def _is_test_partner_name(name: str) -> bool:
    n = (name or "").lower()
    if "тест" in n or "test" in n:
        return True
    if "техническ" in n and "контрагент" in n:
        return True
    return False


def _load_dealer_segments(session: requests.Session) -> list[dict[str, Any]]:
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{SEGMENT_CATALOG}?$format=json"
            f"&$select=Ref_Key,Description,Code&$top=5000&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=3, label="Dealers/Segments")
        if r is None or not r.ok:
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    out = []
    for row in rows:
        desc = (row.get("Description") or "").lower()
        if "дилер" in desc:
            out.append(row)
    return out


def _latest_composition_doc_key(
    session: requests.Session, segment_key: str,
) -> str | None:
    flt = quote(
        f"Сегмент_Key eq guid'{segment_key}' and Posted eq true "
        f"and DeletionMark eq false",
        safe="",
    )
    url = (
        f"{BASE}/{COMPOSITION_DOC}?$format=json&$filter={flt}"
        f"&$select=Ref_Key,Date&$orderby=Date desc&$top=1"
    )
    r = request_with_retry(session, url, timeout=60, retries=3, label="Dealers/CompDoc")
    if r is None or not r.ok:
        return None
    vals = r.json().get("value", [])
    if not vals:
        return None
    return vals[0].get("Ref_Key")


def _load_tab_partners_for_doc(
    session: requests.Session, doc_key: str,
) -> list[str]:
    flt = quote(f"Ref_Key eq guid'{doc_key}'", safe="")
    url = (
        f"{BASE}/{COMPOSITION_TAB}?$format=json&$filter={flt}"
        f"&$select=Партнер_Key&$top=10000"
    )
    r = request_with_retry(session, url, timeout=120, retries=3, label="Dealers/CompTab")
    if r is None or not r.ok:
        return []
    keys = []
    for row in r.json().get("value", []):
        pk = row.get("Партнер_Key")
        if pk and pk != EMPTY:
            keys.append(pk)
    return keys


def build_dealer_partner_keys(session: requests.Session) -> tuple[set[str], dict[str, Any]]:
    """Множество Ref_Key партнёров-дилеров и метаданные определения."""
    segments = _load_dealer_segments(session)
    meta: dict[str, Any] = {
        "method": "segment_composition_document",
        "description_ru": (
            "Партнёры из табличной части «Партнеры» последнего проведённого "
            "документа «ТД: Состав сегмента» по каждому сегменту, "
            "в наименовании которого есть слово «дилер»."
        ),
        "segments": [
            {"Ref_Key": s.get("Ref_Key"), "Description": s.get("Description"), "Code": s.get("Code")}
            for s in segments
        ],
        "documents": [],
    }
    if not segments:
        meta["warning"] = (
            "Не найдено ни одного сегмента партнёров с «дилер» в наименовании. "
            "Проверьте справочник «Сегменты партнёров» или уточните правило отбора."
        )
        return set(), meta

    dealer_keys: set[str] = set()
    for seg in segments:
        sk = seg.get("Ref_Key")
        if not sk:
            continue
        doc_key = _latest_composition_doc_key(session, sk)
        if not doc_key:
            meta["documents"].append({
                "segment_key": sk,
                "segment_description": seg.get("Description"),
                "document_key": None,
                "partner_count": 0,
            })
            continue
        pkeys = _load_tab_partners_for_doc(session, doc_key)
        dealer_keys.update(pkeys)
        meta["documents"].append({
            "segment_key": sk,
            "segment_description": seg.get("Description"),
            "document_key": doc_key,
            "partner_count": len(pkeys),
        })
    return dealer_keys, meta


def _load_ds_rows_range(
    session: requests.Session, d0: datetime, d1: datetime,
) -> list[dict]:
    d_from = d0.strftime("%Y-%m-%dT%H:%M:%S")
    d_to = d1.strftime("%Y-%m-%dT%H:%M:%S")
    sel = (
        "Period,Active,ХозяйственнаяОперация,Партнер_Key,Подразделение_Key,"
        "СуммаОплаты,СуммаОплатыРегл,СуммаПостоплаты,СуммаПостоплатыРегл,"
        "ОбъектРасчетов,ОбъектРасчетов_Type,"
        "РасчетныйДокумент,РасчетныйДокумент_Type,"
        "Recorder,Recorder_Type,Сторно"
    )
    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}' and Active eq true",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = f"{BASE}/{REG_DS}?$format=json&$top=5000&$skip={skip}&$filter={flt}&$select={sel}"
        r = request_with_retry(session, url, timeout=120, retries=4, label="Dealers/DS")
        if r is None or not r.ok:
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return [x for x in rows if not x.get("Сторно")]


def _load_rashod_rows_range(session: requests.Session, d0: datetime, d1: datetime) -> list[dict]:
    d_from = d0.strftime("%Y-%m-%dT%H:%M:%S")
    d_to = d1.strftime("%Y-%m-%dT%H:%M:%S")
    flt = quote(
        f"Period ge datetime'{d_from}' and Period le datetime'{d_to}' "
        f"and Active eq true and ВидДвиженияРегистра eq 'Расход'",
        safe="",
    )
    sel = (
        "Period,Active,Recorder_Type,Распоряжение,Распоряжение_Type,"
        "ВидДвиженияРегистра,Сумма,Сторно"
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{REG_OTGRUZKA}?$format=json"
            f"&$filter={flt}&$select={sel}"
            f"&$top=5000&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="Dealers/Otgr")
        if r is None or not r.ok:
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return [x for x in rows if not x.get("Сторно")]


def _batch_load_orders_dealers(
    session: requests.Session, order_keys: list[str],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for i in range(0, len(order_keys), BATCH):
        batch = order_keys[i : i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json"
            f"&$filter={flt}"
            f"&$select=Ref_Key,Подразделение_Key,Партнер_Key,Валюта_Key,"
            f"Соглашение_Key,ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи,"
            f"Posted,DeletionMark,Статус"
            f"&$top={BATCH}"
        )
        r = request_with_retry(session, url, timeout=60, retries=3, label="Dealers/Orders")
        if r is None or not r.ok:
            continue
        for it in r.json().get("value", []):
            result[it["Ref_Key"]] = {
                "dept": it.get("Подразделение_Key", ""),
                "partner": it.get("Партнер_Key", ""),
                "currency": it.get("Валюта_Key", ""),
                "agreement": it.get("Соглашение_Key", ""),
                "ne_uchit": it.get("ТД_НеУчитыватьВПланФакте", False),
                "soprovozhd": it.get("ТД_СопровождениеПродажи", False),
                "posted": it.get("Posted", False),
                "deletion": it.get("DeletionMark", False),
                "status": (it.get("Статус") or "").strip(),
            }
    return result


def _batch_load_partner_meta(session: requests.Session, keys: list[str]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for i in range(0, len(keys), BATCH):
        batch = keys[i : i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Catalog_Партнеры?$format=json"
            f"&$filter={flt}&$select=Ref_Key,Description,Code&$top={BATCH}"
        )
        r = request_with_retry(session, url, timeout=60, retries=3, label="Dealers/Partners")
        if r is None or not r.ok:
            continue
        for it in r.json().get("value", []):
            rk = it["Ref_Key"]
            out[rk] = {
                "name": (it.get("Description") or "").strip(),
                "code": (it.get("Code") or "").strip(),
            }
    return out


def _order_ok_for_activity(od: dict[str, Any]) -> bool:
    if not od.get("posted"):
        return False
    if od.get("deletion"):
        return False
    if od.get("ne_uchit"):
        return False
    dept = normalize_commercial_dept_guid(od.get("dept", ""))
    if dept not in DEPT_SET or _is_empty_ref(dept):
        return False
    if _is_empty_ref(od.get("agreement")):
        return False
    return True


def _aggregate_payments(
    session: requests.Session,
    ds_rows: list[dict],
    dealer_keys: set[str],
    d0: datetime,
    d1: datetime,
) -> dict[str, dict[str, Any]]:
    """partner_key -> {last_dt, sum, hits}"""
    obj_keys: set[str] = set()
    for row in ds_rows:
        ok = row.get("ОбъектРасчетов", "")
        if ok and ok != EMPTY:
            obj_keys.add(ok)
    catalog = _batch_load_catalog(session, obj_keys)
    order_keys: set[str] = set()
    for cat in catalog.values():
        if ORDER_TYPE_MARKER in (cat.get("obj_type") or ""):
            okey = cat.get("obj", "")
            if okey and okey != EMPTY:
                order_keys.add(okey)
    orders = _batch_load_orders_dealers(session, sorted(order_keys))

    partner_keys: set[str] = set()
    for od in orders.values():
        pk = od.get("partner", "")
        if pk and pk != EMPTY:
            partner_keys.add(pk)
    meta_p = _batch_load_partner_meta(session, sorted(partner_keys))
    names = {k: v["name"] for k, v in meta_p.items()}
    excl_full = set(EXCLUDE_PARTNER_KEYS) | {
        k for k, v in names.items() if v in EXCLUDE_PARTNER_NAMES
    }
    excl_no_mgs = set(EXCLUDE_PARTNER_KEYS_NO_MGS) | {
        k for k, v in names.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS
    }

    agg: dict[str, dict[str, Any]] = {}

    for row in ds_rows:
        try:
            pdt = datetime.fromisoformat((row.get("Period") or "").replace("Z", "+00:00")[:19])
        except ValueError:
            continue
        if pdt < d0 or pdt > d1:
            continue

        obj_key = row.get("ОбъектРасчетов", "")
        if _is_empty_ref(obj_key):
            continue
        cat = catalog.get(obj_key)
        if not cat or ORDER_TYPE_MARKER not in (cat.get("obj_type") or ""):
            continue
        order = orders.get(cat.get("obj", ""))
        if not order or not _order_ok_for_activity(order):
            continue

        dealer_pk = order.get("partner", "")
        if dealer_pk not in dealer_keys:
            continue
        if _is_test_partner_name(names.get(dealer_pk, "")):
            continue

        order_dept = normalize_commercial_dept_guid(order.get("dept", ""))
        cat_partner = cat.get("partner", "")
        if order_dept == OPBO_DEPT:
            if cat_partner in excl_no_mgs:
                continue
        else:
            if cat_partner in excl_full and not order.get("soprovozhd"):
                continue

        if row.get("ХозяйственнаяОперация") == "ВозвратОплатыКлиенту":
            continue
        amt = float(row.get("СуммаОплатыРегл") or row.get("СуммаОплаты") or 0)
        if not amt:
            continue

        slot = agg.setdefault(dealer_pk, {"sum": 0.0, "first": None, "last": None})
        slot["sum"] += amt
        first = slot["first"]
        if first is None or pdt < first:
            slot["first"] = pdt
        last = slot["last"]
        if last is None or pdt > last:
            slot["last"] = pdt

    return agg


def _aggregate_shipments(
    session: requests.Session,
    rashod_rows: list[dict],
    dealer_keys: set[str],
    d0: datetime,
    d1: datetime,
) -> dict[str, dict[str, Any]]:
    order_keys_set: set[str] = set()
    for row in rashod_rows:
        ok = row.get("Распоряжение", "")
        if ok and ok != EMPTY and row.get("Распоряжение_Type") == ORDER_TYPE:
            order_keys_set.add(ok)

    orders = _batch_load_orders_dealers(session, sorted(order_keys_set))
    partner_keys_set: set[str] = set()
    for od in orders.values():
        pk = od.get("partner", "")
        if pk and pk != EMPTY:
            partner_keys_set.add(pk)
    meta_p = _batch_load_partner_meta(session, sorted(partner_keys_set))
    names = {k: v["name"] for k, v in meta_p.items()}
    excl_full = set(EXCLUDE_PARTNER_KEYS) | {
        k for k, v in names.items() if v in EXCLUDE_PARTNER_NAMES
    }
    excl_no_mgs = set(EXCLUDE_PARTNER_KEYS_NO_MGS) | {
        k for k, v in names.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS
    }

    agg: dict[str, dict[str, Any]] = {}

    for row in rashod_rows:
        try:
            pdt = datetime.fromisoformat((row.get("Period") or "").replace("Z", "+00:00")[:19])
        except ValueError:
            continue
        if pdt < d0 or pdt > d1:
            continue

        ok = row.get("Распоряжение", "")
        if not ok or ok == EMPTY:
            continue
        if row.get("Распоряжение_Type") != ORDER_TYPE:
            continue
        od = orders.get(ok)
        if not od or not _order_ok_for_activity(od):
            continue

        pk = od["partner"]
        if pk not in dealer_keys:
            continue
        if _is_test_partner_name(names.get(pk, "")):
            continue

        dept = normalize_commercial_dept_guid(od["dept"])
        soprovozhd = od["soprovozhd"]
        if dept == OPBO_DEPT:
            keeper_transfer_allowed = (
                pk in KEEPER_TRANSFER_PARTNER_KEYS
                and row.get("Recorder_Type") == KEEPER_TRANSFER_TYPE
            )
            if pk in excl_no_mgs and not keeper_transfer_allowed:
                continue
        else:
            if pk in excl_full and not soprovozhd:
                continue

        amount = float(row.get("Сумма") or 0)
        if not amount:
            continue
        cur_code = CURRENCY_KEYS.get(od["currency"], "RUB")
        rate = EXCHANGE_RATES.get(cur_code, 1.0)
        amt = abs(amount) * rate

        slot = agg.setdefault(pk, {"sum": 0.0, "first": None, "last": None})
        slot["sum"] += amt
        first = slot["first"]
        if first is None or pdt < first:
            slot["first"] = pdt
        last = slot["last"]
        if last is None or pdt > last:
            slot["last"] = pdt

    return agg


def _cache_path(as_of: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"komdir_active_dealers_{as_of.isoformat()}_{CACHE_VERSION}.json"


def _new_dealers_cache_path(as_of: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"komdir_new_dealers_{as_of.isoformat()}_{CACHE_VERSION}.json"


def active_dealers_cache_path(as_of: date | None = None) -> Path:
    """Путь к JSON-кэшу отчёта (для cache_manager.is_cache_fresh / прогрева)."""
    return _cache_path(as_of or date.today())


def new_dealers_cache_path(as_of: date | None = None) -> Path:
    """Путь к JSON-кэшу новых дилеров (для cache_manager.is_cache_fresh / прогрева)."""
    return _new_dealers_cache_path(as_of or date.today())


def _load_cache(as_of: date) -> dict[str, Any] | None:
    p = _cache_path(as_of)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _load_latest_stale_cache() -> dict[str, Any] | None:
    candidates = sorted(
        CACHE_DIR.glob(f"komdir_active_dealers_*_{CACHE_VERSION}.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    for p in candidates:
        try:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get("cache_version") == CACHE_VERSION:
                data = dict(data)
                data["cache_refresh_status"] = "running"
                return data
        except (OSError, json.JSONDecodeError):
            continue
    return None


def _save_cache(as_of: date, payload: dict[str, Any]) -> None:
    try:
        to_write = {
            **payload,
            "cached_at": datetime.now().isoformat(timespec="seconds"),
        }
        with open(_cache_path(as_of), "w", encoding="utf-8") as f:
            json.dump(to_write, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _mem_get(key: str) -> dict[str, Any] | None:
    now = time_wall.monotonic()
    with _mem_lock:
        hit = _mem_cache.get(key)
        if hit and now - hit[0] < _MEM_TTL_SEC:
            return hit[1]
    return None


def _mem_set(key: str, payload: dict[str, Any]) -> None:
    with _mem_lock:
        _mem_cache[key] = (time_wall.monotonic(), payload)


def compute_active_dealers_report(as_of: date | None = None) -> dict[str, Any]:
    """
    Полный отчёт: meta + count + rows (дилер, код/Ref, даты, суммы, основание).

    as_of: дата формирования (по умолчанию сегодня).
    """
    as_of = as_of or date.today()
    mem_key = as_of.isoformat()
    mem_hit = _mem_get(mem_key)
    if mem_hit is not None:
        return mem_hit

    cached = _load_cache(as_of)
    if cached is not None and cached.get("cache_version") == CACHE_VERSION:
        _mem_set(mem_key, cached)
        return cached
    if not cache_manager.is_force_compute_context():
        stale = _load_latest_stale_cache()
        if stale is not None:
            _mem_set(mem_key, stale)
            return stale

    session = requests.Session()
    session.auth = AUTH

    d0, d1 = _period_bounds(as_of)
    dealer_keys, dealer_meta = build_dealer_partner_keys(session)

    if not dealer_keys:
        payload = {
            "cache_version": CACHE_VERSION,
            "as_of": as_of.isoformat(),
            "period_from": d0.date().isoformat(),
            "period_to": d1.date().isoformat(),
            "dealer_detection": dealer_meta,
            "active_dealers_count": 0,
            "rows": [],
            "pie_value": 0,
            "has_data": bool(dealer_meta.get("segments")),
        }
        _save_cache(as_of, payload)
        _mem_set(mem_key, payload)
        return payload

    ds_rows = _load_ds_rows_range(session, d0, d1)
    rashod_rows = _load_rashod_rows_range(session, d0, d1)

    pay = _aggregate_payments(session, ds_rows, dealer_keys, d0, d1)
    ship = _aggregate_shipments(session, rashod_rows, dealer_keys, d0, d1)

    all_keys = sorted(set(pay.keys()) | set(ship.keys()))
    meta_all = _batch_load_partner_meta(session, all_keys)

    rows_out: list[dict[str, Any]] = []
    for pk in all_keys:
        p_pay = pay.get(pk, {})
        p_sh = ship.get(pk, {})
        sum_pay = float(p_pay.get("sum") or 0)
        sum_sh = float(p_sh.get("sum") or 0)
        last_pay = p_pay.get("last")
        last_sh = p_sh.get("last")
        has_pay = sum_pay != 0 or last_pay is not None
        has_sh = sum_sh != 0 or last_sh is not None
        if has_pay and has_sh:
            basis = "оплата и отгрузка"
        elif has_pay:
            basis = "оплата"
        elif has_sh:
            basis = "отгрузка"
        else:
            basis = ""

        pm = meta_all.get(pk) or {}
        rows_out.append({
            "dealer_name": (pm.get("name") or "").strip() or pk,
            "dealer_ref": pk,
            "dealer_code": pm.get("code", ""),
            "last_payment_date": last_pay.date().isoformat() if last_pay else "",
            "payments_sum": round(sum_pay, 2),
            "last_shipment_date": last_sh.date().isoformat() if last_sh else "",
            "shipments_sum": round(sum_sh, 2),
            "basis": basis,
        })

    rows_out.sort(key=lambda r: (-(r["payments_sum"] + r["shipments_sum"]), r["dealer_name"]))

    payload = {
        "cache_version": CACHE_VERSION,
        "as_of": as_of.isoformat(),
        "period_from": d0.date().isoformat(),
        "period_to": d1.date().isoformat(),
        "dealer_detection": dealer_meta,
        "active_dealers_count": len(rows_out),
        "rows": rows_out,
        "pie_value": float(len(rows_out)),
        "has_data": True,
    }
    _save_cache(as_of, payload)
    _mem_set(mem_key, payload)
    return payload


def get_active_dealers_count(as_of: date | None = None) -> int:
    return int(compute_active_dealers_report(as_of=as_of).get("active_dealers_count") or 0)


def _load_new_dealers_cache(as_of: date) -> dict[str, Any] | None:
    p = _new_dealers_cache_path(as_of)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") == CACHE_VERSION:
        return data
    return None


def _save_new_dealers_cache(as_of: date, payload: dict[str, Any]) -> None:
    try:
        with open(_new_dealers_cache_path(as_of), "w", encoding="utf-8") as f:
            json.dump(
                {**payload, "cached_at": datetime.now().isoformat(timespec="seconds")},
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def compute_new_dealers_report(as_of: date | None = None) -> dict[str, Any]:
    """Новые дилеры: первая оплата и первая отгрузка обе с начала текущего года.

    Дилерский признак и фильтры заказов/исключений совпадают с отчётом
    действующих дилеров.
    """
    as_of = as_of or date.today()
    mem_key = f"new:{as_of.isoformat()}"
    mem_hit = _mem_get(mem_key)
    if mem_hit is not None:
        return mem_hit

    cached = _load_new_dealers_cache(as_of)
    if cached is not None:
        _mem_set(mem_key, cached)
        return cached

    session = requests.Session()
    session.auth = AUTH

    year_start = date(as_of.year, 1, 1)
    d0 = datetime(2000, 1, 1)
    d1 = datetime.combine(as_of, time.max)
    dealer_keys, dealer_meta = build_dealer_partner_keys(session)

    if not dealer_keys:
        payload = {
            "cache_version": CACHE_VERSION,
            "as_of": as_of.isoformat(),
            "period_from": year_start.isoformat(),
            "period_to": as_of.isoformat(),
            "history_from": d0.date().isoformat(),
            "dealer_detection": dealer_meta,
            "new_dealers_count": 0,
            "rows": [],
            "has_data": bool(dealer_meta.get("segments")),
        }
        _save_new_dealers_cache(as_of, payload)
        _mem_set(mem_key, payload)
        return payload

    ds_rows = _load_ds_rows_range(session, d0, d1)
    rashod_rows = _load_rashod_rows_range(session, d0, d1)
    pay = _aggregate_payments(session, ds_rows, dealer_keys, d0, d1)
    ship = _aggregate_shipments(session, rashod_rows, dealer_keys, d0, d1)

    candidate_keys = sorted(set(pay.keys()) & set(ship.keys()))
    meta_all = _batch_load_partner_meta(session, candidate_keys)
    rows_out: list[dict[str, Any]] = []
    for pk in candidate_keys:
        first_payment = pay.get(pk, {}).get("first")
        first_shipment = ship.get(pk, {}).get("first")
        if first_payment is None or first_shipment is None:
            continue
        if not (year_start <= first_payment.date() <= as_of):
            continue
        if not (year_start <= first_shipment.date() <= as_of):
            continue
        pm = meta_all.get(pk) or {}
        rows_out.append({
            "dealer_name": (pm.get("name") or "").strip() or pk,
            "dealer_ref": pk,
            "dealer_code": pm.get("code", ""),
            "first_payment_date": first_payment.date().isoformat(),
            "first_shipment_date": first_shipment.date().isoformat(),
            "payments_sum_history": round(float(pay.get(pk, {}).get("sum") or 0.0), 2),
            "shipments_sum_history": round(float(ship.get(pk, {}).get("sum") or 0.0), 2),
        })

    rows_out.sort(key=lambda r: (r["first_payment_date"], r["first_shipment_date"], r["dealer_name"]))
    payload = {
        "cache_version": CACHE_VERSION,
        "as_of": as_of.isoformat(),
        "period_from": year_start.isoformat(),
        "period_to": as_of.isoformat(),
        "history_from": d0.date().isoformat(),
        "dealer_detection": dealer_meta,
        "new_dealers_count": len(rows_out),
        "rows": rows_out,
        "has_data": True,
    }
    _save_new_dealers_cache(as_of, payload)
    _mem_set(mem_key, payload)
    return payload


def get_new_dealers_count(as_of: date | None = None) -> int:
    return int(compute_new_dealers_report(as_of=as_of).get("new_dealers_count") or 0)
