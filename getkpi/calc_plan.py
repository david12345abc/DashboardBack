"""
calc_plan.py — Плановые показатели: Договоры, Деньги, Отгрузки (помесячно).

Источники:
  - маркетинговый план: AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType;
  - ожидаемые деньги: AccumulationRegister_РасчетыСКлиентами_RecordType
    (аналог РасчетыСКлиентами.Остатки);
  - ожидаемые отгрузки: AccumulationRegister_РаспоряженияНаОтгрузку_RecordType
    (аналог РаспоряженияНаОтгрузку.Обороты);
  - ожидаемые договоры: Document_КоммерческоеПредложениеКлиенту по плановой дате подписания.

Логика (1С-запрос):
  - Период МЕЖДУ НачалоПериода(месяц) И КонецПериода(месяц)
  - Подразделение <> ПустаяСсылка
  - Подразделение В (&ПодразделениеСписок)
  - СУММА по ВидПланирования:
    Отгрузки → МП_СуммаОтгрузки
    Деньги   → МП_ОжидаемаяСуммаОплаты
    Договоры → МП_СуммаДоговораПлан

API:
  from getkpi.calc_plan import get_plans_monthly
  data = get_plans_monthly(2026, 3)
  data = get_plans_monthly(2026, 3, dept_guid='49480c10-...')
"""
from __future__ import annotations

import calendar
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

from .commercial_department_aliases import (
    COMMERCIAL_DEPT_ALIASES,
    normalize_commercial_dept_guid,
)
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборуд. и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
}
DEPT_SET = frozenset(DEPARTMENTS.keys())

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

REG = "AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType"
EXPECTED_DOC = "Document_ТД_ПланированиеПроцессаПродажЕжемесячное"
EXPECTED_CONTRACT_DOC = "Document_КоммерческоеПредложениеКлиенту"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_VERSION = 5
PLAN_KEYS = ("dengi", "otgruzki", "dogovory")
EXPECTED_KEYS = {
    "dengi": "dengi_expected",
    "otgruzki": "otgruzki_expected",
    "dogovory": "dogovory_expected",
}
EXPECTED_TABLES = {
    "dengi": "Деньги",
    "otgruzki": "Отгрузки",
    "dogovory": "Договоры",
}
OPBO_DEPT = "7587c178-92f6-11f0-96f9-6cb31113810e"
EXPECTED_DEPT_SET = frozenset((*DEPT_SET, *COMMERCIAL_DEPT_ALIASES.keys()))

REG_CUSTOMER_SETTLEMENTS = "AccumulationRegister_РасчетыСКлиентами_RecordType"
REG_SHIPMENT_ORDERS = "AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
OBJ_SETTLEMENTS = "Catalog_ОбъектыРасчетов"
PAYMENT_STAGES = "Document_ЗаказКлиента_ЭтапыГрафикаОплаты"

PREDEFINED_VALUES = "Catalog_ТД_ПредопределенныеЗначения"
PREDEFINED_VALUES_EXTRA = "Catalog_ТД_ПредопределенныеЗначения_ДополнительныеЗначения"
PREDEFINED_RESALE_PARTNERS_REF = "8180316b-7c73-11e9-828e-ac1f6b05524d"
PREDEFINED_MGS_PARTNER_REF = "5bd32178-cf94-11e9-829b-ac1f6b05524d"
ORDER_TYPE_MARKER = "Document_ЗаказКлиента"

CURRENCY_KEYS = {
    "0a7c6f22-e1b6-11df-963e-001cc4d04388": "USD",
    "d328a18d-7405-11e0-81cd-001583b3d75c": "EUR",
    "095e2c36-45dc-11ec-8756-ac1f6b05524d": "BYN",
    "e2bc7bc0-de2e-11ef-95fc-6cb31113810e": "KZT",
    "3fdf75b4-6252-11e7-812d-001e67112509": "RUB",
}

EXCHANGE_RATES = {
    "USD": 90.0,
    "EUR": 98.0,
    "BYN": 28.0,
    "KZT": 0.19,
    "RUB": 1.0,
}


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"plans_{year}_{month:02d}.json"


def _monthly_cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"plans_monthly_{year}_{ref_month:02d}.json"


def _load_cache(year: int, month: int) -> dict | None:
    p = _cache_path(year, month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if (
            data.get("cache_date") == date.today().isoformat()
            and data.get("cache_version") == CACHE_VERSION
            and "by_dept" in data
            and all(k in data for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()))
        ):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, month: int, totals: dict, by_dept: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), "w", encoding="utf-8") as f:
            json.dump({
                "cache_date": date.today().isoformat(),
                "cache_version": CACHE_VERSION,
                **totals,
                "by_dept": by_dept,
            }, f, ensure_ascii=False)
    except OSError:
        pass


def _load_register(session: requests.Session,
                   year: int, ref_month: int) -> list[dict]:
    """Загрузить записи регистра за январь–ref_month."""
    d_from = f"{year}-01-01T00:00:00"
    if ref_month == 12:
        d_to = f"{year + 1}-01-01T00:00:00"
    else:
        d_to = f"{year}-{ref_month + 1:02d}-01T00:00:00"

    sel = quote("Period,Active,Подразделение_Key,ВидПланирования,Сумма", safe=",_")
    flt = quote(
        f"Period ge datetime'{d_from}' and Period lt datetime'{d_to}' and Active eq true",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{REG}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="Plans")
        if r is None:
            logger.error("Plans: request dropped after retries")
            break
        if not r.ok:
            logger.error("Plans register HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _load_expected_documents(session: requests.Session,
                             year: int, ref_month: int) -> list[dict]:
    """Загрузить месячные документы ожиданий по процессу продаж за январь–ref_month."""
    d_from = f"{year}-01-01T00:00:00"
    if ref_month == 12:
        d_to = f"{year + 1}-01-01T00:00:00"
    else:
        d_to = f"{year}-{ref_month + 1:02d}-01T00:00:00"

    sel = quote(
        "Ref_Key,Date,Posted,DeletionMark,ДатаПланирования,Ответственный_Key,Подразделение_Key,Статус,"
        "Деньги,Отгрузки,Договоры",
        safe=",_",
    )
    flt = quote(
        f"ДатаПланирования ge datetime'{d_from}' and ДатаПланирования lt datetime'{d_to}' "
        f"and Posted eq true and DeletionMark eq false",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{EXPECTED_DOC}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="ExpectedPlans")
        if r is None:
            logger.error("Expected plans: request dropped after retries")
            break
        if not r.ok:
            logger.error("Expected plans HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _load_expected_contract_proposals(session: requests.Session,
                                      year: int,
                                      ref_month: int) -> list[dict]:
    """Коммерческие предложения с плановой датой подписания договора."""
    d_from = f"{year}-01-01T00:00:00"
    if ref_month == 12:
        d_to = f"{year + 1}-01-01T00:00:00"
    else:
        d_to = f"{year}-{ref_month + 1:02d}-01T00:00:00"

    sel = quote(
        "Ref_Key,Number,Date,Posted,DeletionMark,ДатаПодписанияПлан,Менеджер_Key,"
        "СуммаДокумента,СуммаБазыКБ,Статус,СогласованоСКлиентом",
        safe=",_",
    )
    flt = quote(
        f"ДатаПодписанияПлан ge datetime'{d_from}' and ДатаПодписанияПлан lt datetime'{d_to}' "
        f"and DeletionMark eq false",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{EXPECTED_CONTRACT_DOC}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="ExpectedContracts")
        if r is None:
            logger.error("Expected contracts: request dropped after retries")
            break
        if not r.ok:
            logger.error("Expected contracts HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _month_end_exclusive(year: int, month: int) -> str:
    if month == 12:
        return f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month + 1:02d}-01T00:00:00"


def _month_start(year: int, month: int) -> str:
    return f"{year}-{month:02d}-01T00:00:00"


def _month_from_date(value: str | None) -> int | None:
    if not value or len(value) < 7:
        return None
    try:
        return int(value[5:7])
    except (ValueError, IndexError):
        return None


def _normalize_expected_dept(dept: str | None) -> str:
    return normalize_commercial_dept_guid(dept or "")


def _is_expected_dept(dept: str | None) -> bool:
    return bool(dept and dept != EMPTY and dept in EXPECTED_DEPT_SET)


def _currency_rate(currency_key: str | None) -> float:
    return EXCHANGE_RATES.get(CURRENCY_KEYS.get(currency_key or "", "RUB"), 1.0)


def _load_resale_partner_keys(session: requests.Session) -> set[str]:
    flt = quote(f"Ref_Key eq guid'{PREDEFINED_RESALE_PARTNERS_REF}'", safe="")
    url = (
        f"{BASE}/{PREDEFINED_VALUES_EXTRA}?$format=json"
        f"&$filter={flt}&$select=Ref_Key,Значение,Значение_Type&$top=5000"
    )
    r = request_with_retry(session, url, timeout=30, retries=3, label="Plans/ResalePartners")
    if r is None or not r.ok:
        return set()
    result: set[str] = set()
    try:
        for row in r.json().get("value", []):
            val = row.get("Значение")
            typ = row.get("Значение_Type") or ""
            if val and "Catalog_Партнеры" in typ:
                result.add(val)
    except Exception:
        return set()
    return result


def _load_mgs_partner_key(session: requests.Session) -> str | None:
    flt = quote(f"Ref_Key eq guid'{PREDEFINED_MGS_PARTNER_REF}'", safe="")
    url = (
        f"{BASE}/{PREDEFINED_VALUES}?$format=json"
        f"&$filter={flt}&$select=Ref_Key,Значение,Значение_Type&$top=1"
    )
    r = request_with_retry(session, url, timeout=30, retries=3, label="Plans/MGSPartner")
    if r is None or not r.ok:
        return None
    try:
        row = (r.json().get("value") or [{}])[0]
    except Exception:
        return None
    val = row.get("Значение")
    typ = row.get("Значение_Type") or ""
    return val if val and "Catalog_Партнеры" in typ else None


def _partner_resale_sets(session: requests.Session) -> tuple[set[str], set[str]]:
    resale = _load_resale_partner_keys(session)
    without_mgs = set(resale)
    mgs = _load_mgs_partner_key(session)
    if mgs:
        without_mgs.discard(mgs)
    return resale, without_mgs


def _batch_load_calc_objects(session: requests.Session,
                             obj_keys: set[str]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    keys = sorted(k for k in obj_keys if k and k != EMPTY)
    for i in range(0, len(keys), 15):
        batch = keys[i:i + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        sel = quote("Ref_Key,Подразделение_Key,Партнер_Key,Соглашение,Объект,Объект_Type", safe=",_")
        url = (
            f"{BASE}/{OBJ_SETTLEMENTS}?$format=json&$filter={flt}"
            f"&$select={sel}&$top={len(batch)}"
        )
        r = request_with_retry(session, url, timeout=30, retries=3, label="Plans/CalcObjects")
        if r is None or not r.ok:
            continue
        try:
            for it in r.json().get("value", []):
                result[it["Ref_Key"]] = {
                    "dept": it.get("Подразделение_Key", ""),
                    "partner": it.get("Партнер_Key", ""),
                    "agreement": it.get("Соглашение", ""),
                    "obj": it.get("Объект", ""),
                    "obj_type": it.get("Объект_Type", ""),
                }
        except Exception:
            pass
    return result


def _batch_load_orders_for_expected(session: requests.Session,
                                    order_keys: set[str]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    keys = sorted(k for k in order_keys if k and k != EMPTY)
    fields = (
        "Ref_Key,Date,Подразделение_Key,Партнер_Key,Соглашение_Key,Валюта_Key,ОбъектРасчетов_Key,"
        "ДатаОтгрузки,ТД_НеУчитыватьВПланФакте,ТД_НеУчитыватьВПланФактеДС,"
        "ТД_НеУчитыватьВПланФактеОтгрузки"
    )
    sel = quote(fields, safe=",_")
    for i in range(0, len(keys), 15):
        batch = keys[i:i + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json&$filter={flt}"
            f"&$select={sel}&$top={len(batch)}"
        )
        r = request_with_retry(session, url, timeout=30, retries=3, label="Plans/Orders")
        if r is None or not r.ok:
            continue
        try:
            for it in r.json().get("value", []):
                result[it["Ref_Key"]] = {
                    "date": it.get("Date", ""),
                    "dept": it.get("Подразделение_Key", ""),
                    "partner": it.get("Партнер_Key", ""),
                    "agreement": it.get("Соглашение_Key", ""),
                    "currency": it.get("Валюта_Key", ""),
                    "calc_obj": it.get("ОбъектРасчетов_Key", ""),
                    "ship_date": it.get("ДатаОтгрузки", ""),
                    "ne_uchit": it.get("ТД_НеУчитыватьВПланФакте", False),
                    "ne_uchit_ds": it.get("ТД_НеУчитыватьВПланФактеДС", False),
                    "ne_uchit_ship": it.get("ТД_НеУчитыватьВПланФактеОтгрузки", False),
                }
        except Exception:
            pass
    return result


def _load_payment_stage_order_months(session: requests.Session,
                                     year: int,
                                     ref_month: int) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    start = _month_start(year, 1)
    end = _month_end_exclusive(year, ref_month)
    sel = quote("Ref_Key,ДатаПлатежа", safe=",_")
    flt = quote(
        f"ДатаПлатежа ge datetime'{start}' and ДатаПлатежа lt datetime'{end}'",
        safe="",
    )
    skip = 0
    while True:
        url = (
            f"{BASE}/{PAYMENT_STAGES}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="Plans/PaymentStagesScan")
        if r is None:
            logger.error("Expected money stages: request dropped after retries")
            break
        if not r.ok:
            logger.error("Expected money stages HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        for it in batch:
            m = _month_from_date(it.get("ДатаПлатежа"))
            order_key = it.get("Ref_Key", "")
            if m is not None and 1 <= m <= ref_month and order_key not in ("", EMPTY):
                result.setdefault(order_key, set()).add(m)
        if len(batch) < 5000:
            break
        skip += 5000
    return result


def _load_payment_stage_months(session: requests.Session,
                               order_keys: set[str],
                               year: int,
                               ref_month: int) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    keys = sorted(k for k in order_keys if k and k != EMPTY)
    start = _month_start(year, 1)[:10]
    end = _month_end_exclusive(year, ref_month)[:10]
    for i in range(0, len(keys), 15):
        batch = keys[i:i + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/{PAYMENT_STAGES}?$format=json&$filter={flt}"
            f"&$select=Ref_Key,ДатаПлатежа&$top=5000"
        )
        r = request_with_retry(session, url, timeout=30, retries=3, label="Plans/PaymentStages")
        if r is None or not r.ok:
            continue
        try:
            for it in r.json().get("value", []):
                pay_date = (it.get("ДатаПлатежа") or "")[:10]
                if not (start <= pay_date < end):
                    continue
                m = _month_from_date(pay_date)
                if m is None or m < 1 or m > ref_month:
                    continue
                result.setdefault(it.get("Ref_Key", ""), set()).add(m)
        except Exception:
            pass
    return result


def _load_customer_settlement_rows(session: requests.Session,
                                   year: int,
                                   ref_month: int,
                                   obj_keys: set[str]) -> list[dict]:
    d_to = _month_end_exclusive(year, ref_month)
    sel = quote(
        "Period,Active,RecordType,ОбъектРасчетов_Key,Валюта_Key,КОплате,Сторно",
        safe=",_",
    )
    rows: list[dict] = []
    keys = sorted(k for k in obj_keys if k and k != EMPTY)
    for i in range(0, len(keys), 12):
        batch_keys = keys[i:i + 12]
        key_filter = " or ".join(f"ОбъектРасчетов_Key eq guid'{k}'" for k in batch_keys)
        flt = quote(
            f"Period lt datetime'{d_to}' and Active eq true and Сторно eq false "
            f"and КОплате ne 0 and ({key_filter})",
            safe="",
        )
        skip = 0
        while True:
            url = (
                f"{BASE}/{REG_CUSTOMER_SETTLEMENTS}?$format=json&$top=5000&$skip={skip}"
                f"&$filter={flt}&$select={sel}"
            )
            r = request_with_retry(session, url, timeout=120, retries=4, label="Plans/ExpectedMoney")
            if r is None:
                logger.error("Expected money: request dropped after retries")
                break
            if not r.ok:
                logger.error("Expected money HTTP %d", r.status_code)
                break
            batch = r.json().get("value", [])
            rows.extend(batch)
            if len(batch) < 5000:
                break
            skip += 5000
    return rows


def _load_shipment_order_rows(session: requests.Session,
                              year: int,
                              ref_month: int,
                              order_keys: set[str]) -> list[dict]:
    d_to = _month_end_exclusive(year, ref_month)
    sel = quote(
        "Period,Active,Распоряжение,Распоряжение_Type,ВидДвиженияРегистра,Сумма,Сторно",
        safe=",_",
    )
    rows: list[dict] = []
    for movement in ("Расход", "Приход"):
        flt = quote(
            f"Period lt datetime'{d_to}' and Active eq true and Сторно eq false "
            f"and Распоряжение_Type eq 'StandardODATA.Document_ЗаказКлиента' "
            f"and ВидДвиженияРегистра eq '{movement}'",
            safe="",
        )
        skip = 0
        while True:
            url = (
                f"{BASE}/{REG_SHIPMENT_ORDERS}?$format=json&$top=5000&$skip={skip}"
                f"&$filter={flt}&$select={sel}"
            )
            r = request_with_retry(session, url, timeout=120, retries=4, label="Plans/ExpectedShipment")
            if r is None:
                logger.error("Expected shipment: request dropped after retries")
                break
            if not r.ok:
                logger.error("Expected shipment HTTP %d", r.status_code)
                break
            batch = r.json().get("value", [])
            rows.extend(batch)
            if len(batch) < 5000:
                break
            skip += 5000
    return rows


def _signed_balance_amount(row: dict, field: str) -> float:
    amount = float(row.get(field) or 0)
    return -amount if row.get("RecordType") == "Expense" else amount


def _expected_order_passes_common_filters(order: dict,
                                          resale_partners: set[str],
                                          resale_without_mgs: set[str]) -> str | None:
    dept = _normalize_expected_dept(order.get("dept", ""))
    if not dept or dept not in DEPT_SET:
        return None
    if not _is_expected_dept(order.get("dept", "")):
        return None
    if order.get("agreement") in ("", EMPTY):
        return None

    partner = order.get("partner", "")
    if dept == OPBO_DEPT:
        if partner in resale_without_mgs:
            return None
    else:
        if partner in resale_partners:
            return None
    return dept


def _merge_expected_money(result: dict[int, dict],
                          session: requests.Session,
                          year: int,
                          ref_month: int,
                          resale_partners: set[str],
                          resale_without_mgs: set[str]) -> None:
    payment_months = _load_payment_stage_order_months(session, year, ref_month)
    order_keys = set(payment_months.keys())
    orders = _batch_load_orders_for_expected(session, order_keys)
    calc_obj_by_order = {
        order_key: order.get("calc_obj", "")
        for order_key, order in orders.items()
        if order.get("calc_obj", "") not in ("", EMPTY)
    }
    order_by_calc_obj = {v: k for k, v in calc_obj_by_order.items()}
    calc_objects = _batch_load_calc_objects(session, set(calc_obj_by_order.values()))
    rows = _load_customer_settlement_rows(session, year, ref_month, set(calc_obj_by_order.values()))

    rows_sorted = sorted(rows, key=lambda x: (x.get("Period") or ""))
    balances: dict[str, float] = {}
    idx = 0
    for m in range(1, ref_month + 1):
        end = _month_end_exclusive(year, m)
        while idx < len(rows_sorted) and (rows_sorted[idx].get("Period") or "") < end:
            row = rows_sorted[idx]
            idx += 1
            obj_key = row.get("ОбъектРасчетов_Key", "")
            if obj_key in ("", EMPTY):
                continue
            balances[obj_key] = balances.get(obj_key, 0.0) + _signed_balance_amount(row, "КОплате")

        for obj_key, balance in balances.items():
            if balance <= 0:
                continue
            calc_obj = calc_objects.get(obj_key)
            if calc_obj and ORDER_TYPE_MARKER not in (calc_obj.get("obj_type") or ""):
                continue
            order_key = order_by_calc_obj.get(obj_key, "")
            order = orders.get(order_key)
            if not order:
                continue
            if m not in payment_months.get(order_key, set()):
                continue
            if order.get("ne_uchit") or order.get("ne_uchit_ds"):
                continue
            dept = _expected_order_passes_common_filters(order, resale_partners, resale_without_mgs)
            if not dept:
                continue
            amount = balance * _currency_rate(order.get("currency", ""))
            result[m]["dengi_expected"] += amount
            result[m]["by_dept"][dept]["dengi_expected"] += amount


def _expected_shipment_row_amount(row: dict, order: dict) -> float:
    amount = float(row.get("Сумма") or 0) * _currency_rate(order.get("currency", ""))
    movement = row.get("ВидДвиженияРегистра")
    if movement == "Расход":
        return -amount
    if movement == "Приход":
        return amount
    return 0.0


def _merge_expected_shipments(result: dict[int, dict],
                              session: requests.Session,
                              year: int,
                              ref_month: int,
                              resale_partners: set[str],
                              resale_without_mgs: set[str]) -> None:
    rows = _load_shipment_order_rows(session, year, ref_month, set())
    order_keys = {
        row.get("Распоряжение", "")
        for row in rows
        if row.get("Распоряжение", "") not in ("", EMPTY)
    }
    orders = _batch_load_orders_for_expected(session, order_keys)
    rows_sorted = sorted(rows, key=lambda x: (x.get("Period") or ""))

    for m in range(1, ref_month + 1):
        end = _month_end_exclusive(year, m)
        by_order: dict[str, float] = {}
        for row in rows_sorted:
            if (row.get("Period") or "") >= end:
                break
            order_key = row.get("Распоряжение", "")
            order = orders.get(order_key)
            if not order:
                continue
            ship_date = (order.get("ship_date") or "")[:10]
            if not ship_date or ship_date >= end[:10]:
                continue
            dept = _expected_order_passes_common_filters(order, resale_partners, resale_without_mgs)
            if not dept:
                continue
            by_order[order_key] = by_order.get(order_key, 0.0) + _expected_shipment_row_amount(row, order)

        for order_key, amount in by_order.items():
            if amount <= 0:
                continue
            order = orders.get(order_key)
            if not order or order.get("ne_uchit") or order.get("ne_uchit_ship"):
                continue
            dept = _expected_order_passes_common_filters(order, resale_partners, resale_without_mgs)
            if not dept:
                continue
            result[m]["otgruzki_expected"] += amount
            result[m]["by_dept"][dept]["otgruzki_expected"] += amount


def _merge_expected_money_and_shipments(result: dict[int, dict],
                                        session: requests.Session,
                                        year: int,
                                        ref_month: int) -> None:
    resale_partners, resale_without_mgs = _partner_resale_sets(session)
    _merge_expected_money(result, session, year, ref_month, resale_partners, resale_without_mgs)
    _merge_expected_shipments(result, session, year, ref_month, resale_partners, resale_without_mgs)

    for m in result:
        for k in ("dengi_expected", "otgruzki_expected"):
            result[m][k] = round(result[m][k], 2)
        for d in result[m]["by_dept"]:
            for k in ("dengi_expected", "otgruzki_expected"):
                result[m]["by_dept"][d][k] = round(result[m]["by_dept"][d][k], 2)


def _calc_plans(entries: list[dict],
                ref_month: int) -> dict[int, dict]:
    """
    Агрегировать записи в помесячный план по отделам.
    Возвращает {month: {"dengi": total, "otgruzki": total, "dogovory": total,
                        "by_dept": {guid: {"dengi": ..., "otgruzki": ..., "dogovory": ...}}}}
    """
    result: dict[int, dict] = {}
    for m in range(1, ref_month + 1):
        result[m] = {
            "dengi": 0.0, "otgruzki": 0.0, "dogovory": 0.0,
            "dengi_expected": 0.0, "otgruzki_expected": 0.0, "dogovory_expected": 0.0,
            "by_dept": {
                d: {
                    "dengi": 0.0, "otgruzki": 0.0, "dogovory": 0.0,
                    "dengi_expected": 0.0, "otgruzki_expected": 0.0, "dogovory_expected": 0.0,
                }
                for d in DEPT_SET
            },
        }

    for row in entries:
        period_str = (row.get("Period") or "")[:10]
        if len(period_str) < 7:
            continue
        try:
            m = int(period_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > ref_month:
            continue

        dept = row.get("Подразделение_Key", "")
        if not dept or dept == EMPTY or dept not in DEPT_SET:
            continue

        vid = row.get("ВидПланирования", "")
        sm = float(row.get("Сумма") or 0)

        if vid == "Договоры":
            key = "dogovory"
        elif vid == "Деньги":
            key = "dengi"
        elif vid == "Отгрузки":
            key = "otgruzki"
        else:
            continue

        result[m][key] += sm
        result[m]["by_dept"][dept][key] += sm

    for m in result:
        for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()):
            result[m][k] = round(result[m][k], 2)
        for d in result[m]["by_dept"]:
            for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()):
                result[m]["by_dept"][d][k] = round(result[m]["by_dept"][d][k], 2)

    return result


def _proposal_expected_contract_amount(row: dict) -> float:
    amount = float(row.get("СуммаБазыКБ") or row.get("СуммаДокумента") or 0)
    # В отчёт «ожидаемые к заключению» не попадают мелкие согласованные КП,
    # которые ещё не согласованы клиентом.
    if (
        row.get("Статус") == "Согласовано"
        and not row.get("СогласованоСКлиентом")
        and amount < 150_000
    ):
        return 0.0
    return amount


def _merge_expected_plans(result: dict[int, dict],
                          documents: list[dict],
                          ref_month: int,
                          proposals: list[dict] | None = None) -> None:
    """Добавить сумму «Договоры, ожидаемые к заключению» в уже собранный план."""
    manager_dept_by_month: dict[tuple[int, str], str] = {}
    for doc in documents:
        date_str = (doc.get("ДатаПланирования") or "")[:10]
        if len(date_str) < 7:
            continue
        try:
            m = int(date_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > ref_month:
            continue

        dept = doc.get("Подразделение_Key", "")
        if not dept or dept == EMPTY or dept not in DEPT_SET:
            continue
        if doc.get("Статус") not in (None, "", "Подготовлен", "Утвержден"):
            continue

        month_row = result[m]
        dept_row = month_row["by_dept"][dept]
        manager = doc.get("Ответственный_Key")
        if manager:
            manager_dept_by_month[(m, manager)] = dept

    for row in proposals or []:
        date_str = (row.get("ДатаПодписанияПлан") or "")[:10]
        if len(date_str) < 7:
            continue
        try:
            m = int(date_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > ref_month:
            continue
        manager = row.get("Менеджер_Key")
        dept = manager_dept_by_month.get((m, manager))
        if not dept:
            continue
        amount = _proposal_expected_contract_amount(row)
        result[m]["dogovory_expected"] += amount
        result[m]["by_dept"][dept]["dogovory_expected"] += amount

    for m in result:
        for k in EXPECTED_KEYS.values():
            result[m][k] = round(result[m][k], 2)
        for d in result[m]["by_dept"]:
            for k in EXPECTED_KEYS.values():
                result[m]["by_dept"][d][k] = round(result[m]["by_dept"][d][k], 2)


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Вернуть полный агрегат или срез по одному подразделению."""
    if dept_guid is None:
        return payload
    sliced_months = []
    for row in payload.get("months", []):
        bd = row.get("by_dept", {}).get(dept_guid, {})
        sliced_months.append({
            "year": row["year"],
            "month": row["month"],
            "dengi": bd.get("dengi", 0),
            "otgruzki": bd.get("otgruzki", 0),
            "dogovory": bd.get("dogovory", 0),
            "dengi_expected": bd.get("dengi_expected", 0),
            "otgruzki_expected": bd.get("otgruzki_expected", 0),
            "dogovory_expected": bd.get("dogovory_expected", 0),
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced_months,
    }


def get_plans_monthly(year: int | None = None,
                      month: int | None = None,
                      dept_guid: str | None = None) -> dict:
    """
    Помесячные планы (январь..ref_month).
    dept_guid=None — агрегат всех отделов.
    dept_guid='...' — план только по указанному подразделению.

    Возвращает:
      {"year": ..., "ref_month": ..., "months": [
          {"year": ..., "month": ..., "dengi": ..., "otgruzki": ..., "dogovory": ...,
           "by_dept": {...}},
      ]}
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    mc = _monthly_cache_path(ref_y, ref_m)
    if mc.exists():
        try:
            with open(mc, "r", encoding="utf-8") as f:
                data = json.load(f)
            first_m = (data.get("months") or [{}])[0]
            if (
                data.get("cache_date") == today.isoformat()
                and data.get("cache_version") == CACHE_VERSION
                and "by_dept" in first_m
                and all(k in first_m for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()))
            ):
                return _slice_payload(data, dept_guid)
        except (OSError, json.JSONDecodeError):
            pass

    all_cached = True
    for m in range(1, ref_m + 1):
        if _load_cache(ref_y, m) is None:
            all_cached = False
            break

    if all_cached:
        out_months = []
        for m in range(1, ref_m + 1):
            cd = _load_cache(ref_y, m)
            out_months.append({
                "year": ref_y, "month": m,
                **{k: cd[k] for k in (*PLAN_KEYS, *EXPECTED_KEYS.values())},
                "by_dept": cd.get("by_dept", {}),
            })
        payload = {
            "cache_date": today.isoformat(),
            "cache_version": CACHE_VERSION,
            "year": ref_y, "ref_month": ref_m,
            "months": out_months,
        }
        try:
            with open(mc, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except OSError:
            pass
        return _slice_payload(payload, dept_guid)

    session = requests.Session()
    session.auth = AUTH

    logger.info("calc_plan: loading register for %d months 1-%d", ref_y, ref_m)
    entries = _load_register(session, ref_y, ref_m)
    computed = _calc_plans(entries, ref_m)
    expected_documents = _load_expected_documents(session, ref_y, ref_m)
    expected_contracts = _load_expected_contract_proposals(session, ref_y, ref_m)
    _merge_expected_plans(computed, expected_documents, ref_m, expected_contracts)
    _merge_expected_money_and_shipments(computed, session, ref_y, ref_m)

    out_months = []
    for m in range(1, ref_m + 1):
        cached = _load_cache(ref_y, m)
        if cached is not None:
            totals = {k: cached[k] for k in (*PLAN_KEYS, *EXPECTED_KEYS.values())}
            by_dept = cached.get("by_dept", {})
        else:
            cm = computed[m]
            totals = {k: cm[k] for k in (*PLAN_KEYS, *EXPECTED_KEYS.values())}
            by_dept = cm["by_dept"]
            _save_cache(ref_y, m, totals, by_dept)
        out_months.append({
            "year": ref_y, "month": m,
            **totals,
            "by_dept": by_dept,
        })

    payload = {
        "cache_date": today.isoformat(),
        "cache_version": CACHE_VERSION,
        "year": ref_y, "ref_month": ref_m,
        "months": out_months,
    }
    try:
        with open(mc, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass
    return _slice_payload(payload, dept_guid)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    import functools
    _print = functools.partial(print, flush=True)

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    _print(f"\n{'═' * 60}")
    _print(f"  ПЛАН (МП)")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_plans_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'Деньги':>18s} {'Отгрузки':>18s} {'Договоры':>18s}")
    _print(f"  {'─' * 68}")
    for row in data.get("months", []):
        _print(f"  {MONTH_RU[row['month']]:<12s} "
               f"{row['dengi']:>18,.2f} "
               f"{row['otgruzki']:>18,.2f} "
               f"{row['dogovory']:>18,.2f}")
    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
