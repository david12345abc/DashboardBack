"""
calc_svoevremennaya_otgruzka.py — FND-T4 «Своевременная отгрузка».

Формулы (по ТЗ):
  План за месяц M = count(DISTINCT ЗаказКлиента) из
    InformationRegister_ТД_КонтрольныеДатыИсполненияДоговора,
    где ДатаОкончанияЮридическихОбязательствПоДоговору в [1-е M .. последний день M].

  Факт за месяц M = количество срывов среди этих заказов:
    ДатаОтгрузки > ДатаОкончанияЮридическихОбязательствПоДоговору.

  % = (План - Факт) / План * 100 (округление до 0.1)

Кэш: dashboard/svoevremennaya_<year>_<month:02d>.json  — данные именно за месяц
     dashboard/svoevremennaya_monthly_<year>_<month:02d>.json — помесячный ряд
     (январь..ref_month).

API:
  from .calc_svoevremennaya_otgruzka import (
      get_svoevremennaya_for_month,     # {year, month, plan, fact, pct}
      get_svoevremennaya_monthly,       # {year, ref_month, months: [...]}
  )
"""
from __future__ import annotations

import calendar
import json
import logging
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
SOURCE_TAG = "svoevremennaya_monthly_v3_npo_delays"

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

REG_KONTROLNIE_DATY = (
    "InformationRegister_"
    "\u0422\u0414_\u041a\u043e\u043d\u0442\u0440\u043e\u043b\u044c\u043d\u044b\u0435"
    "\u0414\u0430\u0442\u044b\u0418\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f"
    "\u0414\u043e\u0433\u043e\u0432\u043e\u0440\u0430"
)
DOC_REALIZACIYA = (
    "Document_"
    "\u0420\u0435\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f"
    "\u0422\u043e\u0432\u0430\u0440\u043e\u0432\u0423\u0441\u043b\u0443\u0433"
)

DATE_FILTER_FIELD = (
    "\u0414\u0430\u0442\u0430\u041e\u043a\u043e\u043d\u0447\u0430\u043d\u0438\u044f"
    "\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0445"
    "\u041e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432"
    "\u041f\u043e\u0414\u043e\u0433\u043e\u0432\u043e\u0440\u0443"
)
ORDER_FIELD = "\u0417\u0430\u043a\u0430\u0437\u041a\u043b\u0438\u0435\u043d\u0442\u0430"
ORDER_SHIP_DATE_FIELD = "\u0414\u0430\u0442\u0430\u041e\u0442\u0433\u0440\u0443\u0437\u043a\u0438"
ORDER_STATUS_FIELD = "\u0421\u0442\u0430\u0442\u0443\u0441"
ORDER_AGREED_FIELD = "\u0421\u043e\u0433\u043b\u0430\u0441\u043e\u0432\u0430\u043d"
ORDER_ORG_FIELD = "\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f_Key"
STATUS_TO_SUPPLY = "\u041a\u041e\u0431\u0435\u0441\u043f\u0435\u0447\u0435\u043d\u0438\u044e"

# Фильтр отчёта ПСД: только заказы НПО «Турбулентность-Дон».
NPO_ORG_KEY = "fbca2148-6cfd-11e7-812d-001e67112509"


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    last_day = calendar.monthrange(year, month)[1]
    return (
        f"{year}-{month:02d}-01T00:00:00",
        f"{year}-{month:02d}-{last_day}T23:59:59",
    )


def _cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"svoevremennaya_{year}_{month:02d}.json"


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    return CACHE_DIR / f"svoevremennaya_monthly_{year}_{ref_month:02d}.json"


def _load_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if (
        data.get("cache_date") == date.today().isoformat()
        and data.get("source_tag") == SOURCE_TAG
    ):
        return data
    return None


def _save_cache(path: Path, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({**payload, "cache_date": date.today().isoformat(), "source_tag": SOURCE_TAG},
                      f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _is_empty_ref(value) -> bool:
    return not value or str(value).lower() == EMPTY


def _valid_ship_date(value) -> str:
    value = str(value or "")
    if len(value) < 10 or value.startswith("0001-01-01"):
        return ""
    return value[:10]


def _fetch_plan_rows(session: requests.Session, year: int, month: int) -> list[dict]:
    """
    Строки регистра КонтрольныеДатыИсполненияДоговора,
    у которых ДатаОкончанияЮридическихОбязательствПоДоговору попадает в указанный месяц.
    """
    d_from, d_to = _month_bounds(year, month)
    flt = (
        f"{DATE_FILTER_FIELD} ge datetime'{d_from}' "
        f"and {DATE_FILTER_FIELD} le datetime'{d_to}'"
    )
    sel = f"{ORDER_FIELD},{DATE_FILTER_FIELD},{ORDER_SHIP_DATE_FIELD}"

    rows_out: list[dict] = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(REG_KONTROLNIE_DATY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="SvOtgr/Plan")
        if r is None or not r.ok:
            if r is not None:
                logger.error("SvOtgr/Plan HTTP %d: %s", r.status_code, r.text[:200])
            break
        rows = r.json().get("value", [])
        rows_out.extend(rows)
        if len(rows) < PAGE:
            break
        skip += PAGE
    return rows_out


def _fetch_customer_orders(session: requests.Session, order_keys: set[str]) -> dict[str, dict]:
    """Загрузить реквизиты заказов клиента для отбора плана."""
    if not order_keys:
        return {}

    result: dict[str, dict] = {}
    keys = sorted(order_keys)
    batch_size = 15
    select = f"Ref_Key,{ORDER_ORG_FIELD},{ORDER_AGREED_FIELD},{ORDER_STATUS_FIELD}"
    for i in range(0, len(keys), batch_size):
        batch = keys[i:i + batch_size]
        flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in batch)
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
            f"&$top={batch_size}"
        )
        r = request_with_retry(session, url, timeout=60, retries=4, label="SvOtgr/Orders")
        if r is None or not r.ok:
            if r is not None:
                logger.error("SvOtgr/Orders HTTP %d: %s", r.status_code, r.text[:200])
            continue
        for row in r.json().get("value", []):
            key = str(row.get("Ref_Key") or "").lower()
            if key:
                result[key] = row
    return result


def _order_is_in_psd_scope(order: dict) -> bool:
    """Повторяет отбор отчёта ПСД: НПО + согласованные или ещё к обеспечению."""
    if str(order.get(ORDER_ORG_FIELD) or "").lower() != NPO_ORG_KEY:
        return False
    return bool(order.get(ORDER_AGREED_FIELD)) or order.get(ORDER_STATUS_FIELD) == STATUS_TO_SUPPLY


def _plan_orders_and_delays(plan_rows: list[dict],
                            order_docs: dict[str, dict]) -> tuple[set[str], int]:
    legal_by_order: dict[str, str] = {}
    ship_by_order: dict[str, str] = {}

    for row in plan_rows:
        order_key = str(row.get(ORDER_FIELD) or "").lower()
        if _is_empty_ref(order_key):
            continue
        order_doc = order_docs.get(order_key)
        if not order_doc or not _order_is_in_psd_scope(order_doc):
            continue

        legal_date = _valid_ship_date(row.get(DATE_FILTER_FIELD))
        if legal_date:
            legal_by_order[order_key] = max(legal_by_order.get(order_key, legal_date), legal_date)

        ship_date = _valid_ship_date(row.get(ORDER_SHIP_DATE_FIELD))
        if ship_date:
            ship_by_order[order_key] = max(ship_by_order.get(order_key, ship_date), ship_date)

    delayed = 0
    for order_key, legal_date in legal_by_order.items():
        ship_date = ship_by_order.get(order_key)
        if ship_date and ship_date > legal_date:
            delayed += 1

    return set(legal_by_order), delayed


def _fetch_realizations_in_window(session: requests.Session,
                                  date_from_iso: str,
                                  date_to_iso: str) -> list[dict]:
    """
    Один запрос: все проведённые реализации с Date ∈ [date_from..date_to].
    1С OData нестабильно обрабатывает OR-фильтр по множеству GUID
    (получаем HTTP 500 на батчах), поэтому тянем по Date и фильтруем в Python.
    """
    flt = (
        f"Posted eq true and DeletionMark eq false "
        f"and Date ge datetime'{date_from_iso}' "
        f"and Date le datetime'{date_to_iso}'"
    )
    sel = f"Ref_Key,Date,{ORDER_FIELD}"
    rows: list[dict] = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(DOC_REALIZACIYA)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=180, retries=4, label="SvOtgr/Fact")
        if r is None or not r.ok:
            if r is not None:
                logger.error("SvOtgr/Fact HTTP %d: %s", r.status_code, r.text[:200])
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < PAGE:
            break
        skip += PAGE
    return rows


def _count_realizations_for_orders(reals_in_window: list[dict],
                                   order_keys: set[str]) -> int:
    """Посчитать уникальные реализации, у которых ЗаказКлиента ∈ order_keys."""
    if not order_keys or not reals_in_window:
        return 0
    seen: set[str] = set()
    for item in reals_in_window:
        order_key = str(item.get(ORDER_FIELD) or "").lower()
        if order_key and order_key in order_keys:
            ref = str(item.get("Ref_Key") or "").lower()
            if ref:
                seen.add(ref)
    return len(seen)


def _fact_window_bounds(year: int, month: int) -> tuple[str, str]:
    """
    Факт считаем строго внутри выбранного месяца.
    Это нужно, чтобы по каждому месяцу возвращались независимые данные,
    а не одно и то же окно, захватывающее следующий месяц.
    """
    start_iso, end_iso = _month_bounds(year, month)
    today = date.today()
    start_dt = date(year, month, 1)
    if start_dt > today:
        return (start_iso, start_iso)

    end_dt = date.fromisoformat(end_iso[:10])
    if end_dt > today:
        end_iso = f"{today.isoformat()}T23:59:59"
    return (start_iso, end_iso)


def get_svoevremennaya_for_month(year: int, month: int) -> dict:
    """
    План / факт «Своевременной отгрузки» за конкретный месяц.

    Возвращает:
      {
        "year", "month",
        "period_start", "period_end",
        "plan": int,          # уникальные заказы с датой ЮО в этом месяце
        "fact": int,          # срывы среди этих заказов
        "pct": float | None,  # (plan - fact) / plan * 100
      }
    """
    cached = _load_cache(_cache_path(year, month))
    if cached is not None:
        return cached

    session = requests.Session()
    session.auth = AUTH

    plan_rows = _fetch_plan_rows(session, year, month)
    raw_orders = {
        str(row.get(ORDER_FIELD) or "").lower()
        for row in plan_rows
        if not _is_empty_ref(row.get(ORDER_FIELD))
    }
    order_docs = _fetch_customer_orders(session, raw_orders)
    orders, fact = _plan_orders_and_delays(plan_rows, order_docs)
    plan = len(orders)

    pct = round((plan - fact) / plan * 100, 1) if plan else None

    d_from, d_to = _month_bounds(year, month)
    payload = {
        "year": int(year),
        "month": int(month),
        "period_start": d_from[:10],
        "period_end": d_to[:10],
        "plan": plan,
        "fact": fact,
        "pct": pct,
        "fact_label": "\u0441\u0440\u044b\u0432\u044b",
    }
    _save_cache(_cache_path(year, month), payload)
    return payload


def get_svoevremennaya_monthly(year: int | None = None,
                               month: int | None = None,
                               dept_guid: str | None = None) -> dict:
    """
    Помесячный ряд (январь..ref_month) для FND-T4.

    dept_guid не используется (FND-T4 считается по всей компании).

    Возвращает:
      {
        "year": int,
        "ref_month": int,
        "months": [
          {"year", "month", "month_name", "plan", "fact", "kpi_pct", "has_data"},
          ...
        ]
      }
    """
    today = date.today()
    y = int(year) if year else today.year
    m = max(1, min(12, int(month))) if month else today.month

    cached = _load_cache(_cache_path_monthly(y, m))
    if cached is not None:
        return cached

    MONTH_RU = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
    }

    rows: list[dict] = []
    for mm in range(1, m + 1):
        data = get_svoevremennaya_for_month(y, mm)
        rows.append({
            "year": y,
            "month": mm,
            "month_name": MONTH_RU[mm],
            "plan": data.get("plan"),
            "fact": data.get("fact"),
            "kpi_pct": data.get("pct"),
            "has_data": (data.get("plan") or 0) > 0,
        })

    payload = {"year": y, "ref_month": m, "months": rows}
    _save_cache(_cache_path_monthly(y, m), payload)
    return payload
