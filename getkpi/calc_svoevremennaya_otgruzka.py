"""
calc_svoevremennaya_otgruzka.py — FND-T4 «Своевременная отгрузка».

Формулы (по ТЗ):
  План за месяц M = count(DISTINCT ЗаказКлиента) из
    InformationRegister_ТД_КонтрольныеДатыИсполненияДоговора,
    где ДатаОкончанияЮридическихОбязательствПоДоговору в [1-е M .. последний день M].

  Факт за месяц M = count(Document_РеализацияТоваровУслуг)
    среди проведённых, без пометки на удаление, у которых ЗаказКлиента входит в
    множество заказов плана этого же месяца.

  % = Факт / План * 100 (округление до 0.1)

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
SOURCE_TAG = "svoevremennaya_monthly_v2"

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


def _fetch_plan_orders(session: requests.Session, year: int, month: int) -> set[str]:
    """
    Уникальные GUID заказов клиента из регистра КонтрольныеДатыИсполненияДоговора,
    у которых ДатаОкончанияЮридическихОбязательствПоДоговору попадает в указанный месяц.
    """
    d_from, d_to = _month_bounds(year, month)
    flt = (
        f"{DATE_FILTER_FIELD} ge datetime'{d_from}' "
        f"and {DATE_FILTER_FIELD} le datetime'{d_to}'"
    )
    sel = f"{ORDER_FIELD},{DATE_FILTER_FIELD}"

    orders: set[str] = set()
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
        for row in rows:
            order_key = str(row.get(ORDER_FIELD) or "").lower()
            if order_key and order_key != EMPTY:
                orders.add(order_key)
        if len(rows) < PAGE:
            break
        skip += PAGE
    return orders


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
        "fact": int,          # реализации по этим заказам
        "pct": float | None,  # fact / plan * 100
      }
    """
    cached = _load_cache(_cache_path(year, month))
    if cached is not None:
        return cached

    session = requests.Session()
    session.auth = AUTH

    orders = _fetch_plan_orders(session, year, month)
    plan = len(orders)

    fact = 0
    if plan > 0:
        fact_from, fact_to = _fact_window_bounds(year, month)
        reals = _fetch_realizations_in_window(session, fact_from, fact_to)
        fact = _count_realizations_for_orders(reals, orders)

    pct = round(fact / plan * 100, 1) if plan else None

    d_from, d_to = _month_bounds(year, month)
    payload = {
        "year": int(year),
        "month": int(month),
        "period_start": d_from[:10],
        "period_end": d_to[:10],
        "plan": plan,
        "fact": fact,
        "pct": pct,
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
