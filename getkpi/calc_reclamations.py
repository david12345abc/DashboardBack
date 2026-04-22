"""
calc_reclamations.py — FND-T5 «Качество рекламаций».

Формулы (по ТЗ):
  План за месяц M = count(Справочник.Претензии),
    где ТД_ДатаОкончанияПлан ∈ [1-е M .. последний день M]
    (и элемент не помечен на удаление).

  Факт за месяц M = count(Справочник.Претензии),
    где ДатаОкончания ∈ [1-е M .. последний день M]
    (и элемент не помечен на удаление).

  % = Факт / План * 100 (округление до 0.1), если План > 0.

Кэш:
  dashboard/reclamations_<year>_<month:02d>.json           — план/факт за месяц
  dashboard/reclamations_monthly_<year>_<month:02d>.json   — помесячный ряд
  (январь..ref_month).

API:
  from .calc_reclamations import (
      get_reclamations_for_month,   # {year, month, plan, fact, pct}
      get_reclamations_monthly,     # {year, ref_month, months: [...]}
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

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

# Catalog_Претензии
CATALOG_PRETENZII = "Catalog_\u041f\u0440\u0435\u0442\u0435\u043d\u0437\u0438\u0438"

# ТД_ДатаОкончанияПлан — план
FIELD_DATE_PLAN = (
    "\u0422\u0414_"
    "\u0414\u0430\u0442\u0430"
    "\u041e\u043a\u043e\u043d\u0447\u0430\u043d\u0438\u044f"
    "\u041f\u043b\u0430\u043d"
)
# ДатаОкончания — факт
FIELD_DATE_FACT = (
    "\u0414\u0430\u0442\u0430"
    "\u041e\u043a\u043e\u043d\u0447\u0430\u043d\u0438\u044f"
)


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    last_day = calendar.monthrange(year, month)[1]
    return (
        f"{year}-{month:02d}-01T00:00:00",
        f"{year}-{month:02d}-{last_day}T23:59:59",
    )


def _cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"reclamations_{year}_{month:02d}.json"


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    return CACHE_DIR / f"reclamations_monthly_{year}_{ref_month:02d}.json"


def _is_past_period(year: int, month: int) -> bool:
    """Прошлые полные месяцы кэшируются навсегда, текущий/будущий — на сутки."""
    today = date.today()
    return (year, month) < (today.year, today.month)


def _load_cache(path: Path, *, perpetual: bool = False) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if perpetual:
        return data
    if data.get("cache_date") == date.today().isoformat():
        return data
    return None


def _save_cache(path: Path, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {**payload, "cache_date": date.today().isoformat()},
                f, ensure_ascii=False, indent=2,
            )
    except OSError:
        pass


def _count_by_date_field(session: requests.Session,
                         field: str,
                         year: int,
                         month: int) -> int:
    """
    Посчитать количество претензий, у которых указанное поле-дата
    попадает в заданный месяц. DeletionMark исключается.
    """
    d_from, d_to = _month_bounds(year, month)
    flt = (
        f"{field} ge datetime'{d_from}' "
        f"and {field} le datetime'{d_to}' "
        f"and DeletionMark eq false"
    )
    sel = f"Ref_Key,{field}"

    total = 0
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(CATALOG_PRETENZII)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4,
                               label=f"Reclam/{field}")
        if r is None or not r.ok:
            if r is not None:
                logger.error("Reclam HTTP %d: %s", r.status_code, r.text[:200])
            break
        rows = r.json().get("value", [])
        total += len(rows)
        if len(rows) < PAGE:
            break
        skip += PAGE
    return total


def get_reclamations_for_month(year: int, month: int) -> dict:
    """
    План/факт по претензиям за конкретный месяц.

    Возвращает:
      {
        "year", "month",
        "period_start", "period_end",
        "plan": int,          # претензий с ТД_ДатаОкончанияПлан в этом месяце
        "fact": int,          # претензий с ДатаОкончания в этом месяце
        "pct": float | None,  # fact / plan * 100
      }
    """
    cache_path = _cache_path(year, month)
    perpetual = _is_past_period(year, month)
    cached = _load_cache(cache_path, perpetual=perpetual)
    if cached is not None:
        return cached

    session = requests.Session()
    session.auth = AUTH

    plan = _count_by_date_field(session, FIELD_DATE_PLAN, year, month)
    fact = _count_by_date_field(session, FIELD_DATE_FACT, year, month)

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
    _save_cache(cache_path, payload)
    return payload


def get_reclamations_monthly(year: int | None = None,
                             month: int | None = None,
                             dept_guid: str | None = None) -> dict:
    """
    Помесячный ряд (январь..ref_month) для FND-T5.

    dept_guid не используется (FND-T5 считается по всей компании).

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
        1: "\u044f\u043d\u0432\u0430\u0440\u044c",
        2: "\u0444\u0435\u0432\u0440\u0430\u043b\u044c",
        3: "\u043c\u0430\u0440\u0442",
        4: "\u0430\u043f\u0440\u0435\u043b\u044c",
        5: "\u043c\u0430\u0439",
        6: "\u0438\u044e\u043d\u044c",
        7: "\u0438\u044e\u043b\u044c",
        8: "\u0430\u0432\u0433\u0443\u0441\u0442",
        9: "\u0441\u0435\u043d\u0442\u044f\u0431\u0440\u044c",
        10: "\u043e\u043a\u0442\u044f\u0431\u0440\u044c",
        11: "\u043d\u043e\u044f\u0431\u0440\u044c",
        12: "\u0434\u0435\u043a\u0430\u0431\u0440\u044c",
    }

    rows: list[dict] = []
    for mm in range(1, m + 1):
        data = get_reclamations_for_month(y, mm)
        plan = data.get("plan") or 0
        fact = data.get("fact") or 0
        rows.append({
            "year": y,
            "month": mm,
            "month_name": MONTH_RU[mm],
            "plan": plan,
            "fact": fact,
            "kpi_pct": data.get("pct"),
            "has_data": (plan > 0 or fact > 0),
        })

    payload = {"year": y, "ref_month": m, "months": rows}
    _save_cache(_cache_path_monthly(y, m), payload)
    return payload
