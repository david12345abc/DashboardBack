from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests

from .calc_fot_management import _normalize_period
from .calc_otgruzki_fact import AUTH, BASE, CURRENCY_KEYS, EXCHANGE_RATES, EMPTY
from .odata_http import request_with_retry
from . import valovaya_pribyl

SOURCE_TAG = "odp_ufg_h_shipments_v2"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

NOMENCLATURE_ENTITY = "Catalog_Номенклатура"
ANALYTICS_ENTITY = "Catalog_КлючиАналитикиУчетаНоменклатуры"
ORDER_ENTITY = "Document_ЗаказКлиента"
ORDER_LINES_ENTITY = "Document_ЗаказКлиента_Товары"
TARGET_TEXT = "UFG-H"


def cache_path(year: int, ref_month: int, dept_guid: str | None = None) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    suffix = ""
    if dept_guid:
        suffix = "_" + "".join(ch for ch in dept_guid if ch.isalnum())
    return CACHE_DIR / f"odp_ufg_h_shipments_{year}_{ref_month:02d}{suffix}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _to_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _currency_rate(currency_key: str | None) -> float:
    code = CURRENCY_KEYS.get(currency_key or "", "RUB")
    return EXCHANGE_RATES.get(code, 1.0)


def _fetch_paged(session: requests.Session, url: str, *, label: str, page_size: int = 5000) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page_size}&$skip={skip}"
        response = request_with_retry(session, page_url, timeout=120, retries=4, label=label)
        if response is None or not response.ok:
            break
        batch = response.json().get("value", []) or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        skip += len(batch)
    return rows


def _load_ufgh_nomenclature_keys(session: requests.Session) -> set[str]:
    keys: set[str] = set()
    for field in ("Артикул", "Description", "НаименованиеПолное"):
        flt = (
            f"substringof('{TARGET_TEXT}',{field}) "
            "and DeletionMark eq false and IsFolder eq false"
        )
        sel = "Ref_Key,Description,Артикул,НаименованиеПолное"
        url = (
            f"{BASE}/{quote(NOMENCLATURE_ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
        )
        for row in _fetch_paged(session, url, label="UFG-H/Nomenclature", page_size=1000):
            key = row.get("Ref_Key")
            if key:
                keys.add(key)
    return keys


def _load_analytics_nomenclature_map(
    session: requests.Session,
    analytics_keys: set[str],
) -> dict[str, str]:
    result: dict[str, str] = {}
    keys = sorted(k for k in analytics_keys if k and k != EMPTY)
    sel = "Ref_Key,Номенклатура_Key"
    for i in range(0, len(keys), 20):
        batch = keys[i:i + 20]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        url = (
            f"{BASE}/{quote(ANALYTICS_ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
        )
        response = request_with_retry(session, url, timeout=60, retries=3, label="UFG-H/Analytics")
        if response is None or not response.ok:
            continue
        for row in response.json().get("value", []) or []:
            result[row.get("Ref_Key")] = row.get("Номенклатура_Key") or ""
    return result


def _vp_entries_by_month(year: int, ref_month: int) -> dict[int, list[dict] | None]:
    by_month: dict[int, list[dict] | None] = {}
    for month in range(1, ref_month + 1):
        valovaya_pribyl._ensure_month_data(month, year)
        path = valovaya_pribyl._cache_path(month, year)
        data = _load_json(path)
        by_month[month] = data.get("entries") if data else None
    return by_month


def _load_fact_by_month(
    session: requests.Session,
    nomenclature_keys: set[str],
    year: int,
    ref_month: int,
    dept_guid: str | None = None,
) -> tuple[dict[int, float | None], dict]:
    entries_by_month = _vp_entries_by_month(year, ref_month)
    analytics_keys = {
        row.get("АналитикаУчетаНоменклатуры_Key")
        for entries in entries_by_month.values()
        for row in (entries or [])
        if row.get("АналитикаУчетаНоменклатуры_Key")
    }
    analytics_map = _load_analytics_nomenclature_map(session, analytics_keys)
    nashe_keys = valovaya_pribyl._load_nashe_keys()

    result: dict[int, float | None] = {}
    entries_count_by_month: dict[int, int] = {}
    matched_count_by_month: dict[int, int] = {}
    for month, entries in entries_by_month.items():
        if entries is None:
            result[month] = None
            entries_count_by_month[month] = 0
            matched_count_by_month[month] = 0
            continue

        total = 0.0
        matched = 0
        for row in entries:
            if dept_guid and row.get("Подразделение_Key") != dept_guid:
                continue
            if row.get("ТипЗапасов") == "КомиссионныйТовар":
                continue
            if row.get("АналитикаУчетаПоПартнерам_Key") in nashe_keys:
                continue
            analytics_key = row.get("АналитикаУчетаНоменклатуры_Key")
            if analytics_map.get(analytics_key) not in nomenclature_keys:
                continue
            total += _to_float(row.get("СуммаВыручки"))
            matched += 1

        result[month] = round(total, 2)
        entries_count_by_month[month] = len(entries)
        matched_count_by_month[month] = matched

    return result, {
        "vp_entries_by_month": entries_count_by_month,
        "matched_entries_by_month": matched_count_by_month,
        "analytics_keys": len(analytics_keys),
    }


def _load_order_lines(session: requests.Session, nomenclature_keys: set[str]) -> list[dict]:
    rows: list[dict] = []
    keys = sorted(nomenclature_keys)
    sel = "Ref_Key,LineNumber,Номенклатура_Key,Количество,Сумма,СуммаСНДС,Отменено"
    for i in range(0, len(keys), 15):
        batch = keys[i:i + 15]
        flt = " or ".join(f"Номенклатура_Key eq guid'{key}'" for key in batch)
        url = (
            f"{BASE}/{quote(ORDER_LINES_ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
        )
        rows.extend(_fetch_paged(session, url, label="UFG-H/OrderLines"))
    return rows


def _load_order_headers(session: requests.Session, order_keys: set[str]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    keys = sorted(k for k in order_keys if k and k != EMPTY)
    sel = (
        "Ref_Key,Posted,DeletionMark,Date,ДатаОтгрузки,Подразделение_Key,Валюта_Key,Соглашение_Key,"
        "ТД_НеУчитыватьВПланФакте,ТД_НеУчитыватьВПланФактеОтгрузки"
    )
    for i in range(0, len(keys), 15):
        batch = keys[i:i + 15]
        flt = " or ".join(f"Ref_Key eq guid'{key}'" for key in batch)
        url = (
            f"{BASE}/{quote(ORDER_ENTITY)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
        )
        response = request_with_retry(session, url, timeout=60, retries=3, label="UFG-H/Orders")
        if response is None or not response.ok:
            continue
        for row in response.json().get("value", []) or []:
            result[row.get("Ref_Key")] = row
    return result


def _load_plan_by_month(
    session: requests.Session,
    nomenclature_keys: set[str],
    year: int,
    ref_month: int,
    dept_guid: str | None = None,
) -> tuple[dict[int, float], dict]:
    lines = _load_order_lines(session, nomenclature_keys)
    order_keys = {
        row.get("Ref_Key")
        for row in lines
        if row.get("Ref_Key") and not row.get("Отменено")
    }
    orders = _load_order_headers(session, order_keys)
    result: dict[int, float] = defaultdict(float)
    used_lines = 0
    for row in lines:
        if row.get("Отменено"):
            continue
        order = orders.get(row.get("Ref_Key"))
        if not order:
            continue
        if dept_guid and order.get("Подразделение_Key") != dept_guid:
            continue
        if not order.get("Posted") or order.get("DeletionMark"):
            continue
        if order.get("ТД_НеУчитыватьВПланФакте") or order.get("ТД_НеУчитыватьВПланФактеОтгрузки"):
            continue
        ship_date = (order.get("ДатаОтгрузки") or order.get("Date") or "")[:10]
        if len(ship_date) < 7 or ship_date[:4] != str(year):
            continue
        try:
            month = int(ship_date[5:7])
        except (TypeError, ValueError):
            continue
        if month < 1 or month > ref_month:
            continue
        amount = _to_float(row.get("Сумма")) * _currency_rate(order.get("Валюта_Key"))
        if amount <= 0:
            amount = _to_float(row.get("СуммаСНДС")) * _currency_rate(order.get("Валюта_Key"))
        if amount <= 0:
            continue
        result[month] += amount
        used_lines += 1

    return {month: round(result.get(month, 0.0), 2) for month in range(1, ref_month + 1)}, {
        "order_lines": len(lines),
        "orders": len(orders),
        "used_plan_lines": used_lines,
    }


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 2)


def get_ufg_h_shipments_monthly(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(ref_year, ref_month, dept_guid)
    cached = _load_json(path)
    if (
        cached is not None
        and cached.get("source") == SOURCE_TAG
        and cached.get("cache_date") == today.isoformat()
    ):
        return cached

    session = requests.Session()
    session.auth = AUTH

    nomenclature_keys = _load_ufgh_nomenclature_keys(session)
    plan_by_month, plan_debug = _load_plan_by_month(
        session,
        nomenclature_keys,
        ref_year,
        ref_month,
        dept_guid,
    )
    fact_by_month, fact_debug = _load_fact_by_month(
        session,
        nomenclature_keys,
        ref_year,
        ref_month,
        dept_guid,
    )

    months = []
    for month_num in range(1, ref_month + 1):
        plan = plan_by_month.get(month_num, 0.0)
        fact = fact_by_month.get(month_num)
        row = {
            "year": ref_year,
            "month": month_num,
            "month_name": valovaya_pribyl.MONTH_NAMES[month_num],
            "plan": plan,
            "fact": fact,
            "kpi_pct": _kpi_pct(plan, fact),
            "has_data": fact is not None or plan > 0,
            "values_unit": "руб.",
        }
        months.append(row)

    ref_row = months[-1] if months else None
    total_plan = round(sum(float(row.get("plan") or 0) for row in months), 2)
    fact_values = [row.get("fact") for row in months if row.get("fact") is not None]
    total_fact = round(sum(float(value or 0) for value in fact_values), 2) if fact_values else None

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "dept_guid": dept_guid,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months,
        "monthly_data": months,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "ytd": {
            "total_plan": total_plan,
            "total_fact": total_fact,
            "kpi_pct": _kpi_pct(total_plan, total_fact),
            "months_with_data": sum(1 for row in months if row.get("has_data")),
            "months_total": len(months),
            "values_unit": "руб.",
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": valovaya_pribyl.MONTH_NAMES[ref_month],
        },
        "debug": {
            "nomenclature_filter": TARGET_TEXT,
            "nomenclature_keys": len(nomenclature_keys),
            **plan_debug,
            **fact_debug,
        },
    }
    _save_json(path, payload)
    return payload
