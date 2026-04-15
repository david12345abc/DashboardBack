"""
Получение претензий (Catalog_Претензии) из 1С OData за указанный месяц.

Логика повторяет export_claims.py, но возвращает список dict (для JSON API),
а не записывает CSV. Результат кэшируется на день в JSON-файл.
"""
from __future__ import annotations

import calendar
import json
import logging
import time
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ALLOWED_DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
}

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"claims_{year}_{month:02d}.json"


def _load_cache(year: int, month: int) -> list[dict] | None:
    p = _cache_path(year, month)
    if not p.exists():
        return None
    try:
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if data.get('date') == date.today().isoformat():
            return data.get('rows')
    except (json.JSONDecodeError, OSError):
        pass
    return None


def _save_cache(year: int, month: int, rows: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), 'w', encoding='utf-8') as f:
            json.dump({'date': date.today().isoformat(), 'rows': rows}, f, ensure_ascii=False)
    except OSError:
        pass


def _fetch_from_odata(year: int, month: int) -> list[dict]:
    """Загружает претензии из 1С OData за указанный месяц."""
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01T00:00:00"
    date_to = f"{year}-{month:02d}-{last_day}T23:59:59"

    session = requests.Session()
    session.auth = AUTH

    select_claims = (
        "Ref_Key,Code,Description,Партнер_Key,ДатаРегистрации,ДатаОкончания,"
        "ТД_ДатаОкончанияПлан,ТД_ЗаказКлиента_Key,ТД_Номенклатура_Key,"
        "ТД_Характеристика_Key,ОписаниеПретензии,DeletionMark,Статус"
    )

    claims = []
    skip = 0
    while True:
        odata_filter = (
            f"ДатаРегистрации ge datetime'{date_from}'"
            f" and ДатаРегистрации le datetime'{date_to}'"
            f" and Статус ne 'Удовлетворена'"
        )
        url = (
            f"{BASE}/Catalog_Претензии?$format=json"
            f"&$select={select_claims}&$top=5000&$skip={skip}"
            f"&$filter={odata_filter}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("Claims HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("Claims HTTP %d: %s", r.status_code, r.text[:300])
            break
        rows = r.json().get("value", [])
        claims.extend(rows)
        if len(rows) < 5000:
            break
        skip += 5000

    partners = {}
    p_skip = 0
    while True:
        url = f"{BASE}/Catalog_Партнеры?$format=json&$top=5000&$skip={p_skip}&$select=Ref_Key,Description"
        try:
            r = session.get(url, timeout=60)
        except Exception:
            break
        if not r.ok:
            break
        rows = r.json().get("value", [])
        for p in rows:
            partners[p["Ref_Key"]] = p.get("Description", "").strip()
        if len(rows) < 5000:
            break
        p_skip += 5000

    depts = {}
    try:
        r = session.get(
            f"{BASE}/Catalog_СтруктураПредприятия?$format=json&$top=5000&$select=Ref_Key,Description",
            timeout=30,
        )
        if r.ok:
            for d in r.json().get("value", []):
                depts[d["Ref_Key"]] = d.get("Description", "").strip()
    except Exception:
        pass

    order_keys_needed = set()
    for c in claims:
        ok = c.get("ТД_ЗаказКлиента_Key")
        if ok and ok != EMPTY:
            order_keys_needed.add(ok)

    orders_info = {}
    o_skip = 0
    o_select = "Ref_Key,Number,Date,Подразделение_Key,СуммаДокумента"
    while True:
        url = f"{BASE}/Document_ЗаказКлиента?$format=json&$top=500&$skip={o_skip}&$select={o_select}"
        try:
            r = session.get(url, timeout=120)
        except Exception:
            break
        if not r.ok:
            break
        chunk = r.json().get("value", [])
        if not chunk:
            break
        for item in chunk:
            rk = item["Ref_Key"]
            if rk in order_keys_needed:
                orders_info[rk] = item
        o_skip += len(chunk)
        if len(orders_info) >= len(order_keys_needed):
            break

    nom_names: dict[str, str] = {}
    char_names: dict[str, str] = {}
    for cat_name, target in [
        ("Catalog_Номенклатура", nom_names),
        ("Catalog_ХарактеристикиНоменклатуры", char_names),
    ]:
        try:
            r = session.get(f"{BASE}/{cat_name}?$format=json&$top=1", timeout=10)
        except Exception:
            continue
        if not r.ok:
            continue
        c_skip = 0
        while True:
            url = f"{BASE}/{cat_name}?$format=json&$top=5000&$skip={c_skip}&$select=Ref_Key,Description"
            try:
                r2 = session.get(url, timeout=60)
            except Exception:
                break
            if not r2.ok:
                break
            rows = r2.json().get("value", [])
            for item in rows:
                target[item["Ref_Key"]] = item.get("Description", "").strip()
            if len(rows) < 5000:
                break
            c_skip += 5000

    result_rows = []
    for c in claims:
        order_key = c.get("ТД_ЗаказКлиента_Key", "")
        order = orders_info.get(order_key)
        if not order:
            continue

        order_dept_key = order.get("Подразделение_Key", "")
        if order_dept_key not in ALLOWED_DEPARTMENTS:
            continue

        partner = partners.get(c.get("Партнер_Key", ""), c.get("Партнер_Key", ""))
        date_reg = (c.get("ДатаРегистрации") or "")[:10]
        date_plan = (c.get("ТД_ДатаОкончанияПлан") or "")[:10]
        date_end = (c.get("ДатаОкончания") or "")[:10]

        order_num = order.get("Number", "").strip()
        order_dept = depts.get(order_dept_key, order_dept_key)
        order_sum = order.get("СуммаДокумента", 0)

        nom_key = c.get("ТД_Номенклатура_Key", "")
        nom = nom_names.get(nom_key, nom_key if nom_key and nom_key != EMPTY else "")

        char_key = c.get("ТД_Характеристика_Key", "")
        char = char_names.get(char_key, char_key if char_key and char_key != EMPTY else "")

        desc = (c.get("ОписаниеПретензии") or "").replace("\r\n", " ").replace("\n", " ")
        status = c.get("Статус", "")

        result_rows.append({
            "code": c.get("Code", ""),
            "name": c.get("Description", ""),
            "partner": partner,
            "date_reg": date_reg,
            "date_plan": date_plan,
            "date_end": date_end,
            "order_num": order_num,
            "order_dept": order_dept,
            "order_dept_key": order_dept_key,
            "nomenclature": nom,
            "characteristic": char,
            "order_sum": order_sum,
            "description": desc,
            "status": status,
        })

    return result_rows


def fetch_claims_for_month(year: int, month: int) -> list[dict]:
    """Возвращает список претензий за месяц (с кэшированием на день)."""
    cached = _load_cache(year, month)
    if cached is not None:
        return cached

    try:
        rows = _fetch_from_odata(year, month)
    except Exception as e:
        logger.error("Failed to fetch claims: %s", e)
        rows = []

    _save_cache(year, month, rows)
    return rows
