"""
calc_postavshchiki.py — задолженность с поставщиками.

Используется для плитки FND-T3 «Соотношение ДЗ и КЗ» (ПСД, «Мой дашборд»).

ИСТОЧНИК: AccumulationRegister_РасчетыСПоставщикамиПланОплат_RecordType.

Почему именно этот регистр:
  - «РасчетыСПоставщикамиПоДокументам» в OData отдаёт только документы
    ВводаОстатков за 2017 год — в оперативном учёте не пополняется, остатки
    по нему «замерли».
  - «РасчетыСПоставщикамиПланОплат» заполняется Document_РегистраторРасчетов
    на каждую операцию закупки/оплаты и имеет свежие движения (прод-запись
    на момент разработки: 2026-04-23 11:11:26).

Логика расчёта (аналог виртуальной таблицы остатков регистра накопления):
  Остаток по заказу поставщику (ОбъектРасчетов):
    net = Σ(Receipt.Сумма) − Σ(Expense.Сумма)   в пределах Period <= cutoff
  Интерпретация знака:
    net > 0  → наш долг поставщику          (КЗ, «Наш долг»)
    net < 0  → наш аванс у поставщика        (ДЗ, «Долг поставщика»)

  На конец периода агрегируем:
    total_kz = Σ max(net, 0)     по всем ОбъектамРасчетов
    total_dz = Σ max(-net, 0)    по всем ОбъектамРасчетов

Валюта: поле `Сумма` хранится в валюте взаиморасчётов (для большинства
контрагентов — РУБ). Валюты не перезачитываем — работаем в собственной
валюте договора, как и в отчёте 1С «Задолженность поставщиков».

Запуск (CLI):
  python calc_postavshchiki.py [ГГГГ-ММ-ДД]        # остаток на конкретную дату
  python calc_postavshchiki.py                     # остаток на сегодня
  python calc_postavshchiki.py --monthly 2026     # помесячно за год
"""
from __future__ import annotations

import calendar
import functools
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

REGISTER = "AccumulationRegister_РасчетыСПоставщикамиПланОплат_RecordType"
EMPTY = "00000000-0000-0000-0000-000000000000"

TOLERANCE = 0.01
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

# Маркер версии кэша. Меняй при изменении формулы расчёта, чтобы
# пересобрать кэш на сервере при первом запросе.
SOURCE_TAG = "plan_oplat_net_v1"


# ──────────────────────────────────────────────────────────────────────
#  Работа с регистром
# ──────────────────────────────────────────────────────────────────────
def _fetch_all_register(session: requests.Session, na_datu_iso: str) -> list[dict]:
    """Выгрузить все записи регистра с Period ≤ na_datu (конец дня)."""
    sel = quote(
        "RecordType,Period,Active,"
        "ОбъектРасчетов_Key,АналитикаУчетаПоПартнерам_Key,"
        "Валюта_Key,Сумма",
        safe=",_",
    )
    flt = quote(
        f"Period le datetime'{na_datu_iso}T23:59:59' and Active eq true",
        safe="",
    )

    records: list[dict] = []
    skip = 0
    PAGE = 5000
    t0 = time.time()

    while True:
        url = (
            f"{BASE}/{REGISTER}"
            f"?$format=json&$select={sel}"
            f"&$filter={flt}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=180, retries=4, label="KZ/Register")
        if r is None or not r.ok:
            logger.warning("Postavshchiki HTTP %s on skip=%d",
                           r.status_code if r else "no-response", skip)
            break
        batch = r.json().get("value", [])
        records.extend(batch)
        if skip % 50000 == 0 or len(batch) < PAGE:
            logger.info("  postavshchiki: %d записей · %.1fс", len(records), time.time() - t0)
        if len(batch) < PAGE:
            break
        skip += PAGE

    return records


def _aggregate_balances_at(records: list[dict], cutoff_period: str) -> tuple[float, float]:
    """
    Свернуть записи до cutoff_period (включительно) и вернуть:
      (total_kz, total_dz)
    КЗ — сумма положительных нетто-остатков по заказам поставщикам (мы должны),
    ДЗ — сумма отрицательных нетто-остатков (наш аванс у поставщика).
    """
    per_obj: dict[str, float] = defaultdict(float)

    for r in records:
        period = r.get("Period") or ""
        if not period or period > cutoff_period:
            continue

        obj = str(r.get("ОбъектРасчетов_Key") or "").lower()
        if not obj or obj == EMPTY:
            continue

        amt = float(r.get("Сумма") or 0)
        rtype = r.get("RecordType") or ""

        if rtype == "Receipt":
            per_obj[obj] += amt
        elif rtype == "Expense":
            per_obj[obj] -= amt

    total_kz = 0.0
    total_dz = 0.0
    for net in per_obj.values():
        if net > TOLERANCE:
            total_kz += net
        elif net < -TOLERANCE:
            total_dz += -net

    return round(total_kz, 2), round(total_dz, 2)


# ──────────────────────────────────────────────────────────────────────
#  Кэш
# ──────────────────────────────────────────────────────────────────────
def _cache_path_snapshot(na_datu: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"postavshchiki_{na_datu.isoformat()}.json"


def _cache_path_monthly(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"postavshchiki_monthly_{year}_{month:02d}.json"


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

    records = _fetch_all_register(session, na_datu.isoformat())
    cutoff = f"{na_datu.isoformat()}T23:59:59"
    total_kz, total_dz = _aggregate_balances_at(records, cutoff)

    payload = {
        "na_datu": na_datu.isoformat(),
        "source": SOURCE_TAG,
        # Основные поля (текущее API, читается chairman_data.py):
        "total_dolg_regl": total_kz,          # КЗ поставщикам (мы должны)
        "total_predoplata_regl": total_dz,    # ДЗ поставщиков (наши авансы)
    }
    _save_json(_cache_path_snapshot(na_datu), payload)
    return payload


def get_supplier_monthly(year: int, ref_month: int) -> dict:
    """
    Помесячные остатки на конец каждого месяца с января по ref_month
    (не позже сегодняшней даты).

    Один запрос к OData (на самую позднюю дату), дальше остатки считаются
    локальной фильтрацией по Period.
    """
    cache_path = _cache_path_monthly(year, ref_month)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        rows = cached.get("months") or []
        if rows and all(
            "dolg_regl" in r and "predoplata_regl" in r for r in rows
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

    latest = snap_dates[-1][1]

    session = requests.Session()
    session.auth = AUTH
    logger.info("calc_postavshchiki: monthly for %s, ref_month=%d, latest=%s",
                year, ref_month, latest.isoformat())

    records = _fetch_all_register(session, latest.isoformat())

    rows_out: list[dict] = []
    for mm, na_datu in snap_dates:
        cutoff = f"{na_datu.isoformat()}T23:59:59"
        total_kz, total_dz = _aggregate_balances_at(records, cutoff)
        rows_out.append({
            "year": year,
            "month": mm,
            "na_datu": na_datu.isoformat(),
            "dolg_regl": total_kz,          # КЗ
            "predoplata_regl": total_dz,    # ДЗ
        })

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
        _print(f"  источник: РасчетыСПоставщикамиПланОплат (tag={data.get('source')})")
        _print("=" * 60)
        _print(f"  {'Мес.':<6s} {'На дату':<12s} {'КЗ (наш долг)':>18s} {'ДЗ (авансы)':>20s}")
        for r in data["months"]:
            _print(
                f"  {r['month']:<6d} {r['na_datu']:<12s} "
                f"{r['dolg_regl']:>18,.2f} {r['predoplata_regl']:>20,.2f}"
            )
        return

    na_datu_str = args[0] if args else date.today().isoformat()
    na_datu = date.fromisoformat(na_datu_str)
    data = get_supplier_snapshot(na_datu)
    _print("=" * 60)
    _print(f"  ПОСТАВЩИКИ на {data['na_datu']}")
    _print(f"  источник: РасчетыСПоставщикамиПланОплат (tag={data.get('source')})")
    _print("=" * 60)
    _print(f"  КЗ (наш долг поставщикам):   {data['total_dolg_regl']:>18,.2f}")
    _print(f"  ДЗ (наши авансы поставщикам): {data['total_predoplata_regl']:>18,.2f}")


if __name__ == "__main__":
    _main_cli()
