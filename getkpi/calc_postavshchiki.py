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
