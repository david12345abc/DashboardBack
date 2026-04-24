"""
calc_shipment_share_bmi_gazprom.py — Доля (БМИ + ПАО Газпром) в отгрузке YTD (MRK-06).

Берёт тот же регистр и те же фильтры, что в calc_fact_fast.py (факт отгрузки),
но период — с начала года до сегодня (или до конца указанного месяца). Считает:
  - отгрузку по каждому из 6 отделов продаж,
  - сумму по БМИ, по Газпрому и (БМИ + Газпром),
  - долю этих двух отделов в общей отгрузке.

Использование (CLI):
  python calc_shipment_share_bmi_gazprom.py [ГГГГ] [--force]
  python calc_shipment_share_bmi_gazprom.py [ГГГГ-ММ-ДД-до]  (ограничить верхней датой)

Использование (как модуль):
  from .calc_shipment_share_bmi_gazprom import get_shipment_share_bmi_gazprom
  data = get_shipment_share_bmi_gazprom(2026)
  # {'total': float, 'bmi': float, 'gp': float, 'pair': float, 'pct_pair': float, ...}
"""
import functools
import json
import os
import sys
import time
from datetime import date
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Эталонное",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Ключевые клиенты",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Дилерские (ОПБО)",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "ПАО Газпром",
}
OPBO_DEPT = "7587c178-92f6-11f0-96f9-6cb31113810e"
BMI_DEPT  = "9edaa7d4-37a5-11ee-93d3-6cb31113810e"
GP_DEPT   = "bd7b5184-9f9c-11e4-80da-001e67112509"

EXCLUDE_PARTNER_NAMES = {
    "АЛМАЗ ООО (рабочий)",
    "Турбулентность-Дон ООО",
    "Турбулентность-ДОН ООО НПО",
    "СКТБ Турбо-Дон ООО",
    "Метрогазсервис ООО",
}
EXCLUDE_PARTNER_NAMES_NO_MGS = EXCLUDE_PARTNER_NAMES - {"Метрогазсервис ООО"}

ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "факт")
PARTNERS_CACHE = os.path.join(CACHE_DIR, "partners_exclude_cache.json")


def _last_day_of_month(y: int, m: int) -> int:
    if m in {1, 3, 5, 7, 8, 10, 12}:
        return 31
    if m == 2:
        return 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28
    return 30


def _compute_end_dt(year: int, month: int | None, today: date) -> date:
    if month is not None:
        m = max(1, min(12, int(month)))
        return date(year, m, _last_day_of_month(year, m))
    if year == today.year:
        return today
    return date(year, 12, 31)


def _fetch_shipment_entries(session: requests.Session, p_start: str, p_end: str,
                             cache_path: str, use_cache: bool) -> list[dict]:
    """Скачать записи регистра РаспоряженияНаОтгрузку за [p_start; p_end)."""
    if use_cache and os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as fh:
                cached = json.load(fh).get("entries")
            if cached is not None:
                return cached
        except Exception:
            pass

    period_filter = quote(
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}' and Active eq true",
        safe="",
    )
    select_fields = "Period,Active,Распоряжение,Распоряжение_Type,ВидДвиженияРегистра,Сумма,Сторно"
    entries: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
            f"?$format=json&$top=5000&$skip={skip}"
            f"&$filter={period_filter}&$select={select_fields}"
        )
        r = session.get(url, timeout=180)
        r.raise_for_status()
        rows = r.json().get("value", [])
        for row in rows:
            if row.get("Распоряжение_Type") == ORDER_TYPE:
                entries.append({
                    "guid": row.get("Распоряжение"),
                    "Сумма": row.get("Сумма", 0) or 0,
                    "ВидДвижения": row.get("ВидДвиженияРегистра", ""),
                    "Сторно": row.get("Сторно", False),
                    "Period": row.get("Period", ""),
                })
        if len(rows) < 5000:
            break
        skip += 5000

    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump({"entries": entries}, fh, ensure_ascii=False)
    except Exception:
        pass

    return entries


def _fetch_order_attributes(session: requests.Session,
                            order_guids: list[str]) -> dict[str, dict]:
    """Загрузить реквизиты заказов клиента батчами по 15."""
    select_order = quote(
        "Ref_Key,Подразделение_Key,Партнер_Key,Соглашение_Key,"
        "ТД_СопровождениеПродажи,ТД_НеУчитыватьВПланФакте",
        safe=",_",
    )
    orders: dict[str, dict] = {}
    batch_size = 15
    for i in range(0, len(order_guids), batch_size):
        batch = order_guids[i:i + batch_size]
        flt = quote(" or ".join(f"Ref_Key eq guid'{g}'" for g in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента"
            f"?$format=json&$select={select_order}&$top={batch_size}&$filter={flt}"
        )
        r = session.get(url, timeout=30)
        if not r.ok:
            continue
        for item in r.json().get("value", []):
            orders[item["Ref_Key"]] = {
                "dept": item.get("Подразделение_Key", EMPTY),
                "partner": item.get("Партнер_Key", EMPTY),
                "agreement": item.get("Соглашение_Key", EMPTY),
                "soprovozhd": item.get("ТД_СопровождениеПродажи", False),
                "ne_uchit": item.get("ТД_НеУчитыватьВПланФакте", False),
            }
    return orders


def _load_partner_names(session: requests.Session,
                        partner_keys: list[str],
                        use_cache: bool) -> dict[str, str]:
    """Имена партнёров для исключения (с файловым кэшем)."""
    partners: dict[str, str] = {}
    if use_cache and os.path.exists(PARTNERS_CACHE):
        try:
            with open(PARTNERS_CACHE, encoding="utf-8") as fh:
                partners = json.load(fh)
        except Exception:
            partners = {}
    missing = [k for k in partner_keys if k not in partners]
    if not missing:
        return partners
    batch_size = 15
    for i in range(0, len(missing), batch_size):
        batch = missing[i:i + batch_size]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Catalog_Партнеры?$format=json&$filter={flt}"
            f"&$select=Ref_Key,Description&$top={batch_size}"
        )
        r = session.get(url, timeout=30)
        if not r.ok:
            continue
        for item in r.json().get("value", []):
            partners[item["Ref_Key"]] = (item.get("Description") or "").strip()
    try:
        os.makedirs(os.path.dirname(PARTNERS_CACHE), exist_ok=True)
        with open(PARTNERS_CACHE, "w", encoding="utf-8") as fh:
            json.dump(partners, fh, ensure_ascii=False)
    except Exception:
        pass
    return partners


def get_shipment_share_bmi_gazprom(year: int | None = None,
                                   *,
                                   month: int | None = None,
                                   dept_guid: str | None = None,  # noqa: ARG001
                                   force: bool = False) -> dict:
    """
    Доля (БМИ + ПАО Газпром) в отгрузке YTD.

    :param year: год отчёта (по умолчанию — текущий).
    :param month: верхняя граница периода (янв..month). По умолчанию — «до сегодня» для текущего года.
    :param dept_guid: игнорируется (плитка всегда по всей коммерции); поддержан для совместимости с cache_manager.
    :param force: обходить файловый кэш регистра отгрузок и партнёров.

    :return: словарь:
        {
          'year': int, 'month': int,
          'period_start': 'YYYY-MM-DD', 'period_end': 'YYYY-MM-DD',
          'total': float,                # отгрузка всех 6 отделов, ₽
          'by_dept': {name: amount},     # разбивка по отделам (имя → ₽)
          'bmi': float, 'gp': float, 'pair': float,
          'pct_bmi': float, 'pct_gp': float, 'pct_pair': float,
          'counted_rows': int,           # строк «Расход» после всех фильтров
          'reject_stats': {...},         # статистика отбраковки
        }
    """
    today = date.today()
    y = int(year) if year else today.year
    end_dt = _compute_end_dt(y, month, today)
    start_dt = date(y, 1, 1)

    p_start = f"{start_dt.isoformat()}T00:00:00"
    p_end = f"{date.fromordinal(end_dt.toordinal() + 1).isoformat()}T00:00:00"

    use_cache = not force
    # v2 — кэш содержит поле Period (нужно для помесячной разбивки).
    cache_path = os.path.join(CACHE_DIR, f"ytd_shipment_v2_{y}_{end_dt.isoformat()}.json")

    session = requests.Session()
    session.auth = AUTH

    entries = _fetch_shipment_entries(session, p_start, p_end, cache_path, use_cache)
    order_guids = sorted({e["guid"] for e in entries if e.get("guid")})

    orders = _fetch_order_attributes(session, order_guids)

    unique_partner_keys = list({o["partner"] for o in orders.values() if o["partner"] != EMPTY})
    partners = _load_partner_names(session, unique_partner_keys, use_cache)

    exclude_keys_full = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES}
    exclude_keys_no_mgs = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    passed_guids: set[str] = set()
    stats = {"dept": 0, "agreement": 0, "partner": 0, "ne_uchit": 0, "not_found": 0, "passed": 0}
    for guid in order_guids:
        o = orders.get(guid)
        if not o:
            stats["not_found"] += 1
            continue
        if o["dept"] not in DEPARTMENTS or o["dept"] == EMPTY:
            stats["dept"] += 1
            continue
        if o["agreement"] == EMPTY or not o["agreement"]:
            stats["agreement"] += 1
            continue
        if o["dept"] == OPBO_DEPT:
            if o["partner"] in exclude_keys_no_mgs:
                stats["partner"] += 1
                continue
        else:
            if o["partner"] in exclude_keys_full and not o["soprovozhd"]:
                stats["partner"] += 1
                continue
        if o["ne_uchit"]:
            stats["ne_uchit"] += 1
            continue
        passed_guids.add(guid)
        stats["passed"] += 1

    by_dept_guid: dict[str, float] = {k: 0.0 for k in DEPARTMENTS}
    total = 0.0
    counted = 0
    for e in entries:
        if e["guid"] not in passed_guids:
            continue
        if e["ВидДвижения"] != "Расход":
            continue
        o = orders[e["guid"]]
        summa = float(e["Сумма"] or 0)
        by_dept_guid[o["dept"]] += summa
        total += summa
        counted += 1

    def _pct(x: float) -> float | None:
        return round(x / total * 100, 1) if total else None

    bmi = by_dept_guid[BMI_DEPT]
    gp = by_dept_guid[GP_DEPT]
    pair = bmi + gp

    by_dept_named = {DEPARTMENTS[k]: round(v, 2) for k, v in by_dept_guid.items()}

    return {
        "year": y,
        "month": end_dt.month,
        "period_start": start_dt.isoformat(),
        "period_end": end_dt.isoformat(),
        "total": round(total, 2),
        "by_dept": by_dept_named,
        "bmi": round(bmi, 2),
        "gp": round(gp, 2),
        "pair": round(pair, 2),
        "pct_bmi": _pct(bmi),
        "pct_gp": _pct(gp),
        "pct_pair": _pct(pair),
        "counted_rows": counted,
        "reject_stats": stats,
    }


def get_shipment_share_bmi_gazprom_monthly(year: int | None = None,
                                           *,
                                           month: int | None = None,
                                           dept_guid: str | None = None,  # noqa: ARG001
                                           force: bool = False) -> dict:
    """
    Помесячная разбивка MRK-06 «Доля Газпром + БМИ в отгрузке».

    Возвращает:
        {
          'year': int,
          'months': [
            {
              'month': int,
              'total': float,  # общая отгрузка 6 отделов за месяц, ₽
              'bmi':   float,
              'gp':    float,
              'pair':  float,  # БМИ + Газпром за месяц
              'pct_pair': float | None,  # доля БМИ+Газпром в отгрузке за месяц, %
              'by_dept': {name: amount},
            },
            ...
          ],
        }
    """
    today = date.today()
    y = int(year) if year else today.year
    end_dt = _compute_end_dt(y, month, today)
    start_dt = date(y, 1, 1)

    p_start = f"{start_dt.isoformat()}T00:00:00"
    p_end = f"{date.fromordinal(end_dt.toordinal() + 1).isoformat()}T00:00:00"

    use_cache = not force
    cache_path = os.path.join(CACHE_DIR, f"ytd_shipment_v2_{y}_{end_dt.isoformat()}.json")

    session = requests.Session()
    session.auth = AUTH

    entries = _fetch_shipment_entries(session, p_start, p_end, cache_path, use_cache)
    order_guids = sorted({e["guid"] for e in entries if e.get("guid")})

    orders = _fetch_order_attributes(session, order_guids)

    unique_partner_keys = list({o["partner"] for o in orders.values() if o["partner"] != EMPTY})
    partners = _load_partner_names(session, unique_partner_keys, use_cache)

    exclude_keys_full = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES}
    exclude_keys_no_mgs = {k for k, v in partners.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    passed_guids: set[str] = set()
    for guid in order_guids:
        o = orders.get(guid)
        if not o:
            continue
        if o["dept"] not in DEPARTMENTS or o["dept"] == EMPTY:
            continue
        if o["agreement"] == EMPTY or not o["agreement"]:
            continue
        if o["dept"] == OPBO_DEPT:
            if o["partner"] in exclude_keys_no_mgs:
                continue
        else:
            if o["partner"] in exclude_keys_full and not o["soprovozhd"]:
                continue
        if o["ne_uchit"]:
            continue
        passed_guids.add(guid)

    months_out: list[dict] = []
    for m in range(1, end_dt.month + 1):
        by_dept_guid: dict[str, float] = {k: 0.0 for k in DEPARTMENTS}
        total_m = 0.0
        for e in entries:
            if e["guid"] not in passed_guids:
                continue
            if e["ВидДвижения"] != "Расход":
                continue
            period_raw = e.get("Period") or ""
            if len(period_raw) < 7:
                continue
            try:
                row_year = int(period_raw[0:4])
                row_month = int(period_raw[5:7])
            except (TypeError, ValueError):
                continue
            if row_year != y or row_month != m:
                continue
            o = orders[e["guid"]]
            summa = float(e["Сумма"] or 0)
            by_dept_guid[o["dept"]] += summa
            total_m += summa

        bmi_m = by_dept_guid[BMI_DEPT]
        gp_m = by_dept_guid[GP_DEPT]
        pair_m = bmi_m + gp_m
        pct_m = round(pair_m / total_m * 100, 1) if total_m else None
        months_out.append({
            "month": m,
            "total": round(total_m, 2),
            "bmi": round(bmi_m, 2),
            "gp": round(gp_m, 2),
            "pair": round(pair_m, 2),
            "pct_pair": pct_m,
            "by_dept": {DEPARTMENTS[k]: round(v, 2) for k, v in by_dept_guid.items()},
        })

    return {
        "year": y,
        "months": months_out,
    }


def _main_cli() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    out = functools.partial(print, flush=True)

    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--force"]

    today = date.today()
    month_arg: int | None = None
    if args and "-" in args[0] and len(args[0]) == 10:
        end_dt = date.fromisoformat(args[0])
        year = end_dt.year
        month_arg = end_dt.month
    elif args:
        year = int(args[0])
    else:
        year = today.year

    t0 = time.time()
    out(f"\n{'═' * 58}")
    out(f"  ДОЛЯ (БМИ + ПАО Газпром) В ОТГРУЗКЕ — YTD {year}")
    out(f"{'═' * 58}")

    data = get_shipment_share_bmi_gazprom(year, month=month_arg, force=force)

    out(f"  Период:  {data['period_start']} — {data['period_end']}")
    out(f"  Строк Расход (после фильтра): {data['counted_rows']}\n")
    out(f"  {'Отдел':30s} {'Отгрузка, ₽':>18s} {'Доля':>8s}")
    out(f"  {'─' * 30} {'─' * 18} {'─' * 8}")
    total = data["total"]
    items = sorted(data["by_dept"].items(), key=lambda x: -x[1])
    for name, v in items:
        mark = " ◆" if name in ("БМИ", "ПАО Газпром") else "  "
        pct_v = (v / total * 100) if total else 0.0
        out(f"{mark}{name:30s} {v:>18,.2f} {pct_v:>7.1f}%")
    out(f"  {'─' * 30} {'─' * 18} {'─' * 8}")
    out(f"  {'ИТОГО (все 6 отделов)':30s} {total:>18,.2f} {'100.0%':>8s}")
    out(f"\n{'═' * 58}")
    out(f"  ДОЛЯ БМИ + ПАО ГАЗПРОМ")
    out(f"{'═' * 58}")
    out(f"  БМИ:             {data['bmi']:>18,.2f}  ({data['pct_bmi']:>5.1f}%)")
    out(f"  ПАО Газпром:     {data['gp']:>18,.2f}  ({data['pct_gp']:>5.1f}%)")
    out(f"  БМИ + Газпром:   {data['pair']:>18,.2f}  ({data['pct_pair']:>5.1f}%)  ← искомая доля")
    out(f"  ИТОГО отгрузка:  {total:>18,.2f}")
    out(f"\n  Время:  {time.time() - t0:.0f}с")


if __name__ == "__main__":
    _main_cli()
