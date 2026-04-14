"""
calc_dogovory_fact.py — Факт подписанных договоров за месяц.

Источник: InformationRegister_ТД_ДоговорыПодписанные (реальный регистр 1С).

Логика (воспроизведение 1С-запроса «ВТ_ДоговорыФакт»):
  1. ДатаПодписания — в пределах указанного месяца
  2. Спецификация.Статус = «Действует» (через Catalog_СоглашенияСКлиентами)
  3. Подразделение_Key — один из 6 целевых отделов продаж, ≠ пусто
  4. ТД_ПодтвержденоБухгалтером — пропуск (параметр ВЫГРУЗКА = False → всегда TRUE)
  5. Партнёры-перепродажа (CASE WHEN с ОПБО / без):
     - ОПБО: НЕ Партнер В (ПартнерыПерепродажиБезМГС)
     - Прочие: НЕ (Партнер В (ПартнерыПерепродажи) И НЕ ТД_СопровождениеПродажи)
  6. ЗаказКлиента пуст → OK, иначе НЕ ЗаказКлиента.ТД_НеУчитыватьВПланФакте
  7. Пересчёт в рубли по валюте заказа (если заказ есть и валюта ≠ RUB)

Примечание: $filter на InformationRegister не поддерживается (HTTP 500),
            поэтому загружаются ВСЕ строки и фильтруются в Python.

Использование:
  python calc_dogovory_fact.py [ГГГГ-ММ]   # по умолчанию 2026-03
"""
from __future__ import annotations

import requests, sys, time, json, os, calendar, functools, logging
from datetime import date
from pathlib import Path
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

logger = logging.getLogger(__name__)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

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
OPBO_DEPT = "7587c178-92f6-11f0-96f9-6cb31113810e"

EXCLUDE_PARTNER_NAMES = {
    "АЛМАЗ ООО (рабочий)",
    "Турбулентность-Дон ООО",
    "Турбулентность-ДОН ООО НПО",
    "СКТБ Турбо-Дон ООО",
    "Метрогазсервис ООО",
}
EXCLUDE_PARTNER_NAMES_NO_MGS = EXCLUDE_PARTNER_NAMES - {"Метрогазсервис ООО"}

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

F_PARTNERS = "partners_exclude_cache.json"

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

REG = "InformationRegister_ТД_ДоговорыПодписанные"
SELECT_FIELDS = (
    "Спецификация_Key,Подразделение_Key,Партнер_Key,"
    "ЗаказКлиента_Key,СуммаДоговора,ДатаПодписания,"
    "ТД_СопровождениеПродажи,ТД_ПодтвержденоБухгалтером"
)
BATCH = 15

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


# ═══════════════════════════════════════════════════════════
# API-функции (импорт из Django)
# ═══════════════════════════════════════════════════════════

def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"dogovory_{year}_{month:02d}.json"


def _load_cache(year: int, month: int) -> float | None:
    p = _cache_path(year, month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_date") == date.today().isoformat():
            return data.get("total")
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, month: int, total: float) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), "w", encoding="utf-8") as f:
            json.dump({"cache_date": date.today().isoformat(), "total": total},
                      f, ensure_ascii=False)
    except OSError:
        pass


def _load_register(session: requests.Session) -> list[dict]:
    """Загрузить ВСЕ строки регистра ТД_ДоговорыПодписанные."""
    all_rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{REG}?$format=json&$top=5000&$skip={skip}"
            f"&$select={SELECT_FIELDS}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception:
            break
        if not r.ok:
            break
        rows = r.json().get("value", [])
        all_rows.extend(rows)
        if len(rows) < 5000:
            break
        skip += 5000
    return all_rows


def _calc_month_total(session: requests.Session, all_rows: list[dict],
                      year: int, month: int) -> float:
    """Посчитать итого договоров за конкретный месяц (полная фильтрация)."""
    last_day = calendar.monthrange(year, month)[1]
    d_from = f"{year}-{month:02d}-01"
    d_to = f"{year}-{month:02d}-{last_day}"

    month_rows = [
        x for x in all_rows
        if (d_from <= (x.get("ДатаПодписания") or "")[:10] <= d_to)
        and x.get("Подразделение_Key", "") in DEPT_SET
        and x.get("Подразделение_Key", "") != EMPTY
    ]

    spec_keys = sorted({
        x.get("Спецификация_Key", "")
        for x in month_rows
        if x.get("Спецификация_Key", "") not in ("", EMPTY)
    })
    spec_status: dict[str, str] = {}
    for i in range(0, len(spec_keys), BATCH):
        batch = spec_keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Catalog_СоглашенияСКлиентами"
            f"?$format=json&$filter={flt}"
            f"&$select=Ref_Key,Статус&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    spec_status[it["Ref_Key"]] = it.get("Статус", "")
        except Exception:
            pass

    spec_ok = [
        x for x in month_rows
        if spec_status.get(x.get("Спецификация_Key", ""), "") == "Действует"
    ]

    unique_pk = sorted({
        x.get("Партнер_Key", "")
        for x in spec_ok
        if x.get("Партнер_Key", "") not in ("", EMPTY)
    })
    partners_map: dict[str, str] = {}
    for i in range(0, len(unique_pk), BATCH):
        batch = unique_pk[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Catalog_Партнеры"
            f"?$format=json&$filter={flt}&$select=Ref_Key,Description&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    partners_map[it["Ref_Key"]] = it.get("Description", "").strip()
        except Exception:
            pass

    excl_full = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES}
    excl_no_mgs = {k for k, v in partners_map.items() if v in EXCLUDE_PARTNER_NAMES_NO_MGS}

    partner_ok = []
    for x in spec_ok:
        pk = x.get("Партнер_Key", "")
        dept = x.get("Подразделение_Key", "")
        soprovozhd = x.get("ТД_СопровождениеПродажи", False)
        if dept == OPBO_DEPT:
            if pk in excl_no_mgs:
                continue
        else:
            if pk in excl_full and not soprovozhd:
                continue
        partner_ok.append(x)

    order_keys = sorted({
        x.get("ЗаказКлиента_Key", "")
        for x in partner_ok
        if x.get("ЗаказКлиента_Key", "") not in ("", EMPTY)
    })
    order_data: dict[str, dict] = {}
    for i in range(0, len(order_keys), BATCH):
        batch = order_keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента"
            f"?$format=json&$filter={flt}"
            f"&$select=Ref_Key,Валюта_Key,ТД_НеУчитыватьВПланФакте"
            f"&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                for it in r.json().get("value", []):
                    order_data[it["Ref_Key"]] = {
                        "cur_key": it.get("Валюта_Key", ""),
                        "ne_uchit": it.get("ТД_НеУчитыватьВПланФакте", False),
                    }
        except Exception:
            pass

    total = 0.0
    for x in partner_ok:
        ok = x.get("ЗаказКлиента_Key", "")
        amt = float(x.get("СуммаДоговора") or 0)
        has_order = ok and ok != EMPTY
        if has_order:
            od = order_data.get(ok)
            if od:
                if od["ne_uchit"]:
                    continue
                cur_code = CURRENCY_KEYS.get(od["cur_key"], "RUB")
                rate = EXCHANGE_RATES.get(cur_code, 1.0)
            else:
                rate = 1.0
        else:
            rate = 1.0
        total += amt * rate

    return round(total, 2)


def _monthly_cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"dogovory_monthly_{year}_{month:02d}.json"


def get_dogovory_fact_for_month(year: int, month: int) -> float:
    """Факт договоров за один месяц (с кэшем на день)."""
    cached = _load_cache(year, month)
    if cached is not None:
        return cached

    session = requests.Session()
    session.auth = AUTH
    all_rows = _load_register(session)
    total = _calc_month_total(session, all_rows, year, month)
    _save_cache(year, month, total)
    return total


def get_dogovory_monthly(year: int | None = None, month: int | None = None) -> dict:
    """
    Помесячные факты договоров (январь..last full month).
    Кэшируется весь набор.
    """
    today = date.today()
    if today.month == 1:
        ref_y, ref_m = today.year - 1, 12
    else:
        ref_y, ref_m = today.year, today.month - 1
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    mc = _monthly_cache_path(ref_y, ref_m)
    if mc.exists():
        try:
            with open(mc, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("cache_date") == today.isoformat():
                return data
        except (OSError, json.JSONDecodeError):
            pass

    session = requests.Session()
    session.auth = AUTH
    all_rows = _load_register(session)

    out_months = []
    for m in range(1, ref_m + 1):
        cached = _load_cache(ref_y, m)
        if cached is not None:
            fact = cached
        else:
            fact = _calc_month_total(session, all_rows, ref_y, m)
            _save_cache(ref_y, m, fact)
        out_months.append({"year": ref_y, "month": m, "fact": fact})

    payload = {
        "cache_date": today.isoformat(),
        "year": ref_y,
        "ref_month": ref_m,
        "months": out_months,
    }
    try:
        with open(mc, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass
    return payload


# ═══════════════════════════════════════════════════════════
# Standalone CLI (python calc_dogovory_fact.py [ГГГГ-ММ])
# ═══════════════════════════════════════════════════════════

def main():
    import functools as _fn
    _print = _fn.partial(print, flush=True)

    period_arg = None
    for a in sys.argv[1:]:
        if len(a) == 7 and a[4] == "-":
            period_arg = a
            break

    if period_arg:
        year, month = int(period_arg[:4]), int(period_arg[5:7])
    else:
        year, month = 2026, 3

    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"

    session = requests.Session()
    session.auth = AUTH
    t0 = time.time()

    _print(f"\n{'═' * 60}")
    _print(f"  ФАКТ ДОГОВОРОВ (ТД_ДоговорыПодписанные) · {MONTH_RU[month]} {year}")
    _print(f"  Период: {date_from} — {date_to}")
    _print(f"{'═' * 60}")

    _print(f"\n▸ Загрузка регистра...")
    all_rows = _load_register(session)
    _print(f"  ✓ Загружено: {len(all_rows)} записей · {time.time()-t0:.1f}с")

    _print(f"\n▸ Расчёт...")
    total = _calc_month_total(session, all_rows, year, month)
    _save_cache(year, month, total)

    _print(f"\n{'═' * 60}")
    _print(f"  ИТОГО ДОГОВОРОВ (руб.):  {total:>20,.2f}")
    _print(f"  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
