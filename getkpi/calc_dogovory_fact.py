"""
calc_dogovory_fact.py — Факт подписанных договоров за месяц.

Источник: InformationRegister_ТД_ДоговорыПодписанные (реальный регистр 1С).

Логика (воспроизведение 1С-запроса «ВТ_ДоговорыФакт»):
  1. ДатаПодписания — в пределах указанного месяца
  2. Спецификация.Статус = «Действует» (через Catalog_СоглашенияСКлиентами)
  3. Подразделение_Key строки регистра (не заказа) — один из 6 целевых отделов продаж
     после нормализации ликвидированных отделов, ≠ пусто
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

from .commercial_department_aliases import normalize_commercial_dept_guid
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"
CACHE_VERSION = 5

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

PREDEFINED_VALUES = "Catalog_ТД_ПредопределенныеЗначения"
PREDEFINED_VALUES_EXTRA = "Catalog_ТД_ПредопределенныеЗначения_ДополнительныеЗначения"
PREDEFINED_RESALE_PARTNERS_REF = "8180316b-7c73-11e9-828e-ac1f6b05524d"
PREDEFINED_MGS_PARTNER_REF = "5bd32178-cf94-11e9-829b-ac1f6b05524d"

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


def _load_cache(year: int, month: int) -> dict | None:
    """Загрузить кэш месяца. Возвращает dict с total и by_dept, или None."""
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
        ):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, month: int, total: float,
                by_dept: dict[str, float]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), "w", encoding="utf-8") as f:
            json.dump({
                "cache_date": date.today().isoformat(),
                "cache_version": CACHE_VERSION,
                "total": total,
                "by_dept": by_dept,
            }, f, ensure_ascii=False)
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
        r = request_with_retry(session, url, timeout=120, retries=4, label="Dogovory")
        if r is None or not r.ok:
            break
        rows = r.json().get("value", [])
        all_rows.extend(rows)
        if len(rows) < 5000:
            break
        skip += 5000
    return all_rows


def _load_resale_partner_keys(session: requests.Session) -> set[str]:
    """Партнёры перепродажи из ТД_ПредопределенныеЗначения.ДополнительныеЗначения."""
    flt = quote(f"Ref_Key eq guid'{PREDEFINED_RESALE_PARTNERS_REF}'", safe="")
    url = (
        f"{BASE}/{PREDEFINED_VALUES_EXTRA}?$format=json"
        f"&$filter={flt}&$select=Ref_Key,Значение,Значение_Type&$top=5000"
    )
    r = request_with_retry(session, url, timeout=30, retries=3, label="Dogovory/ResalePartners")
    if r is None or not r.ok:
        return set()
    keys: set[str] = set()
    try:
        for row in r.json().get("value", []):
            val = row.get("Значение")
            typ = row.get("Значение_Type") or ""
            if val and "Catalog_Партнеры" in typ:
                keys.add(val)
    except Exception:
        return set()
    return keys


def _load_mgs_partner_key(session: requests.Session) -> str | None:
    """Партнёр Метрогазсервис из предопределённых значений 1С."""
    flt = quote(f"Ref_Key eq guid'{PREDEFINED_MGS_PARTNER_REF}'", safe="")
    url = (
        f"{BASE}/{PREDEFINED_VALUES}?$format=json"
        f"&$filter={flt}&$select=Ref_Key,Значение,Значение_Type&$top=1"
    )
    r = request_with_retry(session, url, timeout=30, retries=3, label="Dogovory/MGSPartner")
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
    mgs = _load_mgs_partner_key(session)
    resale_without_mgs = set(resale)
    if mgs:
        resale_without_mgs.discard(mgs)
    return resale, resale_without_mgs


def _dept_from_register(row: dict) -> str:
    """Отдел для KPI — только Подразделение_Key строки регистра (+ алиасы ликвидированных)."""
    return normalize_commercial_dept_guid(row.get("Подразделение_Key", ""))


def _effective_dept(row: dict, order_data: dict[str, dict] | None = None) -> str:
    """Старое имя (reload/.pyc): подразделение только из регистра; order_data не используется."""
    return _dept_from_register(row)


def _calc_month_total(session: requests.Session, all_rows: list[dict],
                      year: int, month: int) -> tuple[float, dict[str, float]]:
    """Посчитать итого договоров за конкретный месяц. Возвращает (total, by_dept)."""
    last_day = calendar.monthrange(year, month)[1]
    d_from = f"{year}-{month:02d}-01"
    d_to = f"{year}-{month:02d}-{last_day}"

    month_rows = [
        x for x in all_rows
        if d_from <= (x.get("ДатаПодписания") or "")[:10] <= d_to
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
        r = request_with_retry(session, url, timeout=30, retries=3, label="Dogovory/Spec")
        if r is None or not r.ok:
            continue
        try:
            for it in r.json().get("value", []):
                spec_status[it["Ref_Key"]] = it.get("Статус", "")
        except Exception:
            pass

    spec_ok = [
        x for x in month_rows
        if spec_status.get(x.get("Спецификация_Key", ""), "") == "Действует"
    ]

    order_keys = sorted({
        x.get("ЗаказКлиента_Key", "")
        for x in spec_ok
        if x.get("ЗаказКлиента_Key", "") not in ("", EMPTY)
    })
    order_data: dict[str, dict] = {}
    for i in range(0, len(order_keys), BATCH):
        batch = order_keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента"
            f"?$format=json&$filter={flt}"
            f"&$select=Ref_Key,Подразделение_Key,Валюта_Key,ТД_НеУчитыватьВПланФакте"
            f"&$top={BATCH}"
        )
        r = request_with_retry(session, url, timeout=30, retries=3, label="Dogovory/Orders")
        if r is None or not r.ok:
            continue
        try:
            for it in r.json().get("value", []):
                order_data[it["Ref_Key"]] = {
                    "dept": it.get("Подразделение_Key", ""),
                    "cur_key": it.get("Валюта_Key", ""),
                    "ne_uchit": it.get("ТД_НеУчитыватьВПланФакте", False),
                }
        except Exception:
            pass

    resale_partners, resale_partners_without_mgs = _partner_resale_sets(session)

    partner_ok = []
    for x in spec_ok:
        pk = x.get("Партнер_Key", "")
        dept = _dept_from_register(x)
        if dept not in DEPT_SET or dept == EMPTY:
            continue
        soprovozhd = x.get("ТД_СопровождениеПродажи", False)
        if dept == OPBO_DEPT:
            if pk in resale_partners_without_mgs:
                continue
        else:
            if pk in resale_partners and not soprovozhd:
                continue
        partner_ok.append(x)

    by_dept: dict[str, float] = {d: 0.0 for d in DEPT_SET}
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

        dept = _dept_from_register(x)
        by_dept[dept] += amt * rate

    by_dept = {d: round(v, 2) for d, v in by_dept.items()}
    total = round(sum(by_dept.values()), 2)
    return total, by_dept


def _monthly_cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"dogovory_monthly_{year}_{month:02d}.json"


def _has_nonzero_months(data: dict) -> bool:
    return any((row.get("fact") or 0) for row in data.get("months", []))


def _load_stale_monthly_cache(year: int, month: int) -> dict | None:
    p = _monthly_cache_path(year, month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        first_m = (data.get("months") or [{}])[0]
        if "by_dept" in first_m and _has_nonzero_months(data):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def get_dogovory_fact_for_month(year: int, month: int) -> float:
    """Факт договоров за один месяц (с кэшем на день)."""
    cached = _load_cache(year, month)
    if cached is not None:
        return cached["total"]

    session = requests.Session()
    session.auth = AUTH
    all_rows = _load_register(session)
    total, by_dept = _calc_month_total(session, all_rows, year, month)
    _save_cache(year, month, total, by_dept)
    return total


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Вернуть полный агрегат или срез по одному подразделению."""
    if dept_guid is None:
        return payload
    sliced_months = []
    for row in payload.get("months", []):
        dept_val = row.get("by_dept", {}).get(dept_guid, 0)
        sliced_months.append({
            "year": row["year"],
            "month": row["month"],
            "fact": dept_val,
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced_months,
    }


def get_dogovory_monthly(year: int | None = None,
                         month: int | None = None,
                         dept_guid: str | None = None) -> dict:
    """
    Помесячные факты договоров (январь..last full month).
    dept_guid=None — агрегат по всем отделам.
    dept_guid='...' — факт только по указанному подразделению.
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
            first_m = (data.get("months") or [{}])[0]
            if (
                data.get("cache_date") == today.isoformat()
                and data.get("cache_version") == CACHE_VERSION
                and "by_dept" in first_m
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
                "fact": cd["total"],
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
    all_rows = _load_register(session)
    if not all_rows:
        fallback = _load_stale_monthly_cache(ref_y, ref_m)
        if fallback is not None:
            return _slice_payload(fallback, dept_guid)
        return _slice_payload({"year": ref_y, "ref_month": ref_m, "months": []}, dept_guid)

    out_months = []
    for m in range(1, ref_m + 1):
        cached = _load_cache(ref_y, m)
        if cached is not None:
            total = cached["total"]
            by_dept = cached.get("by_dept", {})
        else:
            total, by_dept = _calc_month_total(session, all_rows, ref_y, m)
            _save_cache(ref_y, m, total, by_dept)
        out_months.append({
            "year": ref_y, "month": m,
            "fact": total,
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
    total, by_dept = _calc_month_total(session, all_rows, year, month)
    _save_cache(year, month, total, by_dept)

    _print(f"\n{'═' * 60}")
    _print(f"  ИТОГО ДОГОВОРОВ (руб.):  {total:>20,.2f}")
    _print(f"  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
