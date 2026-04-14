"""
calc_debitorka.py — Дебиторская и просроченная дебиторская задолженность.

Источник: AccumulationRegister_РасчетыСКлиентамиПоСрокам.
  - RecordType = Receipt → рост долга (Приход)
  - RecordType = Expense → погашение долга (Расход)

Ресурс: ДолгУпр (управленческий учёт, «В валюте упр. учета»).
  Все значения ДолгУпр неотрицательны; знак определяется RecordType.
  Баланс = Σ(Receipt.ДолгУпр) − Σ(Expense.ДолгУпр).

Просрочка: если ДатаПлановогоПогашения < начала дня «НаДату» (первый день после
  срока уже считается просрочкой).

Подразделение: берётся из Catalog_ОбъектыРасчетов.Подразделение_Key
  (ОбъектРасчетов_Key регистра = Ref_Key каталога).

Классификация aging: 30-60-180 (1-29, 30-59, 60-179, 180+).

Запуск:
  python calc_debitorka.py                       # на сегодня
  python calc_debitorka.py 2026-03-31            # на конкретную дату
  python calc_debitorka.py 2026-03-31 --detail   # с детализацией по объектам
"""

import requests, sys, time, json, os, functools, calendar, logging
from datetime import datetime, date
from requests.auth import HTTPBasicAuth
from urllib.parse import quote
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
# КОНФИГ
# ═══════════════════════════════════════════════════════

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел эталонного оборудования",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел ключевых клиентов",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел ПАО Газпром",
}

TOLERANCE = 0.01

REGISTER = "AccumulationRegister_РасчетыСКлиентамиПоСрокам_RecordType"


def fetch_all_register(session, na_datu: str):
    """Загрузить ВСЕ записи регистра с Period <= НаДату (конец дня)."""

    sel = quote(
        "RecordType,Period,Active,"
        "ОбъектРасчетов_Key,АналитикаУчетаПоПартнерам_Key,"
        "Валюта_Key,ДатаПлановогоПогашения,ДатаВозникновения,"
        "ДолгУпр",
        safe=",_",
    )
    flt = quote(
        f"Period le datetime'{na_datu}T23:59:59' and Active eq true",
        safe="",
    )

    records = []
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
        r = session.get(url, timeout=120)
        if not r.ok:
            print(f"  ⚠ HTTP {r.status_code} при skip={skip}")
            break
        batch = r.json().get("value", [])
        records.extend(batch)
        if skip % 50000 == 0 or len(batch) < PAGE:
            print(f"  загружено {len(records)} записей · {time.time()-t0:.1f}с")
        if len(batch) < PAGE:
            break
        skip += PAGE

    return records


def aggregate_balances(records):
    """
    Агрегация по (ОбъектРасчетов_Key, ДатаПлановогоПогашения).
    Возвращает dict: (obj_key, planned_date_str) → balance (float).
    Receipt — плюс, Expense — минус.
    """
    agg = defaultdict(float)

    for r in records:
        obj = str(r.get("ОбъектРасчетов_Key", EMPTY)).lower()
        if obj == EMPTY:
            continue
        planned = r.get("ДатаПлановогоПогашения", "")
        debt = float(r.get("ДолгУпр") or 0)
        rtype = r.get("RecordType", "")

        if rtype == "Receipt":
            agg[(obj, planned)] += debt
        elif rtype == "Expense":
            agg[(obj, planned)] -= debt

    # Отсечь нулевые балансы
    return {k: v for k, v in agg.items() if abs(v) >= TOLERANCE}


def resolve_objects(session, obj_keys: set):
    """
    Загрузить Catalog_ОбъектыРасчетов страницами, собрать маппинг
    Ref_Key (lower) → {dept, partner, desc, number, date}.
    Подразделение берётся напрямую из каталога.
    """
    sel = quote(
        "Ref_Key,Подразделение_Key,Партнер_Key,Description,Номер,Дата",
        safe=",_",
    )

    catalog = {}
    needed = set(obj_keys)
    skip = 0
    PAGE = 1000
    t0 = time.time()

    while True:
        url = (
            f"{BASE}/Catalog_ОбъектыРасчетов"
            f"?$format=json&$select={sel}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = session.get(url, timeout=120)
        if not r.ok:
            print(f"  ⚠ HTTP {r.status_code} при skip={skip}")
            break
        chunk = r.json().get("value", [])
        if not chunk:
            break
        for item in chunk:
            k = str(item.get("Ref_Key", EMPTY)).lower()
            if k in needed:
                catalog[k] = {
                    "dept": str(item.get("Подразделение_Key", EMPTY)).lower(),
                    "partner": str(item.get("Партнер_Key", EMPTY)).lower(),
                    "desc": item.get("Description", ""),
                    "number": item.get("Номер", "?"),
                    "date": item.get("Дата", ""),
                }
        skip += len(chunk)
        found = sum(1 for k in needed if k in catalog)
        if skip % 10000 == 0 or found >= len(needed):
            print(
                f"  скан каталога: {skip} записей, найдено "
                f"{found}/{len(needed)} · {time.time()-t0:.1f}с"
            )
        if found >= len(needed):
            break
        if len(chunk) < PAGE:
            break

    missing = needed - set(catalog.keys())
    if missing:
        print(f"  ⚠ не найдено в каталоге: {len(missing)} ОбъектРасчетов")

    return catalog


def resolve_partner_names(session, partner_keys: set):
    """Получить имена партнёров по GUID."""
    names = {}
    for pk in partner_keys:
        if pk == EMPTY:
            continue
        url = f"{BASE}/Catalog_Партнеры(guid'{pk}')?$format=json&$select=Description"
        r = session.get(url, timeout=15)
        if r.ok:
            names[pk] = r.json().get("Description", pk[:8])
        else:
            names[pk] = pk[:8]
    return names


def aging_bucket(days: int) -> str:
    """Классификация 30-60-180."""
    if days <= 0:
        return "не просрочено"
    if days <= 29:
        return "1-29 дн."
    if days <= 59:
        return "30-59 дн."
    if days <= 179:
        return "60-179 дн."
    return "180+ дн."


CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
BUCKETS = ["1-29 дн.", "30-59 дн.", "60-179 дн.", "180+ дн."]


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path_snapshot(na_datu: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"debitorka_{na_datu.isoformat()}.json"


def _cache_path_monthly(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"debitorka_monthly_{year}_{month:02d}.json"


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


def _calc_snapshot_for_date(na_datu: date) -> dict:
    """Полный расчёт ДЗ/просрочки на дату (RecordType, медленно, но точно)."""
    session = requests.Session()
    session.auth = AUTH

    na_datu_str = na_datu.isoformat()
    logger.info("calc_debitorka: computing snapshot for %s", na_datu_str)

    records = fetch_all_register(session, na_datu_str)
    balances = aggregate_balances(records)

    obj_keys = {obj for (obj, _) in balances.keys()}
    obj_catalog = resolve_objects(session, obj_keys)

    overdue_cutoff = f"{na_datu_str}T00:00:00"
    dept_keys_lower = {d.lower() for d in DEPARTMENTS}

    dz_by_dept: dict[str, float] = defaultdict(float)
    overdue_by_dept: dict[str, float] = defaultdict(float)
    aging_by_dept: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for (obj_key, planned_dt), balance in balances.items():
        cat = obj_catalog.get(obj_key)
        if not cat:
            continue
        dept = cat["dept"]
        if dept not in dept_keys_lower:
            continue
        if balance < TOLERANCE:
            continue

        dept_name = DEPARTMENTS.get(dept, dept[:8])
        dz_by_dept[dept_name] += balance

        if planned_dt and planned_dt > "0001-01-02" and planned_dt < overdue_cutoff:
            overdue_by_dept[dept_name] += balance
            days_overdue = (na_datu - date.fromisoformat(planned_dt[:10])).days
            aging_by_dept[dept_name][aging_bucket(days_overdue)] += balance

    return {
        "na_datu": na_datu_str,
        "total_dz": round(sum(dz_by_dept.values()), 2),
        "total_overdue": round(sum(overdue_by_dept.values()), 2),
        "by_dept": {
            d: {
                "dz": round(dz_by_dept.get(d, 0), 2),
                "overdue": round(overdue_by_dept.get(d, 0), 2),
                "aging": {b: round(aging_by_dept[d].get(b, 0), 2) for b in BUCKETS},
            }
            for d in sorted(set(list(dz_by_dept.keys()) + list(overdue_by_dept.keys())))
        },
    }


def get_snapshot_for_date(na_datu: date) -> dict:
    """Кэшируемый снимок ДЗ/просрочки на дату."""
    cached = _load_json(_cache_path_snapshot(na_datu))
    if cached is not None:
        return cached
    payload = _calc_snapshot_for_date(na_datu)
    _save_json(_cache_path_snapshot(na_datu), payload)
    return payload


def get_komdir_dz_monthly(year: int | None = None, month: int | None = None) -> dict:
    """
    Помесячные ДЗ/просрочка (январь → последний полный месяц).
    Результат кэшируется целиком и поснимочно.
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    cached = _load_json(_cache_path_monthly(ref_y, ref_m))
    if cached is not None:
        return cached

    out_rows = []
    for mm in range(1, ref_m + 1):
        snapshot = get_snapshot_for_date(_month_end(ref_y, mm))
        out_rows.append({
            "year": ref_y,
            "month": mm,
            "na_datu": snapshot.get("na_datu"),
            "dz_fact": float(snapshot.get("total_dz") or 0),
            "overdue_fact": float(snapshot.get("total_overdue") or 0),
        })

    payload = {"year": ref_y, "ref_month": ref_m, "months": out_rows}
    _save_json(_cache_path_monthly(ref_y, ref_m), payload)
    return payload


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    _print = functools.partial(print, flush=True)
    args = sys.argv[1:]
    detail = "--detail" in args
    args = [a for a in args if a != "--detail"]

    if args:
        na_datu_str = args[0]
        na_datu = date.fromisoformat(na_datu_str)
    else:
        na_datu = date.today()
        na_datu_str = na_datu.isoformat()

    _print("=" * 60)
    _print(f"  ДЕБИТОРСКАЯ ЗАДОЛЖЕННОСТЬ на {na_datu_str}")
    _print("=" * 60)

    session = requests.Session()
    session.auth = AUTH
    t_total = time.time()

    _print(f"\n▸ Шаг 1 · Загрузка записей регистра (Period ≤ {na_datu_str})")
    records = fetch_all_register(session, na_datu_str)
    _print(f"  Итого записей: {len(records)}")

    _print("\n▸ Шаг 2 · Агрегация по (ОбъектРасчетов, ДатаПлановогоПогашения)")
    balances = aggregate_balances(records)
    _print(f"  Ненулевых позиций: {len(balances)}")

    obj_keys = {obj for (obj, _) in balances.keys()}
    _print(f"  Уникальных ОбъектРасчетов: {len(obj_keys)}")

    _print("\n▸ Шаг 3 · Загрузка Catalog_ОбъектыРасчетов (подразделение из каталога)")
    obj_catalog = resolve_objects(session, obj_keys)
    _print(f"  Найдено в каталоге: {len(obj_catalog)}")

    _print("\n▸ Шаг 4 · Фильтрация по подразделениям и расчёт ДЗ")
    dept_keys_lower = {d.lower() for d in DEPARTMENTS}
    overdue_cutoff = f"{na_datu_str}T00:00:00"

    dz_by_dept = defaultdict(float)
    overdue_by_dept = defaultdict(float)
    aging_by_dept = defaultdict(lambda: defaultdict(float))

    detail_rows = []
    partner_keys_used = set()

    skipped_no_order = 0
    skipped_wrong_dept = 0
    skipped_negative = 0

    for (obj_key, planned_dt), balance in balances.items():
        cat = obj_catalog.get(obj_key)
        if not cat:
            skipped_no_order += 1
            continue

        dept = cat["dept"]
        if dept not in dept_keys_lower:
            skipped_wrong_dept += 1
            continue

        if balance < TOLERANCE:
            skipped_negative += 1
            continue

        dept_name = DEPARTMENTS.get(dept, dept[:8])
        dz_by_dept[dept_name] += balance

        is_overdue = False
        days_overdue = 0
        if planned_dt and planned_dt > "0001-01-02":
            is_overdue = planned_dt < overdue_cutoff
            if is_overdue:
                planned_date = date.fromisoformat(planned_dt[:10])
                days_overdue = (na_datu - planned_date).days
                overdue_by_dept[dept_name] += balance
                aging_by_dept[dept_name][aging_bucket(days_overdue)] += balance

        if detail:
            partner_keys_used.add(cat["partner"])
            detail_rows.append({
                "obj_number": cat["number"],
                "obj_date": cat["date"][:10] if cat["date"] else "",
                "obj_desc": cat["desc"],
                "dept": dept_name,
                "partner_key": cat["partner"],
                "planned_date": planned_dt[:10] if planned_dt else "",
                "balance": round(balance, 2),
                "is_overdue": is_overdue,
                "days_overdue": days_overdue,
                "bucket": aging_bucket(days_overdue) if is_overdue else "",
            })

    _print(f"  Пропущено: без каталога={skipped_no_order}, "
           f"другое подразд.={skipped_wrong_dept}, "
           f"отриц. баланс={skipped_negative}")

    _print("\n" + "=" * 70)
    _print(f"  РЕЗУЛЬТАТЫ НА {na_datu_str}")
    _print("=" * 70)

    total_dz = sum(dz_by_dept.values())
    total_overdue = sum(overdue_by_dept.values())

    _print(f"\n{'Подразделение':<45s} {'ДЗ':>18s} {'Просроч. ДЗ':>18s}")
    _print("─" * 83)
    for dept_name in sorted(dz_by_dept.keys()):
        dz = dz_by_dept[dept_name]
        ov = overdue_by_dept.get(dept_name, 0)
        _print(f"  {dept_name:<43s} {dz:>18,.2f} {ov:>18,.2f}")
    _print("─" * 83)
    _print(f"  {'ИТОГО':<43s} {total_dz:>18,.2f} {total_overdue:>18,.2f}")

    _print(f"\n{'Подразделение':<35s} {'1-29 дн.':>14s} {'30-59 дн.':>14s} "
           f"{'60-179 дн.':>14s} {'180+ дн.':>14s}")
    _print("─" * 93)
    aging_totals = defaultdict(float)
    for dept_name in sorted(aging_by_dept.keys()):
        vals = aging_by_dept[dept_name]
        parts = []
        for b in BUCKETS:
            v = vals.get(b, 0)
            aging_totals[b] += v
            parts.append(f"{v:>14,.2f}")
        _print(f"  {dept_name:<33s} {''.join(parts)}")
    _print("─" * 93)
    parts = [f"{aging_totals.get(b,0):>14,.2f}" for b in BUCKETS]
    _print(f"  {'ИТОГО':<33s} {''.join(parts)}")

    result = {
        "na_datu": na_datu_str,
        "total_dz": round(total_dz, 2),
        "total_overdue": round(total_overdue, 2),
        "by_dept": {
            d: {
                "dz": round(dz_by_dept.get(d, 0), 2),
                "overdue": round(overdue_by_dept.get(d, 0), 2),
                "aging": {b: round(aging_by_dept[d].get(b, 0), 2) for b in BUCKETS}
            }
            for d in sorted(set(list(dz_by_dept.keys()) + list(overdue_by_dept.keys())))
        },
    }

    _save_json(_cache_path_snapshot(na_datu), result)
    _print(f"\n  Cache → {_cache_path_snapshot(na_datu)}")

    if detail and detail_rows:
        _print(f"\n▸ Загрузка имён партнёров ({len(partner_keys_used)} шт.)...")
        pnames = resolve_partner_names(session, partner_keys_used)

        detail_rows.sort(key=lambda x: (-x["balance"],))
        detail_path = os.path.join(
            os.path.dirname(__file__), f"debitorka_detail_{na_datu_str}.json"
        )
        for row in detail_rows:
            row["partner"] = pnames.get(row.pop("partner_key"), "?")

        with open(detail_path, "w", encoding="utf-8") as f:
            json.dump(detail_rows, f, ensure_ascii=False, indent=2)
        _print(f"  → {detail_path} ({len(detail_rows)} строк)")

        _print(f"\n  Топ-10 по сумме ДЗ:")
        for row in detail_rows[:10]:
            ov = " ⚠ просрочка" if row["is_overdue"] else ""
            _print(f"    {row['obj_number']:>15s}  {row['dept']:<30s}  "
                   f"{row['balance']:>15,.2f}{ov}")

    elapsed = time.time() - t_total
    _print(f"\n  Время: {elapsed:.1f}с")


if __name__ == "__main__":
    main()
