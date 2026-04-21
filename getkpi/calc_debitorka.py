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


def _fetch_order_details(session, order_numbers: set) -> dict:
    """
    Загрузить ТД_ПричинаОбразованияПросроченнойДЗ и
    ТД_КорректирующееДействиеПоФактуВыявленияДЗ из Document_ЗаказКлиента
    по `Number`. Возвращает dict: {order_number: {"reason", "action"}}.
    """
    numbers_list = [str(n).strip() for n in order_numbers if n and str(n).strip() not in ("", "?")]
    if not numbers_list:
        return {}

    details: dict[str, dict] = {}

    entity = quote("Document_ЗаказКлиента")
    sel = quote(
        "Number,ТД_ПричинаОбразованияПросроченнойДЗ,ТД_КорректирующееДействиеПоФактуВыявленияДЗ",
        safe=",_",
    )

    BATCH = 40
    for i in range(0, len(numbers_list), BATCH):
        batch = numbers_list[i:i + BATCH]
        expr = " or ".join(f"Number eq '{n}'" for n in batch)
        url = (
            f"{BASE}/{entity}?$format=json&$select={sel}"
            f"&$filter={quote(expr, safe='')}&$top=5000"
        )
        try:
            r = session.get(url, timeout=60)
            if not r.ok:
                logger.warning("Order details HTTP %d for batch %d", r.status_code, i)
                continue
            for item in r.json().get("value", []):
                num = (item.get("Number") or "").strip()
                if not num:
                    continue
                details[num] = {
                    "reason": (item.get("ТД_ПричинаОбразованияПросроченнойДЗ") or "").strip(),
                    "action": (item.get("ТД_КорректирующееДействиеПоФактуВыявленияДЗ") or "").strip(),
                }
        except Exception as exc:
            logger.warning("Order details fetch error: %s", exc)
            continue

    return details


def _build_overdue_rows_per_order(na_datu: date, balances: dict, obj_catalog: dict,
                                  session) -> list[dict]:
    """
    Построить список строк просроченной ДЗ с детализацией ПО КАЖДОМУ ЗАКАЗУ КЛИЕНТА.

    Согласно спецификации:
      ПросроченнаяДебиторскаяЗадолженность =
        сумма оборота «Долг*» (ДолгУпр) по ВСЕМ строкам регистра,
        у которых ДатаПлановогоПогашения < НаДату.

    Т.е. считаем НЕТТО-остаток по каждому просроченному сроку внутри заказа
    (включая возможные частичные погашения/переплаты с тем же сроком),
    затем суммируем эти нетто-остатки по заказу. Строка попадает в таблицу
    только если итоговая сумма просрочки по заказу > TOLERANCE.

    Поля строки:
      - dept_key, dept_name
      - partner_key, partner_name (== counterparty)
      - order_key (Ref_Key из Catalog_ОбъектыРасчетов)
      - order_num, order_date
      - amount (сумма просроченной ДЗ по заказу, руб., ДолгУпр, нетто)
      - dz_total (вся ДЗ по заказу, руб., ДолгУпр, нетто)
      - days_overdue (макс. дней просрочки среди просроченных сроков заказа)
      - installments_count (количество просроченных сроков в заказе)
      - installments (детализация: planned_date, amount, days_overdue, bucket)
      - reason (ТД_ПричинаОбразованияПросроченнойДЗ из Document_ЗаказКлиента)
      - action (ТД_КорректирующееДействиеПоФактуВыявленияДЗ из Document_ЗаказКлиента)
    """
    na_datu_str = na_datu.isoformat()
    overdue_cutoff = f"{na_datu_str}T00:00:00"
    dept_keys_lower = {d.lower() for d in DEPARTMENTS}

    by_order: dict[tuple[str, str, str], dict] = defaultdict(
        lambda: {
            "amount": 0.0,         # ПросроченнаяДЗ: сумма НЕТТО-остатков по просроченным срокам
            "dz_total": 0.0,       # Общая ДЗ: сумма НЕТТО-остатков по всем срокам
            "max_days": 0,
            "dept": "", "dept_name": "",
            "partner": "", "order_num": "", "order_date": "",
            "installments": [],    # детализация по срокам погашения
        }
    )
    partner_keys_used: set[str] = set()
    order_numbers_used: set[str] = set()

    for (obj_key, planned_dt), balance in balances.items():
        cat = obj_catalog.get(obj_key)
        if not cat:
            continue
        dept = cat["dept"]
        if dept not in dept_keys_lower:
            continue

        partner_key = cat["partner"]
        order_num = str(cat.get("number") or "").strip()
        order_date = str(cat.get("date") or "")[:10]

        key = (dept, partner_key, obj_key)
        entry = by_order[key]
        entry["dept"] = dept
        entry["dept_name"] = DEPARTMENTS.get(dept, dept[:8])
        entry["partner"] = partner_key
        entry["order_num"] = order_num
        entry["order_date"] = order_date

        # Общая ДЗ по заказу (все сроки, включая будущие) — нетто
        entry["dz_total"] += balance

        # Просрочка: только сроки, у которых ДатаПлановогоПогашения < НаДату
        if not planned_dt or planned_dt <= "0001-01-02" or planned_dt >= overdue_cutoff:
            continue

        # Нетто-оборот по этому просроченному сроку (с учётом частичных погашений
        # по тому же сроку).
        entry["amount"] += balance

        # Детализация: только строки с ненулевым остатком
        if abs(balance) >= TOLERANCE:
            days_overdue = (na_datu - date.fromisoformat(planned_dt[:10])).days
            entry["installments"].append({
                "planned_date": planned_dt[:10],
                "amount": round(balance, 2),
                "days_overdue": days_overdue,
                "bucket": aging_bucket(days_overdue) if balance > TOLERANCE else "",
            })
            if balance > TOLERANCE:
                entry["max_days"] = max(entry["max_days"], days_overdue)

        partner_keys_used.add(partner_key)
        if order_num:
            order_numbers_used.add(order_num)

    partner_names = resolve_partner_names(session, partner_keys_used)
    order_details = _fetch_order_details(session, order_numbers_used)

    rows: list[dict] = []
    for (dept, partner_key, obj_key), data in by_order.items():
        # В таблицу попадают только заказы с нетто-просрочкой > TOLERANCE
        if data["amount"] <= TOLERANCE:
            continue

        order_num = data["order_num"]
        details = order_details.get(order_num, {}) if order_num else {}
        partner_name = partner_names.get(partner_key, partner_key[:8])
        installments = sorted(
            data["installments"], key=lambda x: x["planned_date"]
        )
        rows.append({
            "dept_key": dept,
            "dept_name": data["dept_name"],
            "partner_key": partner_key,
            "partner_name": partner_name,
            "counterparty": partner_name,
            "order_key": obj_key,
            "order_num": order_num,
            "order_date": data["order_date"],
            "amount": round(data["amount"], 2),
            "dz_total": round(data["dz_total"], 2),
            "days_overdue": data["max_days"],
            "installments_count": len(installments),
            "installments": installments,
            "reason": details.get("reason", ""),
            "action": details.get("action", ""),
        })

    rows.sort(key=lambda x: -x["amount"])
    return rows


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

    # См. _calc_snapshots_batch: берём obj_keys из сырых записей, чтобы
    # подразделения подтягивались и для полностью погашенных заказов.
    obj_keys = {
        str(r.get("ОбъектРасчетов_Key", EMPTY)).lower()
        for r in records
        if str(r.get("ОбъектРасчетов_Key", EMPTY)).lower() != EMPTY
    }
    obj_catalog = resolve_objects(session, obj_keys)

    return _build_snapshot_from_balances(na_datu, balances, obj_catalog)


def _build_snapshot_from_data(na_datu: date, records: list, obj_catalog: dict) -> dict:
    """Построить снимок ДЗ из предзагруженных записей + каталога.

    Считаем НЕТТО-остаток по каждому заказу (ОбъектРасчетов): частичные
    погашения и переплаты по тем же срокам вычитаются корректно.
    В итог по подразделению попадают только заказы с положительной ДЗ /
    положительной просрочкой соответственно (как в отчёте 1С «Дебиторская
    задолженность по срокам»).
    """
    na_datu_str = na_datu.isoformat()
    cutoff_period = f"{na_datu_str}T23:59:59"

    filtered = [r for r in records if (r.get("Period") or "") <= cutoff_period]
    balances = aggregate_balances(filtered)

    return _build_snapshot_from_balances(na_datu, balances, obj_catalog)


def _build_snapshot_from_balances(na_datu: date, balances: dict,
                                  obj_catalog: dict) -> dict:
    """Общее ядро расчёта снимка ДЗ из агрегированных балансов.

    ПосТроение: сначала сумма нетто-остатков по заказу (все сроки
    и только просроченные сроки), затем агрегация по подразделению только
    для заказов с положительной суммой.
    """
    na_datu_str = na_datu.isoformat()
    overdue_cutoff = f"{na_datu_str}T00:00:00"
    dept_keys_lower = {d.lower() for d in DEPARTMENTS}

    per_order: dict[str, dict] = defaultdict(lambda: {
        "dept": "",
        "dz_net": 0.0,
        "overdue_net": 0.0,
        "aging_buckets": defaultdict(float),
    })

    for (obj_key, planned_dt), balance in balances.items():
        cat = obj_catalog.get(obj_key)
        if not cat:
            continue
        dept = cat["dept"]
        if dept not in dept_keys_lower:
            continue

        entry = per_order[obj_key]
        entry["dept"] = dept
        entry["dz_net"] += balance

        if planned_dt and planned_dt > "0001-01-02" and planned_dt < overdue_cutoff:
            entry["overdue_net"] += balance
            days_overdue = (na_datu - date.fromisoformat(planned_dt[:10])).days
            entry["aging_buckets"][aging_bucket(days_overdue)] += balance

    dz_by_dept: dict[str, float] = defaultdict(float)
    overdue_by_dept: dict[str, float] = defaultdict(float)
    aging_by_dept: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for data in per_order.values():
        dept = data["dept"]
        dept_name = DEPARTMENTS.get(dept, dept[:8])
        dz_net = data["dz_net"]
        overdue_net = data["overdue_net"]

        if dz_net > TOLERANCE:
            dz_by_dept[dept_name] += dz_net
        if overdue_net > TOLERANCE:
            overdue_by_dept[dept_name] += overdue_net
            for b, amt in data["aging_buckets"].items():
                if amt > TOLERANCE:
                    aging_by_dept[dept_name][b] += amt

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


def _build_overdue_detail_from_data(na_datu: date, records: list,
                                    obj_catalog: dict,
                                    session) -> dict:
    """Построить детализацию просрочки из предзагруженных данных.

    С детализацией ПО КАЖДОМУ ЗАКАЗУ КЛИЕНТА: в каждой строке указан
    номер заказа клиента, причина и корректирующее действие из
    Document_ЗаказКлиента.
    """
    na_datu_str = na_datu.isoformat()
    cutoff_period = f"{na_datu_str}T23:59:59"

    filtered = [r for r in records if (r.get("Period") or "") <= cutoff_period]
    balances = aggregate_balances(filtered)

    rows = _build_overdue_rows_per_order(na_datu, balances, obj_catalog, session)
    total = round(sum(r["amount"] for r in rows), 2)

    detail = {
        "na_datu": na_datu_str,
        "cache_date": date.today().isoformat(),
        "total_overdue": total,
        "rows": rows,
    }
    _save_json(_cache_path_overdue_detail(na_datu), detail)
    logger.info("calc_debitorka: batch overdue detail %s (%d rows)", na_datu_str, len(rows))
    return detail


def _calc_snapshots_batch(dates_to_compute: list[date],
                          also_overdue_detail: bool = False) -> dict[date, dict]:
    """Один fetch из OData → снимки для всех дат + опционально overdue detail.

    Загружает реестр и каталог один раз (по самой поздней дате),
    затем фильтрует записи по Period для каждой более ранней даты.
    """
    if not dates_to_compute:
        return {}

    sorted_dates = sorted(dates_to_compute)
    latest = sorted_dates[-1]

    session = requests.Session()
    session.auth = AUTH

    logger.info("calc_debitorka: batch %d snapshots (latest=%s)",
                len(dates_to_compute), latest)

    records = fetch_all_register(session, latest.isoformat())

    # Собираем ВСЕ ОбъектыРасчетов из сырых записей — нельзя брать только из
    # full_balances (balances по latest дате), т.к. заказы, полностью
    # погашенные к latest, выпадут из набора и в исторических снимках (например,
    # январь) остатки по ним не смогут быть резолвлены в подразделение/партнёра.
    all_obj_keys = {
        str(r.get("ОбъектРасчетов_Key", EMPTY)).lower()
        for r in records
        if str(r.get("ОбъектРасчетов_Key", EMPTY)).lower() != EMPTY
    }
    obj_catalog = resolve_objects(session, all_obj_keys)

    results: dict[date, dict] = {}
    for na_datu in sorted_dates:
        snapshot = _build_snapshot_from_data(na_datu, records, obj_catalog)
        _save_json(_cache_path_snapshot(na_datu), snapshot)
        results[na_datu] = snapshot
        logger.info("calc_debitorka: batch snapshot %s done", na_datu.isoformat())

    if also_overdue_detail:
        for na_datu in sorted_dates:
            _build_overdue_detail_from_data(na_datu, records, obj_catalog, session)

    return results


def get_snapshot_for_date(na_datu: date) -> dict:
    """Кэшируемый снимок ДЗ/просрочки на дату."""
    cached = _load_json(_cache_path_snapshot(na_datu))
    if cached is not None:
        return cached
    payload = _calc_snapshot_for_date(na_datu)
    _save_json(_cache_path_snapshot(na_datu), payload)
    return payload


def get_komdir_dz_monthly(year: int | None = None,
                          month: int | None = None,
                          dept_name: str | None = None) -> dict:
    """
    Помесячные ДЗ/просрочка (январь -> ref_month).
    dept_name=None — агрегат по всем отделам (коммерческий директор).
    dept_name='Отдел ВЭД' — факт только по указанному подразделению.

    Если часть снимков отсутствует — загружает ВСЕ разом (batch),
    вместо отдельной загрузки регистра+каталога на каждый месяц.
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    _ensure_debitorka_caches_for_period(ref_y, ref_m)

    if dept_name is None:
        cached = _load_json(_cache_path_monthly(ref_y, ref_m))
        if cached is not None:
            return cached

    snap_dates = _snap_dates_for_year_through_month(ref_y, ref_m)

    out_rows = []
    for mm, snap_date in snap_dates:
        snapshot = get_snapshot_for_date(snap_date)
        if dept_name is not None:
            dept_data = snapshot.get("by_dept", {}).get(dept_name, {})
            dz = float(dept_data.get("dz", 0))
            overdue = float(dept_data.get("overdue", 0))
        else:
            dz = float(snapshot.get("total_dz") or 0)
            overdue = float(snapshot.get("total_overdue") or 0)
        out_rows.append({
            "year": ref_y,
            "month": mm,
            "na_datu": snapshot.get("na_datu"),
            "dz_fact": dz,
            "overdue_fact": overdue,
        })

    payload = {"year": ref_y, "ref_month": ref_m, "months": out_rows}
    if dept_name is None:
        _save_json(_cache_path_monthly(ref_y, ref_m), payload)
    return payload


def _cache_path_overdue_detail(na_datu: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"overdue_detail_{na_datu.isoformat()}.json"


def overdue_detail_cache_path(year: int, month: int) -> Path:
    """Путь к файлу кэша детализации просрочки для календарного месяца."""
    na = _month_end(year, month)
    today = date.today()
    if na > today:
        na = today
    return _cache_path_overdue_detail(na)


def _snap_dates_for_year_through_month(ref_y: int, ref_m: int) -> list[tuple[int, date]]:
    """Пары (номер месяца, дата снимка) для января..ref_m (конец месяца, не позже сегодня)."""
    today = date.today()
    out: list[tuple[int, date]] = []
    for mm in range(1, ref_m + 1):
        d = _month_end(ref_y, mm)
        if d > today:
            d = today
        out.append((mm, d))
    return out


def _ensure_debitorka_caches_for_period(ref_y: int, ref_m: int) -> None:
    """Один запрос к OData: снимки ДЗ и детализация просрочки за все месяцы 1..ref_m.

    Если каких-то снимков или файлов overdue_detail нет / устарели (не сегодняшняя
    дата кэша) — загружаем регистр по самой поздней нужной дате и строим всё разом.
    """
    today = date.today()
    snap_dates = _snap_dates_for_year_through_month(ref_y, ref_m)

    uncached = [d for _, d in snap_dates if _load_json(_cache_path_snapshot(d)) is None]

    def _overdue_needs_refresh(d: date) -> bool:
        od = _load_json(_cache_path_overdue_detail(d))
        return od is None or od.get("cache_date") != today.isoformat()

    overdue_stale = [d for _, d in snap_dates if _overdue_needs_refresh(d)]

    if not uncached and not overdue_stale:
        return

    work_dates = sorted(set(uncached + overdue_stale))
    need_overdue = bool(overdue_stale)
    _calc_snapshots_batch(work_dates, also_overdue_detail=need_overdue)


def _calc_overdue_detail(na_datu: date) -> dict:
    """Детализация просроченной ДЗ по заказам клиентов на дату.

    Возвращает список строк (partner_name, order_num, amount, days_overdue,
    reason, action) с разбивкой по подразделениям.
    """
    session = requests.Session()
    session.auth = AUTH
    na_datu_str = na_datu.isoformat()
    logger.info("calc_debitorka: overdue detail for %s", na_datu_str)

    records = fetch_all_register(session, na_datu_str)
    balances = aggregate_balances(records)

    obj_keys = {obj for (obj, _) in balances.keys()}
    obj_catalog = resolve_objects(session, obj_keys)

    rows = _build_overdue_rows_per_order(na_datu, balances, obj_catalog, session)
    total = round(sum(r["amount"] for r in rows), 2)

    return {
        "na_datu": na_datu_str,
        "cache_date": date.today().isoformat(),
        "total_overdue": total,
        "rows": rows,
    }


def get_overdue_detail(year: int | None = None,
                       month: int | None = None,
                       dept_guid: str | None = None) -> dict:
    """Детализация просроченной ДЗ по контрагентам.

    dept_guid=None → все отделы (коммерческий директор),
    dept_guid='...' → только указанный отдел.
    Результат кэшируется на день.
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    _ensure_debitorka_caches_for_period(ref_y, ref_m)

    na_datu = _month_end(ref_y, ref_m)
    if na_datu > today:
        na_datu = today
    cache_path = _cache_path_overdue_detail(na_datu)
    cached = _load_json(cache_path)

    if cached is not None and cached.get("cache_date") == today.isoformat():
        data = cached
    else:
        data = _calc_overdue_detail(na_datu)
        _save_json(cache_path, data)

    rows = data.get("rows", [])
    if dept_guid:
        dept_lower = dept_guid.lower()
        rows = [r for r in rows if r.get("dept_key") == dept_lower]

    total = round(sum(r["amount"] for r in rows), 2)

    return {
        "na_datu": data.get("na_datu"),
        "total_overdue": total,
        "rows": rows,
    }


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
