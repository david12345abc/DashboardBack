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

from . import cache_manager
from .commercial_department_aliases import (
    COMMERCIAL_DEPT_ALIASES,
    normalize_commercial_dept_guid,
)
from .odata_http import request_with_retry

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
DEPT_ALIAS_SOURCE = "debitorka_department_aliases_v3"
# Версия только для детализации просрочки: при смене инвалидируется overdue_detail,
# не трогая помесячные снимки ДЗ (dept_alias_source).
OVERDUE_DETAIL_CACHE_VERSION = "overdue_detail_sql_erp_pm_v1"
DEALER_SALES_DEPT_GUID = "7587c178-92f6-11f0-96f9-6cb31113810e"
DEBITORKA_DEPT_ALIASES = {
    source: target
    for source, target in COMMERCIAL_DEPT_ALIASES.items()
    if target != DEALER_SALES_DEPT_GUID
}
LIQUIDATED_DEPT_NAMES = {
    "4edcf3a0-9f99-11e4-80da-001e67112509": "(ликв.) Отдел дилерских продаж бытового оборудования",
    "ff740269-d71e-11e6-8127-001e67112509": "(ликв.) Отдел дилерских продаж промышленного оборудования",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e": "(ликв.) Отдел по работе с холдингами 1",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e": "(ликв.) Отдел по работе с холдингами 2",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e": "(ликв.) Отдел по работе с холдингами 3",
}

REGISTER = "AccumulationRegister_РасчетыСКлиентамиПоСрокам_RecordType"


def normalize_debitorka_dept_guid(dept_guid: str | None) -> str:
    """Для ДЗ не подтягиваем ликвидированные дилерские отделы в текущий ОДП."""
    if not dept_guid:
        return ""
    return DEBITORKA_DEPT_ALIASES.get(dept_guid, dept_guid)


def fetch_all_register(session, na_datu: str):
    """Загрузить ВСЕ записи регистра с Period <= НаДату (конец дня)."""

    sel = quote(
        "RecordType,Period,Active,"
        "ОбъектРасчетов_Key,АналитикаУчетаПоПартнерам_Key,"
        "Валюта_Key,ДатаПлановогоПогашения,ДатаВозникновения,"
        "ДолгУпр,ПредоплатаУпр",
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
        r = request_with_retry(session, url, timeout=180, retries=4, label="DZ/Register")
        if r is None or not r.ok:
            print(f"  ⚠ HTTP {(r.status_code if r else 'no-response')} при skip={skip}")
            break
        batch = r.json().get("value", [])
        records.extend(batch)
        if skip % 50000 == 0 or len(batch) < PAGE:
            print(f"  загружено {len(records)} записей · {time.time()-t0:.1f}с")
        if len(batch) < PAGE:
            break
        skip += PAGE

    return records


def aggregate_balances_full(records):
    """
    Агрегация нетто-остатков по (ОбъектРасчетов_Key, ДатаПлановогоПогашения).

    Возвращает dict: (obj_key, planned_date_str) → {"dolg": float, "predoplata": float}.
      dolg       — нетто-остаток ресурса ДолгУпр (ДЗ клиента).
      predoplata — нетто-остаток ресурса ПредоплатаУпр (наш долг клиенту, КЗ).

    Receipt прибавляет, Expense вычитает (в 1С это «Приход/Расход» регистра).
    """
    agg: dict = defaultdict(lambda: {"dolg": 0.0, "predoplata": 0.0})

    for r in records:
        obj = str(r.get("ОбъектРасчетов_Key", EMPTY)).lower()
        if obj == EMPTY:
            continue
        planned = r.get("ДатаПлановогоПогашения", "")
        dolg = float(r.get("ДолгУпр") or 0)
        predoplata = float(r.get("ПредоплатаУпр") or 0)
        rtype = r.get("RecordType", "")

        if rtype == "Receipt":
            sign = 1
        elif rtype == "Expense":
            sign = -1
        else:
            continue

        entry = agg[(obj, planned)]
        entry["dolg"] += sign * dolg
        entry["predoplata"] += sign * predoplata

    return {
        k: v for k, v in agg.items()
        if abs(v["dolg"]) >= TOLERANCE or abs(v["predoplata"]) >= TOLERANCE
    }


def aggregate_balances(records):
    """
    Совместимость со старым кодом: возвращает только ДолгУпр-балансы.

    dict: (obj_key, planned_date_str) → balance (float) по ресурсу ДолгУпр.
    Используется в логике просрочки (просрочка считается только по Долгу).
    """
    full = aggregate_balances_full(records)
    return {k: v["dolg"] for k, v in full.items() if abs(v["dolg"]) >= TOLERANCE}


def resolve_objects(session, obj_keys: set):
    """
    Загрузить Catalog_ОбъектыРасчетов страницами, собрать маппинг
    Ref_Key (lower) → {dept, partner, desc, number, date}.
    Подразделение берётся из каталога и нормализуется.
    В дебиторке ликвидированные дилерские отделы не попадают в текущий ОДП.
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
        r = request_with_retry(session, url, timeout=120, retries=4, label="DZ/ObjCatalog")
        if r is None or not r.ok:
            print(f"  ⚠ HTTP {(r.status_code if r else 'no-response')} при skip={skip}")
            break
        chunk = r.json().get("value", [])
        if not chunk:
            break
        for item in chunk:
            k = str(item.get("Ref_Key", EMPTY)).lower()
            if k in needed:
                raw_dept = str(item.get("Подразделение_Key", EMPTY)).lower()
                normalized_dept = normalize_debitorka_dept_guid(raw_dept).lower()
                # Всегда сохраняем исходный ликвидированный отдел для колонки
                # «Подразделение» (даже если для KPI GUID не алиасится в ОДП).
                liquidated_dept_name = LIQUIDATED_DEPT_NAMES.get(raw_dept, "")
                catalog[k] = {
                    "dept": normalized_dept,
                    "source_dept": raw_dept,
                    "liquidated_dept_name": liquidated_dept_name,
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
        print(f"  WARNING: не найдено в каталоге: {len(missing)} ОбъектРасчетов")

    return catalog


def resolve_partner_names(session, partner_keys: set):
    """Получить имена партнёров по GUID.

    БАТЧЕВО: один $filter с OR'ами на 40 ключей вместо 40 отдельных GET-ов.
    Это ×30-50 быстрее и в разы меньше нагрузка на 1С (раньше поштучные
    запросы ловили ReadTimeout 15с на медленном 1С).
    """
    names: dict[str, str] = {}
    keys = [pk for pk in partner_keys if pk and pk != EMPTY]
    if not keys:
        return names

    BATCH = 40
    for i in range(0, len(keys), BATCH):
        chunk = keys[i:i + BATCH]
        flt = " or ".join(f"Ref_Key eq guid'{pk}'" for pk in chunk)
        url = (
            f"{BASE}/Catalog_Партнеры?$format=json"
            f"&$select=Ref_Key,Description"
            f"&$filter={quote(flt, safe='')}&$top=5000"
        )
        r = request_with_retry(session, url, timeout=60, retries=4, label="Partners")
        if r is None or not r.ok:
            for pk in chunk:
                names.setdefault(pk, pk[:8])
            continue
        seen: set[str] = set()
        for item in r.json().get("value", []):
            pk = str(item.get("Ref_Key") or "")
            desc = (item.get("Description") or "").strip()
            if pk:
                names[pk] = desc or pk[:8]
                seen.add(pk)
        # Для ключей, которых не оказалось в каталоге — fallback.
        for pk in chunk:
            if pk not in seen and pk not in names:
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
            r = request_with_retry(session, url, timeout=60, retries=4, label="OrderDetails")
            if r is None or not r.ok:
                logger.warning("Order details HTTP %s for batch %d",
                               r.status_code if r else "no-response", i)
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
            "source_dept": "", "liquidated_dept_name": "",
            "partner": "", "order_num": "", "order_date": "",
            "installments": [],    # детализация по срокам погашения
        }
    )
    partner_keys_used: set[str] = set()
    order_numbers_used: set[str] = set()

    liquidated_keys_lower = {k.lower() for k in LIQUIDATED_DEPT_NAMES}

    for (obj_key, planned_dt), balance in balances.items():
        cat = obj_catalog.get(obj_key)
        if not cat:
            continue
        dept = cat["dept"]
        liquidated_name = (cat.get("liquidated_dept_name") or "").strip()
        source_dept = str(cat.get("source_dept") or "").lower()
        # Действующие коммерческие отделы + ликвидированные (даже без алиаса в ОДП).
        if dept not in dept_keys_lower and source_dept not in liquidated_keys_lower:
            continue

        partner_key = cat["partner"]
        order_num = str(cat.get("number") or "").strip()
        order_date = str(cat.get("date") or "")[:10]

        key = (dept, partner_key, obj_key)
        entry = by_order[key]
        entry["dept"] = dept
        entry["dept_name"] = (
            DEPARTMENTS.get(dept)
            or liquidated_name
            or LIQUIDATED_DEPT_NAMES.get(source_dept, dept[:8])
        )
        entry["source_dept"] = source_dept
        entry["liquidated_dept_name"] = liquidated_name or LIQUIDATED_DEPT_NAMES.get(
            source_dept, ""
        )
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
        liquidated_name = (data.get("liquidated_dept_name") or "").strip()
        dept_name = data["dept_name"]
        # В колонке «Подразделение»: действующее имя, а для ликвидированных —
        # исходное с пометкой «(ликв.)» (см. LIQUIDATED_DEPT_NAMES).
        department = liquidated_name or dept_name
        rows.append({
            "dept_key": dept,
            "dept_name": dept_name,
            "source_dept_key": data.get("source_dept", ""),
            "liquidated_dept_name": liquidated_name,
            "department": department,
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


def _sql_ref_to_guid(ref: bytes | bytearray | memoryview | None) -> str:
    if not ref:
        return EMPTY
    hx = bytes(ref).hex()
    if len(hx) != 32 or hx == "0" * 32:
        return EMPTY
    return f"{hx[24:32]}-{hx[20:24]}-{hx[16:20]}-{hx[0:4]}-{hx[4:16]}".lower()


def _sql_1c_date_to_iso(value) -> str:
    if not isinstance(value, datetime):
        return ""
    year = value.year - 2000 if value.year >= 2000 else value.year
    if year <= 1:
        return ""
    return value.replace(year=year).isoformat()


def _calc_overdue_detail_sql(na_datu: date) -> dict:
    """SQL-detail from erp_pm using the same register/filter as KD-M5 tiles."""
    from datetime import timedelta

    from comdir.common import connect_ctx, to_1c_dt, uuid_to_1c_bytes

    accum = "_AccumRg107662"
    obj_col = "_Fld140445RRef"
    plan_col = "_Fld107667"
    dolg_col = "_Fld107672"
    obj_table = "_Reference134945"
    obj_dept = "_Fld138169RRef"
    obj_partner = "_Fld138177RRef"
    obj_number = "_Fld138171"
    obj_date = "_Fld138172"
    partner_table = "_Reference328"

    p_end = to_1c_dt(na_datu + timedelta(days=1))
    na_1c = to_1c_dt(na_datu)
    allowed_depts = list(DEPARTMENTS) + list(LIQUIDATED_DEPT_NAMES)
    liquidated_keys_lower = {k.lower() for k in LIQUIDATED_DEPT_NAMES}

    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        cur.execute("IF OBJECT_ID('tempdb..#dz_depts') IS NOT NULL DROP TABLE #dz_depts")
        cur.execute("CREATE TABLE #dz_depts (id binary(16) PRIMARY KEY, guid char(36))")
        for guid in allowed_depts:
            cur.execute(
                "INSERT INTO #dz_depts (id, guid) VALUES (?, ?)",
                uuid_to_1c_bytes(guid),
                guid,
            )

        cur.execute(
            f"""
            SELECT
              CONVERT(varchar(36), d.guid) AS source_dept_guid,
              s.[{obj_col}] AS obj_ref,
              s.[{plan_col}] AS planned_dt,
              o.[{obj_partner}] AS partner_ref,
              p._Description AS partner_name,
              o.[{obj_number}] AS order_num,
              o.[{obj_date}] AS order_dt,
              SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{dolg_col}] ELSE s.[{dolg_col}] END) AS balance
            FROM [{accum}] s WITH (NOLOCK)
            INNER JOIN [{obj_table}] o WITH (NOLOCK) ON o._IDRRef = s.[{obj_col}]
            INNER JOIN #dz_depts d ON d.id = o.[{obj_dept}]
            LEFT JOIN [{partner_table}] p WITH (NOLOCK) ON p._IDRRef = o.[{obj_partner}]
            WHERE s._Period < ? AND s._Active = 0x01
            GROUP BY
              d.guid, s.[{obj_col}], s.[{plan_col}], o.[{obj_partner}],
              p._Description, o.[{obj_number}], o.[{obj_date}]
            HAVING ABS(SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{dolg_col}] ELSE s.[{dolg_col}] END)) >= ?
            """,
            p_end,
            TOLERANCE,
        )
        sql_rows = cur.fetchall()

    by_order: dict[str, dict] = defaultdict(lambda: {
        "amount": 0.0,
        "dz_total": 0.0,
        "max_days": 0,
        "dept": "",
        "dept_name": "",
        "source_dept": "",
        "liquidated_dept_name": "",
        "partner": "",
        "partner_name": "",
        "order_num": "",
        "order_date": "",
        "installments": [],
    })

    for row in sql_rows:
        source_dept = str(row.source_dept_guid or "").strip().lower()
        dept = normalize_debitorka_dept_guid(source_dept).lower()
        dept_name = (
            LIQUIDATED_DEPT_NAMES.get(source_dept)
            if source_dept in liquidated_keys_lower
            else DEPARTMENTS.get(dept, dept[:8])
        )
        liquidated_name = LIQUIDATED_DEPT_NAMES.get(source_dept, "")
        obj_key = _sql_ref_to_guid(row.obj_ref)
        if not obj_key or obj_key == EMPTY:
            continue

        balance = float(row.balance or 0)
        entry = by_order[obj_key]
        entry["dept"] = dept
        entry["dept_name"] = dept_name or ""
        entry["source_dept"] = source_dept
        entry["liquidated_dept_name"] = liquidated_name
        entry["partner"] = _sql_ref_to_guid(row.partner_ref)
        entry["partner_name"] = (row.partner_name or "").strip()
        entry["order_num"] = (row.order_num or "").strip()
        entry["order_date"] = _sql_1c_date_to_iso(row.order_dt)[:10]
        entry["dz_total"] += balance

        planned_dt = row.planned_dt
        if not isinstance(planned_dt, datetime) or planned_dt.year <= 2000 or planned_dt >= na_1c:
            continue

        entry["amount"] += balance
        if abs(balance) >= TOLERANCE:
            planned_iso = _sql_1c_date_to_iso(planned_dt)[:10]
            if not planned_iso:
                continue
            planned_date = date.fromisoformat(planned_iso)
            days_overdue = (na_datu - planned_date).days
            entry["installments"].append({
                "planned_date": planned_iso,
                "amount": round(balance, 2),
                "days_overdue": days_overdue,
                "bucket": aging_bucket(days_overdue) if balance > TOLERANCE else "",
            })
            if balance > TOLERANCE:
                entry["max_days"] = max(entry["max_days"], days_overdue)

    rows: list[dict] = []
    for obj_key, data in by_order.items():
        if abs(data["amount"]) < TOLERANCE:
            continue
        liquidated_name = (data.get("liquidated_dept_name") or "").strip()
        dept_name = data.get("dept_name") or ""
        department = liquidated_name or dept_name
        partner_name = data.get("partner_name") or data.get("partner") or ""
        installments = sorted(data["installments"], key=lambda x: x["planned_date"])
        rows.append({
            "dept_key": data["dept"],
            "dept_name": dept_name,
            "source_dept_key": data["source_dept"],
            "liquidated_dept_name": liquidated_name,
            "department": department,
            "partner_key": data["partner"],
            "partner_name": partner_name,
            "counterparty": partner_name,
            "order_key": obj_key,
            "order_num": data["order_num"],
            "order_date": data["order_date"],
            "amount": round(data["amount"], 2),
            "dz_total": round(data["dz_total"], 2),
            "days_overdue": data["max_days"],
            "installments_count": len(installments),
            "installments": installments,
            "reason": "",
            "action": "",
            "source": "sql_erp_pm",
        })

    rows.sort(key=lambda x: -x["amount"])
    total = round(sum(r["amount"] for r in rows), 2)
    return {
        "na_datu": na_datu.isoformat(),
        "cache_date": date.today().isoformat(),
        "dept_alias_source": DEPT_ALIAS_SOURCE,
        "overdue_detail_version": OVERDUE_DETAIL_CACHE_VERSION,
        "source": "sql_erp_pm",
        "total_overdue": total,
        "rows": rows,
    }


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


def _snapshot_is_empty(data: dict | None) -> bool:
    """Пустой/сбойный снимок: нет ни ДЗ, ни КЗ (типичный результат failed OData)."""
    if data is None:
        return True
    try:
        dz = float(data.get("total_dz") or 0)
        kz = float(data.get("total_kz") or 0)
    except (TypeError, ValueError):
        return True
    return dz == 0.0 and kz == 0.0


def _find_nonzero_snapshot_on_or_before(na_datu: date) -> dict | None:
    """Последний ненулевой daily-снимок на дату или раньше (сначала тот же месяц)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    candidates: list[tuple[date, Path]] = []
    for path in CACHE_DIR.glob("debitorka_????-??-??.json"):
        try:
            d = date.fromisoformat(path.stem.replace("debitorka_", "", 1))
        except ValueError:
            continue
        if d <= na_datu:
            candidates.append((d, path))
    if not candidates:
        return None

    same_month = [(d, p) for d, p in candidates if d.year == na_datu.year and d.month == na_datu.month]
    # Сначала тот же месяц, затем более ранние даты (если в месяце только нули).
    seen: set[Path] = set()
    ordered: list[tuple[date, Path]] = []
    for pool in (same_month, candidates):
        for item in sorted(pool, key=lambda pair: pair[0], reverse=True):
            if item[1] in seen:
                continue
            seen.add(item[1])
            ordered.append(item)
    for _, path in ordered:
        snap = _load_json(path)
        if (
            snap is not None
            and snap.get("kz_source") == "predoplata_upr"
            and snap.get("dept_alias_source") == DEPT_ALIAS_SOURCE
            and not _snapshot_is_empty(snap)
        ):
            return snap
    return None


def _save_snapshot_json(na_datu: date, data: dict) -> None:
    """Сохранить снимок; не затирать хороший кэш пустым результатом fetch."""
    path = _cache_path_snapshot(na_datu)
    if _snapshot_is_empty(data):
        existing = _load_json(path)
        if existing is not None and not _snapshot_is_empty(existing):
            logger.warning(
                "calc_debitorka: skip saving empty snapshot %s (keep non-zero cache)",
                na_datu.isoformat(),
            )
            return
    _save_json(path, data)


def _resolve_snapshot_for_monthly(na_datu: date) -> dict:
    """Снимок для помесячного ряда: при нулях — fallback на более ранний ненулевой файл."""
    snapshot = get_snapshot_for_date(na_datu)
    if not _snapshot_is_empty(snapshot):
        return snapshot
    fallback = _find_nonzero_snapshot_on_or_before(na_datu)
    if fallback is None:
        return snapshot
    logger.warning(
        "calc_debitorka: empty snapshot %s, fallback to %s",
        na_datu.isoformat(),
        fallback.get("na_datu"),
    )
    resolved = dict(fallback)
    # В monthly-ряду фиксируем ожидаемую дату снимка, чтобы кэш оставался «текущим».
    resolved["na_datu"] = na_datu.isoformat()
    return resolved


def _monthly_cache_is_current(data: dict | None, ref_y: int, ref_m: int) -> bool:
    """Месячный агрегат валиден только если строки построены на нужные даты снимков."""
    if data is None:
        return False
    if data.get("kz_source") != "predoplata_upr":
        return False
    if data.get("dept_alias_source") != DEPT_ALIAS_SOURCE:
        return False

    rows = data.get("months") or []
    if not rows or not all("kz_fact" in row for row in rows):
        return False

    expected_dates = {
        month: snap_date.isoformat()
        for month, snap_date in _snap_dates_for_year_through_month(ref_y, ref_m)
    }
    rows_by_month = {
        int(row.get("month")): row
        for row in rows
        if isinstance(row, dict) and row.get("month") is not None
    }
    for month, expected_date in expected_dates.items():
        row = rows_by_month.get(month)
        if row is None or row.get("na_datu") != expected_date:
            return False
        # Нулевая строка при наличии более раннего ненулевого daily — устаревший/отравленный кэш.
        try:
            dz = float(row.get("dz_fact") or 0)
            kz = float(row.get("kz_fact") or 0)
        except (TypeError, ValueError):
            dz, kz = 0.0, 0.0
        if dz == 0.0 and kz == 0.0:
            try:
                snap_d = date.fromisoformat(expected_date)
            except ValueError:
                return False
            if _find_nonzero_snapshot_on_or_before(snap_d) is not None:
                return False
    return True


def _overdue_detail_cache_is_current(data: dict | None) -> bool:
    """Кэш детализации должен содержать колонку подразделения (в т.ч. ликвидированных)."""
    if data is None:
        return False
    if data.get("cache_date") != date.today().isoformat():
        return False
    if data.get("dept_alias_source") != DEPT_ALIAS_SOURCE:
        return False
    if data.get("overdue_detail_version") != OVERDUE_DETAIL_CACHE_VERSION:
        return False
    rows = data.get("rows") or []
    return not rows or all(
        "department" in row and "liquidated_dept_name" in row for row in rows
    )


def _overdue_detail_cache_is_usable(data: dict | None) -> bool:
    if data is None:
        return False
    if data.get("dept_alias_source") != DEPT_ALIAS_SOURCE:
        return False
    if data.get("overdue_detail_version") != OVERDUE_DETAIL_CACHE_VERSION:
        return False
    rows = data.get("rows") or []
    return not rows or all(
        ("department" in row or "dept_name" in row) and "liquidated_dept_name" in row
        for row in rows
    )


def _monthly_overdue_total(data: dict | None, year: int, month: int) -> float | None:
    if data is None:
        return None
    if data.get("dept_alias_source") != DEPT_ALIAS_SOURCE:
        return None
    for row in data.get("months") or []:
        try:
            if int(row.get("year") or 0) == int(year) and int(row.get("month") or 0) == int(month):
                return round(float(row.get("overdue_fact") or 0), 2)
        except (TypeError, ValueError):
            continue
    return None


def _calc_snapshot_for_date(na_datu: date) -> dict:
    """Полный расчёт ДЗ/просрочки на дату (RecordType, медленно, но точно)."""
    session = requests.Session()
    session.auth = AUTH

    na_datu_str = na_datu.isoformat()
    logger.info("calc_debitorka: computing snapshot for %s", na_datu_str)

    records = fetch_all_register(session, na_datu_str)
    full_balances = aggregate_balances_full(records)

    # См. _calc_snapshots_batch: берём obj_keys из сырых записей, чтобы
    # подразделения подтягивались и для полностью погашенных заказов.
    obj_keys = {
        str(r.get("ОбъектРасчетов_Key", EMPTY)).lower()
        for r in records
        if str(r.get("ОбъектРасчетов_Key", EMPTY)).lower() != EMPTY
    }
    obj_catalog = resolve_objects(session, obj_keys)

    return _build_snapshot_from_balances(na_datu, full_balances, obj_catalog)


def _build_snapshot_from_data(na_datu: date, records: list, obj_catalog: dict) -> dict:
    """Построить снимок ДЗ/КЗ из предзагруженных записей + каталога.

    Считаем НЕТТО-остатки по каждому заказу (ОбъектРасчетов) ОТДЕЛЬНО по
    ресурсам ДолгУпр (ДЗ) и ПредоплатаУпр (КЗ): частичные погашения и
    переплаты по тем же срокам вычитаются корректно. В итог по подразделению
    попадают только заказы с положительной ДЗ / положительной просрочкой
    соответственно (как в отчёте 1С «Задолженность клиентов по срокам» —
    колонки «Долг клиента» и «Наш долг»).
    """
    na_datu_str = na_datu.isoformat()
    cutoff_period = f"{na_datu_str}T23:59:59"

    filtered = [r for r in records if (r.get("Period") or "") <= cutoff_period]
    full_balances = aggregate_balances_full(filtered)

    return _build_snapshot_from_balances(na_datu, full_balances, obj_catalog)


def _build_snapshot_from_balances(na_datu: date, balances: dict,
                                  obj_catalog: dict) -> dict:
    """Общее ядро расчёта снимка ДЗ/КЗ из агрегированных балансов.

    Принимает «full»-балансы из aggregate_balances_full:
      dict[(obj_key, planned_date)] → {"dolg": float, "predoplata": float}.

    Для обратной совместимости принимает и «старый» формат (dict[(obj,planned)] → float),
    где значение трактуется как ДолгУпр-баланс (predoplata=0).

    Построение: сначала сумма нетто-остатков по заказу отдельно по ДолгУпр и
    ПредоплатаУпр (все сроки и только просроченные сроки), затем агрегация по
    подразделению. В ДЗ попадают только заказы с положительным нетто-Долгом,
    в КЗ — только заказы с положительной нетто-Предоплатой (как в отчёте
    1С «Задолженность клиентов по срокам»).
    """
    na_datu_str = na_datu.isoformat()
    overdue_cutoff = f"{na_datu_str}T00:00:00"
    dept_keys_lower = {d.lower() for d in DEPARTMENTS}
    liquidated_keys_lower = {k.lower() for k in LIQUIDATED_DEPT_NAMES}
    allowed_depts = dept_keys_lower | liquidated_keys_lower

    per_order: dict[str, dict] = defaultdict(lambda: {
        "dept": "",
        "source_dept": "",
        "dz_net": 0.0,
        "kz_net": 0.0,
        "overdue_net": 0.0,
        "aging_buckets": defaultdict(float),
    })

    for (obj_key, planned_dt), value in balances.items():
        if isinstance(value, dict):
            dolg_val = float(value.get("dolg", 0.0))
            predoplata_val = float(value.get("predoplata", 0.0))
        else:
            dolg_val = float(value)
            predoplata_val = 0.0

        cat = obj_catalog.get(obj_key)
        if not cat:
            continue
        source_dept = (cat.get("source_dept") or cat["dept"] or "").lower()
        dept = (cat.get("dept") or "").lower()
        # Ликвидированные: в итоге комдира учитываем по исходному GUID
        # (дилерские не алиасятся в ОДП — см. DEBITORKA_DEPT_ALIASES).
        effective = source_dept if source_dept in liquidated_keys_lower else dept
        if effective not in allowed_depts:
            continue

        entry = per_order[obj_key]
        entry["dept"] = dept
        entry["source_dept"] = source_dept
        entry["dz_net"] += dolg_val
        entry["kz_net"] += predoplata_val

        if planned_dt and planned_dt > "0001-01-02" and planned_dt < overdue_cutoff:
            entry["overdue_net"] += dolg_val
            days_overdue = (na_datu - date.fromisoformat(planned_dt[:10])).days
            entry["aging_buckets"][aging_bucket(days_overdue)] += dolg_val

    dz_by_dept: dict[str, float] = defaultdict(float)
    kz_by_dept: dict[str, float] = defaultdict(float)
    overdue_by_dept: dict[str, float] = defaultdict(float)
    aging_by_dept: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for data in per_order.values():
        source_dept = data["source_dept"]
        dept = data["dept"]
        if source_dept in liquidated_keys_lower:
            dept_name = LIQUIDATED_DEPT_NAMES.get(source_dept, source_dept[:8])
        else:
            dept_name = DEPARTMENTS.get(dept, dept[:8])
        dz_net = data["dz_net"]
        kz_net = data["kz_net"]
        overdue_net = data["overdue_net"]

        # Как в SQL comdir: без фильтра «только +заказы» — сверка с 1С ~323.93M.
        dz_by_dept[dept_name] += dz_net
        kz_by_dept[dept_name] += kz_net
        overdue_by_dept[dept_name] += overdue_net
        for b, amt in data["aging_buckets"].items():
            aging_by_dept[dept_name][b] += amt

    depts_all = sorted(
        d
        for d in set(list(dz_by_dept.keys()) + list(kz_by_dept.keys()) + list(overdue_by_dept.keys()))
        if abs(dz_by_dept.get(d, 0)) >= TOLERANCE
        or abs(kz_by_dept.get(d, 0)) >= TOLERANCE
        or abs(overdue_by_dept.get(d, 0)) >= TOLERANCE
    )

    return {
        "na_datu": na_datu_str,
        "dept_alias_source": DEPT_ALIAS_SOURCE,
        "total_dz": round(sum(dz_by_dept.values()), 2),
        "total_kz": round(sum(max(0.0, v) for v in kz_by_dept.values()), 2),
        "total_overdue": round(sum(overdue_by_dept.values()), 2),
        "kz_source": "predoplata_upr",
        "by_dept": {
            d: {
                "dz": round(dz_by_dept.get(d, 0), 2),
                "kz": round(max(0.0, kz_by_dept.get(d, 0)), 2),
                "overdue": round(overdue_by_dept.get(d, 0), 2),
                "aging": {b: round(aging_by_dept[d].get(b, 0), 2) for b in BUCKETS},
            }
            for d in depts_all
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
        "dept_alias_source": DEPT_ALIAS_SOURCE,
        "overdue_detail_version": OVERDUE_DETAIL_CACHE_VERSION,
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
        if _snapshot_is_empty(snapshot):
            existing = _load_json(_cache_path_snapshot(na_datu))
            if existing is not None and not _snapshot_is_empty(existing):
                results[na_datu] = existing
                logger.warning(
                    "calc_debitorka: batch empty %s, keep existing non-zero cache",
                    na_datu.isoformat(),
                )
                continue
            fallback = _find_nonzero_snapshot_on_or_before(na_datu)
            if fallback is not None:
                kept = dict(fallback)
                kept["na_datu"] = na_datu.isoformat()
                results[na_datu] = kept
                logger.warning(
                    "calc_debitorka: batch empty %s, use fallback %s",
                    na_datu.isoformat(),
                    fallback.get("na_datu"),
                )
                continue
        _save_snapshot_json(na_datu, snapshot)
        results[na_datu] = snapshot
        logger.info("calc_debitorka: batch snapshot %s done", na_datu.isoformat())

    if also_overdue_detail:
        for na_datu in sorted_dates:
            _build_overdue_detail_from_data(na_datu, records, obj_catalog, session)

    return results


def get_snapshot_for_date(na_datu: date) -> dict:
    """Кэшируемый снимок ДЗ/просрочки на дату."""
    cached = _load_json(_cache_path_snapshot(na_datu))
    # Старые файлы кэша (без поля kz_source == "predoplata_upr") пересчитываем:
    # до v2 КЗ рассчитывался как отрицательные ДолгУпр-остатки и всегда был 0,
    # теперь КЗ — это ПредоплатаУпр-остатки (колонка «Наш долг» в 1С).
    if (
        cached is not None
        and cached.get("kz_source") == "predoplata_upr"
        and cached.get("dept_alias_source") == DEPT_ALIAS_SOURCE
        and not _snapshot_is_empty(cached)
    ):
        return cached
    # Пустой кэш на дату не считаем валидным — пробуем пересчитать, но не затираем
    # хороший файл нулями (см. _save_snapshot_json).
    if (
        cached is not None
        and cached.get("kz_source") == "predoplata_upr"
        and cached.get("dept_alias_source") == DEPT_ALIAS_SOURCE
        and _snapshot_is_empty(cached)
    ):
        fallback = _find_nonzero_snapshot_on_or_before(na_datu)
        if fallback is not None and fallback.get("na_datu") != na_datu.isoformat():
            # Есть более ранний хороший снимок — не ходим в OData сразу (часто тот же сбой).
            kept = dict(fallback)
            kept["na_datu"] = na_datu.isoformat()
            return kept
    payload = _calc_snapshot_for_date(na_datu)
    if _snapshot_is_empty(payload):
        if cached is not None and not _snapshot_is_empty(cached):
            return cached
        fallback = _find_nonzero_snapshot_on_or_before(na_datu)
        if fallback is not None:
            kept = dict(fallback)
            kept["na_datu"] = na_datu.isoformat()
            return kept
    _save_snapshot_json(na_datu, payload)
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

    _ensure_debitorka_caches_for_period(ref_y, ref_m, include_overdue_detail=False)

    if dept_name is None:
        cached = _load_json(_cache_path_monthly(ref_y, ref_m))
        if _monthly_cache_is_current(cached, ref_y, ref_m):
            return cached

    snap_dates = _snap_dates_for_year_through_month(ref_y, ref_m)

    out_rows = []
    for mm, snap_date in snap_dates:
        snapshot = _resolve_snapshot_for_monthly(snap_date)
        if dept_name is not None:
            dept_data = snapshot.get("by_dept", {}).get(dept_name, {})
            dz = float(dept_data.get("dz", 0))
            kz = float(dept_data.get("kz", 0))
            overdue = float(dept_data.get("overdue", 0))
        else:
            dz = float(snapshot.get("total_dz") or 0)
            kz = float(snapshot.get("total_kz") or 0)
            overdue = float(snapshot.get("total_overdue") or 0)
        out_rows.append({
            "year": ref_y,
            "month": mm,
            "na_datu": snap_date.isoformat(),
            "dz_fact": dz,
            "kz_fact": kz,
            "overdue_fact": overdue,
        })

    payload = {
        "cache_date": date.today().isoformat(),
        "year": ref_y,
        "ref_month": ref_m,
        "kz_source": "predoplata_upr",
        "dept_alias_source": DEPT_ALIAS_SOURCE,
        "months": out_rows,
    }
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


def _ensure_debitorka_caches_for_period(
    ref_y: int,
    ref_m: int,
    *,
    include_overdue_detail: bool = False,
) -> None:
    """Один запрос к OData: снимки ДЗ и детализация просрочки за все месяцы 1..ref_m.

    Если каких-то снимков или файлов overdue_detail нет / устарели (не сегодняшняя
    дата кэша) — загружаем регистр по самой поздней нужной дате и строим всё разом.
    """
    today = date.today()
    snap_dates = _snap_dates_for_year_through_month(ref_y, ref_m)

    def _snapshot_needs_refresh(d: date) -> bool:
        snap = _load_json(_cache_path_snapshot(d))
        # «Старые» файлы (до v2 КЗ по ПредоплатаУпр) пересобираем.
        # Ненулевой снимок со старым dept_alias_source — только патчим метку,
        # без полного OData-batch (иначе пустой/зависший fetch затирает кэш).
        if snap is None or snap.get("na_datu") != d.isoformat():
            return True
        if snap.get("kz_source") != "predoplata_upr":
            return True
        if _snapshot_is_empty(snap):
            # Пустой файл: OData только если нет локального ненулевого fallback.
            return _find_nonzero_snapshot_on_or_before(d) is None
        if snap.get("dept_alias_source") != DEPT_ALIAS_SOURCE:
            snap["dept_alias_source"] = DEPT_ALIAS_SOURCE
            _save_json(_cache_path_snapshot(d), snap)
            return False
        return False

    uncached = [d for _, d in snap_dates if _snapshot_needs_refresh(d)]

    def _overdue_needs_refresh(d: date) -> bool:
        od = _load_json(_cache_path_overdue_detail(d))
        return not _overdue_detail_cache_is_current(od)

    overdue_stale = (
        [d for _, d in snap_dates if _overdue_needs_refresh(d)]
        if include_overdue_detail
        else []
    )

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
    try:
        return _calc_overdue_detail_sql(na_datu)
    except Exception as exc:
        logger.warning("calc_debitorka: SQL overdue detail failed, fallback to OData: %s", exc)

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
        "dept_alias_source": DEPT_ALIAS_SOURCE,
        "overdue_detail_version": OVERDUE_DETAIL_CACHE_VERSION,
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

    na_datu = _month_end(ref_y, ref_m)
    if na_datu > today:
        na_datu = today
    cache_path = _cache_path_overdue_detail(na_datu)
    cached = _load_json(cache_path)

    if _overdue_detail_cache_is_current(cached):
        data = cached
    elif not cache_manager.is_force_compute_context():
        if _overdue_detail_cache_is_usable(cached):
            data = dict(cached)
            data["cache_refresh_status"] = "running"
        else:
            try:
                data = _calc_overdue_detail_sql(na_datu)
                _save_json(cache_path, data)
            except Exception as exc:
                logger.warning(
                    "calc_debitorka: immediate SQL overdue detail failed: %s",
                    exc,
                )
                monthly = _load_json(_cache_path_monthly(ref_y, ref_m))
                total = _monthly_overdue_total(monthly, ref_y, ref_m)
                data = {
                    "na_datu": na_datu.isoformat(),
                    "total_overdue": total or 0,
                    "rows": [],
                    "cache_refresh_status": "running",
                }
    else:
        _ensure_debitorka_caches_for_period(ref_y, ref_m, include_overdue_detail=True)
        cached = _load_json(cache_path)
        if _overdue_detail_cache_is_current(cached):
            data = cached
        else:
            data = _calc_overdue_detail(na_datu)
            _save_json(cache_path, data)

    rows = data.get("rows", [])
    if dept_guid:
        dept_lower = str(dept_guid).lower()
        dept_norm = normalize_debitorka_dept_guid(dept_lower).lower()
        # В т.ч. ликвидированные дилерские (алиас в COMMERCIAL, но не в DEBITORKA).
        rows = [
            r for r in rows
            if str(r.get("dept_key") or "").lower() in {dept_lower, dept_norm}
            or normalize_debitorka_dept_guid(str(r.get("source_dept_key") or "").lower())
            in {dept_lower, dept_norm}
            or normalize_commercial_dept_guid(str(r.get("source_dept_key") or "").lower())
            in {dept_lower, dept_norm}
        ]

    enriched_rows = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        row = dict(r)
        liquidated = (row.get("liquidated_dept_name") or "").strip()
        dept_name = (row.get("dept_name") or "").strip()
        department = (row.get("department") or "").strip() or liquidated or dept_name
        row["department"] = department
        row["liquidated_dept_name"] = liquidated
        enriched_rows.append(row)
    rows = enriched_rows

    if rows:
        total = round(sum(r["amount"] for r in rows), 2)
    else:
        total = round(float(data.get("total_overdue") or 0), 2)

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
