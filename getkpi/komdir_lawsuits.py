"""
komdir_lawsuits.py — Выгрузка судов (Document_ТД_ПретензииСудебныеСпорыИсковаяРабота)
                      из 1С OData за указанный месяц для коммерческого директора.

По аналогии с komdir_claims.py, только источник — документ
«Претензии, судебные споры и исковая работа (ТД)».

Поля для вывода:
  - Номер (Number)
  - Статус                         (Статус)
  - ТипДокумента                   (ТипДокумента)
  - Контрагент                     (Контрагент_Key → Catalog_Контрагенты.Description)
  - ПредметСпора                   (ПредметСпора)
  - СуммаТребований                (СуммаТребований)
  - РольГКВСпоре                   (РольГКВСпоре)
  - ПлощадкаЮрлицоГК               (ПлощадкаЮрлицоГК_Key → Catalog_Организации.Description)
  - Подразделение инициатора       (ИнициаторЗаказчикВнутриГК_Key → Catalog_Пользователи.Подразделение_Key
                                    → Catalog_СтруктураПредприятия.Description)

Фильтр: в выдачу попадают только те документы, где подразделение инициатора
входит в множество «детей коммерческого директора» (ALLOWED_DEPARTMENTS).

Результат кэшируется на день в JSON: dashboard/lawsuits_<year>_<month>.json.
"""
from __future__ import annotations

import calendar
import json
import logging
from datetime import date
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

# Дети «коммерческого директора» (по structure.json + аналогично komdir_claims.ALLOWED_DEPARTMENTS).
ALLOWED_DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",  # Отдел ВЭД
    "34497ef7-810f-11e4-80d6-001e67112509",  # Отдел продаж эталонного оборудования и услуг
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",  # Отдел продаж БМИ
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",  # Отдел по работе с ключевыми клиентами
    "7587c178-92f6-11f0-96f9-6cb31113810e",  # Отдел дилерских продаж (ОДП / ОПБО)
    "bd7b5184-9f9c-11e4-80da-001e67112509",  # Отдел по работе с ПАО «Газпром»
}

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'

DOC_ENTITY = "Document_ТД_ПретензииСудебныеСпорыИсковаяРабота"


def _cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"lawsuits_{year}_{month:02d}.json"


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
            json.dump(
                {'date': date.today().isoformat(), 'rows': rows},
                f, ensure_ascii=False,
            )
    except OSError:
        pass


def _load_catalog_full(session: requests.Session,
                       entity: str, select_fields: str) -> dict[str, dict]:
    """Загрузка справочника целиком с пагинацией и $orderby."""
    result: dict[str, dict] = {}
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$top={PAGE}&$skip={skip}"
            f"&$select={quote(select_fields, safe=',_')}"
            f"&$orderby=Ref_Key"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("%s HTTP error: %s", entity, e)
            break
        if not r.ok:
            logger.error("%s HTTP %d", entity, r.status_code)
            break
        rows = r.json().get("value", [])
        if not rows:
            break
        for item in rows:
            result[item["Ref_Key"]] = item
        if len(rows) < PAGE:
            break
        skip += len(rows)
    return result


def _fetch_single(session: requests.Session,
                  entity: str, guid: str, select_fields: str) -> dict | None:
    """Точечная загрузка одной записи по GUID."""
    url = (
        f"{BASE}/{quote(entity)}(guid'{guid}')"
        f"?$format=json&$select={quote(select_fields, safe=',_')}"
    )
    try:
        r = session.get(url, timeout=15)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None


def _fetch_documents(session: requests.Session,
                     year: int, month: int) -> list[dict]:
    """Документы судов за указанный месяц по Date."""
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01T00:00:00"
    date_to = f"{year}-{month:02d}-{last_day}T23:59:59"

    select_doc = (
        "Ref_Key,Number,Date,Posted,DeletionMark,"
        "Статус,ТипДокумента,"
        "Контрагент_Key,"
        "ПредметСпора,СуммаТребований,"
        "РольГКВСпоре,ПлощадкаЮрлицоГК_Key,"
        "ИнициаторЗаказчикВнутриГК_Key"
    )

    docs: list[dict] = []
    skip = 0
    while True:
        odata_filter = (
            f"Date ge datetime'{date_from}'"
            f" and Date le datetime'{date_to}'"
        )
        url = (
            f"{BASE}/{quote(DOC_ENTITY)}?$format=json"
            f"&$select={quote(select_doc, safe=',_')}&$top=5000&$skip={skip}"
            f"&$filter={quote(odata_filter, safe='')}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("Lawsuits HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("Lawsuits HTTP %d: %s", r.status_code, r.text[:300])
            break
        rows = r.json().get("value", [])
        docs.extend(rows)
        if len(rows) < 5000:
            break
        skip += 5000
    return docs


def _fetch_from_odata(year: int, month: int) -> list[dict]:
    """Загружает документы судов из 1С OData за указанный месяц, резолвит ссылки."""
    session = requests.Session()
    session.auth = AUTH

    docs = _fetch_documents(session, year, month)
    docs = [d for d in docs if not d.get("DeletionMark")]

    # ── Контрагенты ──
    raw_contr = _load_catalog_full(
        session, "Catalog_Контрагенты", "Ref_Key,Description",
    )
    contr_names = {k: (v.get("Description") or "").strip() for k, v in raw_contr.items()}
    needed_contr = {
        d.get("Контрагент_Key") for d in docs
        if d.get("Контрагент_Key") and d.get("Контрагент_Key") != EMPTY
    }
    for ck in needed_contr - set(contr_names.keys()):
        item = _fetch_single(
            session, "Catalog_Контрагенты", ck, "Ref_Key,Description",
        )
        if item:
            contr_names[ck] = (item.get("Description") or "").strip()

    # ── Организации (ПлощадкаЮрлицоГК) ──
    raw_org = _load_catalog_full(
        session, "Catalog_Организации", "Ref_Key,Description",
    )
    org_names = {k: (v.get("Description") or "").strip() for k, v in raw_org.items()}
    needed_org = {
        d.get("ПлощадкаЮрлицоГК_Key") for d in docs
        if d.get("ПлощадкаЮрлицоГК_Key") and d.get("ПлощадкаЮрлицоГК_Key") != EMPTY
    }
    for ok in needed_org - set(org_names.keys()):
        item = _fetch_single(
            session, "Catalog_Организации", ok, "Ref_Key,Description",
        )
        if item:
            org_names[ok] = (item.get("Description") or "").strip()

    # ── Подразделения (структура предприятия) ──
    raw_depts = _load_catalog_full(
        session, "Catalog_СтруктураПредприятия", "Ref_Key,Description",
    )
    dept_names = {k: (v.get("Description") or "").strip() for k, v in raw_depts.items()}

    # ── Пользователи-инициаторы → их подразделение ──
    needed_users = {
        d.get("ИнициаторЗаказчикВнутриГК_Key") for d in docs
        if d.get("ИнициаторЗаказчикВнутриГК_Key")
        and d.get("ИнициаторЗаказчикВнутриГК_Key") != EMPTY
    }
    user_dept: dict[str, str] = {}
    if needed_users:
        raw_users = _load_catalog_full(
            session, "Catalog_Пользователи", "Ref_Key,Description,Подразделение_Key",
        )
        for uk, v in raw_users.items():
            user_dept[uk] = v.get("Подразделение_Key") or ""
        for uk in needed_users - set(user_dept.keys()):
            item = _fetch_single(
                session, "Catalog_Пользователи", uk,
                "Ref_Key,Description,Подразделение_Key",
            )
            if item:
                user_dept[uk] = item.get("Подразделение_Key") or ""

    # ── Сборка результата ──
    result_rows: list[dict] = []
    for d in docs:
        init_key = d.get("ИнициаторЗаказчикВнутриГК_Key") or ""
        init_dept_key = user_dept.get(init_key, "") if init_key and init_key != EMPTY else ""

        # Фильтр: только если подразделение инициатора — ребёнок коммерческого директора.
        if init_dept_key not in ALLOWED_DEPARTMENTS:
            continue

        contr_key = d.get("Контрагент_Key") or ""
        org_key = d.get("ПлощадкаЮрлицоГК_Key") or ""

        subject = (d.get("ПредметСпора") or "").replace("\r\n", " ").replace("\n", " ")

        try:
            amount = float(d.get("СуммаТребований") or 0)
        except (TypeError, ValueError):
            amount = 0.0

        result_rows.append({
            "number": (d.get("Number") or "").strip(),
            "date": (d.get("Date") or "")[:10],
            "status": d.get("Статус", "") or "",
            "doc_type": d.get("ТипДокумента", "") or "",
            "counterparty": contr_names.get(contr_key, "") if contr_key and contr_key != EMPTY else "",
            "subject": subject,
            "claim_amount": amount,
            "gc_role": d.get("РольГКВСпоре", "") or "",
            "gc_entity": org_names.get(org_key, "") if org_key and org_key != EMPTY else "",
            "initiator_dept": dept_names.get(init_dept_key, ""),
            "initiator_dept_key": init_dept_key,
        })

    return result_rows


def fetch_lawsuits_for_month(year: int, month: int) -> list[dict]:
    """
    Возвращает список строк таблицы «Суды» за указанный месяц
    (Document_ТД_ПретензииСудебныеСпорыИсковаяРабота).

    Отфильтрованы только те записи, где подразделение инициатора —
    один из дочерних отделов коммерческого директора (ALLOWED_DEPARTMENTS).
    Кэшируется на день в JSON-файл.
    """
    cached = _load_cache(year, month)
    if cached is not None:
        return cached

    try:
        rows = _fetch_from_odata(year, month)
    except Exception as e:
        logger.error("Failed to fetch lawsuits: %s", e)
        rows = []

    _save_cache(year, month, rows)
    return rows
