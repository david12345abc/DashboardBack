"""Количество и сумма договоров контрагентов по организациям за месяц.

Источник: Catalog_ДоговорыКонтрагентов.
Месяц договора — поле «ДатаНачалаДействия» (на форме «Действует с»).
В расчёт входят договоры без пометки удаления
(статусы «Действует», «Закрыт», «НеСогласован»).
Сумма — поле «Сумма», без пересчёта валюты.
Цель договора — поле «ХозяйственнаяОперация» (на форме: «Цель договора»).
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

from devdir import ytd_json_cache
from devdir.rd_monthly_period import normalize_rd_tile_period
from getkpi.odata_http import request_with_retry

from jurist.companies import COMPANIES

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
ENTITY = "Catalog_ДоговорыКонтрагентов"
PAGE = 2000
CACHE_PREFIX = "jurist_contracts"
CACHE_SOURCE = "jurist_contracts_v4"
CACHE_VERSION = 4

STATUS_LABELS = {
    "Действует": "Действует",
    "Закрыт": "Закрыт",
    "НеСогласован": "Не согласован",
}
EMPTY_GUID = "00000000-0000-0000-0000-000000000000"

# Эти цели договора в количество и сумму не входят и на плитку не выводятся.
EXCLUDED_GOAL_CODES = frozenset({
    "ПередачаНаХранениеСПравомПродажи",
    "ПроизводствоУПереработчика",
    "ПроизводствоУПереработчика2_5",
})
EXCLUDED_GOAL_LABELS = frozenset({
    "Передача на хранение с правом продажи",
    "Передача на ответственное хранение",
    "Производство у переработчика",
    "Передача в переработку",
})

# Подписи как в списке «Цель договора» на форме.
GOAL_LABELS = {
    "ЗакупкаУПоставщика": "Закупка",
    "ЗакупкаПоИмпорту": "Закупка (импорт)",
    "ЗакупкаВСтранахЕАЭС": "Закупка (ЕАЭС)",
    "ЗакупкаУПоставщикаТоварыВПути": "Закупка у поставщика (товары в пути)",
    "РеализацияКлиенту": "Реализация",
    "РеализацияЧерезКомиссионера": "Реализация через комиссионера",
    "ПередачаНаКомиссию": "Передача на комиссию",
    "ПриемНаКомиссию": "Прием на комиссию",
    "ПроизводствоУПереработчика": "Передача в переработку",
    "ПроизводствоУПереработчика2_5": "Передача в переработку",
    "ПроизводствоИзДавальческогоСырья": "Прием в переработку",
    "ПроизводствоИзДавальческогоСырья2_5": "Прием в переработку",
    "ПередачаНаХранениеСПравомПродажи": "Передача на ответственное хранение",
    "ПриемНаХранениеСПравомПродажи": "Прием на ответственное хранение",
    "ПоставкаПодПринципала": "Поставка под принципала",
    "": "Не указана",
}


# Уже записанные в кэш подписи до сокращения по форме.
_STORED_GOAL_LABELS = {
    "Реализация клиенту": "Реализация",
    "Закупка по импорту": "Закупка (импорт)",
    "Закупка в странах ЕАЭС": "Закупка (ЕАЭС)",
    "Прием на хранение с правом продажи": "Прием на ответственное хранение",
    "Передача на хранение с правом продажи": "Передача на ответственное хранение",
    "Производство из давальческого сырья": "Прием в переработку",
    "Производство у переработчика": "Передача в переработку",
}


def goal_label(code: str) -> str:
    text = str(code or "")
    if text in GOAL_LABELS:
        return GOAL_LABELS[text]
    if text in _STORED_GOAL_LABELS:
        return _STORED_GOAL_LABELS[text]
    return text or "Не указана"


def goal_excluded(code: str) -> bool:
    text = str(code or "")
    return text in EXCLUDED_GOAL_CODES or goal_label(text) in EXCLUDED_GOAL_LABELS


def goals_rows(by_goal: dict | None, *, metric: str) -> list[dict]:
    """Разбивка для оборота плитки: имя цели, количество и сумма."""
    merged: dict[str, dict] = {}
    for code, bucket in (by_goal or {}).items():
        if goal_excluded(str(code)):
            continue
        name = goal_label(str(code))
        row = merged.setdefault(name, {"name": name, "count": 0, "sum": 0.0})
        row["count"] += int(bucket.get("count") or 0)
        row["sum"] += float(bucket.get("sum") or 0)
    rows = [
        {"name": row["name"], "count": row["count"], "sum": round(row["sum"], 2)}
        for row in merged.values()
    ]
    key = "count" if metric == "count" else "sum"
    rows.sort(key=lambda row: (-row[key], row["name"].casefold()))
    return rows


def cache_path(year: int | None = None, month: int | None = None) -> Path:
    """``getkpi/dashboard/jurist_contracts_<год>_<месяц>.json``."""
    return ytd_json_cache.public_cache_path(CACHE_PREFIX, year, month)


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    """[начало месяца, начало следующего) — период поля «Действует с»."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start, end


def _odata_datetime(value: date) -> str:
    return value.strftime("%Y-%m-%dT00:00:00")


def _parse_start(raw: object) -> date | None:
    text = str(raw or "")[:10]
    if len(text) < 10:
        return None
    try:
        parsed = date(int(text[0:4]), int(text[5:7]), int(text[8:10]))
    except ValueError:
        return None
    if parsed.year < 1900:
        return None
    return parsed


def _status_label(code: str) -> str:
    text = str(code or "").strip()
    return STATUS_LABELS.get(text, text or "—")


def _iso_date(raw: object) -> str:
    parsed = _parse_start(raw)
    return parsed.isoformat() if parsed else ""


def _fetch_org(session: requests.Session, org_guid: str, year: int, month: int) -> dict:
    count = 0
    total = 0.0
    by_status: dict[str, dict] = {}
    by_goal: dict[str, dict] = {}
    rows: list[dict] = []
    start, end = _month_bounds(year, month)
    seen: set[str] = set()
    skip = 0
    while skip <= 200_000:
        filt = (
            f"Организация_Key eq guid'{org_guid}' and DeletionMark eq false"
            f" and ДатаНачалаДействия ge datetime'{_odata_datetime(start)}'"
            f" and ДатаНачалаДействия lt datetime'{_odata_datetime(end)}'"
        )
        url = (
            f"{BASE}/{ENTITY}?$format=json"
            f"&$filter={filt}"
            f"&$select=Ref_Key,Description,Номер,Статус,Сумма,ХозяйственнаяОперация,"
            f"ДатаНачалаДействия,ДатаОкончанияДействия,Контрагент_Key"
            f"&$top={PAGE}&$skip={skip}"
        )
        response = request_with_retry(
            session, url, timeout=180, retries=3, label="jurist/contracts",
        )
        if response is None or not response.ok:
            status = getattr(response, "status_code", None)
            raise RuntimeError(f"OData {ENTITY} org={org_guid} skip={skip} status={status}")
        chunk = response.json().get("value") or []
        fresh = 0
        for row in chunk:
            ref = str(row.get("Ref_Key") or "")
            if not ref or ref in seen:
                continue
            seen.add(ref)
            fresh += 1
            started = _parse_start(row.get("ДатаНачалаДействия"))
            if started is None or not (start <= started < end):
                continue
            goal_code = str(row.get("ХозяйственнаяОперация") or "")
            if goal_excluded(goal_code):
                continue
            status_name = str(row.get("Статус") or "—")
            amount = float(row.get("Сумма") or 0)
            bucket = by_status.setdefault(status_name, {"count": 0, "sum": 0.0})
            bucket["count"] += 1
            bucket["sum"] += amount
            goal_bucket = by_goal.setdefault(goal_code, {"count": 0, "sum": 0.0})
            goal_bucket["count"] += 1
            goal_bucket["sum"] += amount
            count += 1
            total += amount
            counterparty_key = str(row.get("Контрагент_Key") or "")
            rows.append({
                "number": str(row.get("Номер") or "").strip(),
                "name": str(row.get("Description") or "").strip(),
                "goal": goal_label(goal_code),
                "status": _status_label(status_name),
                "sum": round(amount, 2),
                "start_date": _iso_date(row.get("ДатаНачалаДействия")),
                "end_date": _iso_date(row.get("ДатаОкончанияДействия")),
                "counterparty_key": "" if counterparty_key == EMPTY_GUID else counterparty_key,
            })
        if fresh == 0 or len(chunk) < PAGE:
            break
        skip += PAGE
    return {
        "count": count,
        "sum": round(total, 2),
        "by_status": {
            name: {"count": bucket["count"], "sum": round(bucket["sum"], 2)}
            for name, bucket in sorted(by_status.items())
        },
        "by_goal": {
            code: {"count": bucket["count"], "sum": round(bucket["sum"], 2)}
            for code, bucket in by_goal.items()
        },
        "rows": rows,
    }


def _counterparty_names(session: requests.Session, keys: set[str]) -> dict[str, str]:
    """Имена контрагентов пачками, чтобы не раздувать запрос договоров expand-ом."""
    names: dict[str, str] = {}
    pending = [key for key in keys if key and key != EMPTY_GUID]
    for offset in range(0, len(pending), 20):
        part = pending[offset:offset + 20]
        filt = " or ".join(f"Ref_Key eq guid'{key}'" for key in part)
        url = (
            f"{BASE}/Catalog_Контрагенты?$format=json"
            f"&$filter={filt}&$select=Ref_Key,Description&$top={len(part)}"
        )
        response = request_with_retry(
            session, url, timeout=120, retries=3, label="jurist/counterparties",
        )
        if response is None or not response.ok:
            logger.warning("jurist: не удалось прочитать имена контрагентов, skip=%s", offset)
            continue
        for item in response.json().get("value") or []:
            ref = str(item.get("Ref_Key") or "")
            if ref:
                names[ref] = str(item.get("Description") or "").strip()
    return names


def _fetch_companies(year: int, month: int) -> dict[str, dict]:
    session = requests.Session()
    session.auth = AUTH
    companies: dict[str, dict] = {}
    for company in COMPANIES:
        totals = _fetch_org(session, company["org_guid"], year, month)
        totals["title"] = company["title"]
        totals["odata_name"] = company["odata_name"]
        totals["org_guid"] = company["org_guid"]
        companies[company["code"]] = totals
        logger.info(
            "jurist contracts %s %d-%02d: count=%s sum=%s",
            company["code"], year, month, totals["count"], totals["sum"],
        )
    keys: set[str] = set()
    for totals in companies.values():
        for row in totals.get("rows") or []:
            key = str(row.get("counterparty_key") or "")
            if key:
                keys.add(key)
    names = _counterparty_names(session, keys) if keys else {}
    for totals in companies.values():
        for row in totals.get("rows") or []:
            key = str(row.pop("counterparty_key", "") or "")
            row["counterparty"] = names.get(key) or "—"
    return companies


def contract_table_rows(
    *,
    year: int | None = None,
    month: int | None = None,
) -> list[dict]:
    """Строки таблицы договоров за месяц: те же отборы, что и на плитках."""
    companies = load_company_totals(year=year, month=month)
    rows: list[dict] = []
    for code, totals in companies.items():
        if not isinstance(totals, dict):
            continue
        title = str(totals.get("title") or code)
        for row in totals.get("rows") or []:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item.pop("counterparty_key", None)
            item["goal"] = goal_label(str(item.get("goal") or ""))
            item["company_code"] = code
            item["company"] = title
            rows.append(item)
    rows.sort(key=lambda row: (str(row.get("start_date") or ""), str(row.get("number") or "")), reverse=True)
    return rows


def load_company_totals(
    *,
    year: int | None = None,
    month: int | None = None,
    refresh: bool = False,
) -> dict[str, dict]:
    """Итоги по компаниям за опорный месяц.

    Файл ``jurist_contracts_<год>_<месяц>.json``: прошлый месяц без срока
    годности, текущий — до смены календарного дня. Устаревший файл отдаётся
    сразу, пересчёт идёт в фоне.
    """
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    path = ytd_json_cache.cache_path(CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _store(companies: dict[str, dict]) -> dict:
        payload = {"companies": companies}
        ytd_json_cache.save_payload(
            path,
            payload,
            source_tag=CACHE_SOURCE,
            version=CACHE_VERSION,
        )
        return payload

    if refresh:
        return _store(_fetch_companies(ref_y, ref_m))["companies"]

    def _compute() -> dict:
        cached = ytd_json_cache.load_payload(
            path,
            source_tag=CACHE_SOURCE,
            version=CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        return _store(_fetch_companies(ref_y, ref_m))

    payload = ytd_json_cache.resolve_payload(
        path,
        source_tag=CACHE_SOURCE,
        version=CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"jurist_contracts_{ref_y}_{ref_m:02d}",
        compute_fn=_compute,
    )
    if not isinstance(payload, dict):
        return {}
    companies = payload.get("companies")
    return companies if isinstance(companies, dict) else {}
