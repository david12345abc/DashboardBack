"""Количество и сумма договоров контрагентов по организациям за месяц.

Источник — SQL erp_pm, справочник «Договоры с контрагентами»:

  dbo._Reference155X1
    _Marked          пометка удаления
    _Description     наименование
    _Fld5090         дата окончания действия (+2000 лет)
    _Fld5091RRef     организация
    _Fld5092RRef     контрагент → _Reference233
    _Fld5097         дата «от» рядом с номером (+2000 лет)
    _Fld5098         номер
    _Fld5104RRef     статус → _Enum1629
    _Fld5105RRef     цель (хозяйственная операция) → _Enum1919
    _Fld5116         сумма

Месяц договора — поле «Дата» (на форме дата «от»).
В расчёт входят договоры без пометки удаления.
Сумма — поле «Сумма», без пересчёта валюты.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from pathlib import Path

from devdir import ytd_json_cache
from devdir.rd_monthly_period import normalize_rd_tile_period
from jurist.business_operations import OPERATIONS
from jurist.companies import COMPANIES
from sql_connection import SqlConnection

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
CACHE_PREFIX = "jurist_contracts"
CACHE_SOURCE = "jurist_contracts_v6"
CACHE_VERSION = 6

STATUS_BY_ORDER = {
    0: "НеСогласован",
    1: "Действует",
    2: "Закрыт",
}
STATUS_LABELS = {
    "Действует": "Действует",
    "Закрыт": "Закрыт",
    "НеСогласован": "Не согласован",
}

# Эти цели договора в количество и сумму не входят и на плитку не выводятся.
EXCLUDED_GOAL_CODES = frozenset({
    "ПередачаНаХранениеСПравомПродажи",
    "ПроизводствоУПереработчика",
    "ПроизводствоУПереработчика2_5",
})
EXCLUDED_GOAL_ORDERS = frozenset(
    OPERATIONS.index(code) for code in EXCLUDED_GOAL_CODES
)
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


SQL = """
SELECT
    d._Fld5091RRef,
    d._Description,
    d._Fld5098,
    d._Fld5097,
    d._Fld5090,
    d._Fld5116,
    st._EnumOrder,
    gl._EnumOrder,
    ct._Description
FROM dbo._Reference155X1 AS d WITH (NOLOCK)
LEFT JOIN dbo._Enum1629 AS st WITH (NOLOCK)
    ON st._IDRRef = d._Fld5104RRef
LEFT JOIN dbo._Enum1919 AS gl WITH (NOLOCK)
    ON gl._IDRRef = d._Fld5105RRef
LEFT JOIN dbo._Reference233 AS ct WITH (NOLOCK)
    ON ct._IDRRef = d._Fld5092RRef
WHERE d._Marked = 0x00
  AND d._Fld5097 >= ?
  AND d._Fld5097 < ?
  AND d._Fld5091RRef IN (?, ?, ?, ?)
"""


def _guid_to_1c(guid_str: str) -> bytes:
    raw = uuid.UUID(guid_str).bytes
    return raw[8:16] + raw[6:8] + raw[4:6] + raw[0:4]


def _sql_day(value) -> date | None:
    if not isinstance(value, datetime) or value.year < 3000:
        return None
    return value.replace(year=value.year - YEAR_OFFSET).date()


def _month_sql_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year + YEAR_OFFSET, month, 1)
    if month == 12:
        end = datetime(year + YEAR_OFFSET + 1, 1, 1)
    else:
        end = datetime(year + YEAR_OFFSET, month + 1, 1)
    return start, end


def _status_code(order) -> str:
    if order is None:
        return ""
    return STATUS_BY_ORDER.get(int(order), "")


def _goal_code(order) -> str:
    if order is None:
        return ""
    index = int(order)
    if index < 0 or index >= len(OPERATIONS):
        return ""
    return OPERATIONS[index]


def _status_label(code: str) -> str:
    text = str(code or "").strip()
    return STATUS_LABELS.get(text, text or "—")


def _empty_company(company: dict) -> dict:
    return {
        "count": 0,
        "sum": 0.0,
        "by_status": {},
        "by_goal": {},
        "rows": [],
        "title": company["title"],
        "odata_name": company["odata_name"],
        "org_guid": company["org_guid"],
    }


def _fetch_companies(year: int, month: int) -> dict[str, dict]:
    start, end = _month_sql_bounds(year, month)
    org_bins = [_guid_to_1c(company["org_guid"]) for company in COMPANIES]
    by_org = {org_bins[index]: COMPANIES[index]["code"] for index in range(len(COMPANIES))}
    companies = {company["code"]: _empty_company(company) for company in COMPANIES}

    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute(SQL, start, end, *org_bins)
        fetched = cur.fetchall()

    for (
        org_ref,
        description,
        number,
        started_raw,
        ended_raw,
        amount_raw,
        status_order,
        goal_order,
        counterparty,
    ) in fetched:
        code = by_org.get(bytes(org_ref) if org_ref is not None else b"")
        if not code:
            continue
        started = _sql_day(started_raw)
        if started is None or started.year != year or started.month != month:
            continue
        goal_code = _goal_code(goal_order)
        if goal_excluded(goal_code):
            continue
        status_name = _status_code(status_order) or "—"
        amount = round(float(amount_raw or 0), 2)
        totals = companies[code]
        status_bucket = totals["by_status"].setdefault(status_name, {"count": 0, "sum": 0.0})
        status_bucket["count"] += 1
        status_bucket["sum"] += amount
        goal_bucket = totals["by_goal"].setdefault(goal_code, {"count": 0, "sum": 0.0})
        goal_bucket["count"] += 1
        goal_bucket["sum"] += amount
        totals["count"] += 1
        totals["sum"] += amount
        ended = _sql_day(ended_raw)
        totals["rows"].append({
            "number": str(number or "").strip(),
            "name": str(description or "").strip(),
            "goal": goal_label(goal_code),
            "status": _status_label(status_name),
            "sum": amount,
            "start_date": started.isoformat(),
            "end_date": ended.isoformat() if ended else "",
            "counterparty": str(counterparty or "").strip() or "—",
        })

    for code, totals in companies.items():
        totals["sum"] = round(totals["sum"], 2)
        totals["by_status"] = {
            name: {"count": bucket["count"], "sum": round(bucket["sum"], 2)}
            for name, bucket in sorted(totals["by_status"].items())
        }
        totals["by_goal"] = {
            name: {"count": bucket["count"], "sum": round(bucket["sum"], 2)}
            for name, bucket in totals["by_goal"].items()
        }
        logger.info(
            "jurist contracts %s %d-%02d: count=%s sum=%s",
            code, year, month, totals["count"], totals["sum"],
        )
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
