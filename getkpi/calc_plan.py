"""
calc_plan.py — Плановые показатели: Договоры, Деньги, Отгрузки (помесячно).

Источники:
  - маркетинговый план: AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType;
  - ожидаемые деньги/отгрузки: Document_ТД_ПланированиеПроцессаПродажЕжемесячное;
  - ожидаемые договоры: Document_КоммерческоеПредложениеКлиенту по плановой дате подписания.

Логика (1С-запрос):
  - Период МЕЖДУ НачалоПериода(месяц) И КонецПериода(месяц)
  - Подразделение <> ПустаяСсылка
  - Подразделение В (&ПодразделениеСписок)
  - СУММА по ВидПланирования:
    Отгрузки → МП_СуммаОтгрузки
    Деньги   → МП_ОжидаемаяСуммаОплаты
    Договоры → МП_СуммаДоговораПлан

API:
  from getkpi.calc_plan import get_plans_monthly
  data = get_plans_monthly(2026, 3)
  data = get_plans_monthly(2026, 3, dept_guid='49480c10-...')
"""
from __future__ import annotations

import calendar
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

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

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

REG = "AccumulationRegister_ТД_ПланированиеДоговоровОтгрузокДС_RecordType"
EXPECTED_DOC = "Document_ТД_ПланированиеПроцессаПродажЕжемесячное"
EXPECTED_CONTRACT_DOC = "Document_КоммерческоеПредложениеКлиенту"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_VERSION = 3
PLAN_KEYS = ("dengi", "otgruzki", "dogovory")
EXPECTED_KEYS = {
    "dengi": "dengi_expected",
    "otgruzki": "otgruzki_expected",
    "dogovory": "dogovory_expected",
}
EXPECTED_TABLES = {
    "dengi": "Деньги",
    "otgruzki": "Отгрузки",
    "dogovory": "Договоры",
}


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"plans_{year}_{month:02d}.json"


def _monthly_cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"plans_monthly_{year}_{ref_month:02d}.json"


def _load_cache(year: int, month: int) -> dict | None:
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
            and all(k in data for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()))
        ):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, month: int, totals: dict, by_dept: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, month), "w", encoding="utf-8") as f:
            json.dump({
                "cache_date": date.today().isoformat(),
                "cache_version": CACHE_VERSION,
                **totals,
                "by_dept": by_dept,
            }, f, ensure_ascii=False)
    except OSError:
        pass


def _load_register(session: requests.Session,
                   year: int, ref_month: int) -> list[dict]:
    """Загрузить записи регистра за январь–ref_month."""
    d_from = f"{year}-01-01T00:00:00"
    if ref_month == 12:
        d_to = f"{year + 1}-01-01T00:00:00"
    else:
        d_to = f"{year}-{ref_month + 1:02d}-01T00:00:00"

    sel = quote("Period,Active,Подразделение_Key,ВидПланирования,Сумма", safe=",_")
    flt = quote(
        f"Period ge datetime'{d_from}' and Period lt datetime'{d_to}' and Active eq true",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{REG}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="Plans")
        if r is None:
            logger.error("Plans: request dropped after retries")
            break
        if not r.ok:
            logger.error("Plans register HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _load_expected_documents(session: requests.Session,
                             year: int, ref_month: int) -> list[dict]:
    """Загрузить месячные документы ожиданий по процессу продаж за январь–ref_month."""
    d_from = f"{year}-01-01T00:00:00"
    if ref_month == 12:
        d_to = f"{year + 1}-01-01T00:00:00"
    else:
        d_to = f"{year}-{ref_month + 1:02d}-01T00:00:00"

    sel = quote(
        "Ref_Key,Date,Posted,DeletionMark,ДатаПланирования,Ответственный_Key,Подразделение_Key,Статус,"
        "Деньги,Отгрузки,Договоры",
        safe=",_",
    )
    flt = quote(
        f"ДатаПланирования ge datetime'{d_from}' and ДатаПланирования lt datetime'{d_to}' "
        f"and Posted eq true and DeletionMark eq false",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{EXPECTED_DOC}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="ExpectedPlans")
        if r is None:
            logger.error("Expected plans: request dropped after retries")
            break
        if not r.ok:
            logger.error("Expected plans HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _load_expected_contract_proposals(session: requests.Session,
                                      year: int,
                                      ref_month: int) -> list[dict]:
    """Коммерческие предложения с плановой датой подписания договора."""
    d_from = f"{year}-01-01T00:00:00"
    if ref_month == 12:
        d_to = f"{year + 1}-01-01T00:00:00"
    else:
        d_to = f"{year}-{ref_month + 1:02d}-01T00:00:00"

    sel = quote(
        "Ref_Key,Number,Date,Posted,DeletionMark,ДатаПодписанияПлан,Менеджер_Key,"
        "СуммаДокумента,СуммаБазыКБ,Статус,СогласованоСКлиентом",
        safe=",_",
    )
    flt = quote(
        f"ДатаПодписанияПлан ge datetime'{d_from}' and ДатаПодписанияПлан lt datetime'{d_to}' "
        f"and DeletionMark eq false",
        safe="",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{BASE}/{EXPECTED_CONTRACT_DOC}?$format=json&$top=5000&$skip={skip}"
            f"&$filter={flt}&$select={sel}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="ExpectedContracts")
        if r is None:
            logger.error("Expected contracts: request dropped after retries")
            break
        if not r.ok:
            logger.error("Expected contracts HTTP %d", r.status_code)
            break
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    return rows


def _calc_plans(entries: list[dict],
                ref_month: int) -> dict[int, dict]:
    """
    Агрегировать записи в помесячный план по отделам.
    Возвращает {month: {"dengi": total, "otgruzki": total, "dogovory": total,
                        "by_dept": {guid: {"dengi": ..., "otgruzki": ..., "dogovory": ...}}}}
    """
    result: dict[int, dict] = {}
    for m in range(1, ref_month + 1):
        result[m] = {
            "dengi": 0.0, "otgruzki": 0.0, "dogovory": 0.0,
            "dengi_expected": 0.0, "otgruzki_expected": 0.0, "dogovory_expected": 0.0,
            "by_dept": {
                d: {
                    "dengi": 0.0, "otgruzki": 0.0, "dogovory": 0.0,
                    "dengi_expected": 0.0, "otgruzki_expected": 0.0, "dogovory_expected": 0.0,
                }
                for d in DEPT_SET
            },
        }

    for row in entries:
        period_str = (row.get("Period") or "")[:10]
        if len(period_str) < 7:
            continue
        try:
            m = int(period_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > ref_month:
            continue

        dept = row.get("Подразделение_Key", "")
        if not dept or dept == EMPTY or dept not in DEPT_SET:
            continue

        vid = row.get("ВидПланирования", "")
        sm = float(row.get("Сумма") or 0)

        if vid == "Договоры":
            key = "dogovory"
        elif vid == "Деньги":
            key = "dengi"
        elif vid == "Отгрузки":
            key = "otgruzki"
        else:
            continue

        result[m][key] += sm
        result[m]["by_dept"][dept][key] += sm

    for m in result:
        for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()):
            result[m][k] = round(result[m][k], 2)
        for d in result[m]["by_dept"]:
            for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()):
                result[m]["by_dept"][d][k] = round(result[m]["by_dept"][d][k], 2)

    return result


def _proposal_expected_contract_amount(row: dict) -> float:
    amount = float(row.get("СуммаБазыКБ") or row.get("СуммаДокумента") or 0)
    # В отчёт «ожидаемые к заключению» не попадают мелкие согласованные КП,
    # которые ещё не согласованы клиентом.
    if (
        row.get("Статус") == "Согласовано"
        and not row.get("СогласованоСКлиентом")
        and amount < 150_000
    ):
        return 0.0
    return amount


def _merge_expected_plans(result: dict[int, dict],
                          documents: list[dict],
                          ref_month: int,
                          proposals: list[dict] | None = None) -> None:
    """Добавить суммы из колонок «ожидаемые к ...» в уже собранный план."""
    manager_dept_by_month: dict[tuple[int, str], str] = {}
    for doc in documents:
        date_str = (doc.get("ДатаПланирования") or "")[:10]
        if len(date_str) < 7:
            continue
        try:
            m = int(date_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > ref_month:
            continue

        dept = doc.get("Подразделение_Key", "")
        if not dept or dept == EMPTY or dept not in DEPT_SET:
            continue
        if doc.get("Статус") not in (None, "", "Подготовлен", "Утвержден"):
            continue

        month_row = result[m]
        dept_row = month_row["by_dept"][dept]
        manager = doc.get("Ответственный_Key")
        if manager:
            manager_dept_by_month[(m, manager)] = dept
        for plan_key, table_name in EXPECTED_TABLES.items():
            if plan_key == "dogovory":
                continue
            expected_key = EXPECTED_KEYS[plan_key]
            total = 0.0
            table_rows = doc.get(table_name) or []
            if not isinstance(table_rows, list):
                table_rows = []
            for item in table_rows:
                total += float(item.get("Сумма") or 0)
            month_row[expected_key] += total
            dept_row[expected_key] += total

    for row in proposals or []:
        date_str = (row.get("ДатаПодписанияПлан") or "")[:10]
        if len(date_str) < 7:
            continue
        try:
            m = int(date_str[5:7])
        except (ValueError, IndexError):
            continue
        if m < 1 or m > ref_month:
            continue
        manager = row.get("Менеджер_Key")
        dept = manager_dept_by_month.get((m, manager))
        if not dept:
            continue
        amount = _proposal_expected_contract_amount(row)
        result[m]["dogovory_expected"] += amount
        result[m]["by_dept"][dept]["dogovory_expected"] += amount

    for m in result:
        for k in EXPECTED_KEYS.values():
            result[m][k] = round(result[m][k], 2)
        for d in result[m]["by_dept"]:
            for k in EXPECTED_KEYS.values():
                result[m]["by_dept"][d][k] = round(result[m]["by_dept"][d][k], 2)


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Вернуть полный агрегат или срез по одному подразделению."""
    if dept_guid is None:
        return payload
    sliced_months = []
    for row in payload.get("months", []):
        bd = row.get("by_dept", {}).get(dept_guid, {})
        sliced_months.append({
            "year": row["year"],
            "month": row["month"],
            "dengi": bd.get("dengi", 0),
            "otgruzki": bd.get("otgruzki", 0),
            "dogovory": bd.get("dogovory", 0),
            "dengi_expected": bd.get("dengi_expected", 0),
            "otgruzki_expected": bd.get("otgruzki_expected", 0),
            "dogovory_expected": bd.get("dogovory_expected", 0),
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced_months,
    }


def get_plans_monthly(year: int | None = None,
                      month: int | None = None,
                      dept_guid: str | None = None) -> dict:
    """
    Помесячные планы (январь..ref_month).
    dept_guid=None — агрегат всех отделов.
    dept_guid='...' — план только по указанному подразделению.

    Возвращает:
      {"year": ..., "ref_month": ..., "months": [
          {"year": ..., "month": ..., "dengi": ..., "otgruzki": ..., "dogovory": ...,
           "by_dept": {...}},
      ]}
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
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
                and all(k in first_m for k in (*PLAN_KEYS, *EXPECTED_KEYS.values()))
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
                **{k: cd[k] for k in (*PLAN_KEYS, *EXPECTED_KEYS.values())},
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

    logger.info("calc_plan: loading register for %d months 1-%d", ref_y, ref_m)
    entries = _load_register(session, ref_y, ref_m)
    computed = _calc_plans(entries, ref_m)
    expected_documents = _load_expected_documents(session, ref_y, ref_m)
    expected_contracts = _load_expected_contract_proposals(session, ref_y, ref_m)
    _merge_expected_plans(computed, expected_documents, ref_m, expected_contracts)

    out_months = []
    for m in range(1, ref_m + 1):
        cached = _load_cache(ref_y, m)
        if cached is not None:
            totals = {k: cached[k] for k in (*PLAN_KEYS, *EXPECTED_KEYS.values())}
            by_dept = cached.get("by_dept", {})
        else:
            cm = computed[m]
            totals = {k: cm[k] for k in (*PLAN_KEYS, *EXPECTED_KEYS.values())}
            by_dept = cm["by_dept"]
            _save_cache(ref_y, m, totals, by_dept)
        out_months.append({
            "year": ref_y, "month": m,
            **totals,
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


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    import functools
    _print = functools.partial(print, flush=True)

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    _print(f"\n{'═' * 60}")
    _print(f"  ПЛАН (МП)")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_plans_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'Деньги':>18s} {'Отгрузки':>18s} {'Договоры':>18s}")
    _print(f"  {'─' * 68}")
    for row in data.get("months", []):
        _print(f"  {MONTH_RU[row['month']]:<12s} "
               f"{row['dengi']:>18,.2f} "
               f"{row['otgruzki']:>18,.2f} "
               f"{row['dogovory']:>18,.2f}")
    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
