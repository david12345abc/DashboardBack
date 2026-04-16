"""
calc_tkp_sla.py — ТКП в SLA (KD-M10).

Алгоритм (повторяет логику 1С-запроса):
  1. ВТ_ОпросныеЛистыОтработанные
     ОЛ текущего периода, у которых:
     – статус «Аннулирован» → есть запись МониторингЭтапов с ТочкаЭтапа =
       АннулированиеОЛ и ДатаЗавершенияФакт ≠ пусто;
     – иной статус → есть запись с ТочкаЭтапа = ФормированиеТКП
       и ДатаЗавершенияФакт ≠ пусто.
     Жизненный цикл = рабочие дни от даты создания ОЛ до ДатаЗавершенияФакт.

  2. Итог:
     План = строки с Отработан=Истина И ТекущийПериод=Истина
     Факт = из них строки с ЖизненныйЦиклОЛ ≤ 3

API:
  from getkpi.calc_tkp_sla import get_tkp_sla_monthly
  data = get_tkp_sla_monthly()                        # агрегат, last_full_month
  data = get_tkp_sla_monthly(2026, 3)                 # агрегат, январь-март
  data = get_tkp_sla_monthly(2026, 3, dept_guid='…')  # один отдел

CLI:
  python calc_tkp_sla.py [ГГГГ-ММ]
"""
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"
EMPTY_DATE = "0001-01-01T00:00:00"

ТОЧКА_ТКП   = "314a18fa-de55-11e8-8283-ac1f6b05524d"
ТОЧКА_АННУЛ = "284f6e13-01de-11e9-8286-ac1f6b05524d"

LIMIT_BOTTOM = "2021-11-01T00:00:00"
CAL_KEY = "d658bace-6313-11e7-812d-001e67112509"  # Пятидневка

DOC_TYPES = [
    "Document_ТД_КартаЗаказаUFG",
    "Document_ТД_КартаЗаказаCFM",
    "Document_ТД_КартаЗаказаUFGH",
    "Document_ТД_КартаЗаказаTFG",
    "Document_ТД_КартаЗаказаUFL",
    "Document_ТД_КартаЗаказаПлотномер",
]
DOC_TYPE_SET = {f"StandardODATA.{dt}" for dt in DOC_TYPES}

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


# ══════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ══════════════════════════════════════════════════════════════

def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"tkp_sla_{year}_{ref_month:02d}.json"


def _load_cache(year: int, ref_month: int) -> dict | None:
    p = _cache_path(year, ref_month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_date") == date.today().isoformat():
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, ref_month: int, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, ref_month), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _load_paginated(session, base_url, page_size=5000, timeout=120):
    all_items = []
    skip = 0
    while True:
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}$top={page_size}&$skip={skip}&$format=json"
        try:
            r = session.get(url, timeout=timeout)
        except Exception as e:
            logger.error("TKP_SLA HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("TKP_SLA HTTP %d: %s", r.status_code, r.text[:200])
            break
        items = r.json().get("value", [])
        all_items.extend(items)
        if len(items) < page_size:
            break
        skip += page_size
    return all_items


_cal_cumulative: dict[date, int] = {}


def _load_calendar(session, years: list[int]) -> None:
    global _cal_cumulative
    for yr in years:
        flt = (
            f"Календарь_Key eq guid'{CAL_KEY}'"
            f" and Год eq {yr}"
        )
        url = (
            f"{BASE}/{quote('InformationRegister_КалендарныеГрафики')}"
            f"?$format=json&$top=400"
            f"&$filter={quote(flt, safe='')}"
            f"&$select=ДатаГрафика,КоличествоДнейВГрафикеСНачалаГода"
        )
        try:
            r = session.get(url, timeout=60)
        except Exception as e:
            logger.error("TKP_SLA calendar error: %s", e)
            continue
        if r.ok:
            for item in r.json().get("value", []):
                d = date.fromisoformat(item["ДатаГрафика"][:10])
                _cal_cumulative[d] = item["КоличествоДнейВГрафикеСНачалаГода"]


def _cal_bdays(d: date) -> int:
    if d in _cal_cumulative:
        return _cal_cumulative[d]
    closest = max((k for k in _cal_cumulative if k <= d), default=None)
    if closest:
        return _cal_cumulative[closest]
    return 0


def _business_days_cal(d1: date, d2: date) -> int:
    if d1 >= d2:
        return 0
    return _cal_bdays(d2) - _cal_bdays(d1) + 247 * (d2.year - d1.year)


def _parse_dt(s: str) -> date:
    return datetime.fromisoformat(s.replace("Z", "")).date()


def _is_empty_date(s: str) -> bool:
    return s is None or s == "" or s.startswith("0001-01-01")


# ══════════════════════════════════════════════════════════════
#  Резолвинг менеджер → подразделение
# ══════════════════════════════════════════════════════════════

_manager_dept_cache: dict[str, str] = {}
_detected_mgr_field: str | None = None

MANAGER_FIELD_CANDIDATES = ["Менеджер_Key", "Ответственный_Key"]


def _detect_manager_field(session: requests.Session, flt: str) -> str | None:
    """Определяем, какое поле менеджера есть в КартаЗаказа документах."""
    global _detected_mgr_field
    if _detected_mgr_field is not None:
        return _detected_mgr_field

    for field in MANAGER_FIELD_CANDIDATES:
        url = (
            f"{BASE}/{quote(DOC_TYPES[0])}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select=Ref_Key,{field}"
            f"&$top=1&$format=json"
        )
        try:
            r = session.get(url, timeout=15)
            if r.ok:
                _detected_mgr_field = field
                logger.info("calc_tkp_sla: detected manager field = %s", field)
                return field
        except Exception:
            continue

    logger.warning("calc_tkp_sla: no manager field found on КартаЗаказа documents")
    return None


def _resolve_manager_depts(session: requests.Session,
                           manager_keys: set[str]) -> dict[str, str]:
    """Возвращает {Менеджер_Key: Подразделение_Key} через Catalog_Пользователи."""
    result: dict[str, str] = {}
    to_fetch = manager_keys - set(_manager_dept_cache)

    for mk in to_fetch:
        url = (
            f"{BASE}/{quote('Catalog_Пользователи')}"
            f"(guid'{mk}')?$format=json&$select=Подразделение_Key"
        )
        try:
            r = session.get(url, timeout=15)
        except Exception:
            continue
        if r.ok:
            dept = r.json().get("Подразделение_Key", EMPTY)
            _manager_dept_cache[mk] = dept

    for mk in manager_keys:
        result[mk] = _manager_dept_cache.get(mk, EMPTY)
    return result


# ══════════════════════════════════════════════════════════════
#  Расчёт за один месяц
# ══════════════════════════════════════════════════════════════

def _compute_month(session: requests.Session,
                   year: int, month: int,
                   mgr_to_dept: dict[str, str],
                   mgr_field: str | None) -> dict:
    """
    Полный расчёт SLA для одного месяца.
    Возвращает: {year, month, plan, fact, pct, plan_by_dept, fact_by_dept}
    """
    if month == 12:
        p_start_dt = date(year, month, 1)
        p_end_dt = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        p_start_dt = date(year, month, 1)
        p_end_dt = date(year, month + 1, 1) - timedelta(days=1)

    P_START = f"{p_start_dt.isoformat()}T00:00:00"
    P_END = f"{p_end_dt.isoformat()}T23:59:59"

    # ── Версии объектов: ОЛ текущего периода ──
    flt_ver = (
        f"НомерВерсии eq 1"
        f" and ДатаВерсии ge datetime'{P_START}'"
        f" and ДатаВерсии le datetime'{P_END}'"
    )
    url_ver = (
        f"{BASE}/{quote('InformationRegister_ВерсииОбъектов')}"
        f"?$filter={quote(flt_ver, safe='')}"
        f"&$orderby={quote('ДатаВерсии', safe='')}"
        f"&$select={quote('Объект,Объект_Type,ДатаВерсии', safe='')}"
    )
    ver_recs = _load_paginated(session, url_ver)

    version_dates: dict[str, date] = {}
    for vr in ver_recs:
        if vr.get("Объект_Type", "") in DOC_TYPE_SET:
            version_dates[vr["Объект"]] = _parse_dt(vr["ДатаВерсии"])

    # ── Реквизиты ОЛ ──
    ol_map: dict[str, dict] = {}
    all_ver_keys = set(version_dates.keys())
    new_mgr_keys: set[str] = set()
    flt_wide = f"Date ge datetime'{LIMIT_BOTTOM}' and Date le datetime'{P_END}'"

    select_fields = "Ref_Key,Date,Number,Статус"
    if mgr_field:
        select_fields += f",{mgr_field}"

    for doc_type in DOC_TYPES:
        url_ol = (
            f"{BASE}/{quote(doc_type)}"
            f"?$filter={quote(flt_wide, safe='')}"
            f"&$select={select_fields}"
        )
        ol_list = _load_paginated(session, url_ol)
        for ol in ol_list:
            key = ol["Ref_Key"]
            if key in all_ver_keys:
                mgr_key = EMPTY
                if mgr_field:
                    mgr_key = ol.get(mgr_field, EMPTY) or EMPTY
                ol_map[key] = {
                    "date": version_dates[key],
                    "status": ol.get("Статус", ""),
                    "mgr_key": mgr_key,
                }
                if mgr_key != EMPTY:
                    new_mgr_keys.add(mgr_key)

    logger.info("  OL loaded: %d, with manager: %d",
                len(ol_map), len(new_mgr_keys))

    # Дорезолвим новых менеджеров
    unknown = new_mgr_keys - set(mgr_to_dept)
    if unknown:
        fresh = _resolve_manager_depts(session, unknown)
        mgr_to_dept.update(fresh)
        logger.info("  Resolved %d new managers -> departments", len(unknown))

    # ── Этапы ФормированиеТКП + АннулированиеОЛ ──
    flt_tkp = (
        f"ТочкаЭтапа_Key eq guid'{ТОЧКА_ТКП}'"
        f" and ДатаЗавершенияФакт ne datetime'{EMPTY_DATE}'"
    )
    url_tkp = (
        f"{BASE}/{quote('AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType')}"
        f"?$filter={quote(flt_tkp, safe='')}"
        f"&$select=Recorder,ДатаЗавершенияФакт"
    )
    recs_tkp = _load_paginated(session, url_tkp)
    tkp_completions: dict[str, set[str]] = defaultdict(set)
    for r in recs_tkp:
        tkp_completions[r["Recorder"]].add(r["ДатаЗавершенияФакт"])

    flt_ann = (
        f"ТочкаЭтапа_Key eq guid'{ТОЧКА_АННУЛ}'"
        f" and ДатаЗавершенияФакт ne datetime'{EMPTY_DATE}'"
    )
    url_ann = (
        f"{BASE}/{quote('AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType')}"
        f"?$filter={quote(flt_ann, safe='')}"
        f"&$select=Recorder,ДатаЗавершенияФакт"
    )
    recs_ann = _load_paginated(session, url_ann)
    ann_completions: dict[str, set[str]] = defaultdict(set)
    for r in recs_ann:
        ann_completions[r["Recorder"]].add(r["ДатаЗавершенияФакт"])

    # ── Классификация: отработанные / неотработанные ──
    completed_rows: list[dict] = []

    for ref_key, info in ol_map.items():
        creation = info["date"]
        status = info["status"]
        mgr_key = info["mgr_key"]
        dept_key = mgr_to_dept.get(mgr_key, EMPTY)

        if status == "Аннулирован":
            fact_dates = ann_completions.get(ref_key, set())
        else:
            fact_dates = tkp_completions.get(ref_key, set())

        if fact_dates:
            seen_lc: set[int] = set()
            for fd in fact_dates:
                if _is_empty_date(fd):
                    continue
                end_dt = _parse_dt(fd)
                lc = _business_days_cal(creation, end_dt)
                if lc not in seen_lc:
                    seen_lc.add(lc)
                    completed_rows.append({
                        "ol": ref_key,
                        "lifecycle": lc,
                        "dept_key": dept_key,
                    })

    # ── Агрегация: общий + по подразделениям ──
    plan_total = len(completed_rows)
    fact_total = sum(1 for r in completed_rows if r["lifecycle"] <= 3)

    plan_by_dept: dict[str, int] = defaultdict(int)
    fact_by_dept: dict[str, int] = defaultdict(int)
    for r in completed_rows:
        dk = r["dept_key"]
        if dk != EMPTY:
            plan_by_dept[dk] += 1
            if r["lifecycle"] <= 3:
                fact_by_dept[dk] += 1

    pct_total = round(fact_total / plan_total * 100, 2) if plan_total > 0 else 0.0

    return {
        "year": year,
        "month": month,
        "plan": plan_total,
        "fact": fact_total,
        "pct": pct_total,
        "plan_by_dept": dict(plan_by_dept),
        "fact_by_dept": dict(fact_by_dept),
    }


# ══════════════════════════════════════════════════════════════
#  Слайс по подразделению
# ══════════════════════════════════════════════════════════════

def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Возвращает payload: при dept_guid=None — агрегат, иначе — срез одного отдела."""
    sliced_months = []
    for row in payload.get("months", []):
        if dept_guid is None:
            plan = row["plan"]
            fact = row["fact"]
        else:
            plan = row.get("plan_by_dept", {}).get(dept_guid, 0)
            fact = row.get("fact_by_dept", {}).get(dept_guid, 0)
        pct = round(fact / plan * 100, 2) if plan > 0 else 0.0
        sliced_months.append({
            "year": row["year"],
            "month": row["month"],
            "plan": plan,
            "fact": fact,
            "pct": pct,
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced_months,
    }


# ══════════════════════════════════════════════════════════════
#  Публичный API
# ══════════════════════════════════════════════════════════════

def get_tkp_sla_monthly(year: int | None = None,
                        month: int | None = None,
                        dept_guid: str | None = None) -> dict:
    """
    Помесячные данные ТКП в SLA (январь..ref_month).

    dept_guid=None  — сумма по всем документам (коммерческий директор).
    dept_guid='…'   — только документы указанного подразделения.

    Возвращает:
      {"year": …, "ref_month": …, "months": [
          {"year": …, "month": …, "plan": …, "fact": …, "pct": …}, …
      ]}
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    cached = _load_cache(ref_y, ref_m)
    if cached is not None:
        logger.info("calc_tkp_sla: cache hit for %d-%02d", ref_y, ref_m)
        return _slice_payload(cached, dept_guid)

    session = requests.Session()
    session.auth = AUTH

    logger.info("calc_tkp_sla: loading for %d months 1-%d", ref_y, ref_m)
    t0 = time.time()

    cal_years = sorted(set(range(2021, ref_y + 2)))
    _load_calendar(session, cal_years)
    logger.info("calc_tkp_sla: calendar loaded (%d days)", len(_cal_cumulative))

    flt_probe = f"Date ge datetime'{LIMIT_BOTTOM}'"
    mgr_field = _detect_manager_field(session, flt_probe)
    logger.info("calc_tkp_sla: manager field = %s", mgr_field)

    mgr_to_dept: dict[str, str] = dict(_manager_dept_cache)

    out_months: list[dict] = []
    for m in range(1, ref_m + 1):
        row = _compute_month(session, ref_y, m, mgr_to_dept, mgr_field)
        out_months.append(row)
        logger.info("  %s %d: plan=%d fact=%d pct=%.1f%%",
                     MONTH_RU[m], ref_y, row["plan"], row["fact"], row["pct"])

    logger.info("calc_tkp_sla: done in %.1fs", time.time() - t0)

    payload = {
        "cache_date": today.isoformat(),
        "year": ref_y,
        "ref_month": ref_m,
        "months": out_months,
    }
    _save_cache(ref_y, ref_m, payload)
    return _slice_payload(payload, dept_guid)


# ══════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import functools

    sys.stdout.reconfigure(encoding="utf-8")
    _print = functools.partial(print, flush=True)

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    _print(f"\n{'═' * 55}")
    _print(f"  ТКП в SLA — январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 55}")

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    t0 = time.time()
    data = get_tkp_sla_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'План':>8s} {'Факт':>8s} {'SLA %':>8s}")
    _print(f"  {'─' * 38}")
    for row in data.get("months", []):
        _print(f"  {MONTH_RU[row['month']]:<12s} {row['plan']:>8d} {row['fact']:>8d} {row['pct']:>7.1f}%")

    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 55}")
