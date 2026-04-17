"""
Расчёт Валовой Прибыли коммерческого блока из кэшей 1С OData.

Скрипт: calc_vp_fast.py (оптимизированный, ~1с вместо ~10 мин)
Кэш-файлы хранятся в dashboard/.
Итоговый результат кэшируется на день в dashboard/vp_result_cache.json.
"""
import json
import logging
import subprocess
import sys
from datetime import date
from pathlib import Path

from .kpi_periods import last_full_month, vp_months_for_api

logger = logging.getLogger(__name__)

RESULT_CACHE_VERSION = 8

DASHBOARD_DIR = Path(__file__).resolve().parent / 'dashboard'
FAST_SCRIPT = DASHBOARD_DIR / 'calc_vp_fast.py'
RESULT_CACHE = DASHBOARD_DIR / 'vp_result_cache.json'
NASHE_CACHE = DASHBOARD_DIR / 'nashe_keys_cache.json'

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

PRELIM_ORGS = {
    "171272c0-ef41-11e9-829c-ac1f6b05524d",
    "fbca2148-6cfd-11e7-812d-001e67112509",
    "fbca2145-6cfd-11e7-812d-001e67112509",
    "fbca2143-6cfd-11e7-812d-001e67112509",
}

# Помесячный план ВП задан **только на 2026 год** (руб.). Для других лет — единый fallback.
VP_PLAN_TABLE_YEAR = 2026

PLAN_VP_BY_MONTH: dict[int, int] = {
    1: 16_168_819,
    2: 12_936_321,
    3: 28_450_241,
    4: 34_574_209,
    5: 50_311_779,
    6: 58_484_039,
    7: 58_226_950,
    8: 38_126_690,
    9: 51_054_293,
    10: 33_169_450,
    11: 30_538_147,
    12: 61_361_535,
}

PLAN_VP_FALLBACK = PLAN_VP_BY_MONTH[3]


def vp_plan_for_month(month: int, year: int) -> int:
    """План ВП на месяц 1..12; детальная сетка только для VP_PLAN_TABLE_YEAR."""
    if year == VP_PLAN_TABLE_YEAR:
        return PLAN_VP_BY_MONTH.get(month, PLAN_VP_BY_MONTH[3])
    return PLAN_VP_FALLBACK


def _month_is_complete(year: int, month: int, today: date) -> bool:
    """Месяц полностью завершён (не текущий неполный)."""
    ry, rm = last_full_month(today)
    return (year, month) <= (ry, rm)


def _cache_path(month: int, year: int) -> Path:
    return DASHBOARD_DIR / f"вп_{MONTH_NAMES[month]}_{year}_cache.json"


def _cost_path(month: int, year: int) -> Path:
    return DASHBOARD_DIR / f"стоимость_товаров_{MONTH_NAMES[month]}_{year}.json"


def _org_path(month: int, year: int) -> Path:
    return DASHBOARD_DIR / f"аналитика_орг_{MONTH_NAMES[month]}_{year}.json"


def _entries_for_vp(entries: list, dept_guid: str | None) -> list:
    """Строки регистра для ВП.

    Для дочернего отдела — только строки с ``Подразделение_Key`` = GUID отдела.

    Для агрегата (``dept_guid is None``) — **все** строки кэша регистра за месяц (кроме
    исключений в расчётном цикле: комиссия, «Наше предприятие»). Фиксированный список
    COMMERCIAL_BLOCK_VP_GUIDS даёт занижение: в выгрузке за март встречаются десятки
    других подразделений; сумма только по «матричным» GUID совпадает с ~68–69 млн, тогда
    как полная сумма по файлу кэша ближе к управленческому отчёту по регистру.
    """
    if dept_guid:
        return [e for e in entries if e.get("Подразделение_Key") == dept_guid]
    return list(entries)


def _result_cache_path(dept_guid: str | None = None) -> Path:
    if not dept_guid:
        return RESULT_CACHE
    safe = "".join(c for c in dept_guid if c.isalnum())
    return DASHBOARD_DIR / f"vp_result_cache_{safe}.json"


def _load_nashe_keys() -> set[str]:
    """Загружает ключи «Наше предприятие» из кэша."""
    if NASHE_CACHE.exists():
        try:
            with open(NASHE_CACHE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return set()


def _run_1c_fast(month: int, year: int) -> bool:
    """Запускает calc_vp_fast.py для указанного месяца."""
    if not FAST_SCRIPT.exists():
        logger.error("Script not found: %s", FAST_SCRIPT)
        return False
    try:
        result = subprocess.run(
            [sys.executable, str(FAST_SCRIPT), str(month), str(year)],
            cwd=str(DASHBOARD_DIR),
            capture_output=True,
            encoding='utf-8',
            errors='replace',
            timeout=120,
        )
        if result.returncode != 0:
            logger.error("calc_vp_fast.py failed (m=%d y=%d): %s",
                         month, year, result.stderr[-500:])
            return False
    except subprocess.TimeoutExpired:
        logger.error("calc_vp_fast.py timed out (m=%d y=%d)", month, year)
        return False
    except Exception as exc:
        logger.error("calc_vp_fast.py error: %s", exc)
        return False
    return True


def _calculate_vp_from_cache(month: int, year: int,
                             dept_guid: str | None = None) -> dict | None:
    """Рассчитывает ВП за один месяц из локальных кэш-файлов."""
    cache_file = _cache_path(month, year)
    if not cache_file.exists():
        return None

    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", [])

    cost_lookup = {}
    cost_file = _cost_path(month, year)
    if cost_file.exists():
        with open(cost_file, "r", encoding="utf-8") as f:
            cost_lookup = json.load(f)

    org_map = {}
    org_file = _org_path(month, year)
    if org_file.exists():
        with open(org_file, "r", encoding="utf-8") as f:
            org_map = json.load(f)

    nashe_keys = _load_nashe_keys()

    filtered = _entries_for_vp(entries, dept_guid)

    total_vp = 0.0
    total_vyruchka = 0.0
    total_sebest = 0.0
    count = 0

    for e in filtered:
        if e.get("ТипЗапасов") == "КомиссионныйТовар":
            continue
        if e.get("АналитикаУчетаПоПартнерам_Key") in nashe_keys:
            continue

        vyruchka = e.get("СуммаВыручки", 0) or 0
        stoimost = e.get("Стоимость", 0) or 0
        dop = e.get("ДопРасходы", 0) or 0
        trud = e.get("Трудозатраты", 0) or 0
        post_post = e.get("ПостатейныеПостоянныеСНДС", 0) or 0
        post_per = e.get("ПостатейныеПеременныеСНДС", 0) or 0
        rash_prod = e.get("РасходыНаПродажуСНДС", 0) or 0
        kol = e.get("Количество", 0) or 0

        analitika_key = e.get("АналитикаУчетаПоПартнерам_Key", "")
        org_key = org_map.get(analitika_key, "")
        is_prelim = org_key in PRELIM_ORGS

        if stoimost == 0 and is_prelim and cost_lookup:
            cost_key = f"{e.get('АналитикаУчетаНоменклатуры_Key')}|{e.get('ВидЗапасов_Key')}"
            unit_costs = cost_lookup.get(cost_key)
            if unit_costs:
                stoimost = round(kol * (unit_costs.get("Стоимость", 0) or 0), 2)
                dop = round(kol * (unit_costs.get("СтоимостьДопРасходы", 0) or 0), 2)
                trud = round(kol * (unit_costs.get("Трудозатраты", 0) or 0), 2)
                post_post = round(kol * (unit_costs.get("ПостатейныеПостоянныеСНДС", 0) or 0), 2)
                post_per = round(kol * (unit_costs.get("ПостатейныеПеременныеСНДС", 0) or 0), 2)

        sebest = stoimost + dop + trud + post_post + post_per + rash_prod
        vp = vyruchka - sebest

        total_vp += vp
        total_vyruchka += vyruchka
        total_sebest += sebest
        count += 1

    return {
        "month": month,
        "year": year,
        "month_name": MONTH_NAMES[month],
        "entries_count": count,
        "vyruchka": round(total_vyruchka, 2),
        "sebestoimost": round(total_sebest, 2),
        "valovaya_pribyl": round(total_vp, 2),
    }


def _ensure_month_data(month: int, year: int,
                       dept_guid: str | None = None) -> dict | None:
    """Гарантирует наличие данных за месяц. Если кэша нет — качает из 1С."""
    result = _calculate_vp_from_cache(month, year, dept_guid)
    if result is not None:
        return result

    logger.info("No cache for %s %d, fetching from 1C...", MONTH_NAMES[month], year)
    if _run_1c_fast(month, year):
        return _calculate_vp_from_cache(month, year, dept_guid)

    return None


def _load_result_cache(dept_guid: str | None = None) -> dict | None:
    """Загружает результат ВП, если он был рассчитан сегодня."""
    path = _result_cache_path(dept_guid)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            cached = json.load(f)
        if (
            cached.get("date") == date.today().isoformat()
            and cached.get("cache_version") == RESULT_CACHE_VERSION
        ):
            return cached["data"]
    except (json.JSONDecodeError, KeyError, OSError):
        pass
    return None


def _save_result_cache(data: dict, dept_guid: str | None = None) -> None:
    """Сохраняет результат ВП с пометкой сегодняшней даты."""
    path = _result_cache_path(dept_guid)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "date": date.today().isoformat(),
                    "cache_version": RESULT_CACHE_VERSION,
                    "data": data,
                },
                f,
                ensure_ascii=False,
            )
    except OSError:
        pass


def get_vp_ytd(dept_guid: str | None = None) -> dict:
    """
    Помесячные ряды ВП для графиков (объединение: январь..последний полный месяц
    в его году + месяцы последнего полного квартала).

    Помесячный план — из PLAN_VP_BY_MONTH **только для года VP_PLAN_TABLE_YEAR (2026)**;
    для других лет план = PLAN_VP_FALLBACK (март 2026). Факт только за полные месяцы.

    months_calendar — всегда 12 строк за **VP_PLAN_TABLE_YEAR**: все планы по сетке 2026;
    факт только за завершённые месяцы этого года, иначе null.

    last_full_month_row — одна строка: план, факт и KPI **последнего полного месяца**
    (для таблицы коммерческого директора).

    Итоговый KPI (ytd) — тот же последний полный месяц.

    dept_guid — фильтр по подразделению 1С; None — сумма по **всем** строкам регистра
    в кэше месяца (агрегат дашборда коммерческого директора).

    Кэшируется на день. Если кэша д1С нет — запускает calc_vp_fast.py.
    """
    cached = _load_result_cache(dept_guid)
    if cached is not None:
        return cached

    today = date.today()
    month_tuples, (ref_y, ref_m), _ = vp_months_for_api(today)

    months_data = []
    ref_row: dict | None = None

    for y, m in month_tuples:
        plan = vp_plan_for_month(m, y)
        complete = _month_is_complete(y, m, today)
        vp = _ensure_month_data(m, y, dept_guid) if complete else None
        fact = vp["valovaya_pribyl"] if vp else None
        pct = round(fact / plan * 100, 1) if fact is not None and plan > 0 else None

        month_entry = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": pct,
            "has_data": vp is not None,
            "month_complete": complete,
        }
        if vp:
            month_entry["vyruchka"] = vp["vyruchka"]
            month_entry["sebestoimost"] = vp["sebestoimost"]

        months_data.append(month_entry)
        if y == ref_y and m == ref_m:
            ref_row = month_entry

    plan_ref = vp_plan_for_month(ref_m, ref_y)
    if ref_row and ref_row.get("fact") is not None:
        total_fact = float(ref_row["fact"])
        plan_one = float(ref_row.get("plan") or plan_ref)
        ytd_pct = round(total_fact / plan_one * 100, 1) if plan_one > 0 else None
        months_with_data = 1
    else:
        total_fact = 0.0
        plan_one = float(plan_ref)
        ytd_pct = None
        months_with_data = 0

    months_calendar = []
    calendar_year = VP_PLAN_TABLE_YEAR
    for m in range(1, 13):
        plan = vp_plan_for_month(m, calendar_year)
        complete = _month_is_complete(calendar_year, m, today)
        vp = _ensure_month_data(m, calendar_year, dept_guid) if complete else None
        fact = vp["valovaya_pribyl"] if vp else None
        pct = round(fact / plan * 100, 1) if fact is not None and plan > 0 else None
        row = {
            "month": m,
            "year": calendar_year,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": pct,
            "has_data": vp is not None,
            "month_complete": complete,
        }
        if vp:
            row["vyruchka"] = vp["vyruchka"]
            row["sebestoimost"] = vp["sebestoimost"]
        months_calendar.append(row)

    last_full_month_row: dict | None = None
    if ref_row:
        last_full_month_row = {
            "month": ref_row["month"],
            "year": ref_row["year"],
            "month_name": ref_row["month_name"],
            "plan": ref_row["plan"],
            "fact": ref_row["fact"],
            "kpi_pct": ref_row["kpi_pct"],
            "has_data": ref_row.get("has_data", False),
        }
        if ref_row.get("vyruchka") is not None:
            last_full_month_row["vyruchka"] = ref_row["vyruchka"]
            last_full_month_row["sebestoimost"] = ref_row["sebestoimost"]

    result = {
        "year": ref_y,
        "calendar_year": calendar_year,
        "plans_apply_to_year": VP_PLAN_TABLE_YEAR,
        "plan_monthly": plan_ref,
        "plans_by_month": {
            str(m): vp_plan_for_month(m, VP_PLAN_TABLE_YEAR) for m in range(1, 13)
        },
        "months": months_data,
        "months_calendar": months_calendar,
        "last_full_month_row": last_full_month_row,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": round(plan_one, 2) if months_with_data else round(plan_ref, 2),
            "total_fact": round(total_fact, 2),
            "kpi_pct": ytd_pct,
            "months_with_data": months_with_data,
            "months_total": 1,
        },
    }

    _save_result_cache(result, dept_guid)
    return result
