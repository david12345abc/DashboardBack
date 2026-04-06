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

logger = logging.getLogger(__name__)

DASHBOARD_DIR = Path(__file__).resolve().parent / 'dashboard'
FAST_SCRIPT = DASHBOARD_DIR / 'calc_vp_fast.py'
RESULT_CACHE = DASHBOARD_DIR / 'vp_result_cache.json'
NASHE_CACHE = DASHBOARD_DIR / 'nashe_keys_cache.json'

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d",
    "34497ef7-810f-11e4-80d6-001e67112509",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "7587c178-92f6-11f0-96f9-6cb31113810e",
    "bd7b5184-9f9c-11e4-80da-001e67112509",
}

PRELIM_ORGS = {
    "171272c0-ef41-11e9-829c-ac1f6b05524d",
    "fbca2148-6cfd-11e7-812d-001e67112509",
    "fbca2145-6cfd-11e7-812d-001e67112509",
    "fbca2143-6cfd-11e7-812d-001e67112509",
}

PLAN_VP_MONTHLY = 28_450_241


def _cache_path(month: int, year: int) -> Path:
    return DASHBOARD_DIR / f"вп_{MONTH_NAMES[month]}_{year}_cache.json"


def _cost_path(month: int, year: int) -> Path:
    return DASHBOARD_DIR / f"стоимость_товаров_{MONTH_NAMES[month]}_{year}.json"


def _org_path(month: int, year: int) -> Path:
    return DASHBOARD_DIR / f"аналитика_орг_{MONTH_NAMES[month]}_{year}.json"


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


def _calculate_vp_from_cache(month: int, year: int) -> dict | None:
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

    filtered = [e for e in entries if e.get("Подразделение_Key") in DEPARTMENTS]

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


def _ensure_month_data(month: int, year: int) -> dict | None:
    """Гарантирует наличие данных за месяц. Если кэша нет — качает из 1С."""
    result = _calculate_vp_from_cache(month, year)
    if result is not None:
        return result

    logger.info("No cache for %s %d, fetching from 1C...", MONTH_NAMES[month], year)
    if _run_1c_fast(month, year):
        return _calculate_vp_from_cache(month, year)

    return None


def _load_result_cache() -> dict | None:
    """Загружает результат ВП, если он был рассчитан сегодня."""
    if not RESULT_CACHE.exists():
        return None
    try:
        with open(RESULT_CACHE, "r", encoding="utf-8") as f:
            cached = json.load(f)
        if cached.get("date") == date.today().isoformat():
            return cached["data"]
    except (json.JSONDecodeError, KeyError, OSError):
        pass
    return None


def _save_result_cache(data: dict) -> None:
    """Сохраняет результат ВП с пометкой сегодняшней даты."""
    try:
        with open(RESULT_CACHE, "w", encoding="utf-8") as f:
            json.dump({"date": date.today().isoformat(), "data": data}, f, ensure_ascii=False)
    except OSError:
        pass


def get_vp_ytd(plan_monthly: int = PLAN_VP_MONTHLY) -> dict:
    """
    Валовая прибыль с начала года по текущий месяц.
    Кэшируется на день. Если кэша 1С нет — запускает calc_vp_fast.py.
    """
    cached = _load_result_cache()
    if cached is not None:
        return cached

    today = date.today()
    year = today.year
    current_month = today.month

    months_data = []
    total_fact = 0.0
    months_with_data = 0

    for m in range(1, current_month + 1):
        vp = _ensure_month_data(m, year)
        plan = plan_monthly
        fact = vp["valovaya_pribyl"] if vp else None
        pct = round(fact / plan * 100, 1) if fact is not None and plan > 0 else None

        month_entry = {
            "month": m,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": fact,
            "kpi_pct": pct,
            "has_data": vp is not None,
        }
        if vp:
            month_entry["vyruchka"] = vp["vyruchka"]
            month_entry["sebestoimost"] = vp["sebestoimost"]

        months_data.append(month_entry)

        if fact is not None:
            total_fact += fact
            months_with_data += 1

    plan_available = plan_monthly * months_with_data
    ytd_pct = round(total_fact / plan_available * 100, 1) if plan_available > 0 else None

    result = {
        "year": year,
        "plan_monthly": plan_monthly,
        "months": months_data,
        "ytd": {
            "total_plan": round(plan_available, 2),
            "total_fact": round(total_fact, 2),
            "kpi_pct": ytd_pct,
            "months_with_data": months_with_data,
            "months_total": current_month,
        },
    }

    _save_result_cache(result)
    return result
