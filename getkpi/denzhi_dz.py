"""
KD-M2 «Деньги и просроченная ДЗ»: факт из calc_fact_fast.py (папка dashboard/факт).

Формула KPI: 0,5 × KPI(Факт ДС / План × 100%) + 0,5 × KPI(Просроченная ДЗ в лимите).
Пока план = факт для ДС и отгрузки; просроченная ДЗ без отдельного источника — KPI части = 100%.
"""
import json
import logging
import subprocess
import sys
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

DASHBOARD_DIR = Path(__file__).resolve().parent / 'dashboard'
FACT_DIR = DASHBOARD_DIR / 'факт'
FACT_SCRIPT = FACT_DIR / 'calc_fact_fast.py'
RESULT_CACHE = FACT_DIR / 'kd_m2_ytd_cache.json'

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _kd_m2_file(month: int, year: int) -> Path:
    return FACT_DIR / f"kd_m2_{MONTH_NAMES[month]}_{year}.json"


def _run_fact_script(month: int, year: int) -> bool:
    if not FACT_SCRIPT.exists():
        logger.error("calc_fact_fast.py not found: %s", FACT_SCRIPT)
        return False
    try:
        r = subprocess.run(
            [sys.executable, str(FACT_SCRIPT), str(month), str(year)],
            cwd=str(FACT_DIR),
            capture_output=True,
            encoding='utf-8',
            errors='replace',
            timeout=600,
        )
        if r.returncode != 0:
            logger.error("calc_fact_fast failed m=%d y=%d: %s", month, year, r.stderr[-800:])
            return False
    except subprocess.TimeoutExpired:
        logger.error("calc_fact_fast timeout m=%d y=%d", month, year)
        return False
    except Exception as exc:
        logger.error("calc_fact_fast error: %s", exc)
        return False
    return True


def _load_month_payload(month: int, year: int) -> dict | None:
    p = _kd_m2_file(month, year)
    if not p.exists():
        return None
    try:
        with open(p, encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _money_from_payload(raw: dict) -> tuple[float, float | None, float]:
    """
    Основная сумма для плитки «Деньги» — money_fact из обновлённого calc_fact_fast
    (ИТОГО ДС по отбору; для марта 2026 ожидается ~198 394 990,40).
    Fallback: ds_fact → shipment_fact.
    """
    sh = float(raw.get('shipment_fact') or 0)
    ds = raw.get('ds_fact')
    ds = float(ds) if ds is not None else None
    if raw.get('money_fact') is not None:
        m = float(raw['money_fact'])
    elif ds is not None:
        m = ds
    else:
        m = sh
    return m, ds, sh


def _ensure_month(month: int, year: int) -> dict | None:
    data = _load_month_payload(month, year)
    if data is not None:
        return data
    logger.info("KD-M2: no cache for %s %d, running calc_fact_fast...", MONTH_NAMES[month], year)
    if _run_fact_script(month, year):
        return _load_month_payload(month, year)
    return None


def _kpi_ds_pct(ds_fact: float | None, plan_ds: float | None) -> float:
    if ds_fact is None or plan_ds is None or plan_ds == 0:
        return 100.0
    return round(ds_fact / plan_ds * 100, 1)


def _kpi_shipment_pct(sh_f: float, pl_sh: float) -> float:
    if pl_sh == 0:
        return 100.0
    return round(sh_f / pl_sh * 100, 1)


def _combined_kd_m2_pct(
    ds_fact: float | None,
    plan_ds: float | None,
    shipment_fact: float,
    plan_shipment: float,
) -> float:
    """0,5 × KPI(ДС / план) + 0,5 × KPI(ДЗ). План=факт → 100%. ДЗ пока 100%."""
    if ds_fact is not None:
        k_money = _kpi_ds_pct(ds_fact, plan_ds)
    else:
        k_money = _kpi_shipment_pct(shipment_fact, plan_shipment)
    k_dz = 100.0
    return round(0.5 * k_money + 0.5 * k_dz, 1)


def _load_ytd_cache() -> dict | None:
    if not RESULT_CACHE.exists():
        return None
    try:
        with open(RESULT_CACHE, encoding='utf-8') as f:
            c = json.load(f)
        if c.get('date') == date.today().isoformat():
            return c.get('data')
    except (json.JSONDecodeError, OSError, KeyError):
        pass
    return None


def _save_ytd_cache(data: dict) -> None:
    try:
        with open(RESULT_CACHE, 'w', encoding='utf-8') as f:
            json.dump({'date': date.today().isoformat(), 'data': data}, f, ensure_ascii=False)
    except OSError:
        pass


def get_kd_m2_ytd() -> dict:
    """Помесячно январь → текущий месяц; кэш суточный."""
    cached = _load_ytd_cache()
    if cached is not None:
        return cached

    today = date.today()
    year = today.year
    cur_m = today.month

    months_out = []
    sum_kpi = 0.0
    n_kpi = 0
    total_plan = 0.0
    total_fact = 0.0

    for m in range(1, cur_m + 1):
        raw = _ensure_month(m, year)
        if raw is None:
            months_out.append({
                'month': m,
                'month_name': MONTH_NAMES[m],
                'plan': None,
                'fact': None,
                'kpi_pct': None,
                'has_data': False,
            })
            continue

        money_f, ds_f, sh_f = _money_from_payload(raw)
        plan_money = money_f
        plan_ship = sh_f
        plan_ds = ds_f
        kpi = _combined_kd_m2_pct(ds_f, plan_ds, sh_f, plan_ship)

        row = {
            'month': m,
            'month_name': MONTH_NAMES[m],
            'plan': round(plan_money, 2),
            'fact': round(money_f, 2),
            'kpi_pct': kpi,
            'has_data': True,
            'money_fact': round(money_f, 2),
            'shipment_fact': round(sh_f, 2),
            'plan_shipment': round(plan_ship, 2),
            'ds_fact': round(ds_f, 2) if ds_f is not None else None,
            'plan_ds': round(plan_ds, 2) if plan_ds is not None else None,
            'kpi_dz_placeholder': 100.0,
        }
        months_out.append(row)
        sum_kpi += kpi
        n_kpi += 1
        total_plan += plan_money
        total_fact += money_f

    ytd_pct = round(sum_kpi / n_kpi, 1) if n_kpi else None

    out = {
        'year': year,
        'months': months_out,
        'ytd': {
            'total_plan': round(total_plan, 2),
            'total_fact': round(total_fact, 2),
            'kpi_pct': ytd_pct,
            'months_with_data': n_kpi,
            'months_total': cur_m,
        },
    }
    _save_ytd_cache(out)
    return out
