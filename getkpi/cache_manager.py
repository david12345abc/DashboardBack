"""
cache_manager.py — Предотвращение параллельных вычислений + прогрев кэшей.

1. locked_call(key, fn, ...) — гарантирует, что для одного ключа
   одновременно выполняется только одно вычисление; остальные потоки
   ждут завершения и затем читают из кэша.

2. warm_all_caches() — при старте сервера проверяет все кэши
   и пересчитывает просроченные / отсутствующие (в фоновом потоке).
"""
from __future__ import annotations

import logging
import os
import json
import threading
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'
MAX_AGE_SECONDS = 86400  # 1 день

_locks: dict[str, threading.Lock] = {}
_meta = threading.Lock()
_warming = False


def _get_lock(key: str) -> threading.Lock:
    with _meta:
        if key not in _locks:
            _locks[key] = threading.Lock()
        return _locks[key]


def is_computing(key: str) -> bool:
    return _get_lock(key).locked()


def is_cache_fresh(path: Path | str) -> bool:
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        return False
    try:
        with p.open('r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            cache_date = data.get('cache_date') or data.get('cached_at')
            if cache_date:
                return str(cache_date)[:10] == date.today().isoformat()
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return (datetime.now().timestamp() - p.stat().st_mtime) < MAX_AGE_SECONDS


def locked_call(key: str, fn, *args, **kwargs):
    """Выполнить fn под блокировкой key.

    Если другой поток уже вычисляет тот же key — текущий поток
    ждёт завершения, после чего вызывает fn (который прочитает свежий кэш).
    """
    with _get_lock(key):
        return fn(*args, **kwargs)


def _build_warm_tasks(ref_y: int, ref_m: int) -> list[tuple[str, Path, object]]:
    """Список (key, cache_path, compute_fn) для всех источников данных."""
    from . import (
        calc_debitorka, calc_dengi_fact, calc_dogovory_fact,
        calc_logistics_price_deviation, calc_logistics_supplier_share, calc_logistics_tmc_on_time,
        calc_dz_limits, calc_fot, calc_komdir_active_dealers, calc_kp_price, calc_ks_razvitie,
        calc_otgruzki_fact, calc_otif_vypusk_zam_proizvodstva, calc_plan,
        calc_prod_deputy_output, calc_prod_deputy_pc, calc_prod_deputy_projects,
        calc_prod_deputy_turnover, calc_rashody,
        calc_reclamations,
        calc_svoevremennaya_otgruzka,
        calc_tekuchest, calc_tkp_sla, valovaya_pribyl,
        techdir_m3, techdir_m4, techdir_projects, techdir_tekuchet,
    )
    from .calc_prod_deputy_pc_common import cache_path as prod_deputy_pc_cache_path
    from .devdir import rd_m1_zpr, rd_m3_budget, rd_m4_fot
    from . import gspp_q4
    from .devdir import rd_m1_zpr, rd_m3_budget, rd_m4_fot, rd_q2_tekuchest
    from .komdir_claims import fetch_claims_for_month
    from qualdir import mpp_tasks_report, qd_m1, qd_m3, qd_m4
    from qualdir.turnover import get_qd_q2_ytd, qd_q2_ytd_cache_path

    y, m = ref_y, ref_m
    cd = CACHE_DIR

    tasks: list[tuple[str, Path, object]] = [
        (f'dengi_{y}_{m}',
         cd / f'dengi_monthly_{y}_{m:02d}.json',
         lambda: calc_dengi_fact.get_dengi_monthly(year=y, month=m)),

        (f'otgruzki_{y}_{m}',
         cd / f'otgruzki_monthly_{y}_{m:02d}.json',
         lambda: calc_otgruzki_fact.get_otgruzki_monthly(year=y, month=m)),

        (f'dogovory_{y}_{m}',
         cd / f'dogovory_monthly_{y}_{m:02d}.json',
         lambda: calc_dogovory_fact.get_dogovory_monthly(year=y, month=m)),

        (f'plans_{y}_{m}',
         cd / f'plans_monthly_{y}_{m:02d}.json',
         lambda: calc_plan.get_plans_monthly(year=y, month=m)),

        (f'debitorka_{y}_{m}',
         cd / f'debitorka_monthly_{y}_{m:02d}.json',
         lambda: calc_debitorka.get_komdir_dz_monthly(year=y, month=m)),

        ('dz_limits',
         cd / 'dz_limits_latest.json',
         calc_dz_limits.get_overdue_limits),

        (f'rashody_{y}_{m}',
         cd / f'rashody_{y}_{m:02d}.json',
         lambda: calc_rashody.get_rashody_monthly(year=y, month=m)),

        (f'fot_{y}_{m}',
         cd / f'fot_{y}_{m:02d}.json',
         lambda: calc_fot.get_fot_monthly(year=y, month=m)),

        (f'kp_price_{y}_{m}',
         cd / f'kp_price_{y}_{m:02d}.json',
         lambda: calc_kp_price.get_kp_price_monthly(year=y, month=m)),

        (f'tkp_sla_{y}_{m}',
         cd / f'tkp_sla_{y}_{m:02d}.json',
         lambda: calc_tkp_sla.get_tkp_sla_monthly(year=y, month=m)),

        (f'tekuchest_{y}_{m}',
         cd / f'tekuchest_{y}_{m:02d}.json',
         lambda: calc_tekuchest.get_tekuchest_monthly(year=y, month=m)),

        (f'svoevremennaya_monthly_{y}_{m}',
         cd / f'svoevremennaya_monthly_{y}_{m:02d}.json',
         lambda: calc_svoevremennaya_otgruzka.get_svoevremennaya_monthly(year=y, month=m)),

        (f'reclamations_monthly_{y}_{m}',
         cd / f'reclamations_monthly_{y}_{m:02d}.json',
         lambda: calc_reclamations.get_reclamations_monthly(year=y, month=m)),

        (f'overdue_detail_{y}_{m}',
         calc_debitorka.overdue_detail_cache_path(y, m),
         lambda: calc_debitorka.get_overdue_detail(year=y, month=m)),

        ('vp',
         cd / 'vp_result_cache.json',
         valovaya_pribyl.get_vp_ytd),

        (f'claims_{y}_{m}',
         cd / f'claims_{y}_{m:02d}.json',
         lambda: fetch_claims_for_month(y, m)),

        ('techdir_projects',
         techdir_projects.CACHE_PATH,
         techdir_projects.get_projects_snapshot),

        ('techdir_m3',
         techdir_m3._cache_path(y, m),
         lambda: techdir_m3.get_td_m3_ytd(year=y, month=m)),

        ('techdir_m4',
         techdir_m4._cache_path(y, m),
         lambda: techdir_m4.get_td_m4_ytd(year=y, month=m)),

        ('techdir_tekuchet',
         techdir_tekuchet._cache_path(y, m),
         lambda: techdir_tekuchet.get_td_q2_ytd(year=y, month=m)),

        ('qualdir_qd_q2_ytd',
         qd_q2_ytd_cache_path(y, m),
         lambda yy=y, mm=m: get_qd_q2_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m3_ytd_{y}_{m}',
         qd_m3.qd_m3_ytd_cache_path(y, m),
         lambda yy=y, mm=m: qd_m3.get_qd_m3_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m4_ytd_{y}_{m}',
         qd_m4.qd_m4_ytd_cache_path(y, m),
         lambda yy=y, mm=m: qd_m4.get_qd_m4_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m1_tile_{y}_{m}',
         qd_m1.qd_m1_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m1.get_qd_m1_ytd(year=yy, month=mm)),

        (f'qualdir_qd_q1_tile_{y}_{m}',
         mpp_tasks_report.qd_q1_tile_cache_path(y, m),
         lambda yy=y, mm=m: mpp_tasks_report.get_qd_q1_ytd(year=yy, month=mm)),

        (f'pd_m2_otif_{y}_{m}',
         cd / f'otif_vypusk_prod_monthly_{y}_{m:02d}.json',
         lambda: calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly(year=y, month=m)),

        (f'pd_m1_output_pc1_{y}_{m}',
         calc_prod_deputy_output.cache_path('pc1', y, m),
         lambda: calc_prod_deputy_output.get_prod_deputy_output_monthly('pc1', year=y, month=m)),

        (f'pd_m1_output_pc2_{y}_{m}',
         calc_prod_deputy_output.cache_path('pc2', y, m),
         lambda: calc_prod_deputy_output.get_prod_deputy_output_monthly('pc2', year=y, month=m)),

        (f'pd_pc_budget_pc1_{y}_{m}',
         prod_deputy_pc_cache_path('budget', 'pc1', y, m),
         lambda: calc_prod_deputy_pc.get_pc_budget_monthly('pc1', y, m)),

        (f'pd_pc_budget_pc2_{y}_{m}',
         prod_deputy_pc_cache_path('budget', 'pc2', y, m),
         lambda: calc_prod_deputy_pc.get_pc_budget_monthly('pc2', y, m)),

        (f'pd_pc_fot_pc1_{y}_{m}',
         prod_deputy_pc_cache_path('fot', 'pc1', y, m),
         lambda: calc_prod_deputy_pc.get_pc_fot_monthly('pc1', y, m)),

        (f'pd_pc_fot_pc2_{y}_{m}',
         prod_deputy_pc_cache_path('fot', 'pc2', y, m),
         lambda: calc_prod_deputy_pc.get_pc_fot_monthly('pc2', y, m)),

        ('prod_deputy_projects',
         calc_prod_deputy_projects.CACHE_PATH,
         lambda: calc_prod_deputy_projects.get_pd_q1_monthly(year=y, month=m)),

        (f'pd_q2_turnover_pc1_{y}_{m}',
         calc_prod_deputy_turnover.cache_path('pc1', y, m),
         lambda: calc_prod_deputy_turnover.get_prod_deputy_turnover_monthly('pc1', year=y, month=m)),

        (f'pd_q2_turnover_pc2_{y}_{m}',
         calc_prod_deputy_turnover.cache_path('pc2', y, m),
         lambda: calc_prod_deputy_turnover.get_prod_deputy_turnover_monthly('pc2', year=y, month=m)),

        (f'log_m1_tmc_on_time_{y}_{m}',
         calc_logistics_tmc_on_time.cache_path(y, m),
         lambda: calc_logistics_tmc_on_time.get_logistics_tmc_on_time_monthly(year=y, month=m)),

        (f'log_m2_price_deviation_{y}_{m}',
         calc_logistics_price_deviation.cache_path(y, m),
         lambda: calc_logistics_price_deviation.get_logistics_price_deviation_monthly(year=y, month=m)),

        (f'log_q1_supplier_share_{y}_{m}',
         calc_logistics_supplier_share.cache_path(y, m),
         lambda: calc_logistics_supplier_share.get_logistics_supplier_share_monthly(year=y, month=m)),

        (f'ks_razvitie_{y}',
         calc_ks_razvitie.cache_path(y),
         lambda yy=y: calc_ks_razvitie.get_ks_razvitie_plans(year=yy)),

        (f'devdir_rd_m1_zpr_{y}_{m}',
         rd_m1_zpr.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: rd_m1_zpr.get_rd_m1_zpr_ytd(year=yy, month=mm)),

        (f'devdir_rd_m3_budget_{y}_{m}',
         rd_m3_budget.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: rd_m3_budget.get_rd_m3_budget_ytd(year=yy, month=mm)),

        (f'devdir_rd_m4_fot_{y}_{m}',
         rd_m4_fot.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: rd_m4_fot.get_rd_m4_fot_ytd(year=yy, month=mm)),

        (f'devdir_rd_q2_tekuchest_{y}_{m}',
         rd_q2_tekuchest.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: rd_q2_tekuchest.get_rd_q2_tekuchest_ytd(year=yy, month=mm)),

        (f'gspp_q4_ytd_{y}_{m}',
         gspp_q4.gspp_q4_ytd_cache_path(y, m),
         lambda yy=y, mm=m: gspp_q4.get_gspp_q4_ytd(year=yy, month=mm)),
    ]
    td_as_of = date.today()
    tasks.append((
        f'active_dealers_{td_as_of.isoformat()}',
        calc_komdir_active_dealers.active_dealers_cache_path(td_as_of),
        lambda d=td_as_of: calc_komdir_active_dealers.compute_active_dealers_report(d),
    ))
    tasks.append((
        f'new_dealers_{td_as_of.isoformat()}',
        calc_komdir_active_dealers.new_dealers_cache_path(td_as_of),
        lambda d=td_as_of: calc_komdir_active_dealers.compute_new_dealers_report(d),
    ))
    return tasks


def warm_all_caches():
    """Проверить все кэши и пересчитать просроченные / отсутствующие."""
    global _warming
    _warming = True
    today = date.today()
    ref_y, ref_m = today.year, today.month

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tasks = _build_warm_tasks(ref_y, ref_m)

    logger.info("cache_manager: warming %d cache tasks for %d-%02d", len(tasks), ref_y, ref_m)

    for key, cache_path, fn in tasks:
        if is_cache_fresh(cache_path):
            logger.info("cache_manager: [%s] fresh, skip", key)
            continue
        try:
            logger.info("cache_manager: [%s] computing...", key)
            locked_call(key, fn)
            logger.info("cache_manager: [%s] done", key)
        except Exception:
            logger.exception("cache_manager: [%s] error", key)

    _warming = False
    logger.info("cache_manager: warming complete")


def start_warming():
    """Запустить прогрев кэшей в фоновом потоке (вызывается из AppConfig.ready)."""
    if os.environ.get('RUN_MAIN') != 'true':
        return
    t = threading.Thread(target=warm_all_caches, name='cache-warmer', daemon=True)
    t.start()
    logger.info("cache_manager: warming thread started")
