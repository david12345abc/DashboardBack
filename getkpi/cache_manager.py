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
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'
MAX_AGE_SECONDS = 86400  # 1 день
DASHBOARD_PAYLOAD_MEM_TTL = 3600  # 1 час — повторные запросы дашборда ГСПП
WARM_TASK_DELAY_SECONDS = float(os.getenv('CACHE_WARM_TASK_DELAY_SECONDS', '1.0'))

_locks: dict[str, threading.Lock] = {}
_meta = threading.Lock()
_warming = False
_warm_cycle_lock = threading.Lock()
_payload_mem_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_payload_mem_lock = threading.Lock()

# Задачи без помесячного суффикса в ключе — выполняются один раз за цикл прогрева.
_GLOBAL_WARM_TASK_KEYS = frozenset({
    'dz_limits',
    'vp',
    'techdir_projects',
    'devdir_turboproject_projects_by_resources',
    'devdir_turboproject_ope_projects',
    'dept_protocol_overdue_warm_all',
    'prod_deputy_projects',
})


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
            if p.name.startswith('dengi_') and data.get('cache_version') != 3:
                return False
            if p.name.startswith('dengi_monthly_') and data.get('cache_version') != 3:
                return False
            if p.name.startswith('rashody_') and data.get('cache_version') != 2:
                return False
            if p.name.startswith('psd_vipusk_plan_') and data.get('cache_version') != 5:
                return False
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


def get_memoized_dashboard_payload(key: str) -> dict[str, Any] | None:
    """In-memory кэш собранного JSON дашборда (TTL см. ``DASHBOARD_PAYLOAD_MEM_TTL``)."""
    now = time.monotonic()
    with _payload_mem_lock:
        entry = _payload_mem_cache.get(key)
        if entry is None:
            return None
        expires_at, payload = entry
        if now >= expires_at:
            del _payload_mem_cache[key]
            return None
        return payload


def set_memoized_dashboard_payload(
    key: str,
    payload: dict[str, Any],
    *,
    ttl_seconds: int = DASHBOARD_PAYLOAD_MEM_TTL,
) -> None:
    with _payload_mem_lock:
        _payload_mem_cache[key] = (time.monotonic() + ttl_seconds, payload)


def _build_warm_tasks(ref_y: int, ref_m: int) -> list[tuple[str, Path, object]]:
    """Список (key, cache_path, compute_fn) для всех источников данных."""
    from . import (
        calc_debitorka, calc_dengi_fact, calc_dogovory_fact,
        calc_logistics_price_deviation, calc_logistics_supplier_share, calc_logistics_tmc_on_time,
        calc_dz_limits, calc_fot, calc_komdir_active_dealers, calc_kp_price, calc_ks_razvitie,
        calc_otgruzki_fact, calc_otif_vypusk_zam_proizvodstva,         calc_plan,
        calc_psd_vipusk_plan,
        calc_prod_deputy_output, calc_prod_deputy_pc, calc_prod_deputy_projects,
        calc_prod_deputy_turnover, calc_rashody,
        calc_reclamations,
        calc_svoevremennaya_otgruzka,
        calc_tekuchest, calc_tkp_sla, valovaya_pribyl,
        calc_metrolog_budget, calc_metrolog_fot, calc_metrolog_production_plan,
        calc_metrolog_projects, calc_metrolog_turnover,
        techdir_m3, techdir_m4, techdir_m5, techdir_projects, techdir_tekuchet,
    )
    from .calc_prod_deputy_pc_common import cache_path as prod_deputy_pc_cache_path
    from . import gspp_q4
    from devdir import (
        rd_m1_zpr,
        rd_m3_budget,
        rd_m4_fot,
        rd_q2_tekuchest,
        turboproject_projects_by_resources,
        turboproject_ope_projects,
    )
    from .komdir_claims import fetch_claims_for_month
    from gspp import m3 as gspp_m3
    from gspp import m5 as gspp_m5
    from gspp import ol_gspp_monthly as gspp_ol_m2
    from gspp import q5 as gspp_q5
    from gspp import tkp_lifecycle as gspp_tkp
    from qualdir import mpp_tasks_report, qd_m1, qd_m3, qd_m4, qd_m5, qd_m6, qd_m7, qd_m8
    from qualdir.turnover import get_qd_q2_ytd, qd_q2_ytd_cache_path
    from sup import hrd_m1, hrd_m4, hrd_q4
    from getkpi.autoit.it_m1_sla import (
        cache_file_path_for_period as autoit_it_m1_cache_path,
        get_it_m1_sla_monthly,
        get_it_m1_sla_ytd,
        monthly_cache_path as autoit_it_m1_monthly_cache_path,
    )
    from getkpi.autoit.it_m3 import cache_file_path_for_period as autoit_it_m3_cache_path
    from getkpi.autoit.it_m3 import get_it_m3_ytd
    from getkpi.autoit.it_m4_fot import cache_file_path_for_period as autoit_it_m4_cache_path
    from getkpi.autoit.it_m4_fot import get_it_m4_fot_ytd
    from getkpi.autoit.it_q2_tekuchest import cache_file_path_for_period as autoit_it_q2_cache_path
    from getkpi.autoit.it_q2_tekuchest import get_it_q2_tekuchest_ytd
    from getkpi.c1auto.c1_m1_sla import (
        cache_file_path_for_period as c1auto_c1_m1_cache_path,
        get_c1_m1_sla_monthly,
        get_c1_m1_sla_ytd,
        monthly_cache_path as c1auto_c1_m1_monthly_cache_path,
    )
    from getkpi.c1auto.c1_m3 import cache_file_path_for_period as c1auto_c1_m3_cache_path
    from getkpi.c1auto.c1_m3 import get_c1_m3_ytd
    from getkpi.c1auto.c1_m4_fot import cache_file_path_for_period as c1auto_c1_m4_cache_path
    from getkpi.c1auto.c1_m4_fot import get_c1_m4_fot_ytd
    from getkpi.c1auto.it_q5_tekuchest import cache_file_path_for_period as c1auto_it_q5_cache_path
    from getkpi.c1auto.it_q5_tekuchest import get_it_q5_tekuchest_ytd

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

        (f'psd_vipusk_{y}_{m}',
         calc_psd_vipusk_plan._cache_path_monthly(y, m),
         lambda: calc_psd_vipusk_plan.get_psd_vipusk_plan_monthly(year=y, ref_month=m)),

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

        (f'techdir_m5_{y}_{m}',
         techdir_m5.ytd_cache_path(y, m),
         lambda yy=y, mm=m: techdir_m5.get_td_m5_ytd(year=yy, month=mm)),

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

        (f'qualdir_qd_m5_tile_{y}_{m}',
         qd_m5.qd_m5_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m5.get_qd_m5_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m6_tile_{y}_{m}',
         qd_m6.qd_m6_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m6.get_qd_m6_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m7_tile_{y}_{m}',
         qd_m7.qd_m7_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m7.get_qd_m7_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m8_tile_{y}_{m}',
         qd_m8.qd_m8_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m8.get_qd_m8_ytd(year=yy, month=mm)),

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

        (f'metrolog_m1_production_plan_{y}_{m}',
         calc_metrolog_production_plan.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: calc_metrolog_production_plan.get_metrolog_production_plan_monthly(year=yy, month=mm)),

        (f'metrolog_m3_budget_{y}_{m}',
         calc_metrolog_budget.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: calc_metrolog_budget.get_metrolog_budget_monthly(year=yy, month=mm)),

        (f'metrolog_m3_fot_{y}_{m}',
         calc_metrolog_fot.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: calc_metrolog_fot.get_metrolog_fot_monthly(year=yy, month=mm)),

        (f'metrolog_q1_projects_{y}_{m}',
         calc_metrolog_projects.metrolog_projects_ytd_cache_path(y, m),
         lambda yy=y, mm=m: calc_metrolog_projects.get_metrolog_projects_without_major_deviation_monthly(year=yy, month=mm)),

        (f'metrolog_q3_certification_projects_{y}_{m}',
         calc_metrolog_projects.certification_projects_ytd_cache_path(y, m),
         lambda yy=y, mm=m: calc_metrolog_projects.get_certification_projects_without_major_deviation_monthly(year=yy, month=mm)),

        (f'metrolog_q2_turnover_{y}_{m}',
         calc_metrolog_turnover.ytd_cache_path(y, m),
         lambda yy=y, mm=m: calc_metrolog_turnover.get_metrolog_turnover_ytd(year=yy, month=mm)),

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

        ('devdir_turboproject_projects_by_resources',
         turboproject_projects_by_resources.CACHE_PATH,
         turboproject_projects_by_resources.get_projects_snapshot),

        ('devdir_turboproject_ope_projects',
         turboproject_ope_projects.CACHE_PATH,
         turboproject_ope_projects.get_ope_projects_snapshot),

        (f'devdir_rd_m3_1_projects_{y}_{m}',
         turboproject_projects_by_resources.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: turboproject_projects_by_resources.get_rd_m3_1_ytd(year=yy, month=mm)),

        (f'devdir_rd_m2_1_ope_{y}_{m}',
         turboproject_ope_projects.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: turboproject_ope_projects.get_rd_m2_1_ytd(year=yy, month=mm)),

        (f'sup_hrd_m1_{y}_{m}',
         hrd_m1.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: hrd_m1.get_hrd_m1_ytd(year=yy, month=mm)),

        (f'sup_hrd_m4_{y}_{m}',
         hrd_m4.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: hrd_m4.get_hrd_m4_ytd(year=yy, month=mm)),

        (f'sup_hrd_q4_{y}_{m}',
         hrd_q4.cache_file_path_for_period(y, m),
         lambda yy=y, mm=m: hrd_q4.get_hrd_q4_ytd(year=yy, month=mm)),

        (f'autoit_it_m1_sla_monthly_{y}_{m}',
         autoit_it_m1_monthly_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m1_sla_monthly(yy, mm)),

        (f'autoit_it_m1_sla_{y}_{m}',
         autoit_it_m1_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m1_sla_ytd(year=yy, month=mm)),

        (f'autoit_it_m3_{y}_{m}',
         autoit_it_m3_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m3_ytd(year=yy, month=mm)),

        (f'autoit_it_m4_fot_{y}_{m}',
         autoit_it_m4_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m4_fot_ytd(year=yy, month=mm)),

        (f'autoit_it_q2_tekuchest_{y}_{m}',
         autoit_it_q2_cache_path(y, m),
         lambda yy=y, mm=m: get_it_q2_tekuchest_ytd(year=yy, month=mm)),

        (f'c1auto_c1_m1_sla_monthly_{y}_{m}',
         c1auto_c1_m1_monthly_cache_path(y, m),
         lambda yy=y, mm=m: get_c1_m1_sla_monthly(yy, mm)),

        (f'c1auto_c1_m1_sla_{y}_{m}',
         c1auto_c1_m1_cache_path(y, m),
         lambda yy=y, mm=m: get_c1_m1_sla_ytd(year=yy, month=mm)),

        (f'c1auto_c1_m3_{y}_{m}',
         c1auto_c1_m3_cache_path(y, m),
         lambda yy=y, mm=m: get_c1_m3_ytd(year=yy, month=mm)),

        (f'c1auto_c1_m4_fot_{y}_{m}',
         c1auto_c1_m4_cache_path(y, m),
         lambda yy=y, mm=m: get_c1_m4_fot_ytd(year=yy, month=mm)),

        (f'c1auto_it_q5_tekuchest_{y}_{m}',
         c1auto_it_q5_cache_path(y, m),
         lambda yy=y, mm=m: get_it_q5_tekuchest_ytd(year=yy, month=mm)),
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

    from getkpi import dept_protocol_tables

    _append_gspp_warm_tasks(tasks, y, m, gspp_q4, gspp_tkp, gspp_ol_m2, gspp_m3, gspp_m5, gspp_q5)

    tasks.append((
        "dept_protocol_overdue_warm_all",
        dept_protocol_tables.warm_stamp_path(),
        dept_protocol_tables.warm_all_department_caches,
    ))

    return tasks


def _task_identity(task: tuple[str, Path, object]) -> tuple[str, str]:
    key, cache_path, _ = task
    return key, str(cache_path)


def _collect_warm_tasks(*, all_months: bool = False) -> list[tuple[str, Path, object]]:
    """Собрать задачи прогрева: только текущий месяц или все месяцы с 2026-01."""
    today = date.today()
    ref_y, ref_m = today.year, today.month
    if not all_months:
        return _build_warm_tasks(ref_y, ref_m)

    from tools.dept_protocol.dashboard_table import month_pairs_from_start

    pairs = month_pairs_from_start(ref_y, ref_m)
    if not pairs:
        pairs = [(ref_y, ref_m)]

    seen: set[tuple[str, str]] = set()
    global_keys_done: set[str] = set()
    tasks: list[tuple[str, Path, object]] = []

    for y, m in pairs:
        for task in _build_warm_tasks(y, m):
            key = task[0]
            identity = _task_identity(task)
            if identity in seen:
                continue
            if key in _GLOBAL_WARM_TASK_KEYS and key in global_keys_done:
                continue

            tasks.append(task)
            seen.add(identity)
            if key in _GLOBAL_WARM_TASK_KEYS:
                global_keys_done.add(key)

    return tasks


def _prefetch_gspp_projects() -> None:
    try:
        from getkpi import gspp_q4
        gspp_q4.get_manager_project_pairs()
        logger.info("cache_manager: prefetched GSPP TurboProject projects")
    except Exception:
        logger.exception("cache_manager: GSPP TurboProject prefetch failed")


def _run_warm_tasks(tasks: list[tuple[str, Path, object]], *, force: bool = False) -> None:
    """Последовательно выполнить список задач прогрева.

    На время долгого прогрева включаем OData guard: если 1С возвращает
    401/402/403, текущий цикл останавливается, чтобы не добивать учётку
    повторными запросами к недоступным объектам.
    """
    from . import odata_http

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    odata_http.reset_access_guard(enabled=True)
    try:
        for key, cache_path, fn in tasks:
            if odata_http.is_access_guard_open():
                logger.warning(
                    "cache_manager: stop warming before [%s], OData access guard: %s",
                    key,
                    odata_http.access_guard_reason(),
                )
                break

            if not force and is_cache_fresh(cache_path):
                logger.info("cache_manager: [%s] fresh, skip", key)
                continue
            try:
                logger.info("cache_manager: [%s] computing...", key)
                locked_call(key, fn)
                logger.info("cache_manager: [%s] done", key)
            except Exception:
                logger.exception("cache_manager: [%s] error", key)

            if odata_http.is_access_guard_open():
                logger.warning(
                    "cache_manager: stop warming after [%s], OData access guard: %s",
                    key,
                    odata_http.access_guard_reason(),
                )
                break

            if WARM_TASK_DELAY_SECONDS > 0:
                time.sleep(WARM_TASK_DELAY_SECONDS)
    finally:
        odata_http.disable_access_guard()


def _append_gspp_warm_tasks(
    tasks: list[tuple[str, Path, object]],
    ref_y: int,
    ref_m: int,
    gspp_q4_mod: object,
    gspp_tkp_mod: object,
    gspp_ol_m2_mod: object,
    gspp_m3_mod: object,
    gspp_m5_mod: object,
    gspp_q5_mod: object,
) -> None:
    """Прогреть файловые кэши ГСПП за все месяцы 1..ref_m текущего года."""
    for warm_m in range(1, ref_m + 1):
        tasks.extend([
            (
                f"gspp_q4_ytd_{ref_y}_{warm_m:02d}",
                gspp_q4_mod.gspp_q4_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_q4_mod.get_gspp_q4_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_m1_tkp_{ref_y}_{warm_m:02d}",
                gspp_tkp_mod.gspp_m1_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_tkp_mod.get_gspp_m1_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_m2_ol_{ref_y}_{warm_m:02d}",
                gspp_ol_m2_mod.gspp_m2_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_ol_m2_mod.get_gspp_m2_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_m3_ytd_{ref_y}_{warm_m:02d}",
                gspp_m3_mod.gspp_m3_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_m3_mod.get_gspp_m3_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_m5_ytd_{ref_y}_{warm_m:02d}",
                gspp_m5_mod.gspp_m5_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_m5_mod.get_gspp_m5_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_q5_tekuchest_{ref_y}_{warm_m:02d}",
                gspp_q5_mod.gspp_q5_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_q5_mod.get_gspp_q5_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_q4_deviation_{ref_y}_{warm_m:02d}",
                gspp_q4_mod.gspp_q4_deviation_tables_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_q4_mod.get_gspp_q4_deviation_tables(year=yy, month=mm),
            ),
        ])


def _run_warm_cycle(
    *,
    all_months: bool = False,
    force: bool = False,
    label: str = 'startup',
) -> None:
    """Один цикл прогрева (текущий месяц или все месяцы с 2026-01)."""
    global _warming

    if not _warm_cycle_lock.acquire(blocking=False):
        logger.warning("cache_manager: warm cycle already running, skip (%s)", label)
        return

    _warming = True
    try:
        today = date.today()
        tasks = _collect_warm_tasks(all_months=all_months)
        scope = 'all months' if all_months else f'{today.year}-{today.month:02d}'
        logger.info(
            "cache_manager: warming %d cache tasks (%s, %s)",
            len(tasks),
            scope,
            label,
        )
        _prefetch_gspp_projects()
        _run_warm_tasks(tasks, force=force)
        logger.info("cache_manager: warming complete (%s)", label)
    finally:
        _warming = False
        _warm_cycle_lock.release()


def warm_all_caches(*, force: bool = False):
    """Проверить кэши текущего месяца и пересчитать просроченные / отсутствующие."""
    _run_warm_cycle(all_months=False, force=force, label='startup')


def warm_all_caches_all_months(*, force: bool = False):
    """Прогреть кэши всех реализованных отделов за каждый месяц с 2026-01."""
    _run_warm_cycle(all_months=True, force=force, label='all-months')


def _seconds_until_next_midnight() -> float:
    now = datetime.now()
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    return max(1.0, (next_midnight - now).total_seconds())


def _midnight_cache_scheduler_loop() -> None:
    """В полночь запускать полный помесячный прогрев кэшей."""
    while True:
        delay = _seconds_until_next_midnight()
        logger.info(
            "cache_manager: next midnight warm in %.0f s (%.1f h)",
            delay,
            delay / 3600,
        )
        time.sleep(delay)
        logger.info("cache_manager: midnight warm started")
        try:
            warm_all_caches_all_months(force=True)
        except Exception:
            logger.exception("cache_manager: midnight warm failed")


def start_warming():
    """Запустить прогрев кэшей в фоновом потоке (вызывается из AppConfig.ready)."""
    if os.environ.get('RUN_MAIN') != 'true':
        return
    t = threading.Thread(target=warm_all_caches, name='cache-warmer', daemon=True)
    t.start()
    logger.info("cache_manager: warming thread started")


def start_midnight_cache_scheduler():
    """Запустить планировщик полночного прогрева (вызывается из AppConfig.ready)."""
    if os.environ.get('RUN_MAIN') != 'true':
        return
    t = threading.Thread(
        target=_midnight_cache_scheduler_loop,
        name='midnight-cache-scheduler',
        daemon=True,
    )
    t.start()
    logger.info("cache_manager: midnight cache scheduler started")
