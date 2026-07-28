"""
cache_manager.py — Предотвращение параллельных вычислений + прогрев кэшей.

1. locked_call(key, fn, ...) — гарантирует, что для одного ключа
   одновременно выполняется только одно вычисление; остальные потоки
   ждут завершения и затем читают из кэша.

2. warm_all_caches() — при старте сервера проверяет все кэши
   и пересчитывает просроченные / отсутствующие (в фоновом потоке).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'
MAX_AGE_SECONDS = 86400  # 1 день
DASHBOARD_PAYLOAD_MEM_TTL = 3600  # 1 час — повторные запросы дашборда ГСПП
DASHBOARD_DISK_VERSION = 1

_locks: dict[str, threading.Lock] = {}
_meta = threading.Lock()
_warming = False
_bg_pending: set[str] = set()
_bg_meta = threading.Lock()
_payload_mem_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_payload_mem_lock = threading.Lock()


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
    return (datetime.now().timestamp() - p.stat().st_mtime) < MAX_AGE_SECONDS


def locked_call(key: str, fn, *args, **kwargs):
    """Выполнить fn под блокировкой key.

    Если другой поток уже вычисляет тот же key — текущий поток
    ждёт завершения, после чего вызывает fn (который прочитает свежий кэш).
    """
    with _get_lock(key):
        return fn(*args, **kwargs)


def schedule_background_refresh(key: str, fn, *args, **kwargs) -> None:
    """Запустить пересчёт кэша в фоне, если он ещё не выполняется."""
    with _bg_meta:
        if key in _bg_pending:
            logger.info("cache_manager: [%s] background refresh already pending, skip", key)
            return
        if is_computing(key):
            logger.info("cache_manager: [%s] already computing, skip background schedule", key)
            return
        _bg_pending.add(key)

    logger.info("cache_manager: [%s] background refresh scheduled", key)

    def _worker() -> None:
        t0 = time.monotonic()
        logger.info("cache_manager: [%s] background refresh started", key)
        try:
            locked_call(key, fn, *args, **kwargs)
            logger.info(
                "cache_manager: [%s] background refresh done in %.1fs",
                key,
                time.monotonic() - t0,
            )
        except Exception:
            logger.exception(
                "cache_manager: [%s] background refresh failed after %.1fs",
                key,
                time.monotonic() - t0,
            )
        finally:
            with _bg_meta:
                _bg_pending.discard(key)

    threading.Thread(
        target=_worker,
        name=f"cache-refresh-{key}",
        daemon=True,
    ).start()


def stale_while_revalidate(key: str, load_fresh, load_stale, compute):
    """Stale-while-revalidate: свежий кэш → устаревший + фон → синхронный пересчёт."""
    fresh = load_fresh()
    if fresh is not None:
        return fresh

    stale = load_stale()
    if stale is not None:
        logger.info(
            "cache_manager: [%s] serving stale cache, scheduling background refresh",
            key,
        )
        schedule_background_refresh(key, compute)
        return stale

    logger.info("cache_manager: [%s] no cache file, synchronous compute", key)
    t0 = time.monotonic()
    result = locked_call(key, compute)
    logger.info(
        "cache_manager: [%s] synchronous compute done in %.1fs",
        key,
        time.monotonic() - t0,
    )
    return result


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


def _dashboard_disk_path(disk_key: str) -> Path:
    return CACHE_DIR / f"dashboard_payload_{disk_key}.json"


def _read_dashboard_disk_wrapper(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    if raw.get("cache_version") != DASHBOARD_DISK_VERSION:
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    return raw


def load_dashboard_disk_fresh(disk_key: str) -> dict[str, Any] | None:
    wrapper = _read_dashboard_disk_wrapper(_dashboard_disk_path(disk_key))
    if wrapper is None:
        return None
    if wrapper.get("cache_date") != date.today().isoformat():
        return None
    return wrapper


def load_dashboard_disk_stale(disk_key: str) -> dict[str, Any] | None:
    return _read_dashboard_disk_wrapper(_dashboard_disk_path(disk_key))


def save_dashboard_disk(disk_key: str, payload: dict[str, Any]) -> None:
    path = _dashboard_disk_path(disk_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "cache_version": DASHBOARD_DISK_VERSION,
                    "cache_date": date.today().isoformat(),
                    "payload": payload,
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        logger.exception("cache_manager: failed to save dashboard disk cache %s", path)


def try_serve_dashboard_disk_cache(
    disk_key: str,
    mem_key: str | None,
    *,
    refresh_fn,
) -> dict[str, Any] | None:
    """Вернуть payload с диска (свежий или устаревший) и при необходимости запланировать пересборку."""
    fresh = load_dashboard_disk_fresh(disk_key)
    if fresh is not None:
        payload = fresh["payload"]
        if mem_key:
            set_memoized_dashboard_payload(mem_key, payload)
        return payload

    stale = load_dashboard_disk_stale(disk_key)
    if stale is None:
        return None

    payload = stale["payload"]
    if mem_key:
        set_memoized_dashboard_payload(mem_key, payload)

    refresh_key = f"dashboard_disk_{disk_key}"

    def _worker() -> None:
        t0 = time.monotonic()
        logger.info("cache_manager: dashboard disk refresh started for %s", disk_key)
        try:
            new_payload = refresh_fn()
            save_dashboard_disk(disk_key, new_payload)
            if mem_key:
                set_memoized_dashboard_payload(mem_key, new_payload)
            logger.info(
                "cache_manager: dashboard disk refresh done for %s in %.1fs",
                disk_key,
                time.monotonic() - t0,
            )
        except Exception:
            logger.exception(
                "cache_manager: dashboard disk refresh failed for %s after %.1fs",
                disk_key,
                time.monotonic() - t0,
            )

    schedule_background_refresh(refresh_key, _worker)
    logger.info("cache_manager: served stale dashboard disk cache %s", disk_key)
    return payload


def _build_warm_tasks(ref_y: int, ref_m: int) -> list[tuple[str, Path, object]]:
    """Список (key, cache_path, compute_fn) для всех источников данных."""
    from . import (
        calc_debitorka, calc_dengi_fact, calc_dogovory_fact,
        calc_dz_limits, calc_fot, calc_kp_price,
        calc_otgruzki_fact, calc_plan, calc_rashody,
        calc_reclamations,
        calc_svoevremennaya_otgruzka,
        calc_tekuchest, calc_tkp_sla, valovaya_pribyl,
        techdir_m3, techdir_m4, techdir_m5, techdir_m6_bdds, techdir_projects, techdir_tekuchet,
    )
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
    from servhead import sh_m1 as servhead_sh_m1
    from servhead import sh_m2 as servhead_sh_m2
    from servhead import sh_m3 as servhead_sh_m3
    from servhead import sh_m4 as servhead_sh_m4
    from servhead import sh_m5 as servhead_sh_m5
    from servhead import sh_t1 as servhead_sh_t1
    from servhead import sh_t2 as servhead_sh_t2
    from qualdir import mpp_tasks_report, qd_m1, qd_m3, qd_m4, qd_m5, qd_m6, qd_m7, qd_m8, qd_m9, qd_m10
    from qualdir.qd_q2 import get_qd_q2_ytd, qd_q2_ytd_cache_path
    from sup import hrd_m1, hrd_m2, hrd_m3, hrd_m4, hrd_q4
    from getkpi.autoit.it_m1_sla import (
        cache_file_path_for_period as autoit_it_m1_cache_path,
        get_it_m1_sla_monthly,
        get_it_m1_sla_ytd,
        monthly_cache_path as autoit_it_m1_monthly_cache_path,
    )
    from getkpi.autoit.it_m3 import (
        cache_file_path_for_period as autoit_it_m3_cache_path,
        get_it_m3_fact_monthly,
        get_it_m3_ytd,
        monthly_cache_path as autoit_it_m3_monthly_cache_path,
    )
    from getkpi.autoit.it_m4_fot import (
        cache_file_path_for_period as autoit_it_m4_cache_path,
        get_it_m4_fot_fact_monthly,
        get_it_m4_fot_ytd,
        monthly_cache_path as autoit_it_m4_monthly_cache_path,
    )
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

        (f'techdir_m6_{y}_{m}',
         techdir_m6_bdds._td_m6_cache_path(y, m),
         lambda yy=y, mm=m: techdir_m6_bdds.get_td_m6_ytd(year=yy, month=mm)),

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

        (f'qualdir_qd_m6_month_{y}_{m}',
         qd_m6.otk_predyavlenie_month_cache_path(y, m),
         lambda yy=y, mm=m: qd_m6.compute_qd_m6_month(yy, mm)),

        (f'qualdir_qd_m9_tile_{y}_{m}',
         qd_m9.qd_m9_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m9.get_qd_m9_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m9_month_{y}_{m}',
         qd_m9.otk_predyavlenie_npo_month_cache_path(y, m),
         lambda yy=y, mm=m: qd_m9.compute_qd_m9_month(yy, mm)),

        (f'qualdir_qd_m10_tile_{y}_{m}',
         qd_m10.qd_m10_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m10.get_qd_m10_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m10_month_{y}_{m}',
         qd_m10.otk_predyavlenie_almaz_month_cache_path(y, m),
         lambda yy=y, mm=m: qd_m10.compute_qd_m10_month(yy, mm)),

        (f'qualdir_qd_m7_tile_{y}_{m}',
         qd_m7.qd_m7_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m7.get_qd_m7_ytd(year=yy, month=mm)),

        (f'qualdir_qd_m8_tile_{y}_{m}',
         qd_m8.qd_m8_tile_cache_path(y, m),
         lambda yy=y, mm=m: qd_m8.get_qd_m8_ytd(year=yy, month=mm)),

        (f'qualdir_qd_q1_tile_{y}_{m}',
         mpp_tasks_report.qd_q1_tile_cache_path(y, m),
         lambda yy=y, mm=m: mpp_tasks_report.get_qd_q1_ytd(year=yy, month=mm)),

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

        (f'autoit_it_m1_sla_monthly_{y}_{m}',
         autoit_it_m1_monthly_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m1_sla_monthly(yy, mm)),

        (f'autoit_it_m1_sla_{y}_{m}',
         autoit_it_m1_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m1_sla_ytd(year=yy, month=mm)),

        (f'autoit_it_m3_fact_monthly_{y}_{m}',
         autoit_it_m3_monthly_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m3_fact_monthly(yy, mm)),

        (f'autoit_it_m3_{y}_{m}',
         autoit_it_m3_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m3_ytd(year=yy, month=mm)),

        (f'autoit_it_m4_fot_fact_monthly_{y}_{m}',
         autoit_it_m4_monthly_cache_path(y, m),
         lambda yy=y, mm=m: get_it_m4_fot_fact_monthly(yy, mm)),

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

    from getkpi import dept_protocol_tables

    _append_gspp_warm_tasks(tasks, y, m, gspp_q4, gspp_tkp, gspp_ol_m2, gspp_m3, gspp_m5, gspp_q5)
    _append_servhead_warm_tasks(
        tasks, y, m,
        servhead_sh_m1, servhead_sh_m2, servhead_sh_m3, servhead_sh_m4, servhead_sh_m5,
        servhead_sh_t1, servhead_sh_t2,
    )
    _append_sup_warm_tasks(
        tasks, y, m,
        hrd_m1, hrd_m2, hrd_m3, hrd_m4, hrd_q4,
    )

    tasks.append((
        "dept_protocol_overdue_warm_all",
        dept_protocol_tables.warm_stamp_path(),
        dept_protocol_tables.warm_all_department_caches,
    ))

    return tasks


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


def _append_servhead_warm_tasks(
    tasks: list[tuple[str, Path, object]],
    ref_y: int,
    ref_m: int,
    sh_m1_mod: object,
    sh_m2_mod: object,
    sh_m3_mod: object,
    sh_m4_mod: object,
    sh_m5_mod: object,
    sh_t1_mod: object,
    sh_t2_mod: object,
) -> None:
    """Прогреть файловые кэши servhead за все месяцы 1..ref_m текущего года."""
    for warm_m in range(1, ref_m + 1):
        tasks.append((
            f"servhead_sh_m1_{ref_y}_{warm_m:02d}",
            sh_m1_mod.sh_m1_ytd_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: sh_m1_mod.get_sh_m1_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"servhead_sh_m2_{ref_y}_{warm_m:02d}",
            sh_m2_mod.sh_m2_ytd_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: sh_m2_mod.get_sh_m2_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"servhead_sh_m3_{ref_y}_{warm_m:02d}",
            sh_m3_mod.sh_m3_ytd_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: sh_m3_mod.get_sh_m3_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"servhead_sh_m4_{ref_y}_{warm_m:02d}",
            sh_m4_mod.sh_m4_ytd_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: sh_m4_mod.get_sh_m4_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"servhead_sh_m5_{ref_y}_{warm_m:02d}",
            sh_m5_mod.sh_m5_ytd_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: sh_m5_mod.get_sh_m5_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"servhead_sh_t1_{ref_y}_{warm_m:02d}",
            sh_t1_mod.sh_t1_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: sh_t1_mod.get_sh_t1_table(year=yy, month=mm),
        ))
    tasks.append((
        "servhead_sh_t2_all",
        sh_t2_mod.sh_t2_cache_path(),
        sh_t2_mod.get_sh_t2_table,
    ))


def _append_sup_warm_tasks(
    tasks: list[tuple[str, Path, object]],
    ref_y: int,
    ref_m: int,
    hrd_m1_mod: object,
    hrd_m2_mod: object,
    hrd_m3_mod: object,
    hrd_m4_mod: object,
    hrd_q4_mod: object,
) -> None:
    """Прогреть файловые кэши SUP (HRD-M1…M4, HRD-Q4) за все месяцы 1..ref_m."""
    for warm_m in range(1, ref_m + 1):
        tasks.append((
            f"sup_hrd_m1_{ref_y}_{warm_m:02d}",
            hrd_m1_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m1_mod.get_hrd_m1_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"sup_hrd_m2_fot_fact_{ref_y}_{warm_m:02d}",
            hrd_m2_mod.monthly_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m2_mod.get_hrd_m2_fact_monthly(yy, mm),
        ))
        tasks.append((
            f"sup_hrd_m2_{ref_y}_{warm_m:02d}",
            hrd_m2_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m2_mod.get_hrd_m2_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"sup_hrd_m3_budget_fact_{ref_y}_{warm_m:02d}",
            hrd_m3_mod.monthly_cache_path(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m3_mod.get_hrd_m3_fact_monthly(yy, mm),
        ))
        tasks.append((
            f"sup_hrd_m3_{ref_y}_{warm_m:02d}",
            hrd_m3_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m3_mod.get_hrd_m3_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"sup_hrd_m4_{ref_y}_{warm_m:02d}",
            hrd_m4_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m4_mod.get_hrd_m4_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"sup_hrd_q4_{ref_y}_{warm_m:02d}",
            hrd_q4_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_q4_mod.get_hrd_q4_ytd(year=yy, month=mm),
        ))


def warm_all_caches():
    """Проверить все кэши и пересчитать просроченные / отсутствующие."""
    global _warming
    _warming = True
    today = date.today()
    ref_y, ref_m = today.year, today.month

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tasks = _build_warm_tasks(ref_y, ref_m)

    logger.info("cache_manager: warming %d cache tasks for %d-%02d", len(tasks), ref_y, ref_m)

    try:
        from getkpi import gspp_q4
        gspp_q4.get_manager_project_pairs()
        logger.info("cache_manager: prefetched GSPP TurboProject projects")
    except Exception:
        logger.exception("cache_manager: GSPP TurboProject prefetch failed")

    for key, cache_path, fn in tasks:
        if is_cache_fresh(cache_path):
            logger.info("cache_manager: [%s] fresh, skip", key)
            continue
        if cache_path.exists():
            logger.info("cache_manager: [%s] stale on disk, background refresh", key)
            schedule_background_refresh(key, fn)
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
