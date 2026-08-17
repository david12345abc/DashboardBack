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
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent / 'dashboard'
MAX_AGE_SECONDS = 86400  # 1 день
DASHBOARD_PAYLOAD_MEM_TTL = 3600  # 1 час — повторные запросы дашборда ГСПП
WARM_TASK_DELAY_SECONDS = float(os.getenv('CACHE_WARM_TASK_DELAY_SECONDS', '1.0'))
# v2: после восстановления plan на SQL-плитках текучести — сброс stale aggregate.
DASHBOARD_DISK_VERSION = 2

_locks: dict[str, threading.Lock] = {}
_meta = threading.Lock()
_warming = False
_warm_cycle_lock = threading.Lock()
_bg_pending: set[str] = set()
_bg_meta = threading.Lock()
_payload_mem_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_payload_mem_lock = threading.Lock()
_refresh_local = threading.local()
_known_cache_paths: dict[str, Path] = {}
_active_refresh_paths: dict[str, set[str]] = {}
_active_refresh_lock = threading.Lock()

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


def _register_cache_path(key: str, path: Path | str) -> None:
    with _meta:
        _known_cache_paths[key] = Path(path)


def register_cache_path(key: str, path: Path | str) -> None:
    """Сообщить cache_manager, какой файловый кэш соответствует lock-key."""
    _register_cache_path(key, path)


def is_force_compute_context() -> bool:
    """True, когда код выполняется внутри принудительного фонового пересчёта."""
    return bool(getattr(_refresh_local, 'force_compute', False))


@contextmanager
def force_compute():
    """Временно включить принудительный пересчёт, не удаляя старые файлы кэша."""
    previous = bool(getattr(_refresh_local, 'force_compute', False))
    _refresh_local.force_compute = True
    try:
        yield
    finally:
        _refresh_local.force_compute = previous


def _known_cache_path(key: str) -> Path | None:
    with _meta:
        return _known_cache_paths.get(key)


def _load_json_cache(path: Path | str) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return None
    try:
        with p.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def _mark_refresh_active(key: str, path: Path | None) -> None:
    if path is None:
        return
    with _active_refresh_lock:
        _active_refresh_paths.setdefault(str(path), set()).add(key)


def _mark_refresh_inactive(key: str, path: Path | None) -> None:
    if path is None:
        return
    path_key = str(path)
    with _active_refresh_lock:
        keys = _active_refresh_paths.get(path_key)
        if not keys:
            return
        keys.discard(key)
        if not keys:
            _active_refresh_paths.pop(path_key, None)


def is_cache_path_refreshing(path: Path | str) -> bool:
    with _active_refresh_lock:
        return str(Path(path)) in _active_refresh_paths


def is_any_cache_path_refreshing(paths: list[Path] | tuple[Path, ...]) -> bool:
    with _active_refresh_lock:
        return any(str(Path(path)) in _active_refresh_paths for path in paths)


@contextmanager
def mark_paths_refreshing(key: str, paths: list[Path] | tuple[Path, ...]):
    normalized_paths = [Path(path) for path in paths]
    for path in normalized_paths:
        _mark_refresh_active(key, path)
    try:
        yield
    finally:
        for path in normalized_paths:
            _mark_refresh_inactive(key, path)


def is_cache_fresh(path: Path | str) -> bool:
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        return False
    try:
        data = _load_json_cache(p)
        if isinstance(data, dict):
            if p.name.startswith('dengi_') and data.get('cache_version') != 6:
                return False
            if p.name.startswith('dengi_monthly_') and data.get('cache_version') != 6:
                return False
            if p.name.startswith('rashody_') and data.get('cache_version') != 2:
                return False
            if p.name.startswith('psd_vipusk_plan_') and data.get('cache_version') != 5:
                return False
            if p.name.startswith('chief_metrolog_payload_') and data.get('cache_version') != 3:
                return False
            if p.name.startswith('prod_deputy_payload_') and data.get('cache_version') != 3:
                return False
            cache_date = data.get('cache_date') or data.get('cached_at')
            if cache_date:
                return str(cache_date)[:10] == date.today().isoformat()
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return (datetime.now().timestamp() - p.stat().st_mtime) < MAX_AGE_SECONDS


def locked_call(key: str, fn, *args, **kwargs):
    """Выполнить fn под блокировкой key.

    Если старый файловый кэш есть, но он устарел или уже пересчитывается,
    запрос получает старый JSON сразу, а обновление продолжается в фоне.
    """
    lock = _get_lock(key)
    cache_path = _known_cache_path(key)
    force_compute = bool(getattr(_refresh_local, 'force_compute', False))

    if cache_path is not None and not force_compute:
        stale = _load_json_cache(cache_path)
        if stale is not None and not is_cache_fresh(cache_path):
            if lock.acquire(blocking=False):
                lock.release()
                _start_background_refresh(key, cache_path, fn, args, kwargs)
            return stale

    if not lock.acquire(blocking=False):
        if cache_path is not None:
            stale = _load_json_cache(cache_path)
            if stale is not None:
                return stale
        with lock:
            return fn(*args, **kwargs)

    try:
        _mark_refresh_active(key, cache_path)
        return fn(*args, **kwargs)
    finally:
        _mark_refresh_inactive(key, cache_path)
        lock.release()


def _start_background_refresh(key: str, cache_path: Path, fn, args: tuple, kwargs: dict) -> None:
    _mark_refresh_active(key, cache_path)

    def _runner() -> None:
        lock = _get_lock(key)
        with lock:
            _mark_refresh_active(key, cache_path)
            previous = bool(getattr(_refresh_local, 'force_compute', False))
            _refresh_local.force_compute = True
            try:
                fn(*args, **kwargs)
                clear_memoized_dashboard_payload()
            except Exception:
                logger.exception("cache_manager: background refresh failed [%s]", key)
            finally:
                _refresh_local.force_compute = previous
                _mark_refresh_inactive(key, cache_path)

    threading.Thread(
        target=_runner,
        name=f'cache-refresh-{key}',
        daemon=True,
    ).start()


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


def clear_memoized_dashboard_payload(prefix: str | None = None) -> None:
    """Сбросить in-memory кэш собранных payload дашбордов."""
    with _payload_mem_lock:
        if not prefix:
            _payload_mem_cache.clear()
            return
        for key in list(_payload_mem_cache):
            if key.startswith(prefix):
                del _payload_mem_cache[key]


def invalidate_memoized_dashboard_payload(key: str) -> None:
    with _payload_mem_lock:
        _payload_mem_cache.pop(key, None)


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
    # Не кладём stale в memo: иначе background refresh_fn (тот же
    # _build_universal_payload) сразу попадёт в memo-hit и пересохранит
    # старый payload с новой cache_date — «Обновлено» залипает на старой дате.

    refresh_key = f"dashboard_disk_{disk_key}"

    def _worker() -> None:
        t0 = time.monotonic()
        logger.info("cache_manager: dashboard disk refresh started for %s", disk_key)
        try:
            if mem_key:
                invalidate_memoized_dashboard_payload(mem_key)
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
    from comdir import (
        get_cena_ytd,
        get_debitorka_ytd,
        get_dengi_ytd,
        get_dogovory_ytd,
        get_fot_ytd,
        get_otgruzki_ytd,
        get_rashody_ytd,
        get_shipment_share_bmi_gazprom_monthly,
        get_tkp_sla_ytd,
        get_vp_ytd,
    )
    from comdir.mrk06_share import cache_stamp_path as mrk06_cache_path
    from comdir.ytd import cache_stamp_paths as comdir_cache_stamp_paths
    from . import (
        calc_debitorka,
        calc_logistics_price_deviation, calc_logistics_supplier_share, calc_logistics_tmc_on_time,
        calc_dz_limits, calc_komdir_active_dealers, calc_ks_razvitie,
        calc_otif_vypusk_zam_proizvodstva,
        calc_psd_vipusk_plan,
        calc_prod_deputy_output, calc_prod_deputy_pc, calc_prod_deputy_projects,
        calc_prod_deputy_turnover,
        calc_reclamations,
        calc_svoevremennaya_otgruzka,
        calc_tekuchest,
        calc_metrolog_budget, calc_metrolog_fot, calc_metrolog_production_plan,
        calc_metrolog_projects, calc_metrolog_turnover,
        techdir_m3, techdir_m4, techdir_m5, techdir_m6_bdds, techdir_projects, techdir_tekuchet,
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
    from gspp import m1 as gspp_m1
    from gspp import m3 as gspp_m3
    from gspp import m5 as gspp_m5
    from gspp import ol_gspp_monthly as gspp_ol_m2
    from gspp import q5 as gspp_q5
    from servhead import sh_m1 as servhead_sh_m1
    from servhead import sh_m2 as servhead_sh_m2
    from servhead import sh_m3 as servhead_sh_m3
    from servhead import sh_m4 as servhead_sh_m4
    from servhead import sh_m5 as servhead_sh_m5
    from servhead import sh_t1 as servhead_sh_t1
    from servhead import sh_t2 as servhead_sh_t2
    from qualdir import mpp_tasks_report, qd_m1, qd_m3, qd_m4, qd_m5, qd_m6, qd_m7, qd_m8, qd_m9, qd_m10
    from qualdir.qd_q2 import get_qd_q2_ytd, qd_q2_ytd_cache_path
    from sup import hrd_m1, hrd_m2, hrd_m3, hrd_m4, hrd_m7, hrd_m9, hrd_q4
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

    def _comdir_path(kpi_id: str) -> Path:
        paths = comdir_cache_stamp_paths(kpi_id, y, m)
        return paths[0] if paths else cd / f"comdir_{kpi_id}_{y}_{m:02d}.json"

    tasks: list[tuple[str, Path, object]] = [
        (f'comdir_dengi_{y}_{m}',
         _comdir_path('KD-M1'),
         lambda: get_dengi_ytd(year=y, month=m)),

        (f'comdir_otgruzki_{y}_{m}',
         _comdir_path('KD-M2'),
         lambda: get_otgruzki_ytd(year=y, month=m)),

        (f'comdir_dogovory_{y}_{m}',
         _comdir_path('KD-M3'),
         lambda: get_dogovory_ytd(year=y, month=m)),

        (f'psd_vipusk_{y}_{m}',
         calc_psd_vipusk_plan._cache_path_monthly(y, m),
         lambda: calc_psd_vipusk_plan.get_psd_vipusk_plan_monthly(year=y, ref_month=m)),

        (f'comdir_debitorka_{y}_{m}',
         _comdir_path('KD-M4'),
         lambda: get_debitorka_ytd(year=y, month=m)),

        ('dz_limits',
         cd / 'dz_limits_latest.json',
         calc_dz_limits.get_overdue_limits),

        (f'comdir_rashody_{y}_{m}',
         _comdir_path('KD-M7'),
         lambda: get_rashody_ytd(year=y, month=m)),

        (f'comdir_fot_{y}_{m}',
         _comdir_path('KD-M8'),
         lambda: get_fot_ytd(year=y, month=m)),

        (f'comdir_cena_{y}_{m}',
         _comdir_path('KD-M9'),
         lambda: get_cena_ytd(year=y, month=m)),

        (f'comdir_tkp_sla_{y}_{m}',
         _comdir_path('KD-M10'),
         lambda: get_tkp_sla_ytd(year=y, month=m)),

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

        (f'comdir_vp_{y}_{m}',
         _comdir_path('KD-M6'),
         lambda: get_vp_ytd(year=y, month=m)),

        (f'comdir_mrk06_share_{y}_{m}',
         mrk06_cache_path(y, m),
         lambda: get_shipment_share_bmi_gazprom_monthly(year=y, month=m)),

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

        (f'pd_m2_otif_pc1_{y}_{m}',
         calc_otif_vypusk_zam_proizvodstva.cache_path('pc1', y, m),
         lambda: calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly(year=y, month=m, shop='pc1')),

        (f'pd_m2_otif_pc2_{y}_{m}',
         calc_otif_vypusk_zam_proizvodstva.cache_path('pc2', y, m),
         lambda: calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly(year=y, month=m, shop='pc2')),

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

    _append_gspp_warm_tasks(tasks, y, m, gspp_q4, gspp_m1, gspp_ol_m2, gspp_m3, gspp_m5, gspp_q5)
    _append_servhead_warm_tasks(
        tasks, y, m,
        servhead_sh_m1, servhead_sh_m2, servhead_sh_m3, servhead_sh_m4, servhead_sh_m5,
        servhead_sh_t1, servhead_sh_t2,
    )
    _append_sup_warm_tasks(
        tasks, y, m,
        hrd_m1, hrd_m2, hrd_m3, hrd_m4, hrd_m7, hrd_m9, hrd_q4,
    )

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
    for key, cache_path, _ in tasks:
        _register_cache_path(key, cache_path)
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
                previous = bool(getattr(_refresh_local, 'force_compute', False))
                _refresh_local.force_compute = True
                try:
                    locked_call(key, fn)
                finally:
                    _refresh_local.force_compute = previous
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
    gspp_m1_mod: object,
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
                f"gspp_m1_sql_{ref_y}_{warm_m:02d}",
                gspp_m1_mod.gspp_m1_ytd_cache_path(ref_y, warm_m),
                lambda yy=ref_y, mm=warm_m: gspp_m1_mod.get_gspp_m1_ytd(year=yy, month=mm),
            ),
            (
                f"gspp_m2_sql_{ref_y}_{warm_m:02d}",
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
    hrd_m7_mod: object,
    hrd_m9_mod: object,
    hrd_q4_mod: object,
) -> None:
    """Прогреть файловые кэши SUP (HRD-M1…M4, M7, M9, HRD-Q4) за все месяцы 1..ref_m."""
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
            f"sup_hrd_m7_{ref_y}_{warm_m:02d}",
            hrd_m7_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m7_mod.get_hrd_m7_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"sup_hrd_m9_{ref_y}_{warm_m:02d}",
            hrd_m9_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_m9_mod.get_hrd_m9_ytd(year=yy, month=mm),
        ))
        tasks.append((
            f"sup_hrd_q4_{ref_y}_{warm_m:02d}",
            hrd_q4_mod.cache_file_path_for_period(ref_y, warm_m),
            lambda yy=ref_y, mm=warm_m: hrd_q4_mod.get_hrd_q4_ytd(year=yy, month=mm),
        ))


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


def _run_warm_period_cycle(ref_y: int, ref_m: int, *, force: bool = False, label: str = 'period') -> None:
    """Один цикл прогрева за явно выбранный месяц."""
    global _warming

    if not _warm_cycle_lock.acquire(blocking=False):
        logger.warning("cache_manager: warm cycle already running, skip (%s)", label)
        return

    _warming = True
    try:
        tasks = _build_warm_tasks(int(ref_y), max(1, min(12, int(ref_m))))
        logger.info(
            "cache_manager: warming %d cache tasks (%04d-%02d, %s)",
            len(tasks),
            int(ref_y),
            max(1, min(12, int(ref_m))),
            label,
        )
        _prefetch_gspp_projects()
        _run_warm_tasks(tasks, force=force)
        logger.info("cache_manager: warming complete (%s)", label)
    finally:
        _warming = False
        _warm_cycle_lock.release()


def start_period_warming_if_stale(ref_y: int, ref_m: int) -> dict[str, Any]:
    """Запустить фоновый прогрев выбранного месяца, если есть stale/missing кэши."""
    y = int(ref_y)
    m = max(1, min(12, int(ref_m)))
    tasks = _build_warm_tasks(y, m)
    if not any(not is_cache_fresh(path) for _key, path, _fn in tasks):
        return {'started': False, 'reason': 'fresh', 'year': y, 'month': m}
    if _warm_cycle_lock.locked():
        return {'started': False, 'reason': 'already_running', 'year': y, 'month': m}

    t = threading.Thread(
        target=lambda: _run_warm_period_cycle(y, m, force=True, label='first-access'),
        name=f'cache-warmer-first-access-{y}-{m:02d}',
        daemon=True,
    )
    t.start()
    logger.info("cache_manager: first-access warm queued for %04d-%02d", y, m)
    return {'started': True, 'year': y, 'month': m}


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
