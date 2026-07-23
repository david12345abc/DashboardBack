from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

from . import cache_manager

logger = logging.getLogger(__name__)

COMMERCIAL_CACHE_REFRESH_HOUR = int(os.getenv("COMMERCIAL_CACHE_REFRESH_HOUR", "13"))
COMMERCIAL_CACHE_REFRESH_MINUTE = int(os.getenv("COMMERCIAL_CACHE_REFRESH_MINUTE", "0"))
COMMERCIAL_CACHE_REFRESH_ENABLED = os.getenv("COMMERCIAL_CACHE_REFRESH_ENABLED", "1").lower() not in {
    "0",
    "false",
    "no",
}

_scheduler_started = False
_scheduler_lock = threading.Lock()
_run_lock = threading.Lock()
_first_access_lock = threading.Lock()


def _seconds_until_next_run(now: datetime | None = None) -> float:
    now = now or datetime.now()
    target = now.replace(
        hour=COMMERCIAL_CACHE_REFRESH_HOUR,
        minute=COMMERCIAL_CACHE_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _commercial_period(month: int | None = None, year: int | None = None) -> tuple[int, int, int]:
    from . import komdir_dashboard

    ref_y, ref_m, series_m = komdir_dashboard._komdir_payload_period(month, year)
    return ref_y, ref_m, series_m


def _commercial_source_tasks(ref_y: int, ref_m: int, series_m: int) -> list[tuple[str, Path, object]]:
    from . import (
        calc_debitorka,
        calc_dengi_fact,
        calc_dogovory_fact,
        calc_dz_limits,
        calc_fot,
        calc_komdir_active_dealers,
        calc_kp_price,
        calc_ks_razvitie,
        calc_otgruzki_fact,
        calc_plan,
        calc_rashody,
        calc_tekuchest,
        calc_tkp_sla,
        valovaya_pribyl,
    )
    from .komdir_claims import fetch_claims_for_month
    from .komdir_lawsuits import fetch_lawsuits_for_month

    cd = cache_manager.CACHE_DIR
    today = date.today()
    return [
        (
            f"commercial_dengi_{ref_y}_{series_m:02d}",
            cd / f"dengi_monthly_{ref_y}_{series_m:02d}.json",
            lambda: calc_dengi_fact.get_dengi_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_otgruzki_{ref_y}_{series_m:02d}",
            cd / f"otgruzki_monthly_{ref_y}_{series_m:02d}.json",
            lambda: calc_otgruzki_fact.get_otgruzki_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_dogovory_{ref_y}_{series_m:02d}",
            cd / f"dogovory_monthly_{ref_y}_{series_m:02d}.json",
            lambda: calc_dogovory_fact.get_dogovory_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_plans_{ref_y}_{series_m:02d}",
            cd / f"plans_monthly_{ref_y}_{series_m:02d}.json",
            lambda: calc_plan.get_plans_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_debitorka_{ref_y}_{series_m:02d}",
            cd / f"debitorka_monthly_{ref_y}_{series_m:02d}.json",
            lambda: calc_debitorka.get_komdir_dz_monthly(year=ref_y, month=series_m),
        ),
        ("commercial_dz_limits", cd / "dz_limits_latest.json", calc_dz_limits.get_overdue_limits),
        (
            f"commercial_rashody_{ref_y}_{series_m:02d}",
            cd / f"rashody_{ref_y}_{series_m:02d}.json",
            lambda: calc_rashody.get_rashody_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_fot_{ref_y}_{series_m:02d}",
            cd / f"fot_{ref_y}_{series_m:02d}.json",
            lambda: calc_fot.get_fot_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_kp_price_{ref_y}_{series_m:02d}",
            cd / f"kp_price_{ref_y}_{series_m:02d}.json",
            lambda: calc_kp_price.get_kp_price_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_tkp_sla_{ref_y}_{series_m:02d}",
            cd / f"tkp_sla_{ref_y}_{series_m:02d}.json",
            lambda: calc_tkp_sla.get_tkp_sla_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_tekuchest_{ref_y}_{series_m:02d}",
            cd / f"tekuchest_{ref_y}_{series_m:02d}.json",
            lambda: calc_tekuchest.get_tekuchest_monthly(year=ref_y, month=series_m),
        ),
        (
            f"commercial_overdue_detail_{ref_y}_{series_m:02d}",
            calc_debitorka.overdue_detail_cache_path(ref_y, series_m),
            lambda: calc_debitorka.get_overdue_detail(year=ref_y, month=series_m),
        ),
        ("commercial_vp", cd / "vp_result_cache.json", valovaya_pribyl.get_vp_ytd),
        (
            f"commercial_claims_{ref_y}_{series_m:02d}",
            cd / f"claims_all_{ref_y}_{series_m:02d}.json",
            lambda: fetch_claims_for_month(ref_y, series_m, include_all=True),
        ),
        (
            f"commercial_lawsuits_{ref_y}_{series_m:02d}",
            cd / f"lawsuits_all_{ref_y}_{series_m:02d}.json",
            lambda: fetch_lawsuits_for_month(ref_y, series_m, include_all=True),
        ),
        (
            f"commercial_active_dealers_{today.isoformat()}",
            cd / f"active_dealers_{today.isoformat()}.json",
            lambda: calc_komdir_active_dealers.compute_active_dealers_report(today),
        ),
        (
            f"commercial_ks_razvitie_{ref_y}",
            cd / f"ks_razvitie_{ref_y}.json",
            lambda: calc_ks_razvitie.get_ks_razvitie_plans(year=ref_y),
        ),
    ]


def _commercial_payload_specs(ref_y: int, ref_m: int) -> list[tuple[str, str, str | None, Path]]:
    from . import komdir_dashboard
    from .commercial_tiles import KPI_KEY_TO_DEPT_GUID

    specs: list[tuple[str, str, str | None, Path]] = [
        ("коммерческий директор", "коммерческий директор", None, komdir_dashboard._payload_cache_path(ref_y, ref_m, None)),
    ]
    for kpi_key, dept_guid in sorted(KPI_KEY_TO_DEPT_GUID.items()):
        if not dept_guid:
            continue
        specs.append((kpi_key, kpi_key, dept_guid, komdir_dashboard._payload_cache_path(ref_y, ref_m, dept_guid)))
    return specs


def _filtered_payload_paths(ref_y: int, ref_m: int, departments: Iterable[str] | None = None) -> list[Path]:
    wanted = {str(dep).strip().lower() for dep in departments or [] if str(dep).strip()}
    paths: list[Path] = []
    for department, _kpi_key, _dept_guid, path in _commercial_payload_specs(ref_y, ref_m):
        if wanted and department.strip().lower() not in wanted:
            continue
        paths.append(path)
    return paths


def _commercial_cache_paths(
    ref_y: int,
    ref_m: int,
    series_m: int,
    *,
    payload_departments: Iterable[str] | None = None,
) -> list[Path]:
    return [
        path
        for _key, path, _fn in _commercial_source_tasks(ref_y, ref_m, series_m)
    ] + _filtered_payload_paths(ref_y, ref_m, payload_departments)


def _has_stale_commercial_cache(
    ref_y: int,
    ref_m: int,
    series_m: int,
    *,
    payload_departments: Iterable[str] | None = None,
) -> bool:
    paths = _commercial_cache_paths(
        ref_y,
        ref_m,
        series_m,
        payload_departments=payload_departments,
    )
    return any(not cache_manager.is_cache_fresh(path) for path in paths)


def _rebuild_payload_snapshots(ref_y: int, ref_m: int, departments: Iterable[str] | None = None) -> None:
    from . import komdir_dashboard
    from . import views

    wanted = {str(dep).strip().lower() for dep in departments or [] if str(dep).strip()}
    for department, kpi_key, dept_guid, _path in _commercial_payload_specs(ref_y, ref_m):
        if wanted and department.strip().lower() not in wanted:
            continue
        try:
            kpis = views._get_kpi_dicts(kpi_key)
            if dept_guid is not None:
                # Дочерние отделы используют тот же набор KPI, что и коммерческий блок.
                kpis = views._get_kpi_dicts("коммерческий директор") or kpis
            komdir_dashboard._build_komdir_payload_fresh(
                kpis,
                month=ref_m,
                year=ref_y,
                dept_guid=dept_guid,
            )
            logger.info("commercial cache scheduler: payload rebuilt [%s]", department)
        except Exception:
            logger.exception("commercial cache scheduler: payload rebuild failed [%s]", department)


def run_commercial_cache_refresh_once(
    *,
    force: bool = True,
    task_keys: set[str] | None = None,
    rebuild_payloads: bool = True,
    payload_departments: Iterable[str] | None = None,
    month: int | None = None,
    year: int | None = None,
) -> dict[str, object]:
    """Запустить один цикл пересчёта коммерческих кэшей.

    Используется scheduler-ом и тестом из shell. Старые файлы не удаляются:
    active paths помечаются как refreshing, поэтому API продолжает отдавать
    stale payload с индикатором загрузки.
    """
    if not _run_lock.acquire(blocking=False):
        logger.warning("commercial cache scheduler: refresh already running, skip")
        return {"started": False, "reason": "already_running"}
    try:
        ref_y, ref_m, series_m = _commercial_period(month, year)
        tasks = _commercial_source_tasks(ref_y, ref_m, series_m)
        if task_keys:
            wanted = {str(key).strip().lower() for key in task_keys}
            tasks = [task for task in tasks if any(token in task[0].lower() for token in wanted)]
        payload_paths = _filtered_payload_paths(ref_y, ref_m, payload_departments)
        logger.info(
            "commercial cache scheduler: start refresh, tasks=%d, period=%04d-%02d",
            len(tasks),
            ref_y,
            series_m,
        )
        with cache_manager.mark_paths_refreshing("commercial_daily_payloads", payload_paths):
            cache_manager._run_warm_tasks(tasks, force=force)
            if rebuild_payloads:
                _rebuild_payload_snapshots(ref_y, ref_m, departments=payload_departments)
        cache_manager.clear_memoized_dashboard_payload("komdir_payload_")
        logger.info("commercial cache scheduler: refresh complete")
        return {
            "started": True,
            "tasks": len(tasks),
            "year": ref_y,
            "month": series_m,
            "payloads_rebuilt": bool(rebuild_payloads),
        }
    finally:
        _run_lock.release()


def start_first_access_refresh_if_stale(
    *,
    month: int | None = None,
    year: int | None = None,
    payload_departments: Iterable[str] | None = None,
) -> dict[str, object]:
    """Фоновый пересчёт коммерческих кэшей при первом открытии дашборда за день."""
    ref_y, ref_m, series_m = _commercial_period(month, year)
    if not _has_stale_commercial_cache(
        ref_y,
        ref_m,
        series_m,
        payload_departments=payload_departments,
    ):
        return {"started": False, "reason": "fresh", "year": ref_y, "month": series_m}
    if _run_lock.locked():
        return {"started": False, "reason": "already_running", "year": ref_y, "month": series_m}
    if not _first_access_lock.acquire(blocking=False):
        return {"started": False, "reason": "first_access_already_starting", "year": ref_y, "month": series_m}

    def _runner() -> None:
        try:
            run_commercial_cache_refresh_once(
                force=True,
                rebuild_payloads=True,
                payload_departments=payload_departments,
                month=month,
                year=year,
            )
        except Exception:
            logger.exception("commercial cache scheduler: first-access refresh failed")
        finally:
            _first_access_lock.release()

    threading.Thread(
        target=_runner,
        name=f"commercial-first-access-refresh-{ref_y}-{series_m:02d}",
        daemon=True,
    ).start()
    logger.info(
        "commercial cache scheduler: first-access refresh queued, period=%04d-%02d",
        ref_y,
        series_m,
    )
    return {"started": True, "year": ref_y, "month": series_m}


def _scheduler_loop() -> None:
    while True:
        delay = _seconds_until_next_run()
        logger.info(
            "commercial cache scheduler: next run in %.0fs at %02d:%02d",
            delay,
            COMMERCIAL_CACHE_REFRESH_HOUR,
            COMMERCIAL_CACHE_REFRESH_MINUTE,
        )
        time.sleep(delay)
        try:
            run_commercial_cache_refresh_once(force=True)
        except Exception:
            logger.exception("commercial cache scheduler: scheduled refresh failed")


def start_commercial_cache_scheduler() -> None:
    global _scheduler_started
    if not COMMERCIAL_CACHE_REFRESH_ENABLED:
        logger.info("commercial cache scheduler: disabled")
        return
    if os.environ.get("RUN_MAIN") != "true":
        return
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
        thread = threading.Thread(
            target=_scheduler_loop,
            name="commercial-cache-scheduler",
            daemon=True,
        )
        thread.start()
    logger.info(
        "commercial cache scheduler: started, daily at %02d:%02d",
        COMMERCIAL_CACHE_REFRESH_HOUR,
        COMMERCIAL_CACHE_REFRESH_MINUTE,
    )
