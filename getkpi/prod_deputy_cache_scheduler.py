# -*- coding: utf-8 -*-
"""Ежедневный пересчёт OTIF (PD-M2.1 / PD-M2.2) из erp_pm в 08:00."""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from . import cache_manager

logger = logging.getLogger(__name__)

PROD_DEPUTY_CACHE_REFRESH_HOUR = int(os.getenv("PROD_DEPUTY_CACHE_REFRESH_HOUR", "8"))
PROD_DEPUTY_CACHE_REFRESH_MINUTE = int(os.getenv("PROD_DEPUTY_CACHE_REFRESH_MINUTE", "0"))
PROD_DEPUTY_CACHE_REFRESH_ENABLED = os.getenv("PROD_DEPUTY_CACHE_REFRESH_ENABLED", "1").lower() not in {
    "0",
    "false",
    "no",
}

PROD_DEPUTY_DASHBOARD_DEPT = "Заместитель директора по производству"

_scheduler_started = False
_scheduler_lock = threading.Lock()
_run_lock = threading.Lock()


def _seconds_until_next_run(now: datetime | None = None) -> float:
    now = now or datetime.now()
    target = now.replace(
        hour=PROD_DEPUTY_CACHE_REFRESH_HOUR,
        minute=PROD_DEPUTY_CACHE_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _ref_period(month: int | None = None, year: int | None = None) -> tuple[int, int]:
    today = date.today()
    if year is not None and month is not None:
        return int(year), max(1, min(12, int(month)))
    if year is not None:
        ref_y = int(year)
        return ref_y, today.month if ref_y == today.year else 12
    if month is not None:
        return today.year, max(1, min(12, int(month)))
    return today.year, today.month


def _otif_tasks(ref_y: int, ref_m: int) -> list[tuple[str, Path, object]]:
    from . import calc_otif_vypusk_zam_proizvodstva

    return [
        (
            f"pd_m2_otif_pc1_{ref_y}_{ref_m}",
            calc_otif_vypusk_zam_proizvodstva.cache_path("pc1", ref_y, ref_m),
            lambda: calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly(
                year=ref_y, month=ref_m, shop="pc1"
            ),
        ),
        (
            f"pd_m2_otif_pc2_{ref_y}_{ref_m}",
            calc_otif_vypusk_zam_proizvodstva.cache_path("pc2", ref_y, ref_m),
            lambda: calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly(
                year=ref_y, month=ref_m, shop="pc2"
            ),
        ),
    ]


def _payload_path(ref_y: int, ref_m: int) -> Path:
    from . import views

    return views._prod_deputy_payload_cache_path(ref_y, ref_m)


def _cache_date_is_today(path: Path) -> bool:
    raw = cache_manager._load_json_cache(path)
    return bool(raw) and raw.get("cache_date") == date.today().isoformat()


def _is_today_snapshot_stale(ref_y: int, ref_m: int) -> bool:
    from . import views

    payload_path = _payload_path(ref_y, ref_m)
    if not payload_path.exists():
        return True
    raw = cache_manager._load_json_cache(payload_path)
    if not raw:
        return True
    if raw.get("cache_version") != views.PROD_DEPUTY_PAYLOAD_CACHE_VERSION:
        return True
    if raw.get("cache_date") != date.today().isoformat():
        return True
    return any(not _cache_date_is_today(path) for _key, path, _fn in _otif_tasks(ref_y, ref_m))


def _patch_otif_tiles_in_payload(ref_y: int, ref_m: int) -> bool:
    """Обновить PD-M2.1/PD-M2.2 в существующем snapshot, не трогая остальные плитки."""
    from . import calc_otif_vypusk_zam_proizvodstva
    from . import views

    path = _payload_path(ref_y, ref_m)
    raw = cache_manager._load_json_cache(path)
    payload = views._unwrap_payload_cache_envelope(raw) if raw else None
    if not payload:
        return False

    kpis_by_id = {
        str(row.get("kpi_id") or ""): row
        for row in views._get_kpi_dicts(PROD_DEPUTY_DASHBOARD_DEPT)
    }
    items = ((payload.get("Плитки") or {}).get("items") or [])
    patched = 0
    for tile in items:
        if not isinstance(tile, dict):
            continue
        kid = str(tile.get("kpi_id") or "").strip()
        if kid not in {"PD-M2", "PD-M2.1", "PD-M2.2"}:
            continue
        shop = "pc2" if kid == "PD-M2.2" else "pc1"
        data = calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly(
            year=ref_y,
            month=ref_m,
            shop=shop,
        )
        row = data.get("last_full_month_row") or {}
        entry = {
            "plan": row.get("plan"),
            "fact": row.get("fact"),
            "kpi_pct": row.get("kpi_pct"),
            "has_data": row.get("has_data"),
            "monthly_data": data.get("months") or [],
            "last_full_month_row": row,
            "ytd": data.get("ytd") or {},
            "kpi_period": data.get("kpi_period"),
        }
        kpi = kpis_by_id.get(kid) or {"kpi_id": kid}
        _pct, color = views._tile_color(kpi, entry)
        tile["plan"] = row.get("plan")
        tile["fact"] = row.get("fact")
        tile["kpi_pct"] = row.get("kpi_pct")
        tile["has_data"] = row.get("has_data")
        if color:
            tile["color"] = color
        tile["last_full_month_row"] = row
        tile["monthly_data"] = data.get("months") or []
        if data.get("ytd"):
            tile["ytd"] = data["ytd"]
        if data.get("kpi_period"):
            tile["kpi_period"] = data["kpi_period"]
        updated_at = views._tile_cache_updated_at(kid, ref_y, ref_m)
        if updated_at:
            tile["cache_updated_at"] = updated_at
        patched += 1

    if not patched:
        return False
    views._save_prod_deputy_payload_cache(ref_y, ref_m, payload)
    cache_manager.clear_memoized_dashboard_payload("prod_deputy_payload_")
    logger.info(
        "prod deputy cache scheduler: patched %d OTIF tiles in payload %04d-%02d",
        patched,
        ref_y,
        ref_m,
    )
    return True


def _rebuild_payload(ref_y: int, ref_m: int) -> None:
    if _patch_otif_tiles_in_payload(ref_y, ref_m):
        return
    from . import views

    kpis = views._get_kpi_dicts(PROD_DEPUTY_DASHBOARD_DEPT)
    views._build_prod_deputy_payload_fresh(
        PROD_DEPUTY_DASHBOARD_DEPT,
        kpis,
        month=ref_m,
        year=ref_y,
    )
    cache_manager.clear_memoized_dashboard_payload("prod_deputy_payload_")
    logger.info(
        "prod deputy cache scheduler: payload rebuilt [%s] %04d-%02d",
        PROD_DEPUTY_DASHBOARD_DEPT,
        ref_y,
        ref_m,
    )


def run_prod_deputy_otif_refresh_once(
    *,
    force: bool = True,
    month: int | None = None,
    year: int | None = None,
) -> dict[str, object]:
    """Один цикл: OTIF из erp_pm, затем snapshot дашборда заместителя по производству."""
    if not _run_lock.acquire(blocking=False):
        logger.warning("prod deputy cache scheduler: refresh already running, skip")
        return {"started": False, "reason": "already_running"}
    try:
        ref_y, ref_m = _ref_period(month, year)
        tasks = _otif_tasks(ref_y, ref_m)
        payload_path = _payload_path(ref_y, ref_m)
        cache_manager.register_cache_path(f"prod_deputy_payload_{ref_y}_{ref_m:02d}", payload_path)
        logger.info(
            "prod deputy cache scheduler: start refresh, period=%04d-%02d",
            ref_y,
            ref_m,
        )
        with cache_manager.mark_paths_refreshing("prod_deputy_daily_payload", [payload_path]):
            cache_manager._run_warm_tasks(tasks, force=force)
            _rebuild_payload(ref_y, ref_m)
        logger.info("prod deputy cache scheduler: refresh complete")
        return {
            "started": True,
            "tasks": len(tasks),
            "year": ref_y,
            "month": ref_m,
        }
    finally:
        _run_lock.release()


def start_catch_up_if_stale() -> dict[str, object]:
    """Если сервер подняли после 08:00, а снимок за сегодня ещё не готов — пересчитать сразу."""
    now = datetime.now()
    scheduled = now.replace(
        hour=PROD_DEPUTY_CACHE_REFRESH_HOUR,
        minute=PROD_DEPUTY_CACHE_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    ref_y, ref_m = _ref_period()
    if now < scheduled:
        return {"started": False, "reason": "before_scheduled_time", "year": ref_y, "month": ref_m}
    if not _is_today_snapshot_stale(ref_y, ref_m):
        return {"started": False, "reason": "fresh", "year": ref_y, "month": ref_m}
    if _run_lock.locked():
        return {"started": False, "reason": "already_running", "year": ref_y, "month": ref_m}

    def _runner() -> None:
        try:
            run_prod_deputy_otif_refresh_once(force=True)
        except Exception:
            logger.exception("prod deputy cache scheduler: catch-up refresh failed")

    threading.Thread(
        target=_runner,
        name=f"prod-deputy-otif-catch-up-{ref_y}-{ref_m:02d}",
        daemon=True,
    ).start()
    logger.info(
        "prod deputy cache scheduler: catch-up refresh queued, period=%04d-%02d",
        ref_y,
        ref_m,
    )
    return {"started": True, "year": ref_y, "month": ref_m}


def _scheduler_loop() -> None:
    while True:
        delay = _seconds_until_next_run()
        logger.info(
            "prod deputy cache scheduler: next run in %.0fs at %02d:%02d",
            delay,
            PROD_DEPUTY_CACHE_REFRESH_HOUR,
            PROD_DEPUTY_CACHE_REFRESH_MINUTE,
        )
        time.sleep(delay)
        try:
            run_prod_deputy_otif_refresh_once(force=True)
        except Exception:
            logger.exception("prod deputy cache scheduler: scheduled refresh failed")


def start_prod_deputy_cache_scheduler() -> None:
    global _scheduler_started
    if not PROD_DEPUTY_CACHE_REFRESH_ENABLED:
        logger.info("prod deputy cache scheduler: disabled")
        return
    if os.environ.get("RUN_MAIN") != "true":
        return
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
        thread = threading.Thread(
            target=_scheduler_loop,
            name="prod-deputy-cache-scheduler",
            daemon=True,
        )
        thread.start()
    logger.info(
        "prod deputy cache scheduler: started, daily at %02d:%02d",
        PROD_DEPUTY_CACHE_REFRESH_HOUR,
        PROD_DEPUTY_CACHE_REFRESH_MINUTE,
    )
    start_catch_up_if_stale()
