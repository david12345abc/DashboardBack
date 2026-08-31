"""Daily native erp_pm restore at 21:00, then commercial cache refresh."""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
load_dotenv()

logger = logging.getLogger(__name__)

_scheduler_started = False
_scheduler_lock = threading.Lock()
_run_lock = threading.Lock()


def _env_flag(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).lower() not in {"0", "false", "no"}


def _restore_hour() -> int:
    return int(os.getenv("MSSQL_RESTORE_HOUR", "21"))


def _restore_minute() -> int:
    return int(os.getenv("MSSQL_RESTORE_MINUTE", "0"))


def _seconds_until_next_run(now: datetime | None = None) -> float:
    now = now or datetime.now()
    target = now.replace(
        hour=_restore_hour(),
        minute=_restore_minute(),
        second=0,
        microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def run_restore_once(*, force: bool = False) -> dict[str, Any]:
    """Run one restore cycle; on success with a new DB, refresh commercial caches."""
    if not _run_lock.acquire(blocking=False):
        logger.warning("restore scheduler: restore already running, skip")
        return {"started": False, "reason": "already_running"}

    try:
        from sql_restore.restore_native import run_restore

        logger.info("restore scheduler: starting native restore (force=%s)", force)
        result = run_restore(force=force)
        status = str(result.get("status") or "")
        logger.info("restore scheduler: restore finished status=%s", status)

        if status == "restored":
            try:
                from . import commercial_cache_scheduler

                logger.info(
                    "restore scheduler: starting commercial cache refresh after restore"
                )
                refresh = commercial_cache_scheduler.run_commercial_cache_refresh_once(
                    force=True
                )
                result["commercial_refresh"] = refresh
            except Exception:
                logger.exception(
                    "restore scheduler: commercial cache refresh failed after restore"
                )
                result["commercial_refresh"] = {"started": False, "reason": "error"}
            try:
                from datetime import date as date_cls

                from . import cache_manager, calc_metrolog_production_plan

                today = date_cls.today()
                logger.info(
                    "restore scheduler: refreshing METD-M1 from erp_pm after restore"
                )
                with cache_manager.force_compute():
                    calc_metrolog_production_plan.get_metrolog_production_plan_monthly(
                        today.year, today.month
                    )
                payload_path = (
                    cache_manager.CACHE_DIR
                    / f"chief_metrolog_payload_{today.year}_{today.month:02d}.json"
                )
                payload_path.unlink(missing_ok=True)
                cache_manager.clear_memoized_dashboard_payload("chief_metrolog_payload_")
                result["metrolog_m1_refresh"] = {"started": True, "year": today.year, "month": today.month}
            except Exception:
                logger.exception(
                    "restore scheduler: METD-M1 refresh failed after restore"
                )
                result["metrolog_m1_refresh"] = {"started": False, "reason": "error"}
        elif status == "skipped":
            logger.info(
                "restore scheduler: bak unchanged; commercial refresh not triggered"
            )
        return {"started": True, **result}
    except Exception:
        logger.exception("restore scheduler: native restore failed")
        return {"started": True, "status": "error"}
    finally:
        _run_lock.release()


def _scheduler_loop() -> None:
    while True:
        delay = _seconds_until_next_run()
        logger.info(
            "restore scheduler: next run in %.0fs at %02d:%02d",
            delay,
            _restore_hour(),
            _restore_minute(),
        )
        time.sleep(delay)
        try:
            run_restore_once(force=False)
        except Exception:
            logger.exception("restore scheduler: scheduled run failed")


def start_restore_scheduler() -> None:
    global _scheduler_started
    if not _env_flag("MSSQL_RESTORE_SCHEDULER_ENABLED", "1"):
        logger.info("restore scheduler: disabled")
        return
    if os.environ.get("RUN_MAIN") != "true":
        return
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
        thread = threading.Thread(
            target=_scheduler_loop,
            name="mssql-restore-scheduler",
            daemon=True,
        )
        thread.start()
    logger.info(
        "restore scheduler: started, daily at %02d:%02d",
        _restore_hour(),
        _restore_minute(),
    )
