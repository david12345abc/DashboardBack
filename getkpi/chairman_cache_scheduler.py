# -*- coding: utf-8 -*-
"""Пересчёт дашборда председателя совета директоров в 06:00, 13:00 и 17:00.

Запускает сервер, независимо от того, открыт ли дашборд у клиента.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)


def _parse_hours(raw: str) -> tuple[int, ...]:
    hours: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit() and 0 <= int(part) <= 23:
            hours.add(int(part))
    return tuple(sorted(hours))


CHAIRMAN_REFRESH_HOURS = _parse_hours(os.getenv("CHAIRMAN_REFRESH_HOURS", "6,13,17"))
CHAIRMAN_REFRESH_ENABLED = os.getenv("CHAIRMAN_REFRESH_ENABLED", "1").lower() not in {
    "0",
    "false",
    "no",
}

CHAIRMAN_DEPARTMENT = "Председатель совета директоров"

_scheduler_started = False
_scheduler_lock = threading.Lock()
_run_lock = threading.Lock()


def _next_run_at(now: datetime | None = None) -> datetime:
    now = now or datetime.now()
    for day_offset in (0, 1):
        day = now.date() + timedelta(days=day_offset)
        for hour in CHAIRMAN_REFRESH_HOURS:
            candidate = datetime(day.year, day.month, day.day, hour)
            if candidate > now:
                return candidate
    return now + timedelta(days=1)


def run_chairman_refresh_once(
    *,
    month: int | None = None,
    year: int | None = None,
) -> dict[str, object]:
    """Один цикл: «Мой дашборд» и коммерческий блок ПСД за текущий месяц."""
    from . import chairman_data
    from . import views

    if not _run_lock.acquire(blocking=False):
        logger.warning("chairman cache scheduler: refresh already running, skip")
        return {"started": False, "reason": "already_running"}
    try:
        today = date.today()
        ref_y = int(year or today.year)
        ref_m = max(1, min(12, int(month or today.month)))
        blocks = (
            chairman_data.CHAIRMAN_BLOCK_MY_DASHBOARD,
            chairman_data.CHAIRMAN_BLOCK_COMMERCE,
        )
        results: dict[str, str] = {}
        for for_block in blocks:
            claimed, key, _entry = views._begin_chairman_refresh(
                CHAIRMAN_DEPARTMENT, for_block, ref_y, ref_m, initiator="server",
            )
            if not claimed:
                results[for_block] = "already_running"
                continue
            t0 = time.monotonic()
            logger.info(
                "chairman cache scheduler: refresh [%s] %04d-%02d started",
                for_block,
                ref_y,
                ref_m,
            )
            views._chairman_dashboard_refresh_worker(
                key=key,
                department=CHAIRMAN_DEPARTMENT,
                for_raw=for_block,
                ref_y=ref_y,
                ref_m=ref_m,
            )
            status = str(views._chairman_refresh_payload(key).get("status") or "")
            results[for_block] = status
            logger.info(
                "chairman cache scheduler: refresh [%s] %s in %.1fs",
                for_block,
                status,
                time.monotonic() - t0,
            )
        return {"started": True, "year": ref_y, "month": ref_m, "blocks": results}
    finally:
        _run_lock.release()


def _scheduler_loop() -> None:
    while True:
        run_at = _next_run_at()
        delay = max(1.0, (run_at - datetime.now()).total_seconds())
        logger.info(
            "chairman cache scheduler: next run in %.0fs at %s",
            delay,
            run_at.strftime("%Y-%m-%d %H:%M"),
        )
        time.sleep(delay)
        try:
            run_chairman_refresh_once()
        except Exception:
            logger.exception("chairman cache scheduler: scheduled refresh failed")


def start_chairman_cache_scheduler() -> None:
    global _scheduler_started
    if not CHAIRMAN_REFRESH_ENABLED or not CHAIRMAN_REFRESH_HOURS:
        logger.info("chairman cache scheduler: disabled")
        return
    if os.environ.get("RUN_MAIN") != "true":
        return
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
        threading.Thread(
            target=_scheduler_loop,
            name="chairman-cache-scheduler",
            daemon=True,
        ).start()
    logger.info(
        "chairman cache scheduler: started, daily at %s",
        ", ".join(f"{h:02d}:00" for h in CHAIRMAN_REFRESH_HOURS),
    )
