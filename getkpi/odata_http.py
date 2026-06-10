"""
odata_http.py — общий HTTP-хелпер для запросов к 1С OData с retry/backoff.

1С OData периодически отвечает 401 «Доступ запрещён», 429 или 5xx при:
  - пиковой нагрузке (много параллельных сессий);
  - кратковременных блокировках в 1С;
  - исчерпании лицензий / лимита подключений.

Повторный запрос через 1–5 секунд в ≥95% случаев проходит. Этот модуль
даёт единую точку для таких повторов, чтобы не дублировать retry-логику
по всем calc_*.py.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# HTTP-коды, на которых делаем повтор (всё, кроме 200…399 и 4xx-логических).
# 401/402/403 не ретраим: для 1С это часто ошибка прав/учётки, а повторные попытки
# могут быстро заблокировать OData-пользователя.
RETRY_STATUS = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
ACCESS_DENIED_STATUS = frozenset({401, 402, 403})

_guard_lock = threading.Lock()
_access_guard_enabled = False
_access_guard_reason: str | None = None


def reset_access_guard(*, enabled: bool = True) -> None:
    """Сбросить и при необходимости включить guard для долгого прогрева кэшей."""
    global _access_guard_enabled, _access_guard_reason
    with _guard_lock:
        _access_guard_enabled = enabled
        _access_guard_reason = None


def disable_access_guard() -> None:
    """Выключить guard для обычных пользовательских запросов."""
    global _access_guard_enabled, _access_guard_reason
    with _guard_lock:
        _access_guard_enabled = False
        _access_guard_reason = None


def is_access_guard_open() -> bool:
    with _guard_lock:
        return _access_guard_reason is not None


def access_guard_reason() -> str | None:
    with _guard_lock:
        return _access_guard_reason


def _trip_access_guard(status_code: int, url: str, label: str) -> None:
    global _access_guard_reason
    with _guard_lock:
        if not _access_guard_enabled or _access_guard_reason is not None:
            return
        _access_guard_reason = f"{label}: HTTP {status_code} for {url[:220]}"


def request_with_retry(
    session: requests.Session,
    url: str,
    *,
    method: str = "GET",
    timeout: int | float = 60,
    retries: int = 4,
    backoff: float = 1.7,
    initial_delay: float = 1.2,
    label: str = "odata",
) -> Optional[requests.Response]:
    """
    Выполнить HTTP-запрос с повторами при временных сбоях.

    Возвращает Response (даже если итоговый статус != 200) или None,
    если ни одной попытки не удалось дойти до сервера.

    retries=4 → максимум 5 попыток; базовая пауза 1.2с → 2.0с → 3.5с → 6с → 10с.
    """
    if is_access_guard_open():
        logger.warning("%s: OData access guard is open, skip request: %s", label, access_guard_reason())
        return None

    delay = float(initial_delay)
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            r = session.request(method, url, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= retries:
                logger.warning("%s: network error after %d attempts: %s", label, attempt + 1, exc)
                return None
            logger.info("%s: network error, retry %d/%d in %.1fs: %s",
                        label, attempt + 1, retries, delay, exc)
            time.sleep(delay)
            delay *= backoff
            continue

        if r.status_code in ACCESS_DENIED_STATUS:
            _trip_access_guard(r.status_code, url, label)
            return r

        if r.status_code in RETRY_STATUS and attempt < retries:
            logger.info("%s: HTTP %d, retry %d/%d in %.1fs",
                        label, r.status_code, attempt + 1, retries, delay)
            time.sleep(delay)
            delay *= backoff
            continue

        return r

    if last_exc:
        raise last_exc
    return None
