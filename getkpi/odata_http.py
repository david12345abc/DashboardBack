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
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# HTTP-коды, на которых делаем повтор (всё, кроме 200…399 и 4xx-логических).
RETRY_STATUS = frozenset({401, 403, 408, 409, 425, 429, 500, 502, 503, 504})


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
