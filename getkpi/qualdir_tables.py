"""Таблицы брака (QD-M1 / QD-M5) в ответе API qualdir."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

TABLE_ID_EXTERNAL = "QD-T-M1"
TABLE_ID_INTERNAL = "QD-T-M5"


def merge_qualdir_brak_tables(
    tablitsy: dict,
    *,
    year: int,
    month: int,
) -> None:
    from qualdir.brak_tables import merge_qualdir_brak_tables as _merge

    try:
        _merge(tablitsy, year=year, month=month)
    except Exception:
        logger.exception("Не удалось загрузить таблицы брака qualdir за %d-%02d", year, month)


def enrich_payload_tables(payload: dict) -> dict:
    """Добавить таблицы QD-T-M1 и QD-T-M5 в готовый payload qualdir."""
    tables = payload.get("Таблицы")
    if not isinstance(tables, dict):
        tables = {}
        payload["Таблицы"] = tables

    year = payload.get("year")
    month = payload.get("month") or payload.get("kpi_ref_month")
    if year is None or month is None:
        return payload

    merge_qualdir_brak_tables(tables, year=int(year), month=int(month))
    return payload
