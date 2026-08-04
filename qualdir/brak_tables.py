"""Таблицы внешнего (QD-M1), внутреннего (QD-M5) брака и формы 03-17 (QD-M8) для qualdir.

Источник данных — SQL-бэкенд 1С (erp_pm) через ``qualdir.brak_tables_sql``
(эталон-справочники, как в плитках qualdir.qd_m1 / qd_m5 / qd_m8). Ранее строки
таблиц загружались через OData (qualdir.brak_report); теперь OData не используется.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from getkpi.cache_manager import stale_while_revalidate
from devdir import ytd_json_cache
from getkpi.techdir_tekuchet import MONTH_RU

from sql_connection import SqlConnection
from qualdir.brak_tables_sql import (
    BRAK_TABLE_COLUMNS,
    load_brak_table_rows_sql,
)

logger = logging.getLogger(__name__)

TABLE_ID_EXTERNAL = "QD-T-M1"
TABLE_ID_INTERNAL = "QD-T-M5"
TABLE_ID_FORMA0317 = "QD-T-M8"

# Логические источники (документы 1С) — для полей source/description в payload.
EXTERNAL_BRAK_ENTITY = "Document_ТД_Форма0319"
INTERNAL_BRAK_ENTITY = "Document_ТД_Форма0318"
FORM_0317_ENTITY = "Document_ТД_Форма0317"

_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
# v8 — переход с OData на SQL-источник строк.
TABLE_MONTH_CACHE_VERSION = 8
TABLE_YTD_DISK_TAG = "qualdir_brak_table_ytd_v8"
TABLE_YTD_DISK_VERSION = 8


def _month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    ref_month = max(1, min(12, int(ref_month)))
    return [(year, mm) for mm in range(1, ref_month + 1)]


def _month_table_cache_path(table_kind: str, year: int, month: int) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_{table_kind}_brak_table_{year}_{month:02d}.json"


def _ytd_table_cache_path(table_kind: str, year: int, month: int) -> Path:
    return _CACHE_ROOT / f"qualdir_{table_kind}_brak_table_ytd_{year}_{month:02d}.json"


def _month_cache_is_perpetual(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def _load_month_table_cache(table_kind: str, year: int, month: int) -> list[dict[str, str]] | None:
    path = _month_table_cache_path(table_kind, year, month)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") != TABLE_MONTH_CACHE_VERSION:
        return None
    if data.get("table_kind") != table_kind:
        return None
    if not _month_cache_is_perpetual(year, month):
        if data.get("cache_date") != date.today().isoformat():
            return None
    rows = data.get("rows")
    if not isinstance(rows, list):
        return None
    if rows and isinstance(rows[0], dict) and (
        "Значимая форма" not in rows[0] or "Статус" not in rows[0]
    ):
        return None
    return rows


def _save_month_table_cache(
    table_kind: str,
    year: int,
    month: int,
    rows: list[dict[str, str]],
) -> None:
    try:
        with _month_table_cache_path(table_kind, year, month).open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "table_kind": table_kind,
                    "cache_version": TABLE_MONTH_CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "year": year,
                    "month": month,
                    "rows": rows,
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def _load_rows_cached(
    table_kind: str,
    year: int,
    month: int,
    *,
    connection: Any | None = None,
) -> list[dict[str, str]]:
    cached = _load_month_table_cache(table_kind, year, month)
    if cached is not None:
        return cached

    try:
        rows = load_brak_table_rows_sql(table_kind, year, month, connection=connection)
    except Exception as exc:
        logger.warning(
            "qualdir brak table %s %d-%02d: %s",
            table_kind,
            year,
            month,
            exc,
        )
        rows = []
    else:
        _save_month_table_cache(table_kind, year, month, rows)

    return rows


def _month_block(
    table_kind: str,
    year: int,
    month: int,
    *,
    connection: Any | None = None,
) -> dict[str, Any]:
    rows = _load_rows_cached(table_kind, year, month, connection=connection)
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month].lower(),
        "columns": list(BRAK_TABLE_COLUMNS),
        "rows": rows,
        "row_count": len(rows),
        "has_data": len(rows) > 0,
    }


def _assemble_brak_table(
    *,
    table_id: str,
    kpi_id: str,
    title: str,
    description: str,
    source_entity: str,
    table_kind: str,
    ref_y: int,
    ref_m: int,
    connection: Any | None = None,
) -> dict[str, Any]:
    monthly_data: list[dict[str, Any]] = []
    for y, m in _month_pairs(ref_y, ref_m):
        monthly_data.append(
            _month_block(table_kind, y, m, connection=connection),
        )

    ref_block = next(
        (block for block in monthly_data if block.get("year") == ref_y and block.get("month") == ref_m),
        monthly_data[-1] if monthly_data else {},
    )
    ref_rows = ref_block.get("rows") or []
    month_name = MONTH_RU[ref_m]

    return {
        "kpi_id": kpi_id,
        "name": f"{title} (январь–{month_name.lower()} {ref_y})",
        "periodicity": "ежемесячно",
        "data_granularity": "monthly",
        "description": description,
        "period": {
            "year": ref_y,
            "month": ref_m,
            "month_name": month_name.lower(),
        },
        "source": source_entity,
        "columns": list(BRAK_TABLE_COLUMNS),
        "monthly_data": monthly_data,
        "rows": ref_rows,
        "row_count": ref_block.get("row_count", len(ref_rows)),
        "months_with_data": sum(1 for block in monthly_data if block.get("has_data")),
        "months_total": len(monthly_data),
    }


def _load_ytd_table_cache(table_kind: str, ref_y: int, ref_m: int) -> dict[str, Any] | None:
    path = _ytd_table_cache_path(table_kind, ref_y, ref_m)
    return ytd_json_cache.load_payload(
        path,
        source_tag=f"{TABLE_YTD_DISK_TAG}_{table_kind}",
        version=TABLE_YTD_DISK_VERSION,
        perpetual=ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m),
    )


def _save_ytd_table_cache(table_kind: str, ref_y: int, ref_m: int, payload: dict[str, Any]) -> None:
    ytd_json_cache.save_payload(
        _ytd_table_cache_path(table_kind, ref_y, ref_m),
        payload,
        source_tag=f"{TABLE_YTD_DISK_TAG}_{table_kind}",
        version=TABLE_YTD_DISK_VERSION,
    )


def _all_months_cached(table_kind: str, ref_y: int, ref_m: int) -> bool:
    return all(
        _load_month_table_cache(table_kind, y, m) is not None
        for y, m in _month_pairs(ref_y, ref_m)
    )


def _build_table(
    *,
    table_kind: str,
    table_id: str,
    kpi_id: str,
    title: str,
    description: str,
    source_entity: str,
    ref_y: int,
    ref_m: int,
) -> dict[str, Any]:
    cached = _load_ytd_table_cache(table_kind, ref_y, ref_m)
    if cached is not None:
        return cached

    def _assemble(connection: Any | None) -> dict[str, Any]:
        return _assemble_brak_table(
            table_id=table_id,
            kpi_id=kpi_id,
            title=title,
            description=description,
            source_entity=source_entity,
            table_kind=table_kind,
            ref_y=ref_y,
            ref_m=ref_m,
            connection=connection,
        )

    # Одно SQL-подключение на все месяцы; если все месяцы уже в кэше — без подключения.
    if _all_months_cached(table_kind, ref_y, ref_m):
        payload = _assemble(None)
    else:
        sql = SqlConnection()
        with sql.connect_ctx() as conn:
            conn.timeout = 0
            payload = _assemble(conn)

    _save_ytd_table_cache(table_kind, ref_y, ref_m, payload)
    return payload


def build_external_brak_table(ref_y: int, ref_m: int) -> dict[str, Any]:
    return _build_table(
        table_kind="external",
        table_id=TABLE_ID_EXTERNAL,
        kpi_id="QD-M1",
        title="Внешний брак",
        description="Документы Document_ТД_Форма0319 помесячно с января",
        source_entity=EXTERNAL_BRAK_ENTITY,
        ref_y=ref_y,
        ref_m=ref_m,
    )


def build_internal_brak_table(ref_y: int, ref_m: int) -> dict[str, Any]:
    return _build_table(
        table_kind="internal",
        table_id=TABLE_ID_INTERNAL,
        kpi_id="QD-M5",
        title="Внутренний брак",
        description="Документы Document_ТД_Форма0318 помесячно с января",
        source_entity=INTERNAL_BRAK_ENTITY,
        ref_y=ref_y,
        ref_m=ref_m,
    )


def build_forma0317_table(ref_y: int, ref_m: int) -> dict[str, Any]:
    return _build_table(
        table_kind="forma0317",
        table_id=TABLE_ID_FORMA0317,
        kpi_id="QD-M8",
        title="Форма 03-17",
        description="Документы Document_ТД_Форма0317 помесячно с января",
        source_entity=FORM_0317_ENTITY,
        ref_y=ref_y,
        ref_m=ref_m,
    )


def merge_qualdir_brak_tables(
    tablitsy: dict,
    *,
    year: int,
    month: int,
) -> None:
    """QD-T-M1, QD-T-M5, QD-T-M8 — помесячно с января."""
    ref_y, ref_m = int(year), max(1, min(12, int(month)))
    lock_key = f"qualdir_brak_tables_{ref_y}_{ref_m:02d}"

    def _load_fresh() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        external = _load_ytd_table_cache("external", ref_y, ref_m)
        internal = _load_ytd_table_cache("internal", ref_y, ref_m)
        forma0317 = _load_ytd_table_cache("forma0317", ref_y, ref_m)
        if external is None or internal is None or forma0317 is None:
            return None
        return external, internal, forma0317

    def _load_stale() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        external = ytd_json_cache.load_stale_payload(
            _ytd_table_cache_path("external", ref_y, ref_m),
            source_tag=f"{TABLE_YTD_DISK_TAG}_external",
            version=TABLE_YTD_DISK_VERSION,
        )
        internal = ytd_json_cache.load_stale_payload(
            _ytd_table_cache_path("internal", ref_y, ref_m),
            source_tag=f"{TABLE_YTD_DISK_TAG}_internal",
            version=TABLE_YTD_DISK_VERSION,
        )
        forma0317 = ytd_json_cache.load_stale_payload(
            _ytd_table_cache_path("forma0317", ref_y, ref_m),
            source_tag=f"{TABLE_YTD_DISK_TAG}_forma0317",
            version=TABLE_YTD_DISK_VERSION,
        )
        if external is None or internal is None or forma0317 is None:
            return None
        return external, internal, forma0317

    def _compute() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        return (
            build_external_brak_table(ref_y, ref_m),
            build_internal_brak_table(ref_y, ref_m),
            build_forma0317_table(ref_y, ref_m),
        )

    external_table, internal_table, forma0317_table = stale_while_revalidate(
        lock_key,
        _load_fresh,
        _load_stale,
        _compute,
    )
    tablitsy[TABLE_ID_EXTERNAL] = external_table
    tablitsy[TABLE_ID_INTERNAL] = internal_table
    tablitsy[TABLE_ID_FORMA0317] = forma0317_table
