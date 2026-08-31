"""Таблица просроченных задач протоколов 1С в ответе API по department."""

from __future__ import annotations



import logging
import os

from pathlib import Path



from getkpi.cache_manager import locked_call, schedule_background_refresh



logger = logging.getLogger(__name__)



TABLE_ID = "DEPT-T-PROTOCOL-OVERDUE"





def warm_stamp_path() -> Path:

    from tools.dept_protocol.table_cache import warm_stamp_path as _path



    return _path()





def warm_all_department_caches() -> None:

    """Прогреть кэш всех подразделений одним проходом OData."""

    from tools.dept_protocol.dashboard_table import warm_all_department_tables



    # Lock is already held by cache_manager.locked_call(warm_lock_key()).

    # A nested locked_call on the same key deadlocks (threading.Lock is not reentrant).

    warm_all_department_tables()





def warm_department_cache(

    department: str,

    *,

    year: int | None = None,

    month: int | None = None,

) -> dict | None:

    """Прогреть кэш для одного подразделения."""

    from tools.dept_protocol.dashboard_table import build_protocol_overdue_table



    return build_protocol_overdue_table(department, year=year, month=month)





def build_protocol_overdue_table_cached(

    department: str,

    *,

    year: int | None = None,

    month: int | None = None,

) -> dict | None:

    from tools.dept_protocol.dashboard_table import (

        build_from_cached_months,

        build_protocol_overdue_table,

        month_pairs_from_start,

        normalize_ref_period,

    )

    from tools.dept_protocol.table_cache import lock_key



    ref_y, ref_m = normalize_ref_period(year, month)

    pairs = month_pairs_from_start(ref_y, ref_m)

    if not pairs:

        return None



    cached_table = build_from_cached_months(department, ref_y, ref_m)

    if cached_table is not None and cached_table.get("months_total") == len(pairs):

        return cached_table

    stale_table = build_from_cached_months(department, ref_y, ref_m, allow_stale=True)

    if stale_table is not None:

        return stale_table

    if os.getenv("DEPT_PROTOCOL_LIVE_ON_REQUEST", "").strip().lower() not in {"1", "true", "yes"}:

        logger.info(

            "%s: нет готового кэша для «%s», пропускаем live OData при обычном запросе",

            TABLE_ID,

            department,

        )

        return None



    stale_table = build_from_cached_months(department, ref_y, ref_m, allow_stale=True)

    if stale_table is not None and stale_table.get("months_total") == len(pairs):

        key = lock_key(department)

        schedule_background_refresh(

            key,

            lambda: build_protocol_overdue_table(department, year=ref_y, month=ref_m),

        )

        return stale_table



    key = lock_key(department)



    def _runner() -> dict | None:

        return build_protocol_overdue_table(department, year=ref_y, month=ref_m)



    result = locked_call(key, _runner)

    if result is not None:

        return result



    return build_from_cached_months(department, ref_y, ref_m, allow_stale=True)





def merge_protocol_overdue_table(

    tablitsy: dict,

    department: str,

    *,

    year: int | None = None,

    month: int | None = None,

) -> None:

    """

    Добавляет в ``tablitsy`` таблицу DEPT-T-PROTOCOL-OVERDUE, если department

    есть в source_department_holders_compact.json и удалось определить руководителя.

    """

    try:

        table = build_protocol_overdue_table_cached(department, year=year, month=month)

    except Exception:

        logger.exception(

            "%s: не удалось загрузить просроченные задачи протоколов для «%s»",

            TABLE_ID,

            department,

        )

        return



    if table is not None:

        tablitsy[TABLE_ID] = table





def enrich_payload_tables(payload: dict, department: str) -> dict:

    """Добавить таблицу протоколов в готовый payload (komdir/chairman/universal)."""

    tables = payload.get("Таблицы")

    if not isinstance(tables, dict):

        tables = {}

        payload["Таблицы"] = tables

    year = payload.get("year")

    month = payload.get("month") or payload.get("kpi_ref_month")

    merge_protocol_overdue_table(tables, department, year=year, month=month)

    return payload

