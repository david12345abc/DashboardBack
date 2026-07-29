"""Склейка KPI дашборда службы управления персоналом для общего ``getkpi.views``."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from sup.hrd_m1 import get_hrd_m1_ytd
from sup.hrd_m2 import get_hrd_m2_ytd
from sup.hrd_m3 import get_hrd_m3_ytd
from sup.hrd_m4 import get_hrd_m4_ytd
from sup.hrd_q4 import get_hrd_q4_ytd

SUP_KPI_IDS: frozenset[str] = frozenset({"HRD-M1", "HRD-M2", "HRD-M3", "HRD-M4", "HRD-Q4"})
SUP_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = SUP_KPI_IDS
SUP_FOT_LIMIT_KPI_IDS: frozenset[str] = frozenset({"HRD-M2"})
SUP_BUDGET_LIMIT_KPI_IDS: frozenset[str] = frozenset({"HRD-M3"})
SUP_TURNOVER_FACT_RAG_IDS: frozenset[str] = frozenset({"HRD-M4", "HRD-Q4"})
SUP_HIGHER_BETTER_90_80_IDS: frozenset[str] = frozenset({"HRD-M1"})


def _normalize_sup_kpi_id(kpi_id: str) -> str:
    kid = str(kpi_id or "").strip().upper()
    for cyr, lat in (("М", "M"), ("С", "C")):
        kid = kid.replace(cyr, lat)
    return kid


_SUP_KPI_IDS_NORM = frozenset(_normalize_sup_kpi_id(x) for x in SUP_KPI_IDS)


def is_sup_tile_kpi_id(kpi_id: str) -> bool:
    return _normalize_sup_kpi_id(kpi_id) in _SUP_KPI_IDS_NORM


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    """Файлы кэша, по mtime которых на плитке показывается ``cache_updated_at``."""
    from sup.hrd_m1 import cache_file_path_for_period as hrd_m1_cache
    from sup.hrd_m2 import cache_file_path_for_period as hrd_m2_cache, monthly_cache_path as hrd_m2_monthly
    from sup.hrd_m3 import cache_file_path_for_period as hrd_m3_cache, monthly_cache_path as hrd_m3_monthly
    from sup.hrd_m4 import cache_file_path_for_period as hrd_m4_cache
    from sup.hrd_q4 import cache_file_path_for_period as hrd_q4_cache

    kid = _normalize_sup_kpi_id(kpi_id)
    paths: list[Path] = []

    if kid == "HRD-M1":
        # Только JSON-кэш расчёта. HC_сводный_*.xls не включаем: иначе
        # «Обновлено» залипает на дате файла HR (напр. 02.07), хотя плитку
        # пересчитали сегодня. Инвалидация по xls — через source_mtime_ns в hrd_m1.
        paths.append(hrd_m1_cache(ref_y, ref_m))
    elif kid == "HRD-M2":
        paths.extend([
            hrd_m2_cache(ref_y, ref_m),
            hrd_m2_monthly(ref_y, ref_m),
        ])
    elif kid == "HRD-M3":
        paths.extend([
            hrd_m3_cache(ref_y, ref_m),
            hrd_m3_monthly(ref_y, ref_m),
        ])
    elif kid == "HRD-M4":
        paths.append(hrd_m4_cache(ref_y, ref_m))
    elif kid == "HRD-Q4":
        paths.append(hrd_q4_cache(ref_y, ref_m))

    return paths


def rag_hrd_m1_pct(pct: float | None) -> str:
    """HRD-M1: ≥90 % — зелёный, 80–89,9 % — жёлтый, <80 % — красный (пороги из БД)."""
    if pct is None:
        return "unknown"
    if pct >= 90:
        return "green"
    if pct >= 80:
        return "yellow"
    return "red"


def rag_hrd_turnover_fact_pct(fact_pct: float | None, *, kpi_id: str) -> str:
    """HRD-M4 / HRD-Q4: цвет по факту (%, меньше — лучше), пороги из карточки KPI."""
    if fact_pct is None:
        return "unknown"
    kid = str(kpi_id or "").strip().upper()
    if kid == "HRD-M4":
        if fact_pct < 85:
            return "green"
    else:
        if fact_pct <= 85:
            return "green"
    if fact_pct <= 94.9:
        return "yellow"
    if fact_pct > 95:
        return "red"
    return "yellow"

_PAYLOAD_BUILDERS = {
    "HRD-M1": get_hrd_m1_ytd,
    "HRD-M2": get_hrd_m2_ytd,
    "HRD-M3": get_hrd_m3_ytd,
    "HRD-M4": get_hrd_m4_ytd,
    "HRD-Q4": get_hrd_q4_ytd,
}


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    builder = _PAYLOAD_BUILDERS.get(str(kpi_id or "").strip().upper())
    if builder is None:
        return False
    payload = builder(year=year, month=month)
    if payload is None:
        return False
    entry["data_granularity"] = payload.get("data_granularity", "monthly")
    entry["monthly_data"] = payload.get("monthly_data") or []
    entry["last_full_month_row"] = payload.get("last_full_month_row")
    entry["ytd"] = payload.get("ytd") or {}
    entry["kpi_period"] = payload.get("kpi_period")
    if payload.get("tables") is not None:
        entry["tables"] = payload["tables"]
    if payload.get("reference_analytics") is not None:
        entry["reference_analytics"] = payload["reference_analytics"]
    if payload.get("debug") is not None:
        entry["debug"] = payload["debug"]
    return True


def merge_sup_tables_into_universal_payload(
    tablitsy: dict[str, Any],
    entries_by_id: dict[str, dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> None:
    hrd_m1 = entries_by_id.get("HRD-M1") or {}
    tables = hrd_m1.get("tables") or {}
    if not isinstance(tables, dict):
        return
    for key, table in tables.items():
        if isinstance(table, dict):
            tablitsy[key] = table

    # Если фронт меняет месяц без ?month=, подменяем основную таблицу
    # из BY-MONTH по запрошенному ref_m (уже лежит в payload).
    by_month = tables.get("HRD-T-M1-LATE-VACANCIES-BY-MONTH")
    if isinstance(by_month, dict):
        chosen = by_month.get(str(ref_m)) or by_month.get(ref_m)
        if isinstance(chosen, dict):
            tablitsy["HRD-T-M1-LATE-VACANCIES"] = chosen
