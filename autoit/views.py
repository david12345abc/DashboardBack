"""Склейка KPI дашборда «Начальник отдела автоматизации ИТ» для ``getkpi.views``."""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

from getkpi.autoit.it_m1_sla import get_it_m1_sla_ytd
from getkpi.autoit.it_m3 import get_it_m3_ytd
from getkpi.autoit.it_m4_fot import get_it_m4_fot_ytd
from getkpi.autoit.it_q2_tekuchest import get_it_q2_tekuchest_ytd

from c1auto.views import is_c1auto_department

DEPARTMENT = "Начальник отдела автоматизации ИТ"

AUTOIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M1", "IT-M1",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "ИТ-M4", "IT-M4",
    "ИТ-Q2", "IT-Q2",
    "ИТ-Y1", "IT-Y1",
    "ИТ-C1", "IT-C1",
    "ИТ-C2", "IT-C2",
    "ИТ-C3", "IT-C3",
    "ИТ-B1", "IT-B1",
    "ИТ-B2", "IT-B2",
})

AUTOIT_CACHED_TILE_IDS: frozenset[str] = frozenset({
    "IT-M1", "IT-M3", "IT-M4", "IT-Q2",
})

AUTOIT_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "ИТ-M1", "IT-M1",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "ИТ-M4", "IT-M4",
    "ИТ-Q2", "IT-Q2",
    "ИТ-Y1", "IT-Y1",
})

AUTOIT_RUB_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M3", "IT-M3",
    "ИТ-M4", "IT-M4",
})

AUTOIT_BUDGET_LIMIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M3", "IT-M3",
})

AUTOIT_SLA_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M1", "IT-M1",
})

AUTOIT_FOT_LIMIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M4", "IT-M4",
})


def _normalize_autoit_kpi_id(kpi_id: str) -> str:
    kid = str(kpi_id or "").strip().upper()
    for cyr, lat in (("М", "M"), ("С", "C"), ("Р", "P"), ("Т", "T"), ("И", "I")):
        kid = kid.replace(cyr, lat)
    return kid


_AUTOIT_CACHED_TILE_IDS_NORM = frozenset(
    _normalize_autoit_kpi_id(x) for x in AUTOIT_CACHED_TILE_IDS
)


def is_autoit_tile_kpi_id(kpi_id: str) -> bool:
    return _normalize_autoit_kpi_id(kpi_id) in _AUTOIT_CACHED_TILE_IDS_NORM


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    """Файлы кэша, по mtime которых на плитке показывается ``cache_updated_at``."""
    from getkpi.autoit.it_m1_sla import (
        cache_file_path_for_period as it_m1_cache,
        monthly_cache_path as it_m1_monthly,
    )
    from getkpi.autoit.it_m3 import (
        cache_file_path_for_period as it_m3_cache,
        monthly_cache_path as it_m3_monthly,
    )
    from getkpi.autoit.it_m4_fot import (
        cache_file_path_for_period as it_m4_cache,
        monthly_cache_path as it_m4_monthly,
    )
    from getkpi.autoit.it_q2_tekuchest import cache_file_path_for_period as it_q2_cache

    kid = _normalize_autoit_kpi_id(kpi_id)
    paths: list[Path] = []

    if kid == "IT-M1":
        paths.extend([
            it_m1_cache(ref_y, ref_m),
            it_m1_monthly(ref_y, ref_m),
        ])
    elif kid == "IT-M3":
        paths.extend([
            it_m3_cache(ref_y, ref_m),
            it_m3_monthly(ref_y, ref_m),
        ])
    elif kid == "IT-M4":
        paths.extend([
            it_m4_cache(ref_y, ref_m),
            it_m4_monthly(ref_y, ref_m),
        ])
    elif kid == "IT-Q2":
        paths.append(it_q2_cache(ref_y, ref_m))

    return paths


def is_autoit_department(dept: str | None) -> bool:
    normalized = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", (dept or "").strip()).lower().replace("ё", "е"),
    )
    return normalized in {
        "начальник отдела автоматизации ит",
        "служба автоматизации",
        "autoit",
    }


def _merge_monthly(entry: dict[str, Any], payload: dict[str, Any] | None) -> None:
    if payload is None:
        return
    entry["data_granularity"] = payload.get("data_granularity", "monthly")
    entry["monthly_data"] = payload.get("monthly_data") or []
    entry["last_full_month_row"] = payload.get("last_full_month_row")
    entry["ytd"] = payload.get("ytd") or {}
    entry["kpi_period"] = payload.get("kpi_period")
    if payload.get("debug") is not None:
        entry["debug"] = payload["debug"]


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
    department: str | None = None,
) -> bool:
    """Если ``kpi_id`` — KPI контура ИТ, заполняет ``entry`` и возвращает True."""
    kid = _normalize_autoit_kpi_id(kpi_id)
    if kid == "IT-M1":
        if is_c1auto_department(department):
            return False
        _merge_monthly(entry, get_it_m1_sla_ytd(year=year, month=month))
        return True
    if kid == "IT-M3":
        if is_c1auto_department(department):
            return False
        _merge_monthly(entry, get_it_m3_ytd(year=year, month=month))
        return True
    if kid == "IT-M4":
        _merge_monthly(entry, get_it_m4_fot_ytd(year=year, month=month))
        return True
    if kid == "IT-Q2":
        _merge_monthly(entry, get_it_q2_tekuchest_ytd(year=year, month=month))
        return True
    return False
