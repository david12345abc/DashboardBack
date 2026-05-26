"""Склейка KPI дашборда «Начальник отдела сопровождения 1С» (логин 1cauto) для ``getkpi.views``."""
from __future__ import annotations

import re
import unicodedata
from typing import Any

from getkpi.c1auto.c1_m1_sla import get_c1_m1_sla_ytd
from getkpi.c1auto.c1_m3 import get_c1_m3_ytd
from getkpi.c1auto.c1_m4_fot import get_c1_m4_fot_ytd
from getkpi.c1auto.it_q5_tekuchest import get_it_q5_tekuchest_ytd

DEPARTMENT = "Начальник отдела сопровождения 1С"

# В БД коды с кириллической «С»: 1С-M4, 1С-Q5 → после нормализации 1C-M4, 1C-Q5.
C1AUTO_KPI_IDS: frozenset[str] = frozenset({
    "1С-M1", "1C-M1",
    "ИТ-M1-1", "IT-M1-1",
    "ИТ-M1-2", "IT-M1-2",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "1С-M3", "1C-M3",
    "1С-M4", "1C-M4",
    "ИТ-Q4", "IT-Q4",
    "ИТ-Q5", "IT-Q5",
    "1С-Q5", "1C-Q5",
    "ИТ-C1", "IT-C1",
    "ИТ-B1", "IT-B1",
    "ИТ-B2", "IT-B2",
})

C1AUTO_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "1С-M1", "1C-M1",
    "ИТ-M1-1", "IT-M1-1",
    "ИТ-M1-2", "IT-M1-2",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "1С-M3", "1C-M3",
    "1С-M4", "1C-M4",
    "ИТ-Q4", "IT-Q4",
    "ИТ-Q5", "IT-Q5",
    "1С-Q5", "1C-Q5",
})

C1AUTO_RUB_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M3", "IT-M3",
    "1С-M3", "1C-M3",
    "1С-M4", "1C-M4",
})

C1AUTO_BUDGET_LIMIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M3", "IT-M3",
    "1С-M3", "1C-M3",
})

C1AUTO_SLA_KPI_IDS: frozenset[str] = frozenset({
    "1С-M1", "1C-M1",
    "ИТ-M1-2", "IT-M1-2",
})

C1AUTO_FOT_LIMIT_KPI_IDS: frozenset[str] = frozenset({
    "1С-M4", "1C-M4",
})

C1AUTO_TURNOVER_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-Q5", "IT-Q5",
    "1С-Q5", "1C-Q5",
})


def _normalize_c1auto_kpi_id(kpi_id: str) -> str:
    kid = str(kpi_id or "").strip().upper()
    for cyr, lat in (("М", "M"), ("С", "C"), ("Р", "P"), ("Т", "T"), ("И", "I")):
        kid = kid.replace(cyr, lat)
    return kid


def is_c1auto_department(dept: str | None) -> bool:
    normalized = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", (dept or "").strip()).lower().replace("ё", "е"),
    )
    return normalized in {
        "начальник отдела сопровождения 1с",
        "1cauto",
        "c1auto",
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
    """Если ``kpi_id`` — KPI контура 1С, заполняет ``entry`` и возвращает True."""
    kid = _normalize_c1auto_kpi_id(kpi_id)
    if kid in {"1C-M1", "IT-M1-2"}:
        _merge_monthly(entry, get_c1_m1_sla_ytd(year=year, month=month))
        return True
    if kid in {"1C-M3", "IT-M3"}:
        if kid == "IT-M3" and department is not None and not is_c1auto_department(department):
            return False
        _merge_monthly(entry, get_c1_m3_ytd(year=year, month=month))
        return True
    if kid == "1C-M4":
        _merge_monthly(entry, get_c1_m4_fot_ytd(year=year, month=month))
        return True
    if kid in {"1C-Q5", "IT-Q5"}:
        _merge_monthly(entry, get_it_q5_tekuchest_ytd(year=year, month=month))
        return True
    return False
