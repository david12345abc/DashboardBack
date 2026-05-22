"""Склейка KPI дашборда «Начальник отдела автоматизации ИТ» для ``getkpi.views``."""
from __future__ import annotations

import re
import unicodedata
from typing import Any

from getkpi.autoit.it_m4_fot import get_it_m4_fot_ytd
from getkpi.autoit.it_q2_tekuchest import get_it_q2_tekuchest_ytd

DEPARTMENT = "Начальник отдела автоматизации ИТ"

AUTOIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M1", "IT-M1",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "ИТ-M4", "IT-M4",
    "ИТ-Q1", "IT-Q1",
    "ИТ-Q2", "IT-Q2",
    "ИТ-Y1", "IT-Y1",
    "ИТ-C1", "IT-C1",
    "ИТ-C2", "IT-C2",
    "ИТ-C3", "IT-C3",
    "ИТ-B1", "IT-B1",
    "ИТ-B2", "IT-B2",
})

AUTOIT_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "ИТ-M1", "IT-M1",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "ИТ-M4", "IT-M4",
    "ИТ-Q1", "IT-Q1",
    "ИТ-Q2", "IT-Q2",
    "ИТ-Y1", "IT-Y1",
})

AUTOIT_RUB_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M3", "IT-M3",
    "ИТ-M4", "IT-M4",
})

AUTOIT_FOT_LIMIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M4", "IT-M4",
})


def is_autoit_department(dept: str | None) -> bool:
    normalized = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", (dept or "").strip()).lower().replace("ё", "е"),
    )
    return normalized in {
        "начальник отдела автоматизации ит",
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
) -> bool:
    """Если ``kpi_id`` — KPI контура ИТ, заполняет ``entry`` и возвращает True."""
    kid = str(kpi_id or "").strip().upper()
    for cyr, lat in (("М", "M"), ("С", "C"), ("Р", "P"), ("Т", "T"), ("И", "I")):
        kid = kid.replace(cyr, lat)
    if kid in {"IT-M4", "ИТ-M4"}:
        _merge_monthly(entry, get_it_m4_fot_ytd(year=year, month=month))
        return True
    if kid in {"IT-Q2", "ИТ-Q2"}:
        _merge_monthly(entry, get_it_q2_tekuchest_ytd(year=year, month=month))
        return True
    return False
