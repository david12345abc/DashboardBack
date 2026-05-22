"""Склейка KPI дашборда «Начальник отдела автоматизации ИТ» для ``getkpi.views``."""
from __future__ import annotations

import re
import unicodedata
from typing import Any

DEPARTMENT = "Начальник отдела автоматизации ИТ"

AUTOIT_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M1", "IT-M1",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
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
    "ИТ-Q1", "IT-Q1",
    "ИТ-Q2", "IT-Q2",
    "ИТ-Y1", "IT-Y1",
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


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    """Заполнение плиток autoit — по мере появления расчётных модулей."""
    _ = (kpi_id, entry, year, month)
    return False
