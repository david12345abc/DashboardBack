"""Склейка KPI дашборда «Начальник отдела сопровождения 1С» (логин 1cauto) для ``getkpi.views``."""
from __future__ import annotations

import re
import unicodedata
from typing import Any

DEPARTMENT = "Начальник отдела сопровождения 1С"

C1AUTO_KPI_IDS: frozenset[str] = frozenset({
    "ИТ-M1-1", "IT-M1-1",
    "ИТ-M1-2", "IT-M1-2",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "ИТ-Q4", "IT-Q4",
    "ИТ-Q5", "IT-Q5",
    "ИТ-C1", "IT-C1",
    "ИТ-B1", "IT-B1",
    "ИТ-B2", "IT-B2",
})

C1AUTO_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "ИТ-M1-1", "IT-M1-1",
    "ИТ-M1-2", "IT-M1-2",
    "ИТ-M2", "IT-M2",
    "ИТ-M3", "IT-M3",
    "ИТ-Q4", "IT-Q4",
    "ИТ-Q5", "IT-Q5",
})


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


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    """Заполнение плиток c1auto — по мере появления расчётных модулей."""
    _ = (kpi_id, entry, year, month)
    return False
