"""Склейка KPI дашборда сервисной службы (servhead) для общего ``getkpi.views``."""
from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

from devdir.rd_monthly_period import MONTH_NAMES

logger = logging.getLogger(__name__)

from servhead.sh_m1 import get_sh_m1_ytd
from servhead.sh_m2 import get_sh_m2_ytd
from servhead.sh_m3 import get_sh_m3_ytd
from servhead.sh_m4 import get_sh_m4_ytd
from servhead.sh_m5 import get_sh_m5_ytd
from servhead.sh_t1 import TABLE_ID as SH_T1_TABLE_ID, get_sh_t1_table

SERVHEAD_TILE_KPI_IDS: frozenset[str] = frozenset({
    "SH-M1",
    "SH-M2",
    "SH-M3",
    "SH-M4",
    "SH-M5",
})

SERVHEAD_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "SH-M1",
    "SH-M2",
    "SH-M3",
    "SH-M4",
    "SH-M5",
})

SERVHEAD_M1_TILE_IDS: frozenset[str] = frozenset({"SH-M1"})
SERVHEAD_M2_TILE_IDS: frozenset[str] = frozenset({"SH-M2"})
SERVHEAD_M3_TILE_IDS: frozenset[str] = frozenset({"SH-M3"})
SERVHEAD_M4_TILE_IDS: frozenset[str] = frozenset({"SH-M4"})
SERVHEAD_M5_TILE_IDS: frozenset[str] = frozenset({"SH-M5"})

SERVHEAD_HIGHER_BETTER_TILE_IDS: frozenset[str] = frozenset({
    "SH-M1",
    "SH-M4",
})

SERVHEAD_LOWER_BETTER_TILE_IDS: frozenset[str] = frozenset({
    "SH-M2",
    "SH-M3",
    "SH-M5",
})

_PAYLOAD_BUILDERS = {
    "SH-M1": get_sh_m1_ytd,
    "SH-M2": get_sh_m2_ytd,
    "SH-M3": get_sh_m3_ytd,
    "SH-M4": get_sh_m4_ytd,
    "SH-M5": get_sh_m5_ytd,
}


def is_servhead_department(dept: str | None) -> bool:
    normalized = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", (dept or "").strip()).lower(),
    )
    return normalized in {
        "servhead",
        "сервисная служба",
        "начальник сервисной службы",
    }


def _normalize_kpi_id(raw: object) -> str:
    s = str(raw or "").strip().upper()
    for d in (
        "\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015",
        "\u2212", "\ufe58", "\ufe63", "\uff0d",
    ):
        s = s.replace(d, "-")
    s = s.replace("М", "M")
    return s


def servhead_m1_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_M1_TILE_IDS


def servhead_m2_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_M2_TILE_IDS


def servhead_m3_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_M3_TILE_IDS


def servhead_m4_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_M4_TILE_IDS


def servhead_m5_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_M5_TILE_IDS


def servhead_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_TILE_KPI_IDS


def servhead_lower_better_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_LOWER_BETTER_TILE_IDS


def servhead_higher_better_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in SERVHEAD_HIGHER_BETTER_TILE_IDS


def rag_servhead_m1_pct(pct: float | None) -> str:
    """SH-M1/M4: ≥90 % — зелёный, 80–89,9 % — жёлтый, <80 % — красный (пороги из БД)."""
    if pct is None:
        return "unknown"
    if pct >= 90:
        return "green"
    if pct >= 80:
        return "yellow"
    return "red"


def rag_servhead_lower_better_pct(pct: float | None) -> str:
    """SH-M2/M3: ≤5 % — зелёный, 5,1–10 % — жёлтый, >10 % — красный."""
    if pct is None:
        return "unknown"
    if pct <= 5:
        return "green"
    if pct <= 10:
        return "yellow"
    return "red"


def rag_servhead_m2_pct(pct: float | None) -> str:
    return rag_servhead_lower_better_pct(pct)


def rag_servhead_m3_pct(pct: float | None) -> str:
    return rag_servhead_lower_better_pct(pct)


def rag_servhead_m4_pct(pct: float | None) -> str:
    return rag_servhead_m1_pct(pct)


def rag_servhead_m5_pct(pct: float | None) -> str:
    return rag_servhead_lower_better_pct(pct)


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    kid = _normalize_kpi_id(kpi_id)
    builder = _PAYLOAD_BUILDERS.get(kid)
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
    if payload.get("debug") is not None:
        entry["debug"] = payload["debug"]
    return True


def merge_servhead_tables_into_universal_payload(
    tablitsy: dict[str, Any],
    ref_y: int,
    ref_m: int,
) -> None:
    try:
        table = get_sh_t1_table(year=ref_y, month=ref_m)
    except Exception as exc:
        logger.exception("SH-T1: ошибка сборки таблицы для дашборда servhead")
        table = {
            "kpi_id": SH_T1_TABLE_ID,
            "name": f"Обращения по клиентам — {MONTH_NAMES[ref_m].capitalize()} {ref_y}",
            "periodicity": "ежемесячно",
            "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
            "columns": ["Клиент", "Всего обращений", "В срок", "Не в срок"],
            "rows": [],
            "totals": {"total": 0, "on_time": 0, "late": 0},
            "debug": {"kpi_id": SH_T1_TABLE_ID, "status": "error", "error": str(exc)[:500]},
        }
    if isinstance(table, dict):
        tablitsy[SH_T1_TABLE_ID] = table
