"""Склейка KPI дашборда ГСП для общего ``getkpi.views``."""
from __future__ import annotations

from typing import Any

from gspp.tkp import get_gspp_m1_ytd
from gspp.m3 import get_gspp_m3_ytd
from gspp.m5 import get_gspp_m5_ytd
from gspp.q5 import get_gspp_q5_ytd
from getkpi.gspp_q4 import get_gspp_q4_deviation_tables, get_gspp_q4_ytd, gspp_q4_kpi_id_matches

GSPP_TILE_KPI_IDS: frozenset[str] = frozenset({
    "ГСП-Q4", "GSP-Q4", "ГCP-Q4", "ГCП-Q4",
    "ГСП-M1", "ГCП-M1", "GSP-M1", "ГСПП-M1", "ГCПП-M1", "GSPP-M1",
    "ГСП-M3", "ГCП-M3", "GSP-M3", "ГСПП-M3", "ГCПП-M3", "GSPP-M3",
    "ГСП-M5", "ГCП-M5", "GSP-M5", "ГСПП-M5", "ГCПП-M5", "GSPP-M5",
    "ГСП-Q5", "ГCП-Q5", "GSP-Q5", "ГСПП-Q5", "ГCПП-Q5", "GSPP-Q5",
})

GSPP_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "ГСП-Q4", "GSP-Q4", "ГCP-Q4", "ГCП-Q4",
    "ГСП-M1", "ГCП-M1", "GSP-M1", "ГСПП-M1", "ГCПП-M1", "GSPP-M1",
    "ГСП-M3", "ГCП-M3", "GSP-M3", "ГСПП-M3", "ГCПП-M3", "GSPP-M3",
    "ГСП-M5", "ГCП-M5", "GSP-M5", "ГСПП-M5", "ГCПП-M5", "GSPP-M5",
    "ГСП-Q5", "ГCП-Q5", "GSP-Q5", "ГСПП-Q5", "ГCПП-Q5", "GSPP-Q5",
})

GSPP_M1_TILE_IDS: frozenset[str] = frozenset({
    "ГСП-M1", "ГCП-M1", "GSP-M1", "ГСПП-M1", "ГCПП-M1", "GSPP-M1",
})

GSPP_M3_TILE_IDS: frozenset[str] = frozenset({
    "ГСП-M3", "ГCП-M3", "GSP-M3", "ГСПП-M3", "ГCПП-M3", "GSPP-M3",
})

GSPP_M5_TILE_IDS: frozenset[str] = frozenset({
    "ГСП-M5", "ГCП-M5", "GSP-M5", "ГСПП-M5", "ГCПП-M5", "GSPP-M5",
})

GSPP_Q5_TILE_IDS: frozenset[str] = frozenset({
    "ГСП-Q5", "ГCП-Q5", "GSP-Q5", "ГСПП-Q5", "ГCПП-Q5", "GSPP-Q5",
})


def _normalize_kpi_id(raw: object) -> str:
    s = str(raw or "").strip().upper()
    for d in (
        "\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015",
        "\u2212", "\ufe58", "\ufe63", "\uff0d",
    ):
        s = s.replace(d, "-")
    # В KPI-кодах часто смешиваются латинские M/C/P и похожие кириллические М/С/Р.
    for cyr, lat in (("М", "M"), ("С", "C"), ("Р", "P")):
        s = s.replace(cyr, lat)
    return s


def _merge_monthly(entry: dict[str, Any], payload: dict[str, Any]) -> None:
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
    if _normalize_kpi_id(kpi_id) in GSPP_M1_TILE_IDS:
        payload = get_gspp_m1_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if _normalize_kpi_id(kpi_id) in GSPP_M3_TILE_IDS:
        payload = get_gspp_m3_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if _normalize_kpi_id(kpi_id) in GSPP_M5_TILE_IDS:
        payload = get_gspp_m5_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if _normalize_kpi_id(kpi_id) in GSPP_Q5_TILE_IDS:
        payload = get_gspp_q5_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if not gspp_q4_kpi_id_matches(kpi_id):
        return False
    payload = get_gspp_q4_ytd(year=year, month=month)
    if payload is None:
        # Иначе в ``getkpi.views`` сработает синтетика по полю «Ежеквартально» из БД:
        # неверные план/факт и снова «ежеквартально» на плитке.
        from getkpi.devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

        ref_y, ref_m = normalize_rd_tile_period(year, month)
        mn = MONTH_NAMES[ref_m]
        row: dict[str, Any] = {
            "month": ref_m,
            "year": ref_y,
            "month_name": mn,
            "plan": 0.0,
            "fact": 0.0,
            "kpi_pct": None,
            "has_data": False,
            "values_unit": "шт.",
        }
        payload = {
            "data_granularity": "monthly",
            "monthly_data": [dict(row)],
            "last_full_month_row": dict(row),
            "kpi_period": {
                "type": "last_full_month",
                "year": ref_y,
                "month": ref_m,
                "month_name": mn,
            },
            "ytd": {
                "total_plan": 0.0,
                "total_fact": 0.0,
                "kpi_pct": None,
                "months_with_data": 0,
                "months_total": 1,
                "values_unit": "шт.",
            },
            "debug": {
                "kpi_id": "ГСП-Q4",
                "status": "no_payload",
                "hint": "get_gspp_q4_ytd вернул None (кэш/блокировка/исключение при сборке)",
            },
        }
    _merge_monthly(entry, payload)
    return True


def merge_gspp_tables_into_universal_payload(
    tablitsy: dict[str, Any],
    ref_y: int,
    ref_m: int,
) -> None:
    """Добавить в ``Таблицы`` универсального ответа таблицу отклонений по вехам ГСП-Q4 (как TD-T-*-DEVIATIONS)."""
    extra = get_gspp_q4_deviation_tables(year=ref_y, month=ref_m)
    if isinstance(extra, dict):
        tablitsy.update(extra)
