"""Склейка KPI дашборда ГСП для общего ``getkpi.views``."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from gspp.tkp_lifecycle import get_gspp_m1_ytd, gspp_m1_ytd_cache_path
from gspp.ol_gspp_monthly import get_gspp_m2_ytd, gspp_m2_ytd_cache_path
from gspp.m3 import get_gspp_m3_ytd, gspp_m3_ytd_cache_path
from gspp.m5 import get_gspp_m5_ytd, gspp_m5_ytd_cache_path
from gspp.q5 import get_gspp_q5_ytd, gspp_q5_ytd_cache_path
from getkpi.gspp_q4 import (
    get_gspp_q4_deviation_tables,
    get_gspp_q4_ytd,
    gspp_q4_kpi_id_matches,
    gspp_q4_ytd_cache_path,
    _MANAGER_PROJECTS_DISK_PATH,
)

GSPP_TILE_KPI_IDS: frozenset[str] = frozenset({
    "ГСП-Q4", "GSP-Q4", "GSPP-Q4", "ГCP-Q4", "ГCП-Q4", "ГСПП-Q4", "ГCПП-Q4",
    "ГСП-M1", "ГCП-M1", "GSP-M1", "ГСПП-M1", "ГCПП-M1", "GSPP-M1",
    "ГСП-M2", "ГCП-M2", "GSP-M2", "ГСПП-M2", "ГCПП-M2", "GSPP-M2",
    "ГСП-M3", "ГCП-M3", "GSP-M3", "ГСПП-M3", "ГCПП-M3", "GSPP-M3",
    "ГСП-M5", "ГCП-M5", "GSP-M5", "ГСПП-M5", "ГCПП-M5", "GSPP-M5",
    "ГСП-Q5", "ГCП-Q5", "GSP-Q5", "ГСПП-Q5", "ГCПП-Q5", "GSPP-Q5",
})

GSPP_KPI_IDS_USE_BUILDER_KP_PERIOD: frozenset[str] = frozenset({
    "ГСП-Q4", "GSP-Q4", "GSPP-Q4", "ГCP-Q4", "ГCП-Q4", "ГСПП-Q4", "ГCПП-Q4",
    "ГСП-M1", "ГCП-M1", "GSP-M1", "ГСПП-M1", "ГCПП-M1", "GSPP-M1",
    "ГСП-M2", "ГCП-M2", "GSP-M2", "ГСПП-M2", "ГCПП-M2", "GSPP-M2",
    "ГСП-M3", "ГCП-M3", "GSP-M3", "ГСПП-M3", "ГCПП-M3", "GSPP-M3",
    "ГСП-M5", "ГCП-M5", "GSP-M5", "ГСПП-M5", "ГCПП-M5", "GSPP-M5",
    "ГСП-Q5", "ГCП-Q5", "GSP-Q5", "ГСПП-Q5", "ГCПП-Q5", "GSPP-Q5",
})

GSPP_M1_TILE_IDS: frozenset[str] = frozenset({
    "ГСП-M1", "ГCП-M1", "GSP-M1", "ГСПП-M1", "ГCПП-M1", "GSPP-M1",
})

GSPP_M2_TILE_IDS: frozenset[str] = frozenset({
    "ГСП-M2", "ГCП-M2", "GSP-M2", "ГСПП-M2", "ГCПП-M2", "GSPP-M2",
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


def rag_gspp_m1_m2_pct(pct: float | None) -> str:
    """ГСП-M1/M2: ≥95 % — зелёный, 90–94,9 % — жёлтый, <90 % — красный."""
    if pct is None:
        return "unknown"
    if pct >= 95:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def gspp_m1_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in _GSPP_M1_IDS_NORM


def gspp_m2_tile_matches(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in _GSPP_M2_IDS_NORM


def gspp_m1_m2_tile_matches(kpi_id: str) -> bool:
    return gspp_m1_tile_matches(kpi_id) or gspp_m2_tile_matches(kpi_id)


def rag_gspp_q4_pct(pct: float | None) -> str:
    """ГСП-Q4: ≥90 % — зелёный, 80–89,9 % — жёлтый, <80 % — красный."""
    if pct is None:
        return "unknown"
    if pct >= 90:
        return "green"
    if pct >= 80:
        return "yellow"
    return "red"


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


_GSPP_M1_IDS_NORM = frozenset(_normalize_kpi_id(x) for x in GSPP_M1_TILE_IDS)
_GSPP_M2_IDS_NORM = frozenset(_normalize_kpi_id(x) for x in GSPP_M2_TILE_IDS)
_GSPP_M3_IDS_NORM = frozenset(_normalize_kpi_id(x) for x in GSPP_M3_TILE_IDS)
_GSPP_M5_IDS_NORM = frozenset(_normalize_kpi_id(x) for x in GSPP_M5_TILE_IDS)
_GSPP_Q5_IDS_NORM = frozenset(_normalize_kpi_id(x) for x in GSPP_Q5_TILE_IDS)
_GSPP_TILE_IDS_NORM = frozenset(_normalize_kpi_id(x) for x in GSPP_TILE_KPI_IDS)


def is_gspp_tile_kpi_id(kpi_id: str) -> bool:
    return _normalize_kpi_id(kpi_id) in _GSPP_TILE_IDS_NORM


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    """Файлы кэша, по mtime которых на плитке показывается ``cache_updated_at`` (как TD-*)."""
    kid = _normalize_kpi_id(kpi_id)
    paths: list[Path] = []

    if kid in _GSPP_M1_IDS_NORM:
        paths.append(gspp_m1_ytd_cache_path(ref_y, ref_m))
    elif kid in _GSPP_M2_IDS_NORM:
        paths.append(gspp_m2_ytd_cache_path(ref_y, ref_m))
    elif kid in _GSPP_M3_IDS_NORM:
        paths.append(gspp_m3_ytd_cache_path(ref_y, ref_m))
    elif kid in _GSPP_M5_IDS_NORM:
        paths.extend([
            gspp_m5_ytd_cache_path(ref_y, ref_m),
            _MANAGER_PROJECTS_DISK_PATH,
        ])
    elif gspp_q4_kpi_id_matches(kid):
        paths.extend([
            gspp_q4_ytd_cache_path(ref_y, ref_m),
            _MANAGER_PROJECTS_DISK_PATH,
        ])
    elif kid in _GSPP_Q5_IDS_NORM:
        paths.append(gspp_q5_ytd_cache_path(ref_y, ref_m))

    return paths


def _merge_monthly(entry: dict[str, Any], payload: dict[str, Any]) -> None:
    entry["data_granularity"] = payload.get("data_granularity", "monthly")
    entry["monthly_data"] = payload.get("monthly_data") or []
    entry["last_full_month_row"] = payload.get("last_full_month_row")
    entry["ytd"] = payload.get("ytd") or {}
    entry["kpi_period"] = payload.get("kpi_period")
    if payload.get("debug") is not None:
        entry["debug"] = payload["debug"]


def _target_to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("\xa0", " ")
    if not text:
        return None
    matches = re.findall(r"\d[\d\s]*(?:[,.]\d+)?", text)
    if not matches:
        return None
    raw = matches[-1].replace(" ", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _ensure_m5_zero_project_month(entry: dict[str, Any], *, year: int | None, month: int | None) -> None:
    """Для ГСП-M5 отсутствие проектов = реальный факт 0, а не пустая плитка."""
    from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

    ref_y, ref_m = normalize_rd_tile_period(year, month)
    kper = entry.get("kpi_period") or {}
    if isinstance(kper, dict):
        ref_y = int(kper.get("year") or ref_y)
        ref_m = max(1, min(12, int(kper.get("month") or ref_m)))

    rows = [dict(row) for row in (entry.get("monthly_data") or []) if isinstance(row, dict)]
    idx = next(
        (
            i for i, row in enumerate(rows)
            if int(row.get("year") or ref_y) == ref_y and int(row.get("month") or 0) == ref_m
        ),
        -1,
    )
    row = rows[idx] if idx >= 0 else {
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "values_unit": "руб.",
    }
    if row.get("has_data") and row.get("plan") is not None and row.get("fact") is not None:
        return

    # Нет активных проектов в месяце: план/факт 0 ₽.
    # Не брать «Ежемесячно/Ежегодно» из карточки KPI — там пороги вроде «≤100%», не рубли.
    plan = 0.0 if row.get("plan") is None else row.get("plan")
    fact = 0.0 if row.get("fact") is None else row.get("fact")
    try:
        pct = round(float(fact) / float(plan) * 100, 2) if float(plan) > 0 else None
    except (TypeError, ValueError):
        pct = None

    row.update({
        "month": ref_m,
        "year": ref_y,
        "month_name": MONTH_NAMES[ref_m],
        "plan": plan,
        "fact": fact,
        "kpi_pct": pct,
        "has_data": True,
        "values_unit": "руб.",
    })
    if idx >= 0:
        rows[idx] = row
    else:
        rows.append(row)
        rows.sort(key=lambda item: (int(item.get("year") or ref_y), int(item.get("month") or 0)))

    entry["monthly_data"] = rows
    entry["last_full_month_row"] = dict(row)
    entry["ytd"] = {
        "total_plan": row.get("plan"),
        "total_fact": row.get("fact"),
        "kpi_pct": row.get("kpi_pct"),
        "months_with_data": sum(1 for item in rows if item.get("has_data")),
        "months_total": len(rows),
        "values_unit": "руб.",
    }
    entry["kpi_period"] = {
        "type": "last_full_month",
        "year": ref_y,
        "month": ref_m,
        "month_name": MONTH_NAMES[ref_m],
    }


def merge_kpi_entry_if_applicable(
    kpi_id: str,
    entry: dict[str, Any],
    *,
    year: int | None,
    month: int | None,
) -> bool:
    kid = _normalize_kpi_id(kpi_id)

    if kid in _GSPP_M1_IDS_NORM:
        payload = get_gspp_m1_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if kid in _GSPP_M2_IDS_NORM:
        payload = get_gspp_m2_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if kid in _GSPP_M3_IDS_NORM:
        payload = get_gspp_m3_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        return True

    if kid in _GSPP_M5_IDS_NORM:
        payload = get_gspp_m5_ytd(year=year, month=month)
        if payload is None:
            return False
        _merge_monthly(entry, payload)
        _ensure_m5_zero_project_month(entry, year=year, month=month)
        return True

    if kid in _GSPP_Q5_IDS_NORM:
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
        from devdir.rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period

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
