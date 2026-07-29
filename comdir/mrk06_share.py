"""
MRK-06: доля (БМИ + ПАО Газпром) в отгрузке — SQL через comdir отгрузки.

Считает помесячно из get_otgruzki_ytd (тот же факт, что KD-M2), без OData.
Результат кэшируется в getkpi/dashboard/comdir_mrk06_share_ytd_YYYY_MM.json.
"""
from __future__ import annotations

import sys
from calendar import monthrange
from datetime import date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from comdir.common import empty_error_payload  # noqa: E402
from comdir.sql_tile_cache import get_ytd_via_cache, normalize_period  # noqa: E402
from comdir.ytd import get_otgruzki_ytd  # noqa: E402

CACHE_VERSION = 1

# Те же 6 отделов продаж, что в OData-эталоне MRK-06
SHARE_DEPARTMENTS: dict[str, str] = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Эталонное",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Ключевые клиенты",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Дилерские (ОПБО)",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "ПАО Газпром",
}
BMI_DEPT = "9edaa7d4-37a5-11ee-93d3-6cb31113810e"
GP_DEPT = "bd7b5184-9f9c-11e4-80da-001e67112509"


def _dept_fact(by_dept: dict, guid: str) -> float:
    cell = (by_dept or {}).get(guid)
    if isinstance(cell, dict):
        return float(cell.get("fact") or 0)
    try:
        return float(cell or 0)
    except (TypeError, ValueError):
        return 0.0


def _pct(part: float, total: float) -> float | None:
    if not total:
        return None
    return round(part / total * 100, 1)


def _share_row_from_otg_month(row: dict[str, Any]) -> dict[str, Any]:
    by_dept = row.get("by_dept") or {}
    by_named: dict[str, float] = {}
    total = 0.0
    for guid, name in SHARE_DEPARTMENTS.items():
        fact = _dept_fact(by_dept, guid)
        by_named[name] = round(fact, 2)
        total += fact
    bmi = _dept_fact(by_dept, BMI_DEPT)
    gp = _dept_fact(by_dept, GP_DEPT)
    pair = bmi + gp
    return {
        "month": int(row.get("month") or 0),
        "year": int(row.get("year") or 0) or None,
        "total": round(total, 2),
        "bmi": round(bmi, 2),
        "gp": round(gp, 2),
        "pair": round(pair, 2),
        "pct_pair": _pct(pair, total),
        "pct_bmi": _pct(bmi, total),
        "pct_gp": _pct(gp, total),
        "by_dept": by_named,
    }


def build_share_monthly_payload(year: int, month: int) -> dict[str, Any]:
    """Помесячная доля Jan..month из SQL-отгрузок (KD-M2)."""
    otg = get_otgruzki_ytd(year=year, month=month)
    if (otg.get("debug") or {}).get("status") == "error":
        err = (otg.get("debug") or {}).get("error") or "otgruzki sql error"
        raise RuntimeError(str(err))

    months_out: list[dict[str, Any]] = []
    for row in otg.get("months") or []:
        m = int(row.get("month") or 0)
        if 1 <= m <= month:
            share = _share_row_from_otg_month(row)
            share["year"] = year
            months_out.append(share)
    months_out.sort(key=lambda r: int(r.get("month") or 0))

    # YTD-агрегат на конец периода (для get_shipment_share_bmi_gazprom)
    total = sum(float(r.get("total") or 0) for r in months_out)
    bmi = sum(float(r.get("bmi") or 0) for r in months_out)
    gp = sum(float(r.get("gp") or 0) for r in months_out)
    pair = bmi + gp
    by_dept_ytd: dict[str, float] = {name: 0.0 for name in SHARE_DEPARTMENTS.values()}
    for r in months_out:
        for name, val in (r.get("by_dept") or {}).items():
            by_dept_ytd[name] = round(by_dept_ytd.get(name, 0.0) + float(val or 0), 2)

    last_day = monthrange(year, month)[1]
    return {
        "year": year,
        "month": month,
        "ref_month": month,
        "period_start": f"{year}-01-01",
        "period_end": f"{year}-{month:02d}-{last_day:02d}",
        "months": months_out,
        "total": round(total, 2),
        "by_dept": by_dept_ytd,
        "bmi": round(bmi, 2),
        "gp": round(gp, 2),
        "pair": round(pair, 2),
        "pct_bmi": _pct(bmi, total),
        "pct_gp": _pct(gp, total),
        "pct_pair": _pct(pair, total),
        "debug": {"status": "ok", "kpi_id": "MRK-06", "source": "comdir.sql.otgruzki"},
    }


def get_shipment_share_bmi_gazprom_monthly(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,  # noqa: ARG001 — совместимость с cache_manager
    **_kwargs: Any,
) -> dict[str, Any]:
    """Помесячная серия MRK-06 (кэш SWR)."""
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_mrk06_share_ytd",
        source_tag="comdir_mrk06_share_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_mrk06_share",
        compute_fn=build_share_monthly_payload,
        kpi_id="MRK-06",
        error_factory=lambda y, m, e: {
            **empty_error_payload(y, m, "MRK-06", e),
            "months": [],
            "total": 0.0,
            "bmi": 0.0,
            "gp": 0.0,
            "pair": 0.0,
            "pct_pair": None,
            "by_dept": {},
        },
    )


def get_shipment_share_bmi_gazprom(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,  # noqa: ARG001
    **_kwargs: Any,
) -> dict[str, Any]:
    """YTD-агрегат MRK-06 (тот же кэш, что и monthly)."""
    payload = get_shipment_share_bmi_gazprom_monthly(
        year=year, month=month, dept_guid=dept_guid,
    )
    ref_y, ref_m = normalize_period(year, month)
    return {
        "year": payload.get("year", ref_y),
        "month": payload.get("month", ref_m),
        "period_start": payload.get("period_start") or f"{ref_y}-01-01",
        "period_end": payload.get("period_end") or date(ref_y, ref_m, 1).isoformat(),
        "total": payload.get("total") or 0.0,
        "by_dept": payload.get("by_dept") or {},
        "bmi": payload.get("bmi") or 0.0,
        "gp": payload.get("gp") or 0.0,
        "pair": payload.get("pair") or 0.0,
        "pct_bmi": payload.get("pct_bmi"),
        "pct_gp": payload.get("pct_gp"),
        "pct_pair": payload.get("pct_pair"),
        "months": payload.get("months") or [],
        "debug": payload.get("debug") or {"status": "ok", "source": "comdir.sql.otgruzki"},
    }


def cache_stamp_path(ref_y: int, ref_m: int) -> Path:
    dash = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
    return dash / f"comdir_mrk06_share_ytd_{ref_y}_{ref_m:02d}.json"
