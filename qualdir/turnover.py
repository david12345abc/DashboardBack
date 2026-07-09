"""
QD-Q2 — текучесть персонала контура директора по качеству.

План: top2 по группам из Document_ТД_ТекучестьПерсонала (как TD-Q2).
Факт: уволено / штат × 100 % по HR (как TD-Q2).

Кэш: помесячно ``qualdir_tekuchet_<Y>_<MM>.json``; полный YTD —
``qualdir_qd_q2_ytd_<Y>_<MM>.json`` (``ytd_json_cache``).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from devdir import ytd_json_cache
from getkpi.techdir_tekuchet import MONTH_RU, TURNOVER_VALUES_UNIT, build_turnover_month_payload
from getkpi.turnover_hr_scope import TurnoverHrScope

# Кэш в каталоге getkpi/dashboard — как у остальных KPI-бэкендов.
_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
SOURCE_TAG = "qualdir_qd_q2_monthly_v6"
CACHE_VERSION = 6

QD_Q2_GROUP_ALIASES: dict[str, list[str]] = {
    "ОТК-1": [
        "отк-1",
        "отк 1",
    ],
    "ОТК-2": [
        "отк-2",
        "отк 2",
    ],
    "Лаборатория неразрушающего контроля": [
        "лаборатория неразрушающего контроля",
        "лнк",
    ],
    "Отдел управления несоответствиями": [
        "отдел управления несоответствиями",
        "отдел управления несоотвествиями",
    ],
}

QD_Q2_GROUP_ORDER = list(QD_Q2_GROUP_ALIASES.keys())

QD_Q2_HR_SCOPE = TurnoverHrScope(
    group_aliases=QD_Q2_GROUP_ALIASES,
    group_order=QD_Q2_GROUP_ORDER,
)

QD_Q2_YTD_CACHE_PREFIX = "qualdir_qd_q2_ytd"
QD_Q2_YTD_DISK_TAG = "qualdir_qd_q2_ytd_payload_v3"
QD_Q2_YTD_DISK_VERSION = 4


def _qd_q2_kpi_pct(plan: Any, fact: Any) -> float | None:
    """KPI по формуле плитки: факт / план × 100 % (QD-Q1 и др.)."""
    if plan is None or fact is None:
        return None
    try:
        pv = float(plan)
        fv = float(fact)
    except (TypeError, ValueError):
        return None
    if pv <= 0:
        if fv <= 0:
            return 100.0
        return None
    return round(fv / pv * 100.0, 1)


def _turnover_kpi_pct(fact: Any) -> float | None:
    """KPI текучести на плитке = факт (%), как TD-Q2."""
    if fact is None:
        return None
    try:
        return round(float(fact), 1)
    except (TypeError, ValueError):
        return None


def turnover_month_cache_path(year: int, month: int) -> Path:
    """Путь к JSON-кэшу помесячного снимка QD-Q2 (для views / cache_manager)."""
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_tekuchet_{year}_{month:02d}.json"


def _month_row_cache_is_perpetual(year: int, month: int) -> bool:
    today = date.today()
    return (year, month) < (today.year, today.month)


def _load_cache(year: int, month: int) -> dict | None:
    path = turnover_month_cache_path(year, month)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if not _month_row_cache_is_perpetual(year, month):
        if data.get("cache_date") != date.today().isoformat():
            return None
    return data


def _save_cache(year: int, month: int, payload: dict[str, Any]) -> None:
    try:
        with turnover_month_cache_path(year, month).open("w", encoding="utf-8") as f:
            json.dump(
                {
                    **payload,
                    "source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def compute_qd_q2_turnover_month(year: int, month: int) -> dict[str, Any]:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached
    result = build_turnover_month_payload(
        year,
        month,
        group_aliases=QD_Q2_GROUP_ALIASES,
        group_order=QD_Q2_GROUP_ORDER,
        aggregate="top2",
        fact_from_hr=True,
        hr_scope=QD_Q2_HR_SCOPE,
    )
    _save_cache(year, month, result)
    return result


def _tile_month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def qd_q2_ytd_cache_path(year: int | None = None, month: int | None = None) -> Path:
    """Путь к JSON-кэшу полного YTD-payload QD-Q2 (``ytd_json_cache``)."""
    cy, cm = year, month
    if cy is None or cm is None:
        cy, cm = _last_full_month()
    return ytd_json_cache.cache_path(QD_Q2_YTD_CACHE_PREFIX, cy, cm)


def get_qd_q2_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """QD-Q2: текучесть персонала службы по качеству (помесячно, как TD-Q2)."""

    cy, cm = year, month
    if cy is None or cm is None:
        cy, cm = _last_full_month()
    _disk_path = ytd_json_cache.cache_path(QD_Q2_YTD_CACHE_PREFIX, cy, cm)
    _perpetual = ytd_json_cache.is_ref_period_fully_past(cy, cm)

    def _runner() -> dict[str, Any]:
        _cached = ytd_json_cache.load_payload(
            _disk_path,
            source_tag=QD_Q2_YTD_DISK_TAG,
            version=QD_Q2_YTD_DISK_VERSION,
            perpetual=_perpetual,
        )
        if _cached is not None:
            return _cached
        try:
            month_rows: list[dict[str, Any]] = []
            for row_year, row_month in _tile_month_pairs(cy, cm):
                snapshot = compute_qd_q2_turnover_month(row_year, row_month)
                plan = snapshot["total_plan"]
                fact = snapshot["total_fact"]
                has_data = plan is not None and fact is not None
                month_rows.append({
                    "year": row_year,
                    "month": row_month,
                    "month_name": MONTH_RU[row_month].lower(),
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": _turnover_kpi_pct(fact),
                    "has_data": has_data,
                    "values_unit": TURNOVER_VALUES_UNIT,
                })

            with_data = [row for row in month_rows if row["has_data"]]
            months_with_data = len(with_data)
            ref_row = next(
                (row for row in month_rows if row["month"] == cm and row["year"] == cy),
                None,
            )
            if ref_row is None and month_rows:
                ref_row = month_rows[-1]

            _out: dict[str, Any] = {
                "data_granularity": "monthly",
                "monthly_data": month_rows,
                "last_full_month_row": dict(ref_row) if ref_row else None,
                "ytd": {
                    "total_plan": ref_row.get("plan") if ref_row else None,
                    "total_fact": ref_row.get("fact") if ref_row else None,
                    "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                    "months_with_data": months_with_data,
                    "months_total": len(month_rows),
                    "values_unit": TURNOVER_VALUES_UNIT,
                },
                "kpi_period": {
                    "type": "last_full_month",
                    "year": cy,
                    "month": cm,
                    "month_name": MONTH_RU[cm],
                    "data_complete": ref_row is not None,
                },
                "debug": {
                    "status": "ok",
                    "kpi_id": "QD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала + HR staffing/dismissals",
                    "plan_source": "group_max_top2_1c_tekuchet",
                    "fact_source": "hr_staff_dismissals_turnover_pct",
                    "groups": QD_Q2_GROUP_ORDER,
                },
            }
        except Exception as exc:
            _out = {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 0,
                    "values_unit": TURNOVER_VALUES_UNIT,
                },
                "kpi_period": {
                    "type": "last_full_month",
                    "year": cy,
                    "month": cm,
                    "month_name": MONTH_RU[cm],
                    "data_complete": False,
                },
                "debug": {
                    "status": "error",
                    "kpi_id": "QD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала",
                    "error": str(exc),
                },
            }
        if (_out.get("debug") or {}).get("status") != "error":
            ytd_json_cache.save_payload(
                _disk_path,
                _out,
                source_tag=QD_Q2_YTD_DISK_TAG,
                version=QD_Q2_YTD_DISK_VERSION,
            )
        return _out

    return ytd_json_cache.resolve_payload(
        _disk_path,
        source_tag=QD_Q2_YTD_DISK_TAG,
        version=QD_Q2_YTD_DISK_VERSION,
        perpetual=_perpetual,
        lock_key=f"qualdir_qd_q2_{cy}_{cm:02d}",
        compute_fn=_runner,
    )
