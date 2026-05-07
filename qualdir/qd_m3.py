"""
QD-M3 — бюджет (директор по качеству / qualdir).

План по месяцам: константы ``QD_M3_PLAN_BY_MONTH_2026``.
Факт: оплаты по заявкам на расход ДС — см. ``qualdir.qd_m3_fact``.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

from getkpi.cache_manager import locked_call
from getkpi.fot_techdir_fact import AUTH

from qualdir.qd_m3_fact import compute_qd_m3_fact_monthly
from qualdir.turnover import _qd_q2_kpi_pct

logger = logging.getLogger(__name__)

SOURCE_TAG = "qualdir_qd_m3_v6"

# Плановый бюджет, 2026, руб./мес. (сумма 7 строк по столбцу месяца).
QD_M3_PLAN_BY_MONTH_2026: dict[int, int] = {
    1: 219_250,
    2: 161_100,
    3: 549_950,
    4: 148_350,
    5: 198_950,
    6: 203_950,
    7: 574_450,
    8: 387_450,
    9: 225_950,
    10: 249_950,
    11: 217_450,
    12: 221_300,
}

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    """Опорный месяц не позже первого дня «сегодня» (нет факта по будущему месяцу)."""
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    try:
        if date(ref_year, ref_month, 1) > date(today.year, today.month, 1):
            ref_year, ref_month = today.year, today.month
    except ValueError:
        pass
    return ref_year, ref_month


def _tile_month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    """Помесячные строки только с января по выбранный опорный месяц включительно.

    Раньше для 2026 верхней границей ошибочно бралось ``max(..., 12)`` из словаря
    планов — всегда декабрь, что тянуло лишние запросы к 1С и путало YTD графика
    относительно выбранного периода.
    """
    rm = max(1, min(12, int(ref_month)))
    return [(year, mm) for mm in range(1, rm + 1)]


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in QD_M3_PLAN_BY_MONTH_2026:
        return float(QD_M3_PLAN_BY_MONTH_2026[month])
    return None


def get_qd_m3_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """QD-M3: план из констант; факт из 1С (регистр ДДС + заявки, см. qd_m3_fact)."""

    def _runner() -> dict[str, Any]:
        try:
            ref_y, ref_m = _normalize_period(year, month)
            pairs = _tile_month_pairs(ref_y, ref_m)
            monthly_rows: list[dict[str, Any]] = []
            ref_row: dict[str, Any] | None = None
            fact_debug_ref_month: dict[str, Any] = {}
            fact_monthly_diag: list[dict[str, Any]] = []

            session = requests.Session()
            session.auth = AUTH

            for y, m in pairs:
                plan = _plan_for_month(y, m)
                fact_payload = compute_qd_m3_fact_monthly(y, m, session=session)
                fact_dbg = {
                    **(fact_payload.get("debug") or {}),
                    "counts": fact_payload.get("counts") or {},
                }
                if (y, m) == (ref_y, ref_m):
                    fact_debug_ref_month = fact_dbg
                fact_monthly_diag.append(
                    {
                        "year": y,
                        "month": m,
                        "total_fact": fact_payload.get("total_fact"),
                        "counts": dict(fact_payload.get("counts") or {}),
                    }
                )
                fact = fact_payload.get("total_fact")
                if isinstance(fact, (int, float)):
                    fact = float(fact)
                else:
                    fact = None
                has_data = plan is not None and fact is not None
                kpi_pct = _qd_q2_kpi_pct(plan, fact) if has_data else None
                row: dict[str, Any] = {
                    "month": m,
                    "year": y,
                    "month_name": MONTH_NAMES[m],
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": kpi_pct,
                    "has_data": has_data,
                }
                if plan is not None or fact is not None:
                    row["values_unit"] = "руб."
                monthly_rows.append(row)
                if (y, m) == (ref_y, ref_m):
                    ref_row = row

            article_probe: dict[str, Any] | None = None
            if fact_monthly_diag:
                heavy = max(
                    fact_monthly_diag,
                    key=lambda d: int((d.get("counts") or {}).get("skipped_no_article_match") or 0),
                )
                h_cnt = int((heavy.get("counts") or {}).get("skipped_no_article_match") or 0)
                if h_cnt >= 20:
                    probe_pl = compute_qd_m3_fact_monthly(
                        heavy["year"], heavy["month"], session=session
                    )
                    pdeb = probe_pl.get("debug") or {}
                    article_probe = {
                        "year": heavy["year"],
                        "month": heavy["month"],
                        "skipped_no_article_match": h_cnt,
                        "dds_no_match_samples": pdeb.get("dds_no_match_samples"),
                        "rejected_cfo_samples_norm": pdeb.get("rejected_cfo_samples_norm"),
                        "recorder_types_in_fact_rows": pdeb.get(
                            "recorder_types_in_fact_rows"
                        ),
                    }

            return {
                "data_granularity": "monthly",
                "monthly_data": monthly_rows,
                "last_full_month_row": dict(ref_row)
                if ref_row
                and (ref_row.get("plan") is not None or ref_row.get("fact") is not None)
                else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {
                    "total_plan": ref_row.get("plan") if ref_row else None,
                    "total_fact": ref_row.get("fact") if ref_row else None,
                    "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                    "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
                    "months_total": len(monthly_rows),
                    **(
                        {"values_unit": "руб."}
                        if ref_row
                        and (
                            ref_row.get("plan") is not None or ref_row.get("fact") is not None
                        )
                        else {}
                    ),
                },
                "debug": {
                    "status": "ok"
                    if any(row.get("plan") is not None for row in monthly_rows)
                    else "no_data",
                    "kpi_id": "QD-M3",
                    "source": SOURCE_TAG,
                    "plan_source": "qualdir.qd_m3.QD_M3_PLAN_BY_MONTH_2026 (7 строк × месяц)",
                    "fact_source": "qualdir.qd_m3_fact (ДДС по заявкам, ЦФО контура качества, статьи ДДС)",
                    "fact_reference_month": fact_debug_ref_month,
                    "fact_last_month": fact_debug_ref_month,
                    "fact_article_mismatch_probe": article_probe,
                    "fact_monthly_diag": fact_monthly_diag,
                    "how_to_debug_fact_zero": (
                        "Смотрите fact_reference_month (опорный месяц дашборда), "
                        "fact_article_mismatch_probe (примеры ДДС при массовом отсеве), "
                        "fact_monthly_diag помесячно: "
                        "если register_rows≈0 — нет строк регистра за период (OData/месяц); "
                        "если fact_like_rows≈0 — нет типов регистраторов «оплата по заявке» "
                        "(см. recorder_types_in_fact_rows в fact_reference_month); "
                        "если большой skipped_no_article_match — см. dds_no_match_samples, "
                        "дополнить QD_M3_DDS_ARTICLE_DESCRIPTIONS/markers; "
                        "если большой skipped_td_cfo — см. rejected_cfo_samples_norm и "
                        "QD_M3_CFO_LABELS; большой skipped_no_request_header — сущность OData заявок."
                    ),
                },
            }
        except Exception as exc:
            logger.exception("Ошибка при расчёте QD-M3 (бюджет qualdir)")
            ref_y, ref_m = _normalize_period(year, month)
            plan_fallback = _plan_for_month(ref_y, ref_m)
            fallback_row: dict[str, Any] | None = None
            if plan_fallback is not None:
                fallback_row = {
                    "month": ref_m,
                    "year": ref_y,
                    "month_name": MONTH_NAMES[ref_m],
                    "plan": plan_fallback,
                    "fact": None,
                    "kpi_pct": None,
                    "has_data": False,
                    "values_unit": "руб.",
                }
            return {
                "data_granularity": "monthly",
                "monthly_data": [fallback_row] if fallback_row else [],
                "last_full_month_row": dict(fallback_row) if fallback_row else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_NAMES[ref_m],
                },
                "ytd": {
                    "total_plan": fallback_row.get("plan") if fallback_row else None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 1 if fallback_row else 0,
                    **({"values_unit": "руб."} if fallback_row else {}),
                },
                "debug": {
                    "status": "error",
                    "kpi_id": "QD-M3",
                    "source": SOURCE_TAG,
                    "error": str(exc),
                    "plan_fallback_row": bool(fallback_row),
                },
            }

    lock_y, lock_m = year, month
    if lock_y is None or lock_m is None:
        lock_y, lock_m = _normalize_period(None, None)
    return locked_call(f"qualdir_qd_m3_v6_{lock_y}_{lock_m:02d}", _runner)
