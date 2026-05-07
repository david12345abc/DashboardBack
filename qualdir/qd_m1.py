"""
QD-M1 — внешний брак (директор по качеству / qualdir).

Источник: отчёт Excel в сетевой папке за год/месяц — см. ``qualdir.external_defect_report_total``.
**Плана нет:** только факт (общий итог «Общий итог» + разрез по типам 1–15 в ``articles``).

Путь к каталогу отчётов: переменная ``QD_M1_REPORTS_BASE_PATH`` (иначе см. константу
``BASE_REPORTS_PATH`` в ``external_defect_report_total``).
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

from getkpi.cache_manager import locked_call
from getkpi.techdir_tekuchet import MONTH_RU

from qualdir import external_defect_report_total as edr

logger = logging.getLogger(__name__)

SOURCE_TAG = "qualdir_qd_m1_external_report_v2"


def _reports_base_path() -> Path:
    raw = (os.environ.get("QD_M1_REPORTS_BASE_PATH") or "").strip()
    return Path(raw) if raw else edr.BASE_REPORTS_PATH


def _normalize_period(year: int | None, month: int | None) -> tuple[int, int]:
    """Год и месяц запроса как передал клиент (или текущие по умолчанию).

    Запрошенный «будущий» месяц не режем до today: иначе в ``monthly_data`` нет
    строки за этот месяц, а плитки ищут ряд по выбранному месяцу и ошибочно
    берут ``rows[-1]`` — факт предыдущего месяца (напр. за май показывался апрель).
    """
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    return ref_year, ref_month


def _month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    return [(year, mm) for mm in range(1, ref_month + 1)]


def _num_for_json(x: float) -> float | int:
    """Целые в JSON как int, иначе float (без строк)."""
    if x != x:  # NaN
        return 0
    r = round(float(x), 6)
    if abs(r - round(r)) < 1e-6:
        return int(round(r))
    return r


def read_qd_m1_external_report_snapshot(
    year: int,
    month: int,
    *,
    base_path: Path | None = None,
) -> dict[str, Any]:
    """
    Один месяц: общий факт, статьи по классификатору, путь к файлу или ошибка.
    """
    base = base_path or _reports_base_path()
    try:
        year_path = edr.find_year_folder(base, year)
        month_path = edr.find_month_folder(year_path, str(month))
        report_path = edr.find_report_file(month_path)
        (
            _cell,
            total_value,
            classifier_totals,
            classifier_names,
            classifier_warn,
        ) = edr.read_report_metrics(report_path)
        total_fact = float(edr.parse_number(total_value))
        articles: list[dict[str, Any]] = []
        for ct in edr.CLASSIFIER_TYPES:
            raw = float(classifier_totals.get(ct, 0.0))
            val = _num_for_json(raw)
            name = classifier_names.get(ct, f"Тип {ct}")
            articles.append(
                {
                    "classifier_type": ct,
                    "name": name,
                    "plan": None,
                    "fact": val,
                }
            )
        return {
            "ok": True,
            "year": year,
            "month": month,
            "fact": _num_for_json(total_fact),
            "articles": articles,
            "report_path": str(report_path),
            "error": None,
            "classifier_parse_warning": classifier_warn,
        }
    except Exception as exc:
        logger.warning("QD-M1: нет данных за %d-%02d: %s", year, month, exc)
        return {
            "ok": False,
            "year": year,
            "month": month,
            "fact": None,
            "articles": [],
            "report_path": None,
            "error": str(exc),
            "classifier_parse_warning": None,
        }


def qd_m1_excel_paths_for_cache_stamp(
    ref_y: int | None = None,
    ref_m: int | None = None,
) -> list[Path]:
    """Файл отчёта за выбранный период (для ``cache_updated_at``)."""
    if ref_y is None or ref_m is None:
        ref_y, ref_m = _normalize_period(None, None)
    base = _reports_base_path()
    try:
        yp = edr.find_year_folder(base, ref_y)
        mp = edr.find_month_folder(yp, str(ref_m))
        p = edr.find_report_file(mp)
        return [p] if p.is_file() else []
    except (FileNotFoundError, OSError, ValueError):
        return []


def get_qd_m1_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Помесячно до выбранного месяца: только факт, план везде ``None``."""

    def _runner() -> dict[str, Any]:
        try:
            ref_y, ref_m = _normalize_period(year, month)
            pairs = _month_pairs(ref_y, ref_m)
            monthly_rows: list[dict[str, Any]] = []
            articles_out: list[dict[str, Any]] | None = None
            ref_snap: dict[str, Any] | None = None

            for y, m in pairs:
                snap = read_qd_m1_external_report_snapshot(y, m)
                fact = snap.get("fact") if snap.get("ok") else None
                has_data = snap.get("ok") is True
                if has_data and isinstance(fact, (int, float)):
                    fact = _num_for_json(float(fact))
                row = {
                    "year": y,
                    "month": m,
                    "month_name": MONTH_RU[m].lower(),
                    "plan": None,
                    "fact": fact if has_data else None,
                    "kpi_pct": None,
                    "has_data": has_data,
                }
                if has_data:
                    row["values_unit"] = "шт."
                monthly_rows.append(row)
                if (y, m) == (ref_y, ref_m):
                    ref_snap = snap

            if ref_snap and ref_snap.get("ok"):
                articles_out = list(ref_snap.get("articles") or [])

            ref_row = next(
                (r for r in monthly_rows if r["year"] == ref_y and r["month"] == ref_m),
                monthly_rows[-1] if monthly_rows else None,
            )
            months_with_data = sum(1 for r in monthly_rows if r.get("has_data"))

            debug_base: dict[str, Any] = {
                "status": "ok",
                "kpi_id": "QD-M1",
                "source": SOURCE_TAG,
                "logic": "qualdir.external_defect_report_total",
                "reports_base": str(_reports_base_path()),
            }
            if ref_snap:
                debug_base.update(
                    {
                        "report_path": ref_snap.get("report_path"),
                        "snapshot_ok": ref_snap.get("ok"),
                        "snapshot_error": ref_snap.get("error"),
                        "classifier_parse_warning": ref_snap.get("classifier_parse_warning"),
                    }
                )
            if ref_row and ref_row.get("has_data"):
                lfr_source = dict(ref_row)
            else:
                lfr_source = None

            return {
                "data_granularity": "monthly",
                "monthly_data": monthly_rows,
                "last_full_month_row": dict(lfr_source) if lfr_source else None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_RU[ref_m],
                    "data_complete": bool(ref_snap and ref_snap.get("ok")),
                },
                "ytd": {
                    "total_plan": None,
                    "total_fact": (lfr_source.get("fact") if lfr_source else None),
                    "kpi_pct": None,
                    "months_with_data": months_with_data,
                    "months_total": len(monthly_rows),
                    **(
                        {"values_unit": "шт."}
                        if lfr_source and lfr_source.get("has_data")
                        else {}
                    ),
                },
                "articles": articles_out,
                "debug": debug_base,
            }
        except Exception as exc:
            logger.exception("Ошибка при расчёте QD-M1 (внешний отчёт)")
            ref_y, ref_m = _normalize_period(year, month)
            return {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "kpi_period": {
                    "type": "last_full_month",
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": MONTH_RU[ref_m],
                    "data_complete": False,
                },
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 0,
                },
                "articles": None,
                "debug": {
                    "status": "error",
                    "kpi_id": "QD-M1",
                    "source": SOURCE_TAG,
                    "error": str(exc),
                    "reports_base": str(_reports_base_path()),
                },
            }

    lock_y, lock_m = year, month
    if lock_y is None or lock_m is None:
        lock_y, lock_m = _normalize_period(None, None)
    return locked_call(f"qualdir_qd_m1_v4_{lock_y}_{lock_m:02d}", _runner)
