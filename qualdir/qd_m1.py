"""
QD-M1 — внешний брак (директор по качеству / qualdir).

Источник: отчёт Excel в сетевой папке за год/месяц — см. ``qualdir.external_defect_report_total``.
**Плана нет:** только факт (общий итог «Общий итог» + разрез по типам 1–15).

Разрез по статьям классификатора в JSON вынесен в блок ``classifier`` (разделы
``reference_month`` — как на плитке за выбранный месяц, ``by_month`` — по каждому
месяцу YTD). Поле ``articles`` дублирует ``classifier.reference_month.items`` для
совместимости со старым фронтом.

Путь к каталогу отчётов: переменная ``QD_M1_REPORTS_BASE_PATH`` (иначе см. константу
``BASE_REPORTS_PATH`` в ``external_defect_report_total``).

Для API: ``get_qd_m1_ytd`` — JSON-кэш в ``getkpi/dashboard/qualdir_qd_m1_tile_<Y>_<MM>.json``
(календарный день + совпадение mtime всех найденных отчётов за январь..опорный месяц),
параллельные запросы — ``locked_call``.
"""

from __future__ import annotations

import json
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
QD_M1_TILE_CACHE_VERSION = 5
_CACHE_ROOT = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"


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


def _qd_m1_classifier_placeholder_rows() -> list[dict[str, Any]]:
    """15 типов классификатора без факта — чтобы API всегда отдавал разрез для UI."""
    rows: list[dict[str, Any]] = []
    for ct in edr.CLASSIFIER_TYPES:
        rows.append(
            {
                "classifier_type": ct,
                "name": edr.CLASSIFIER_DISPLAY_NAMES.get(ct, f"Тип {ct}"),
                "plan": None,
                "fact": None,
            }
        )
    return rows


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
            "articles": _qd_m1_classifier_placeholder_rows(),
            "report_path": None,
            "error": str(exc),
            "classifier_parse_warning": None,
        }


def qd_m1_tile_cache_path(year: int, month: int) -> Path:
    """Путь к JSON-кэшу плитки QD-M1 за пару (год, месяц)."""
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"qualdir_qd_m1_tile_{year}_{month:02d}.json"


def _qd_m1_excel_signature_for_cache(ref_y: int, ref_m: int) -> list[list[Any]]:
    """Снимок путей отчётов (янв..ref_m) и mtime — для сравнения с записью в кэше."""
    rows: list[tuple[str, float]] = []
    base = _reports_base_path()
    for mm in range(1, max(1, min(12, ref_m)) + 1):
        try:
            yp = edr.find_year_folder(base, ref_y)
            mp = edr.find_month_folder(yp, str(mm))
            p = edr.find_report_file(mp)
            if p.is_file():
                rows.append((str(p), float(p.stat().st_mtime)))
        except (FileNotFoundError, OSError, ValueError):
            continue
    rows.sort(key=lambda x: x[0])
    return [[a, b] for a, b in rows]


def _qd_m1_signatures_match(
    stored: list[Any] | None,
    current: list[list[Any]],
) -> bool:
    if not isinstance(stored, list):
        return False
    norm_stored: list[tuple[str, float]] = []
    for item in stored:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            norm_stored.append((str(item[0]), float(item[1])))
    norm_stored.sort(key=lambda x: x[0])
    norm_cur = [(str(a), float(b)) for a, b in current]
    return norm_stored == norm_cur


def _load_qd_m1_tile_cache(ref_y: int, ref_m: int) -> dict[str, Any] | None:
    path = qd_m1_tile_cache_path(ref_y, ref_m)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != QD_M1_TILE_CACHE_VERSION:
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    cur_sig = _qd_m1_excel_signature_for_cache(ref_y, ref_m)
    if not _qd_m1_signatures_match(data.get("excel_signature"), cur_sig):
        return None
    tile = data.get("tile")
    if not isinstance(tile, dict):
        return None
    # Старые кэши без разреза по типам (articles: null) — промах, пересчитать из Excel.
    if "articles" not in tile or tile.get("articles") is None:
        return None
    art = tile.get("articles")
    if not isinstance(art, list):
        return None
    if len(art) < len(edr.CLASSIFIER_TYPES):
        return None
    clf = tile.get("classifier")
    if not isinstance(clf, dict):
        return None
    refm = clf.get("reference_month")
    if not isinstance(refm, dict):
        return None
    ref_items = refm.get("items") or []
    if len(ref_items) < len(edr.CLASSIFIER_TYPES):
        return None
    if not isinstance(clf.get("by_month"), list):
        return None
    return tile


def _save_qd_m1_tile_cache(
    ref_y: int,
    ref_m: int,
    tile: dict[str, Any],
    *,
    excel_signature: list[list[Any]],
) -> None:
    try:
        path = qd_m1_tile_cache_path(ref_y, ref_m)
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "tile": tile,
                    "source": SOURCE_TAG,
                    "cache_version": QD_M1_TILE_CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                    "excel_signature": excel_signature,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def _qd_m1_classifier_json_sections(
    monthly_rows: list[dict[str, Any]],
    articles_out: list[dict[str, Any]],
    ref_y: int,
    ref_m: int,
) -> dict[str, Any]:
    """Структура «по разделам»: опорный месяц + помесячные срезы по типам брака."""
    by_month: list[dict[str, Any]] = []
    for r in monthly_rows:
        raw_items = r.get("articles")
        by_month.append(
            {
                "year": r["year"],
                "month": r["month"],
                "month_name": r["month_name"],
                "total_fact": r.get("fact"),
                "has_data": r.get("has_data"),
                "items": [dict(a) for a in raw_items] if raw_items else None,
            }
        )
    return {
        "reference_month": {
            "year": ref_y,
            "month": ref_m,
            "items": [dict(a) for a in articles_out],
        },
        "by_month": by_month,
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


def _compute_qd_m1_tile(ref_y: int, ref_m: int) -> dict[str, Any]:
    """Полный payload плитки QD-M1 без дискового кэша (чтение Excel)."""
    try:
        pairs = _month_pairs(ref_y, ref_m)
        monthly_rows: list[dict[str, Any]] = []
        ref_snap: dict[str, Any] | None = None
        last_ok_snap: dict[str, Any] | None = None

        n_types = len(edr.CLASSIFIER_TYPES)
        for y, m in pairs:
            snap = read_qd_m1_external_report_snapshot(y, m)
            if snap.get("ok"):
                last_ok_snap = snap
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
            snap_arts = snap.get("articles") or []
            if snap.get("ok") and len(snap_arts) >= n_types:
                row["articles"] = [dict(a) for a in snap_arts]
            monthly_rows.append(row)
            if (y, m) == (ref_y, ref_m):
                ref_snap = snap

        # Разрез по 15 типам (плитка): при успешном опорном месяце — из отчёта; иначе — с последнего
        # успешно прочитанного месяца в году (напр. май без файла → апрель); иначе заглушки.
        articles_out: list[dict[str, Any]]
        used_articles_fallback = False
        if ref_snap and ref_snap.get("ok") and len(ref_snap.get("articles") or []) >= n_types:
            articles_out = [dict(a) for a in (ref_snap.get("articles") or [])]
        elif (
            (not ref_snap or not ref_snap.get("ok"))
            and last_ok_snap
            and last_ok_snap.get("ok")
            and len(last_ok_snap.get("articles") or []) >= n_types
        ):
            articles_out = [dict(a) for a in (last_ok_snap.get("articles") or [])]
            used_articles_fallback = last_ok_snap is not ref_snap
        elif ref_snap and len(ref_snap.get("articles") or []) >= n_types:
            articles_out = [dict(a) for a in (ref_snap.get("articles") or [])]
        else:
            articles_out = _qd_m1_classifier_placeholder_rows()

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
        if used_articles_fallback and last_ok_snap:
            debug_base["articles_fallback_from"] = {
                "year": last_ok_snap.get("year"),
                "month": last_ok_snap.get("month"),
            }
        if ref_row and ref_row.get("has_data"):
            lfr_source = dict(ref_row)
        else:
            lfr_source = None

        classifier_payload = _qd_m1_classifier_json_sections(
            monthly_rows, articles_out, ref_y, ref_m,
        )
        for r in monthly_rows:
            r.pop("articles", None)
        if lfr_source:
            lfr_source.pop("articles", None)

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
            "classifier": classifier_payload,
            "articles": [dict(a) for a in articles_out],
            "debug": debug_base,
        }
    except Exception as exc:
        logger.exception("Ошибка при расчёте QD-M1 (внешний отчёт)")
        ph = _qd_m1_classifier_placeholder_rows()
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
            "classifier": {
                "reference_month": {"year": ref_y, "month": ref_m, "items": ph},
                "by_month": [],
            },
            "articles": ph,
            "debug": {
                "status": "error",
                "kpi_id": "QD-M1",
                "source": SOURCE_TAG,
                "error": str(exc),
                "reports_base": str(_reports_base_path()),
            },
        }


def get_qd_m1_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Помесячно до выбранного месяца: только факт, план везде ``None``.

    Чтение из JSON-кэша (тот же календарный день + те же mtime отчётов) либо пересчёт из Excel.
    """

    lock_y, lock_m = _normalize_period(year, month)

    def _runner() -> dict[str, Any]:
        cached = _load_qd_m1_tile_cache(lock_y, lock_m)
        if cached is not None:
            return cached
        payload = _compute_qd_m1_tile(lock_y, lock_m)
        if (payload.get("debug") or {}).get("status") == "ok":
            sig = _qd_m1_excel_signature_for_cache(lock_y, lock_m)
            _save_qd_m1_tile_cache(lock_y, lock_m, payload, excel_signature=sig)
        return payload

    return locked_call(f"qualdir_qd_m1_v4_{lock_y}_{lock_m:02d}", _runner)
