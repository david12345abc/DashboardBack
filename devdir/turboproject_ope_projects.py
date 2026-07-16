"""TurboProject: проекты типа «ОПЭ» для KPI RD-M2-1.

План — проекты с ``tip_proekta = ОПЭ``, «живые» в месяце.
Факт — без отклонения ≥10 р.д. по вехам в месяце (как RD-M3-1).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from getkpi.cache_manager import stale_while_revalidate
from . import ytd_json_cache
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from .turboproject_projects_by_resources import (
    _api_overdue_milestones,
    _build_projects_deviation_table,
    _build_projects_monthly_payload,
    _overdue_milestone_rows,
    _project_summary,
    api_get,
    build_project_resources,
    login,
)

logger = logging.getLogger(__name__)

TARGET_PROJECT_TYPE = "ОПЭ"

CACHE_DIR = Path(__file__).resolve().parent.parent / "dashboard"
CACHE_PATH = CACHE_DIR / "devdir_turboproject_ope_projects_snapshot.json"
CACHE_VERSION = 2
TABLE_CACHE_PREFIX = "devdir_turboproject_ope_deviations"
TABLE_CACHE_VERSION = 2
TILE_CACHE_PREFIX = "devdir_rd_m2_1_turboproject_ope"
TILE_CACHE_SOURCE_TAG = "devdir_rd_m2_1_turboproject_ope_ytd"
TILE_CACHE_VERSION = 2


def normalize_project_type(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return "".join(value.strip().lower().replace("ё", "е").split())


def is_ope_project_type(value: Any) -> bool:
    normalized = normalize_project_type(value)
    return normalized in {"опэ", "ope"}


def project_type_from_details(details: dict[str, Any]) -> str:
    data_1c = details.get("data_1c") or {}
    project_meta = details.get("project") or {}
    return str(
        data_1c.get("tip_proekta")
        or project_meta.get("tip_proekta")
        or ""
    ).strip()


def _load_cache() -> dict | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        data.get("cache_version") == CACHE_VERSION
        and data.get("cache_date") == date.today().isoformat()
    ):
        return data
    return None


def _save_cache(payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        **payload,
        "cache_date": date.today().isoformat(),
        "cache_version": CACHE_VERSION,
    }
    try:
        CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш ОПЭ-проектов в %s", CACHE_PATH)


def _compute_ope_snapshot() -> dict:
    cached = _load_cache()
    if cached is not None:
        return cached

    session = requests.Session()
    token = login(session)
    summary = api_get(session, "/api/projects/files", token)
    items = summary.get("items") or []
    as_of_date = datetime.now().date()

    ope_projects: list[dict[str, Any]] = []
    for item in items:
        file_id = item.get("id")
        if not file_id:
            continue
        try:
            details = api_get(session, f"/api/projects/files/{file_id}", token)
        except Exception:
            logger.exception("Не удалось получить детали проекта %s", file_id)
            continue

        if not is_ope_project_type(project_type_from_details(details)):
            continue

        resources = build_project_resources(details)
        tasks = details.get("tasks") or []
        overdue_milestones = _overdue_milestone_rows(
            _api_overdue_milestones(details),
            tasks,
            as_of_date=as_of_date,
        )
        project = _project_summary(item, details, overdue_milestones, resources)
        project["tip_proekta"] = project_type_from_details(details)
        ope_projects.append(project)

    fact_projects = [project for project in ope_projects if project.get("is_fact")]
    payload = {
        "projects": ope_projects,
        "summary": {
            "plan": len(ope_projects),
            "fact": len(fact_projects),
            "kpi_pct": (
                round(len(fact_projects) / len(ope_projects) * 100, 1)
                if ope_projects
                else None
            ),
            "selection_scope": "project_type_ope",
            "target_project_type": TARGET_PROJECT_TYPE,
        },
        "debug": {
            "target_project_type": TARGET_PROJECT_TYPE,
            "target_projects_count": len(ope_projects),
            "fact_projects_count": len(fact_projects),
            "cache_path": str(CACHE_PATH),
        },
    }
    _save_cache(payload)
    return payload


def get_ope_projects_snapshot() -> dict:
    return _compute_ope_snapshot()


def _build_ope_monthly_payload(
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    snapshot = get_ope_projects_snapshot()
    payload = _build_projects_monthly_payload(
        year=year,
        month=month,
        projects=list(snapshot.get("projects") or []),
    )
    payload["debug"] = {
        "target_project_type": TARGET_PROJECT_TYPE,
        "target_projects_count": len(snapshot.get("projects") or []),
        "kpi_route": TILE_CACHE_PREFIX,
        "selection_scope": "project_type_ope",
    }
    payload["ytd"]["values_unit"] = "шт."
    for row in payload.get("monthly_data") or []:
        row["values_unit"] = "шт."
    return payload


def get_rd_m2_1_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(TILE_CACHE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _compute_and_save() -> dict | None:
        t0 = time.monotonic()
        logger.info("devdir: RD-M2-1 compute started for %04d-%02d", ref_y, ref_m)
        try:
            payload = _build_ope_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception(
                "devdir: RD-M2-1 compute failed after %.1fs for %04d-%02d",
                time.monotonic() - t0,
                ref_y,
                ref_m,
            )
            stale = ytd_json_cache.load_stale_payload(
                c_path,
                source_tag=TILE_CACHE_SOURCE_TAG,
                version=TILE_CACHE_VERSION,
            )
            if stale is not None:
                logger.info(
                    "devdir: RD-M2-1 serving stale fallback for %04d-%02d",
                    ref_y,
                    ref_m,
                )
                return stale
            return None
        ytd_json_cache.save_payload(
            c_path,
            payload,
            source_tag=TILE_CACHE_SOURCE_TAG,
            version=TILE_CACHE_VERSION,
        )
        ref_row = next(
            (
                row
                for row in (payload.get("monthly_data") or [])
                if row.get("year") == ref_y and row.get("month") == ref_m
            ),
            payload.get("last_full_month_row") or {},
        )
        logger.info(
            "devdir: RD-M2-1 compute done in %.1fs for %04d-%02d plan=%s fact=%s",
            time.monotonic() - t0,
            ref_y,
            ref_m,
            ref_row.get("plan"),
            ref_row.get("fact"),
        )
        return payload

    return ytd_json_cache.resolve_payload(
        c_path,
        source_tag=TILE_CACHE_SOURCE_TAG,
        version=TILE_CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"{TILE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}",
        compute_fn=_compute_and_save,
    )


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> Path:
    y, m = normalize_rd_tile_period(year, month)
    return ytd_json_cache.public_cache_path(TILE_CACHE_PREFIX, y, m)


def _table_cache_path(ref_y: int, ref_m: int) -> Path:
    return CACHE_DIR / f"{TABLE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}.json"


def _load_table_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") != TABLE_CACHE_VERSION:
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    payload = data.get("payload")
    return payload if isinstance(payload, dict) else None


def _save_table_cache(path: Path, payload: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "cache_date": date.today().isoformat(),
        "cache_version": TABLE_CACHE_VERSION,
        "payload": payload,
    }
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш таблицы ОПЭ в %s", path)


def _load_table_cache_stale(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("cache_version") != TABLE_CACHE_VERSION:
        return None
    payload = data.get("payload")
    return payload if isinstance(payload, dict) else None


def get_ope_projects_deviation_table(
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    ref_y = int(year) if year is not None else date.today().year
    ref_m = int(month) if month is not None else date.today().month
    ref_m = max(1, min(12, ref_m))
    cache_path = _table_cache_path(ref_y, ref_m)
    lock_key = f"{TABLE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}"

    def _compute() -> dict[str, Any]:
        snapshot = get_ope_projects_snapshot()
        payload = _build_projects_deviation_table(
            ref_y,
            ref_m,
            projects=list(snapshot.get("projects") or []),
            table_name="Проекты ОПЭ с отклонениями по вехам",
        )
        _save_table_cache(cache_path, payload)
        return payload

    return stale_while_revalidate(
        lock_key,
        lambda: _load_table_cache(cache_path),
        lambda: _load_table_cache_stale(cache_path),
        _compute,
    )
