"""TurboProject: проекты типа «ОПЭ» для KPI RD-M2-1.

План — проекты с ``tip_proekta = ОПЭ``, «живые» в месяце.
Факт — без просроченных вех в этом месяце (как RD-M3-1).
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from ..cache_manager import locked_call
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
CACHE_VERSION = 1
TABLE_CACHE_PREFIX = "devdir_turboproject_ope_deviations"
TABLE_CACHE_VERSION = 1
TILE_CACHE_PREFIX = "devdir_rd_m2_1_turboproject_ope"
TILE_CACHE_SOURCE_TAG = "devdir_rd_m2_1_turboproject_ope_ytd"
TILE_CACHE_VERSION = 1


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

    def _runner() -> dict | None:
        cached = ytd_json_cache.load_payload(
            c_path,
            source_tag=TILE_CACHE_SOURCE_TAG,
            version=TILE_CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_ope_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M2-1 (TurboProject ОПЭ)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                c_path,
                payload,
                source_tag=TILE_CACHE_SOURCE_TAG,
                version=TILE_CACHE_VERSION,
            )
        return payload

    return locked_call(f"{TILE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}", _runner)


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


def get_ope_projects_deviation_table(
    year: int | None = None,
    month: int | None = None,
) -> dict[str, Any]:
    ref_y = int(year) if year is not None else date.today().year
    ref_m = int(month) if month is not None else date.today().month
    ref_m = max(1, min(12, ref_m))
    cache_path = _table_cache_path(ref_y, ref_m)

    def _runner() -> dict[str, Any]:
        cached = _load_table_cache(cache_path)
        if cached is not None:
            return cached
        snapshot = get_ope_projects_snapshot()
        payload = _build_projects_deviation_table(
            ref_y,
            ref_m,
            projects=list(snapshot.get("projects") or []),
            table_name="Проекты ОПЭ с отклонениями по вехам",
        )
        _save_table_cache(cache_path, payload)
        return payload

    return locked_call(f"{TABLE_CACHE_PREFIX}_{ref_y}_{ref_m:02d}", _runner)
