"""In-memory кэш справочника KpiDefinition с инвалидацией через файл-версию.

Один SELECT на процесс; сброс по mtime файла — работает между воркерами и после import-команд.
"""
from __future__ import annotations

from pathlib import Path

from .cache_manager import CACHE_DIR
from .models import KpiDefinition

_CACHE_VERSION_FILE = CACHE_DIR / '.kpi_definitions_version'

_kpi_by_department: dict[str, list[dict]] | None = None
_cache_version_mtime: float | None = None


def _current_version_mtime() -> float:
    if not _CACHE_VERSION_FILE.exists():
        return 0.0
    return _CACHE_VERSION_FILE.stat().st_mtime


def bump_kpi_definitions_cache_version() -> None:
    """Сбросить кэш в текущем процессе и обновить маркер на диске."""
    global _kpi_by_department, _cache_version_mtime
    _kpi_by_department = None
    _cache_version_mtime = None
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _CACHE_VERSION_FILE.touch()


def get_kpi_definitions_by_department() -> dict[str, list[dict]]:
    """Все KPI, сгруппированные по department (ключ — точное имя из БД)."""
    global _kpi_by_department, _cache_version_mtime
    mtime = _current_version_mtime()
    if _kpi_by_department is not None and mtime == _cache_version_mtime:
        return _kpi_by_department

    by_dept: dict[str, list[dict]] = {}
    for obj in KpiDefinition.objects.order_by('department', 'position', 'kpi_id'):
        by_dept.setdefault(obj.department, []).append(obj.to_dict())

    _kpi_by_department = by_dept
    _cache_version_mtime = mtime
    return _kpi_by_department


def get_kpi_dicts_for_department(department: str) -> list[dict]:
    """KPI подразделения (точное совпадение department)."""
    return list(get_kpi_definitions_by_department().get(department, []))


def lookup_kpi_dicts_for_department(department: str) -> list[dict] | None:
    """KPI подразделения; при отсутствии — поиск без учёта регистра."""
    by_dept = get_kpi_definitions_by_department()
    if department in by_dept:
        return list(by_dept[department])
    norm = department.casefold()
    for dept_key, rows in by_dept.items():
        if dept_key.casefold() == norm:
            return list(rows)
    return None


def get_department_names() -> list[str]:
    return sorted(get_kpi_definitions_by_department().keys())


def get_all_department_names() -> set[str]:
    return set(get_kpi_definitions_by_department().keys())
