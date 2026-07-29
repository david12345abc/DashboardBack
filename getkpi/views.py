import json
import logging
import calendar
import copy
import random
import re
import threading
import time
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_http_methods

from User.views import login_required
from . import (
    cache_manager,
    calc_chief_accountant,
    calc_budget_limit,
    calc_dengi_fact,
    calc_fot_management,
    calc_otif_vypusk_zam_proizvodstva,
    calc_plan,
    calc_postavshchiki,
    calc_tekuchest_opdir,
    calc_vyruchka_opdir,
    chairman_data,
    commercial_cache_scheduler,
    denzhi_dz,
    dept_budget_m3,
    dept_dz,
    dept_protocol_tables,
    dept_turnover_q5,
    logistics_views,
    employee_headcount,
    komdir_dashboard,
    komdir_quarterly,
    qualdir_tables,
    techdir_dashboard,
    techdir_projects,
    valovaya_pribyl,
)
from devdir import turboproject_projects_by_resources as _devdir_turboproject_projects
from devdir import turboproject_ope_projects as _devdir_turboproject_ope
from .commercial_tiles import commercial_kpi_key, dept_guid_for_kpi_key, is_komdir_child
from .calc_sudy_by_dept import get_sudy_by_department
from .kpi_periods import last_full_month, last_full_quarter, pick_monthly_row_for_period
from .kpi_definitions_cache import (
    get_all_department_names,
    get_department_names,
    get_kpi_dicts_for_department,
    lookup_kpi_dicts_for_department,
)
import devdir.views as _devdir_kpi_views
import gspp.views as _gspp_kpi_views
import qualdir.views as _qualdir_kpi_views
import sup.views as _sup_kpi_views
import servhead.views as _servhead_kpi_views
import autoit.views as _autoit_kpi_views
import c1auto.views as _c1auto_kpi_views
from . import techdir_kpi_entry
from qualdir.turnover import _qd_q2_kpi_pct

_STRUCTURE_FILE = Path(__file__).resolve().parent / 'structure.json'
_structure_cache: dict | None = None
_structure_mtime: float | None = None
logger = logging.getLogger(__name__)

MANUAL_TILE_REFRESH_COOLDOWN_SECONDS = 6 * 60 * 60
MANUAL_TILE_REFRESH_STATE_PATH = cache_manager.CACHE_DIR / 'manual_tile_refresh_state.json'
_manual_tile_refresh_state_lock = threading.Lock()

PSD_CLAIM_REASON_PRETENSION_KEY = "7a4719be-3e1b-11ec-8742-ac1f6b05524d"
PSD_CLAIM_MIN_ORDER_SUM = 1_000_000

PROD_DEPUTY_OUTPUT_PERIOD_BY_ID = {
    'PD-M1.1.M': ('pc1', 'month'),
    'PD-M1.1.W': ('pc1', 'week'),
    'PD-M1.1.T': ('pc1', 'total'),
    'PD-M1.2.M': ('pc2', 'month'),
    'PD-M1.2.W': ('pc2', 'week'),
    'PD-M1.2.T': ('pc2', 'total'),
}

PROD_DEPUTY_TILE_ORDER = {
    'PD-M1.1.W': 0,
    'PD-M1.1.M': 1,
    'PD-M1.1.T': 2,
    'PD-M1.2.W': 3,
    'PD-M1.2.M': 4,
    'PD-M1.2.T': 5,
}


def get_structure_data() -> dict:
    """
    Иерархия подразделений всегда из structure.json на диске.
    Кэш сбрасывается при изменении файла — правки JSON видны без перезапуска сервера.
    """
    global _structure_cache, _structure_mtime
    mtime = _STRUCTURE_FILE.stat().st_mtime
    if _structure_cache is not None and mtime == _structure_mtime:
        return _structure_cache
    with open(_STRUCTURE_FILE, encoding='utf-8') as _f:
        _structure_cache = json.load(_f)
    _structure_mtime = mtime
    return _structure_cache

def _get_departments() -> list[str]:
    return get_department_names()


def _get_kpi_dicts(department: str) -> list[dict]:
    """Все KPI подразделения в формате dict (как был kpi_data.json)."""
    rows = get_kpi_dicts_for_department(department)
    if logistics_views.is_logistics_head_department(department):
        if not rows:
            return logistics_views.kpi_definition_fallback(department) or rows
        rows = logistics_views.normalize_kpi_definitions(department, rows)
    if _is_chief_constructor_department(department):
        rows = _normalize_chief_constructor_kpi_definitions(department, rows)
    if _is_chief_metrolog_department(department):
        rows = _normalize_chief_metrolog_kpi_definitions(department, rows)
    if _is_chief_accountant_department(department):
        rows = _normalize_chief_accountant_kpi_definitions(department, rows)
    if _is_prod_deputy_department(department):
        rows = [row for row in rows if str(row.get('kpi_id') or '') != 'UFG-H']
        rows = _filter_prod_deputy_rows_for_department(department, rows)
        split_ids = _required_prod_deputy_kpi_ids(department)
        has_split_pd = split_ids.issubset({str(row.get('kpi_id') or '') for row in rows})
        if not has_split_pd:
            try:
                from .management.commands.import_prod_deputy_kpi import PD_KPI_DEFINITIONS
            except Exception:
                return rows
            fallback = _prod_deputy_fallback_rows_for_department(department, PD_KPI_DEFINITIONS)
            if fallback:
                return sorted(fallback, key=_prod_deputy_tile_sort_key)
        rows = sorted(rows, key=_prod_deputy_tile_sort_key)
    return rows


def _lookup_kpi_data(department: str) -> list[dict] | None:
    """Case-insensitive lookup в справочнике kpi_definition."""
    rows = lookup_kpi_dicts_for_department(department)
    if rows is None:
        if logistics_views.is_logistics_head_department(department):
            return logistics_views.kpi_definition_fallback(department)
        if _is_prod_deputy_department(department):
            try:
                from .management.commands.import_prod_deputy_kpi import PD_KPI_DEFINITIONS
            except Exception:
                return None
            return _prod_deputy_fallback_rows_for_department(department, PD_KPI_DEFINITIONS)
        if _is_chief_accountant_department(department):
            return _normalize_chief_accountant_kpi_definitions(department, [])
        return None
    if logistics_views.is_logistics_head_department(department):
        rows = logistics_views.normalize_kpi_definitions(department, rows)
    if _is_chief_constructor_department(department):
        rows = _normalize_chief_constructor_kpi_definitions(department, rows)
    if _is_chief_metrolog_department(department):
        rows = _normalize_chief_metrolog_kpi_definitions(department, rows)
    if _is_chief_accountant_department(department):
        rows = _normalize_chief_accountant_kpi_definitions(department, rows)
    if _is_prod_deputy_department(department):
        rows = [row for row in rows if str(row.get('kpi_id') or '') != 'UFG-H']
        rows = _filter_prod_deputy_rows_for_department(department, rows)
        split_ids = _required_prod_deputy_kpi_ids(department)
        has_split_pd = split_ids.issubset({str(row.get('kpi_id') or '') for row in rows})
        if not has_split_pd:
            try:
                from .management.commands.import_prod_deputy_kpi import PD_KPI_DEFINITIONS
            except Exception:
                return rows
            return sorted(
                _prod_deputy_fallback_rows_for_department(department, PD_KPI_DEFINITIONS),
                key=_prod_deputy_tile_sort_key,
            )
        rows = sorted(rows, key=_prod_deputy_tile_sort_key)
    return rows


def _all_department_names() -> set[str]:
    """Множество всех уникальных department из справочника KPI."""
    return get_all_department_names()


def _normalize_department_name(value: str | None) -> str:
    """Нормализация имени подразделения для сравнений в дереве."""
    return " ".join(str(value or "").replace("ё", "е").lower().split())


def _collect_all_keys(tree) -> set[str]:
    """Рекурсивно собирает все названия подразделений из дерева."""
    result = set()
    if isinstance(tree, dict):
        for key, children in tree.items():
            result.add(key)
            result.update(_collect_all_keys(children))
    elif isinstance(tree, list):
        for item in tree:
            if isinstance(item, str):
                result.add(item)
            elif isinstance(item, dict):
                result.update(_collect_all_keys(item))
    return result


def _chairman_and_immediate_children() -> list[str]:
    """Сам ПСД (каноническое имя из structure.json) + только его непосредственные дети (первый уровень)."""
    tree = get_structure_data()

    def find(node) -> tuple[str, object] | None:
        if isinstance(node, dict):
            for key, child in node.items():
                if chairman_data.is_chairman_department(key):
                    return key, child
                found = find(child)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for it in node:
                found = find(it)
                if found is not None:
                    return found
        return None

    pair = find(tree)
    if pair is None:
        return []
    root_name, children_tree = pair
    return [root_name] + _immediate_children_of_node(children_tree)


def _find_subordinates(tree, target: str) -> set[str] | None:
    """
    Находит target в дереве (case-insensitive) и возвращает множество всех подразделений
    ниже по иерархии (включая сам target). Возвращает None если не найден.
    """
    target_lower = _normalize_department_name(target)
    if isinstance(tree, dict):
        for key, children in tree.items():
            if _normalize_department_name(key) == target_lower:
                subs = {key}
                subs.update(_collect_all_keys(children))
                return subs
            found = _find_subordinates(children, target)
            if found is not None:
                return found
    elif isinstance(tree, list):
        for item in tree:
            if isinstance(item, str) and _normalize_department_name(item) == target_lower:
                return {item}
            elif isinstance(item, dict):
                found = _find_subordinates(item, target)
                if found is not None:
                    return found
    return None


def _get_allowed_departments(user_department: str) -> set[str]:
    """
    Возвращает множество подразделений, которые пользователь имеет право просматривать.
    Включает как ключи из structure.json, так и соответствующие ключи из БД KPI
    (case-insensitive matching).
    """
    subordinates = _find_subordinates(get_structure_data(), user_department)
    if subordinates is None:
        subordinates = {user_department}

    result = set(subordinates)
    result.add(user_department)
    db_depts = _all_department_names()
    lower_map = {d.lower(): d for d in db_depts}
    for sub in subordinates:
        kpi_key = lower_map.get(sub.lower())
        if kpi_key:
            result.add(kpi_key)
    user_kpi_key = lower_map.get(user_department.lower())
    if user_kpi_key:
        result.add(user_kpi_key)

    extra = set()
    for name in result:
        ck = commercial_kpi_key(name)
        if isinstance(ck, str):
            extra.add(ck)
    result |= extra

    return result


def _immediate_children_of_node(children) -> list[str]:
    """Только непосредственные дочерние подразделения (без рекурсии вглубь)."""
    if children is None:
        return []
    if isinstance(children, dict):
        return list(children.keys())
    if isinstance(children, list):
        out: list[str] = []
        for item in children:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                out.extend(item.keys())
        return out
    return []


def _find_immediate_children(tree, target: str) -> tuple[str, list[str]] | None:
    """
    Находит подразделение в дереве (без учёта регистра) и возвращает
    (каноническое имя из JSON, список непосредственных потомков).
    """
    target_lower = _normalize_department_name(target)
    if isinstance(tree, dict):
        for key, child_tree in tree.items():
            if _normalize_department_name(key) == target_lower:
                return key, _immediate_children_of_node(child_tree)
            found = _find_immediate_children(child_tree, target)
            if found is not None:
                return found
    elif isinstance(tree, list):
        for item in tree:
            if isinstance(item, str) and _normalize_department_name(item) == target_lower:
                return item, []
            if isinstance(item, dict):
                found = _find_immediate_children(item, target)
                if found is not None:
                    return found
    return None


def _is_komdir_department(dept: str) -> bool:
    d = dept.strip().lower()
    return 'коммерческий' in d and 'директор' in d


def _is_devdir_department(dept: str | None) -> bool:
    normalized = re.sub(r'\s+', ' ', (dept or '').strip().lower())
    return normalized in {
        'директор по развитию',
        'директор по развитию / руководитель службы развития',
    }


QUALDIR_BAR_TILE_IDS = frozenset({'QD-M1', 'QD-M5', 'QD-M8'})


def _is_qualdir_department(dept: str | None) -> bool:
    """Дашборд службы качества (QD-*): одна роль, разные подписи в оргструктуре и БД.

    «Заместитель / зам. технического директора по качеству» и «директор по качеству» —
    это один контур для KPI; опорный месяц в API — календарный (не last_full_month).
    Ключ ``qualdir`` — служебное имя отдела в настройках.
    """
    normalized = re.sub(r'\s+', ' ', (dept or '').strip().lower()).replace('ё', 'е')
    return normalized in {
        'директор по качеству',
        'qualdir',
        'заместитель тех. директора по качеству',
        'заместитель технического директора по качеству',
        'зам. технического директора по качеству',
    }


def _is_qualdir_dashboard(dept: str | None, all_kpis: list[dict] | None = None) -> bool:
    """Qualdir по имени отдела или по наличию QD-* плиток в метаданных."""
    if _is_qualdir_department(dept):
        return True
    if not all_kpis:
        return False
    tile_ids = {
        _normalize_dashboard_kpi_id(k.get('kpi_id'))
        for k in all_kpis
        if k.get('block', 'плитка') == 'плитка'
    }
    return bool(tile_ids & (QUALDIR_BAR_TILE_IDS | {'QD-M3', 'QD-M4'}))


def _entry_by_kpi_id(entries_by_id: dict[str, dict], kpi_id: str) -> dict:
    kid = _normalize_dashboard_kpi_id(kpi_id)
    direct = entries_by_id.get(kid)
    if direct is not None:
        return direct
    for raw_key, entry in entries_by_id.items():
        if _normalize_dashboard_kpi_id(raw_key) == kid:
            return entry
    return {}


def _qualdir_monthly_rows_for_chart(entry: dict, ref_y: int, ref_m: int) -> list[dict]:
    """Помесячные строки QD-M1/M5/M8: с января ref_y по выбранный ref_m."""
    rows: list[dict] = []
    for row in entry.get('monthly_data') or []:
        if not isinstance(row, dict):
            continue
        y = row.get('year')
        m = row.get('month')
        if y is None or m is None:
            continue
        yi, mi = int(y), int(m)
        if yi == ref_y and 1 <= mi <= ref_m:
            rows.append(row)
    rows.sort(key=lambda r: (int(r.get('year', 0)), int(r.get('month', 0))))
    return rows


def _is_prod_deputy_department(dept: str | None) -> bool:
    normalized = re.sub(r'\s+', ' ', (dept or '').strip().lower())
    normalized = re.sub(r'\s*-\s*', '-', normalized)
    return normalized in {
        'заместитель операционного директора-директор по производству',
        'заместитель директора по производству',
    }


def _is_production_director_department(dept: str | None) -> bool:
    normalized = re.sub(r'\s+', ' ', (dept or '').strip().lower())
    normalized = re.sub(r'\s*-\s*', '-', normalized)
    return normalized == 'заместитель директора по производству'


def _is_chief_constructor_department(dept: str | None) -> bool:
    normalized = re.sub(r'\s+', ' ', (dept or '').strip().lower())
    return normalized == 'главный конструктор'


def _is_chief_metrolog_department(dept: str | None) -> bool:
    normalized = re.sub(r'\s+', ' ', (dept or '').strip().lower())
    return normalized == 'главный метролог'


def _normalize_chief_accountant_department(dept: str | None) -> str:
    normalized = re.sub(
        r'\s+', ' ',
        unicodedata.normalize('NFKC', (dept or '').strip()).lower(),
    )
    normalized = re.sub(r'\s*-\s*', '-', normalized)
    return normalized


def _is_chief_accountant_department(dept: str | None) -> bool:
    return _normalize_chief_accountant_department(dept) in {
        'главный бухгалтер',
        'главный бухгалтер нпо',
        'главный бухгалтер алмаз',
        'турбулентность-дон',
        'турбулентность дон',
        'алмаз',
    }


CHIEF_METROLOG_TILE_ORDER = (
    'METD-M1',
    'METD-M2',
    'METD-M3.B',
    'METD-M3.F',
    'METD-Q1',
    'METD-Q3',
    'METD-Q4',
    'METD-Q2',
)


def _required_prod_deputy_kpi_ids(department: str | None) -> set[str]:
    return {
        *PROD_DEPUTY_OUTPUT_PERIOD_BY_ID.keys(),
        'PD-M2',
        'PD-M3.B1', 'PD-M3.B2',
        'PD-M3.F1', 'PD-M3.F2',
        'PD-Q1',
        'PD-Q3',
        'PD-Q2.1', 'PD-Q2.2',
    }


def _filter_prod_deputy_rows_for_department(department: str | None, rows: list[dict]) -> list[dict]:
    if not _is_production_director_department(department):
        return rows
    allowed = _required_prod_deputy_kpi_ids(department)
    return [row for row in rows if str(row.get('kpi_id') or '') in allowed]


def _prod_deputy_tile_sort_key(row: dict) -> tuple[int, int, str]:
    kpi_id = str(row.get('kpi_id') or '')
    if kpi_id in PROD_DEPUTY_TILE_ORDER:
        return PROD_DEPUTY_TILE_ORDER[kpi_id], 0, kpi_id
    try:
        position = int(row.get('position'))
    except (TypeError, ValueError):
        position = 10_000
    return 100 + position, position, kpi_id


def _prod_deputy_fallback_rows_for_department(
    department: str,
    definitions: list[dict],
) -> list[dict]:
    allowed = _required_prod_deputy_kpi_ids(department)
    fallback = []
    for item in definitions:
        if str(item.get('kpi_id') or '') not in allowed:
            continue
        row = dict(item)
        row['department'] = department
        fallback.append(row)
    return fallback


def _chief_constructor_split_m3_rows(department: str, source: dict) -> list[dict]:
    base_position = source.get('position')
    try:
        base_position = int(base_position)
    except (TypeError, ValueError):
        base_position = 2
    base_weight = source.get('weight_pct')
    try:
        split_weight = round(float(base_weight) / 2, 2)
    except (TypeError, ValueError):
        split_weight = 10.0

    budget = {
        **dict(source),
        'department': department,
        'kpi_id': 'GK-M3.B',
        'name': 'Бюджет блока в пределах лимита',
        'goal': 'Контролировать бюджет блока главного конструктора',
        'formula': 'Факт затрат / План затрат × 100%',
        'unit': '%',
        'source': 'БДР / 1С',
        'weight_pct': split_weight,
        'position': base_position,
    }
    fot = {
        **dict(source),
        'department': department,
        'kpi_id': 'GK-M3.F',
        'name': 'ФОТ блока в пределах лимита',
        'goal': 'Контролировать ФОТ блока главного конструктора',
        'formula': 'Факт ФОТ / План ФОТ × 100%',
        'unit': '%',
        'source': '1С / ЗУП / HRIS',
        'weight_pct': split_weight,
        'position': base_position + 1,
    }
    return [budget, fot]


def _normalize_chief_constructor_kpi_definitions(department: str, rows: list[dict]) -> list[dict]:
    """Split GK-M3 «Бюджет и ФОТ» into separate budget and FOT tiles."""
    if not rows:
        return rows
    existing_ids = {str(row.get('kpi_id') or '') for row in rows}
    normalized: list[dict] = []
    source_m3 = next((row for row in rows if str(row.get('kpi_id') or '') == 'GK-M3'), None)
    for row in rows:
        kid = str(row.get('kpi_id') or '')
        if kid == 'GK-M3':
            if 'GK-M3.B' not in existing_ids and 'GK-M3.F' not in existing_ids:
                normalized.extend(_chief_constructor_split_m3_rows(department, row))
            continue
        normalized.append(row)
    if source_m3 and not {'GK-M3.B', 'GK-M3.F'}.issubset({str(row.get('kpi_id') or '') for row in normalized}):
        current_ids = {str(row.get('kpi_id') or '') for row in normalized}
        split_rows = _chief_constructor_split_m3_rows(department, source_m3)
        normalized.extend(row for row in split_rows if str(row.get('kpi_id') or '') not in current_ids)
    return normalized


def _normalize_chief_accountant_kpi_definitions(department: str, rows: list[dict]) -> list[dict]:
    if not _is_chief_accountant_department(department):
        return rows
    return calc_chief_accountant.normalize_kpi_definitions(department, rows)


def _metrolog_row_template(rows: list[dict], *ids: str) -> dict:
    by_id = {str(row.get('kpi_id') or ''): row for row in rows}
    for kpi_id in ids:
        if kpi_id in by_id:
            return dict(by_id[kpi_id])
    return {
        'department': 'Главный метролог',
        'block': 'плитка',
        'frequency': 'Ежемесячно',
        'perspective': 'Финансы',
        'goal': '',
        'formula': '',
        'unit': '%',
        'source': '',
        'monthly_target': None,
        'quarterly_target': None,
        'yearly_target': None,
        'green_threshold': '≥90%',
        'yellow_threshold': '80–89,9%',
        'red_threshold': '<80%',
        'weight_pct': 0.0,
    }


def _normalize_chief_metrolog_kpi_definitions(department: str, rows: list[dict]) -> list[dict]:
    """Keep only approved Chief Metrologist tiles and split budget/FOT."""
    if not _is_chief_metrolog_department(department):
        return rows

    combined_m3 = _metrolog_row_template(rows, 'METD-M3', 'МЕТ-M3-2')
    budget = {
        **combined_m3,
        'department': department,
        'kpi_id': 'METD-M3.B',
        'name': 'Бюджет метрологической службы в пределах лимита',
        'goal': 'Контролировать бюджет метрологической службы в пределах лимита',
        'formula': 'Факт бюджета / План бюджета × 100%',
        'unit': '%',
    }
    fot = {
        **combined_m3,
        'department': department,
        'kpi_id': 'METD-M3.F',
        'name': 'ФОТ метрологической службы в пределах лимита',
        'goal': 'Контролировать ФОТ метрологической службы в пределах лимита',
        'formula': 'Факт ФОТ / План ФОТ × 100%',
        'unit': '%',
    }

    q1_source = _metrolog_row_template(rows, 'METD-Q1', 'МЕТ-Q4-1')
    q1 = {
        **q1_source,
        'department': department,
        'kpi_id': 'METD-Q1',
        'name': 'Доля проектов МС без отклонения >10 р.д.',
        'goal': 'Контролировать проекты МС без отклонений по вехам более 10 рабочих дней',
        'formula': 'Проекты МС без отклонения >10 р.д. / Все активные проекты МС × 100%',
        'unit': '%',
    }

    q4_source = _metrolog_row_template(rows, 'METD-Q4', 'МЕТ-Q4-2')
    q4 = {
        **q4_source,
        'department': department,
        'kpi_id': 'METD-Q4',
        'name': 'Доля проектов МС без отклонения бюджета <10%',
        'goal': 'Контролировать проекты МС без отклонения бюджета более 10%',
        'formula': 'Проекты МС без отклонения бюджета <10% / Все активные проекты МС × 100%',
        'unit': '%',
        'source': q4_source.get('source') or 'Turbo Project',
    }

    by_id = {str(row.get('kpi_id') or ''): dict(row) for row in rows}
    normalized_by_id = {
        'METD-M1': by_id.get('METD-M1') or _metrolog_row_template(rows, 'МЕТ-M1'),
        'METD-M2': by_id.get('METD-M2') or _metrolog_row_template(rows, 'МЕТ-M2'),
        'METD-M3.B': budget,
        'METD-M3.F': fot,
        'METD-Q1': q1,
        'METD-Q2': by_id.get('METD-Q2') or _metrolog_row_template(rows, 'МЕТ-Q5'),
        'METD-Q3': by_id.get('METD-Q3') or _metrolog_row_template(rows),
        'METD-Q4': q4,
    }

    result = []
    for pos, kpi_id in enumerate(CHIEF_METROLOG_TILE_ORDER):
        row = dict(normalized_by_id[kpi_id])
        row['department'] = department
        row['kpi_id'] = kpi_id
        row['block'] = 'плитка'
        row['position'] = pos
        result.append(row)
    return result


def _is_gspp_department(dept: str | None) -> bool:
    normalized = re.sub(
        r'\s+', ' ', unicodedata.normalize('NFKC', (dept or '').strip()).lower(),
    )
    return normalized in {'гспп', 'gspp'}


def _is_sup_department(dept: str | None) -> bool:
    normalized = re.sub(
        r'\s+', ' ', unicodedata.normalize('NFKC', (dept or '').strip()).lower(),
    )
    return normalized in {
        'sup',
        'суп',
        'служба управления персоналом',
        'директор по персоналу',
        'hrd',
    }


def _is_autoit_department(dept: str | None) -> bool:
    return _autoit_kpi_views.is_autoit_department(dept)


def _is_c1auto_department(dept: str | None) -> bool:
    return _c1auto_kpi_views.is_c1auto_department(dept)


DEVDIR_GENERATED_TILE_IDS = frozenset({'RD-M2', 'RD-Q1', 'RD-Y1'})


def _thresholds_block(kpi: dict) -> dict:
    return {
        'green': kpi.get('green_threshold'),
        'yellow': kpi.get('yellow_threshold'),
        'red': kpi.get('red_threshold'),
    }


def _period_label_from_kpi(kpi: dict) -> str:
    f = (kpi.get('frequency') or '').lower()
    if 'квартал' in f:
        return 'ежеквартально'
    if 'год' in f or 'ежегодн' in f:
        return 'ежегодно'
    if 'месяц' in f or 'ежемесячно' in f:
        return 'ежемесячно'
    return kpi.get('frequency') or ''


def _rag_higher_better(pct: float | None) -> str:
    if pct is None:
        return 'unknown'
    if pct >= 100:
        return 'green'
    if pct >= 90:
        return 'yellow'
    return 'red'


def _rag_lower_turnover(fact_pct: float | None) -> str:
    """Fallback без плана: абсолютные пороги по факту текучести (%)."""
    if fact_pct is None:
        return 'unknown'
    if fact_pct < 90:
        return 'green'
    if fact_pct <= 100:
        return 'yellow'
    return 'red'


def _turnover_fact_div_plan_pct(entry: dict) -> float | None:
    ytd = entry.get('ytd') or {}
    plan = ytd.get('total_plan')
    fact = ytd.get('total_fact')
    if plan is None or fact is None:
        row = entry.get('last_full_month_row') or {}
        if not isinstance(row, dict):
            row = {}
        plan = row.get('plan')
        fact = row.get('fact')
    if plan is None or fact is None:
        quarterly = entry.get('quarterly_data') or []
        row = quarterly[-1] if quarterly else {}
        if isinstance(row, dict):
            plan = row.get('plan') if row.get('plan') is not None else row.get('plan_max_turnover_pct')
            fact = row.get('fact') if row.get('fact') is not None else row.get('fact_turnover_pct')
    try:
        plan_value = float(plan)
        fact_value = float(fact)
    except (TypeError, ValueError):
        return None
    if plan_value <= 0:
        return None
    return round(fact_value / plan_value * 100, 1)


def _rag_turnover_vs_plan(plan: float | None, fact: float | None) -> str:
    """Текучесть: меньше — лучше. Цвет относительно плана, не абсолютный факт."""
    if fact is None:
        return 'unknown'
    try:
        fact_v = float(fact)
    except (TypeError, ValueError):
        return 'unknown'
    if plan is None:
        return _rag_lower_turnover(fact_v)
    try:
        plan_v = float(plan)
    except (TypeError, ValueError):
        return _rag_lower_turnover(fact_v)
    if plan_v <= 0:
        return _rag_lower_turnover(fact_v)
    # fact/plan×100: ≤100 % план выполнен, 100–110 жёлтый, >110 красный
    ratio = fact_v / plan_v * 100.0
    if ratio <= 100:
        return 'green'
    if ratio <= 110:
        return 'yellow'
    return 'red'


def _normalize_dashboard_kpi_id(raw: object) -> str:
    """Код KPI для веток views: ASCII-дефис, без ZWSP/BOM, латиница вместо «похожей» кириллицы.

    Иначе «QD-М3» (кириллическая М) не попадает в if kpi_id == 'QD-M3' и уходит в синтетику plan=100.
    NFKC снимает полноширинные латинские буквы/цифры (напр. «ＲＤ－Ｍ４» → RD-M4), если код в источнике
    уже правильный, но скопирован из Excel с полноширинными символами.
    """
    s = unicodedata.normalize('NFKC', str(raw or '')).strip()
    for z in ('\u200b', '\u200c', '\u200d', '\ufeff'):
        s = s.replace(z, '')
    for d in (
        '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015',
        '\u2212', '\ufe58', '\ufe63', '\uff0d',
    ):
        s = s.replace(d, '-')
    s = s.upper()
    # Заглавные кириллические буквы, часто перепутанные с латиницей в кодах KPI (напр. «М» в QD-М3).
    for cyr, lat in (
        ('\u0410', 'A'),
        ('\u0412', 'B'),
        ('\u0421', 'C'),
        ('\u0415', 'E'),
        ('\u041d', 'H'),
        ('\u0418', 'I'),  # «ИТ-M3» → IT-M3 (иначе ИT-M3 и RAG как higher_better)
        ('\u041a', 'K'),
        ('\u041c', 'M'),
        ('\u041e', 'O'),
        ('\u0420', 'P'),
        ('\u0422', 'T'),
        ('\u0423', 'Y'),
        ('\u0425', 'X'),
    ):
        s = s.replace(cyr, lat)
    s = s.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    return s


def _is_gspp_m5_tile(kpi: dict) -> bool:
    kid = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
    return kid in {'GSP-M5', 'GSPP-M5', 'ГСП-M5', 'ГCП-M5', 'ГСПП-M5', 'ГCПП-M5'}


def _is_gspp_m3_tile(kpi: dict) -> bool:
    kid = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
    return kid in {'GSP-M3', 'GSPP-M3', 'ГСП-M3', 'ГCП-M3', 'ГСПП-M3', 'ГCПП-M3'}


def _is_gspp_m1_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _gspp_kpi_views.gspp_m1_tile_matches(kid) or _gspp_kpi_views.gspp_m1_tile_matches(raw)


def _is_gspp_m2_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _gspp_kpi_views.gspp_m2_tile_matches(kid) or _gspp_kpi_views.gspp_m2_tile_matches(raw)


def _is_servhead_m1_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _servhead_kpi_views.servhead_m1_tile_matches(kid) or _servhead_kpi_views.servhead_m1_tile_matches(raw)


def _is_servhead_m2_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _servhead_kpi_views.servhead_m2_tile_matches(kid) or _servhead_kpi_views.servhead_m2_tile_matches(raw)


def _is_servhead_m3_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _servhead_kpi_views.servhead_m3_tile_matches(kid) or _servhead_kpi_views.servhead_m3_tile_matches(raw)


def _is_servhead_m4_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _servhead_kpi_views.servhead_m4_tile_matches(kid) or _servhead_kpi_views.servhead_m4_tile_matches(raw)


def _is_servhead_m5_tile(kpi: dict) -> bool:
    raw = kpi.get('kpi_id')
    kid = _normalize_dashboard_kpi_id(raw)
    return _servhead_kpi_views.servhead_m5_tile_matches(kid) or _servhead_kpi_views.servhead_m5_tile_matches(raw)


def _is_servhead_lower_better_tile(kpi: dict) -> bool:
    return _is_servhead_m2_tile(kpi) or _is_servhead_m3_tile(kpi) or _is_servhead_m5_tile(kpi)


def _is_servhead_higher_better_tile(kpi: dict) -> bool:
    return _is_servhead_m1_tile(kpi) or _is_servhead_m4_tile(kpi)


def _is_servhead_tile(kpi: dict) -> bool:
    return (
        _is_servhead_m1_tile(kpi) or _is_servhead_m2_tile(kpi)
        or _is_servhead_m3_tile(kpi) or _is_servhead_m4_tile(kpi)
        or _is_servhead_m5_tile(kpi)
    )


def _is_gspp_q5_tile(kpi: dict) -> bool:
    kid = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
    return kid in {'GSP-Q5', 'GSPP-Q5', 'ГСП-Q5', 'ГCП-Q5', 'ГСПП-Q5', 'ГCПП-Q5'}


def _is_budget_limit_m3_kpi(kpi_id: str) -> bool:
    """Плитки «в пределах лимита»: M3 плюс разрез ПЦ1/ПЦ2 для зам. операционного."""
    normalized = (kpi_id or '').upper()
    if normalized in {'LOG-M3.B', 'LOG-M3.F', 'METD-M3.B', 'METD-M3.F', 'GB-M3.B', 'GB-M3.F'}:
        return True
    if normalized.startswith('PD-M3.B') or normalized.startswith('PD-M3.F'):
        return True
    return normalized.endswith(('-M3-1', '-M3-2', 'M3.1', 'M3.2'))


def _is_prod_deputy_pc_m3_kpi(kpi_id: str) -> bool:
    normalized = (kpi_id or '').upper()
    return normalized.startswith('PD-M3.B') or normalized.startswith('PD-M3.F')


def _is_turnover_style_tile(kpi: dict) -> bool:
    kid = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
    nm = (kpi.get('name') or '').lower()
    if kid == 'TD-Q2' or _is_gspp_q5_tile(kpi):
        return True
    if 'текучесть' in nm:
        return True
    if kid.endswith('-Q5') or kid in {'ZKD-Q2', 'TD-Q2', 'QD-Q2', 'RD-Q2', 'IT-Q2', '1C-Q5'}:
        return True
    return False


def _rag_dz_lower_better(pct: float | None) -> str:
    """ДЗ: < 100 % → зелёный, 100–110 % → жёлтый, > 110 % → красный."""
    if pct is None:
        return 'unknown'
    if pct < 100:
        return 'green'
    if pct <= 110:
        return 'yellow'
    return 'red'


def _rag_td_m4_limit(pct: float | None) -> str:
    """TD-M4: <= 100 % → зелёный, 100,1–110 % → жёлтый, > 110 % → красный."""
    if pct is None:
        return 'unknown'
    if pct <= 100:
        return 'green'
    if pct <= 110:
        return 'yellow'
    return 'red'


def _budget_fact_div_plan_pct(entry: dict) -> float | None:
    row = entry.get('last_full_month_row') or {}
    if not isinstance(row, dict):
        row = {}
    plan = row.get('plan')
    fact = row.get('fact')
    if plan is None or fact is None:
        ytd = entry.get('ytd') or {}
        plan = ytd.get('total_plan')
        fact = ytd.get('total_fact')
    try:
        plan_value = float(plan)
        fact_value = float(fact)
    except (TypeError, ValueError):
        return None
    if plan_value <= 0:
        return None
    return round(fact_value / plan_value * 100, 1)


def _rag_budget_fact_div_plan(pct: float | None) -> str:
    """Лимитные бюджет/ФОТ KPI: факт < план — зелёный, факт = план — жёлтый, факт > план — красный."""
    if pct is None:
        return 'unknown'
    if pct < 100:
        return 'green'
    if abs(pct - 100.0) < 1e-9:
        return 'yellow'
    return 'red'


def _synthetic_quarter_row_for_tile(kpi: dict) -> tuple[dict, dict]:
    ly, lq = last_full_quarter(date.today())
    random.seed(hash((kpi.get('kpi_id'), ly, lq)))
    kid = kpi.get('kpi_id') or ''
    nm = (kpi.get('name') or '').lower()
    period = {'type': 'last_full_quarter', 'year': ly, 'quarter': lq}
    if 'текучесть' in nm or kid.endswith('-Q5') or kid == 'ZKD-Q2':
        fact = round(random.uniform(2.0, 8.0), 2)
        target = 5.0
        row = {
            'quarter': lq,
            'year': ly,
            'label': f'Q{lq} {ly}',
            'plan_max_turnover_pct': target,
            'fact_turnover_pct': fact,
            'kpi_pct': fact,
            'has_data': False,
        }
        return row, period
    plan = 100.0
    fact = round(random.uniform(80, 120), 1)
    kpi_pct = round(fact / plan * 100, 1)
    row = {
        'quarter': lq,
        'year': ly,
        'label': f'Q{lq} {ly}',
        'plan': plan,
        'fact': fact,
        'kpi_pct': kpi_pct,
        'has_data': False,
    }
    return row, period


def _synthetic_year_row_for_tile(kpi: dict) -> tuple[dict, dict]:
    ref_year = date.today().year - 1
    random.seed(hash((kpi.get('kpi_id'), ref_year)))
    plan = 100.0
    fact = round(random.uniform(90, 118), 1)
    kpi_pct = round(fact / plan * 100, 1)
    period = {'type': 'last_full_year', 'year': ref_year}
    row = {'year': ref_year, 'plan': plan, 'fact': fact, 'kpi_pct': kpi_pct, 'has_data': False}
    return row, period


def _qd_q2_pct_from_entry(entry: dict) -> float | None:
    """Факт/план×100 для QD-Q2 из ytd или последней строки monthly_data (не из ytd['kpi_pct'])."""
    ytd = entry.get('ytd') or {}
    tp, tf = ytd.get('total_plan'), ytd.get('total_fact')
    if tp is None or tf is None:
        md = entry.get('monthly_data') or []
        lr = md[-1] if md else {}
        tp = tp if tp is not None else lr.get('plan')
        tf = tf if tf is not None else lr.get('fact')
    return _qd_q2_kpi_pct(tp, tf)


def _tile_color(kpi: dict, entry: dict) -> tuple[float | None, str]:
    """Вычислить kpi_pct и RAG-цвет для плитки."""
    ytd = entry.get('ytd') or {}
    kid = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))

    if kid in _qualdir_kpi_views.TILE_FACT_ONLY_IDS:
        return None, None

    if kid == 'METD-Q2':
        fact = ytd.get('total_fact')
        plan = ytd.get('total_plan')
        if fact is None or plan is None:
            quarterly = entry.get('quarterly_data') or []
            row = quarterly[-1] if quarterly else {}
            if isinstance(row, dict):
                fact = row.get('fact')
                plan = row.get('plan')
        try:
            pct = float(fact) if fact is not None else None
            ratio = round(float(fact) / float(plan) * 100, 1) if fact is not None and plan else None
        except (TypeError, ValueError, ZeroDivisionError):
            pct = None
            ratio = None
        color = _rag_lower_turnover(ratio)
        return pct, color

    if kid in _qualdir_kpi_views.TILE_COLOR_PLAN_FACT_IDS:
        pct = ytd.get('kpi_pct')
        if pct is not None:
            pct = float(pct)
        color = _qualdir_kpi_views.rag_plan_fact_pct(pct)
        return pct, color

    pct = ytd.get('kpi_pct')
    if pct is not None:
        pct = float(pct)
    if kid in _sup_kpi_views.SUP_TURNOVER_FACT_RAG_IDS:
        ref = entry.get('last_full_month_row') or {}
        fact = ref.get('fact')
        if fact is None:
            md = entry.get('monthly_data') or []
            last_row = md[-1] if md else {}
            fact = last_row.get('fact') if md else None
        color = _sup_kpi_views.rag_hrd_turnover_fact_pct(
            float(fact) if fact is not None else None,
            kpi_id=kid,
        )
    elif kid in {'PD-M1.1', 'PD-M1.2'} or kid in PROD_DEPUTY_OUTPUT_PERIOD_BY_ID:
        ref_row = entry.get('last_full_month_row') or {}
        pct = ref_row.get('kpi_pct')
        if pct is not None:
            pct = float(pct)
        color = _rag_higher_better(pct)
    elif (logistics_color := logistics_views.tile_color(kid, entry)) is not None:
        pct, color = logistics_color
    elif _is_turnover_style_tile(kpi):
        ref = entry.get('last_full_month_row') or {}
        md = entry.get('monthly_data') or []
        last_row = md[-1] if md else {}
        plan = ref.get('plan')
        if plan is None:
            plan = last_row.get('plan') if md else None
        if plan is None:
            plan = ytd.get('total_plan')
        fact = ref.get('fact')
        if fact is None:
            fact = last_row.get('fact') if md else None
        if fact is None:
            fact = ytd.get('total_fact')
        color = _rag_turnover_vs_plan(plan, fact)
    elif kid in {'OD-M3.1', 'OD-M3.2'}:
        pct = _budget_fact_div_plan_pct(entry)
        color = _rag_budget_fact_div_plan(pct)
    elif dept_dz.is_dz_kpi(kid):
        color = _rag_dz_lower_better(pct)
    elif kid in _devdir_kpi_views.DEVDIR_PLAN_FACT_COLOR_IDS:
        ref = entry.get('last_full_month_row') or {}
        pct = _devdir_kpi_views.kpi_pct_from_plan_fact(ref.get('plan'), ref.get('fact'))
        if pct is None:
            row_pct = ref.get('kpi_pct')
            if row_pct is not None:
                pct = float(row_pct)
            elif ytd.get('kpi_pct') is not None:
                pct = float(ytd['kpi_pct'])
        color = _devdir_kpi_views.rag_devdir_plan_fact_pct(pct)
    elif kid in (
        _autoit_kpi_views.AUTOIT_FOT_LIMIT_KPI_IDS
        | _autoit_kpi_views.AUTOIT_BUDGET_LIMIT_KPI_IDS
        | _c1auto_kpi_views.C1AUTO_FOT_LIMIT_KPI_IDS
        | _c1auto_kpi_views.C1AUTO_BUDGET_LIMIT_KPI_IDS
        | _sup_kpi_views.SUP_FOT_LIMIT_KPI_IDS
        | _sup_kpi_views.SUP_BUDGET_LIMIT_KPI_IDS
    ):
        # Цвет от plan/fact опорной строки, не от ytd.kpi_pct текущего
        # незакрытого месяца (часто fact=0 → pct=0 → ложный green).
        pct = _budget_fact_div_plan_pct(entry)
        if pct is None and ytd.get('kpi_pct') is not None:
            pct = float(ytd['kpi_pct'])
        color = _rag_td_m4_limit(pct)
    elif kid in _devdir_kpi_views.DEVDIR_KPI_IDS:
        color = _rag_td_m4_limit(pct)
    elif _is_gspp_m3_tile(kpi) or _is_gspp_m5_tile(kpi):
        color = _rag_td_m4_limit(pct)
    elif _is_gspp_m1_tile(kpi) or _is_gspp_m2_tile(kpi):
        color = _gspp_kpi_views.rag_gspp_m1_m2_pct(pct)
    elif _is_servhead_higher_better_tile(kpi):
        color = _servhead_kpi_views.rag_servhead_m1_pct(pct)
    elif _is_servhead_lower_better_tile(kpi):
        color = _servhead_kpi_views.rag_servhead_lower_better_pct(pct)
    elif _gspp_kpi_views.gspp_q4_kpi_id_matches(kid):
        color = _gspp_kpi_views.rag_gspp_q4_pct(pct)
    elif _is_budget_limit_m3_kpi(kid):
        pct = _budget_fact_div_plan_pct(entry)
        color = _rag_budget_fact_div_plan(pct)
    elif kid in techdir_kpi_entry.TILE_COLOR_DZ_LOWER_IDS:
        color = _rag_dz_lower_better(pct)
    elif kid in _qualdir_kpi_views.TILE_COLOR_TD_M4_LIMIT_IDS:
        color = _rag_td_m4_limit(pct)
    elif kid in komdir_dashboard.LOWER_IS_BETTER_IDS:
        color = komdir_dashboard._rag_lower_better(pct)
    else:
        color = _rag_higher_better(pct)
    return pct, color


def _extract_tile_plan_fact(entry: dict) -> dict:
    """Краткие plan/fact для плитки из уже собранного payload KPI."""
    ref_row = entry.get('last_full_month_row')
    if isinstance(ref_row, dict):
        out = {
            'plan': ref_row.get('plan'),
            'fact': ref_row.get('fact'),
            'has_data': ref_row.get('has_data'),
        }
        if ref_row.get('expected_plan') is not None:
            out['expected_plan'] = ref_row.get('expected_plan')
        if ref_row.get('values_unit'):
            out['unit'] = ref_row.get('values_unit')
        return out

    quarterly = entry.get('quarterly_data') or []
    if quarterly:
        row = quarterly[-1]
        plan = row.get('plan')
        if plan is None:
            plan = row.get('plan_max_turnover_pct')
        fact = row.get('fact')
        if fact is None:
            fact = row.get('fact_turnover_pct')
        out = {
            'plan': plan,
            'fact': fact,
        }
        if 'has_data' in row:
            out['has_data'] = row.get('has_data')
        elif 'data_complete' in row:
            out['has_data'] = row.get('data_complete')
        if row.get('values_unit'):
            out['unit'] = row.get('values_unit')
        elif 'plan_max_turnover_pct' in row or 'fact_turnover_pct' in row:
            out['unit'] = '%'
        return out

    yearly = entry.get('yearly_data') or []
    if yearly:
        row = yearly[-1]
        out = {
            'plan': row.get('plan'),
            'fact': row.get('fact'),
        }
        if 'has_data' in row:
            out['has_data'] = row.get('has_data')
        if row.get('values_unit'):
            out['unit'] = row.get('values_unit')
        return out

    ytd = entry.get('ytd') or {}
    out = {
        'plan': ytd.get('total_plan'),
        'fact': ytd.get('total_fact'),
    }
    if out['plan'] is not None or out['fact'] is not None:
        out['has_data'] = True
    if ytd.get('values_unit'):
        out['unit'] = ytd.get('values_unit')
    return out


def _public_unit_row(row: dict) -> dict:
    out = dict(row)
    out.pop('values_unit', None)
    return out


def _tile_cache_updated_at(kpi_id: str, ref_y: int | None, ref_m: int | None) -> str | None:
    if ref_y is None or ref_m is None:
        return None
    kid = _normalize_dashboard_kpi_id(kpi_id)

    if kid == 'METD-M3.B':
        cache_files = [cache_manager.CACHE_DIR / f'metrolog_budget_{ref_y}_{ref_m:02d}.json']
    elif kid == 'METD-M3.F':
        cache_files = [cache_manager.CACHE_DIR / f'metrolog_fot_{ref_y}_{ref_m:02d}.json']
    elif kid in {'METD-M1', 'МЕТ-M1'}:
        cache_files = [cache_manager.CACHE_DIR / f'metrolog_production_plan_{ref_y}_{ref_m:02d}.json']
    elif kid in {'METD-Q1', 'MET-Q4-1'}:
        cache_files = [cache_manager.CACHE_DIR / f'metrolog_projects_ytd_{ref_y}_{ref_m:02d}.json']
    elif kid == 'METD-Q2':
        cache_files = [cache_manager.CACHE_DIR / f'metrolog_turnover_q2_ytd_{ref_y}_{ref_m:02d}.json']
    elif kid == 'METD-Q3':
        cache_files = [cache_manager.CACHE_DIR / f'metrolog_certification_projects_ytd_{ref_y}_{ref_m:02d}.json']
    elif kid.startswith('PD-'):
        cache_files = _prod_deputy_cache_files_for_kpi(kid, ref_y, ref_m)
    elif kid == 'KD-T-OVERDUE':
        from . import calc_debitorka
        from comdir.ytd import cache_stamp_paths as comdir_cache_stamp_paths

        cache_files = [
            calc_debitorka.overdue_detail_cache_path(ref_y, ref_m),
            *comdir_cache_stamp_paths('KD-M5', ref_y, ref_m),
        ]
    elif kid == 'IT-M3':
        cache_files = (
            _autoit_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
            + _c1auto_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
        )
    elif _qualdir_kpi_views.is_qualdir_tile_kpi_id(kpi_id):
        cache_files = _qualdir_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
    elif _sup_kpi_views.is_sup_tile_kpi_id(kpi_id):
        cache_files = _sup_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
    elif _autoit_kpi_views.is_autoit_tile_kpi_id(kpi_id):
        cache_files = _autoit_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
    elif _c1auto_kpi_views.is_c1auto_tile_kpi_id(kpi_id):
        cache_files = _c1auto_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
    elif _servhead_kpi_views.is_servhead_tile_kpi_id(kpi_id):
        cache_files = _servhead_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
    elif _devdir_kpi_views.is_devdir_tile_kpi_id(kpi_id):
        cache_files = _devdir_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
    else:
        cache_files = techdir_dashboard.cache_stamp_paths(kpi_id, ref_y, ref_m)
        if not cache_files:
            gspp_paths = _gspp_kpi_views.cache_stamp_paths(kpi_id, ref_y, ref_m)
            if gspp_paths:
                cache_files = gspp_paths

    latest_mtime: float | None = None
    for path in cache_files:
        if path.exists():
            mt = path.stat().st_mtime
            if latest_mtime is None or mt > latest_mtime:
                latest_mtime = mt
    if latest_mtime is None:
        return None
    return datetime.fromtimestamp(latest_mtime).isoformat(timespec='seconds')


def _manual_tile_refresh_key(department: str, kpi_id: str, ref_y: int | None, ref_m: int | None) -> str:
    dept_part = str(department or '').strip().casefold()
    kid_part = _normalize_dashboard_kpi_id(kpi_id)
    return f'{dept_part}|{kid_part}|{ref_y or ""}|{ref_m or ""}'


def _manual_tile_refresh_now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _manual_tile_refresh_parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _manual_tile_refresh_read_state() -> dict[str, dict]:
    try:
        with MANUAL_TILE_REFRESH_STATE_PATH.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _manual_tile_refresh_write_state(state: dict[str, dict]) -> None:
    cache_manager.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = MANUAL_TILE_REFRESH_STATE_PATH.with_suffix('.tmp')
    with tmp_path.open('w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp_path.replace(MANUAL_TILE_REFRESH_STATE_PATH)


def _manual_tile_refresh_state_for_key(key: str) -> dict:
    with _manual_tile_refresh_state_lock:
        return dict(_manual_tile_refresh_read_state().get(key) or {})


def _manual_tile_refresh_update_state(key: str, **updates) -> dict:
    with _manual_tile_refresh_state_lock:
        state = _manual_tile_refresh_read_state()
        entry = dict(state.get(key) or {})
        entry.update(updates)
        state[key] = entry
        _manual_tile_refresh_write_state(state)
        return dict(entry)


def _manual_tile_refresh_cache_files(kpi_id: str, ref_y: int | None, ref_m: int | None) -> list[Path]:
    """Кэш-файлы, которые нужно удалить перед ручным пересчётом плитки.

    Для KPI без файлового кэша список пустой: расчёт всё равно запускается,
    но старые значения на экране остаются до завершения фоновой задачи.
    """
    if ref_y is None or ref_m is None:
        return []

    kid = _normalize_dashboard_kpi_id(kpi_id)
    paths: list[Path] = []
    cd = cache_manager.CACHE_DIR

    if kid == 'KD-M1':
        for m in range(1, ref_m + 1):
            for fn_name in ('_cache_path', '_monthly_cache_path'):
                fn = getattr(calc_dengi_fact, fn_name, None)
                if callable(fn):
                    try:
                        paths.append(fn(ref_y, m))
                    except TypeError:
                        pass
                fn = getattr(calc_plan, fn_name, None)
                if callable(fn):
                    try:
                        paths.append(fn(ref_y, m))
                    except TypeError:
                        pass
        paths.append(cd / f'dengi_monthly_{ref_y}_{ref_m:02d}.json')
        paths.append(cd / f'plans_monthly_{ref_y}_{ref_m:02d}.json')
    elif kid == 'KD-M2':
        paths.append(cd / f'otgruzki_monthly_{ref_y}_{ref_m:02d}.json')
        paths.append(cd / f'plans_monthly_{ref_y}_{ref_m:02d}.json')
    elif kid == 'KD-M3':
        paths.append(cd / f'dogovory_monthly_{ref_y}_{ref_m:02d}.json')
        paths.append(cd / f'plans_monthly_{ref_y}_{ref_m:02d}.json')
    elif kid in {'KD-M4', 'KD-M5'}:
        from comdir.ytd import cache_stamp_paths as comdir_cache_stamp_paths

        paths.extend(comdir_cache_stamp_paths(kid, ref_y, ref_m))
    elif kid == 'KD-M8':
        paths.append(cd / f'fot_{ref_y}_{ref_m:02d}.json')
    elif kid == 'KD-M11':
        paths.append(cd / f'tekuchest_{ref_y}_{ref_m:02d}.json')
    elif kid in {'KD-M6', 'KD-Q1'}:
        paths.append(cd / 'vp_result_cache.json')
    elif kid == 'METD-M3.B':
        paths.append(cd / f'metrolog_budget_{ref_y}_{ref_m:02d}.json')
    elif kid == 'METD-M3.F':
        paths.append(cd / f'metrolog_fot_{ref_y}_{ref_m:02d}.json')
    elif kid in {'МЕТ-M1', 'METD-M1'}:
        paths.append(cd / f'metrolog_production_plan_{ref_y}_{ref_m:02d}.json')
    elif kid in {'MET-Q4-1', 'METD-Q1'}:
        paths.append(cd / f'metrolog_projects_ytd_{ref_y}_{ref_m:02d}.json')
    elif kid == 'METD-Q2':
        paths.append(cd / f'metrolog_turnover_q2_ytd_{ref_y}_{ref_m:02d}.json')
    elif kid == 'METD-Q3':
        paths.append(cd / f'metrolog_certification_projects_ytd_{ref_y}_{ref_m:02d}.json')
    elif kid == 'KD-T-OVERDUE':
        from . import calc_debitorka
        from comdir.ytd import cache_stamp_paths as comdir_cache_stamp_paths

        paths.append(calc_debitorka.overdue_detail_cache_path(ref_y, ref_m))
        paths.extend(comdir_cache_stamp_paths('KD-M5', ref_y, ref_m))
    elif kid == 'FND-T6':
        paths.append(cd / f'psd_portfolio_monthly_{ref_y}_{ref_m:02d}.json')
        for m in range(1, ref_m + 1):
            paths.append(cd / f'psd_portfolio_{ref_y}-{m:02d}.json')
    elif kid.startswith('PD-'):
        paths.extend(_prod_deputy_cache_files_for_kpi(kid, ref_y, ref_m))

    if kid in {'METD-M1', 'МЕТ-M1', 'METD-M3.B', 'METD-M3.F', 'METD-Q1', 'MET-Q4-1', 'METD-Q2', 'METD-Q3'}:
        paths.append(cd / f'chief_metrolog_payload_{ref_y}_{ref_m:02d}.json')
    if kid == 'KD-T-OVERDUE' and ref_y is not None and ref_m is not None:
        paths.append(cd / f'chief_metrolog_payload_{ref_y}_{ref_m:02d}.json')
    elif _qualdir_kpi_views.is_qualdir_tile_kpi_id(kid):
        paths.extend(_qualdir_kpi_views.cache_stamp_paths(kid, ref_y, ref_m))

    if kid in _autoit_kpi_views.AUTOIT_SLA_KPI_IDS:
        from getkpi.autoit.it_m1_sla import (
            cache_file_path_for_period as it_m1_cache,
            monthly_cache_path as it_m1_monthly_cache,
        )

        paths.extend([
            it_m1_cache(ref_y, ref_m),
            it_m1_monthly_cache(ref_y, ref_m),
        ])
    if kid in _autoit_kpi_views.AUTOIT_BUDGET_LIMIT_KPI_IDS:
        from getkpi.autoit.it_m3 import (
            cache_file_path_for_period as it_m3_cache,
            monthly_cache_path as it_m3_monthly_cache,
        )

        paths.extend([
            it_m3_cache(ref_y, ref_m),
            it_m3_monthly_cache(ref_y, ref_m),
        ])
    if kid == 'IT-Q2':
        from getkpi.autoit.it_q2_tekuchest import cache_file_path_for_period as it_q2_cache

        paths.append(it_q2_cache(ref_y, ref_m))
    if kid in _c1auto_kpi_views.C1AUTO_SLA_KPI_IDS:
        from getkpi.c1auto.c1_m1_sla import (
            cache_file_path_for_period as c1_m1_cache,
            monthly_cache_path as c1_m1_monthly_cache,
        )

        paths.extend([
            c1_m1_cache(ref_y, ref_m),
            c1_m1_monthly_cache(ref_y, ref_m),
        ])
    if kid in _c1auto_kpi_views.C1AUTO_BUDGET_LIMIT_KPI_IDS:
        from getkpi.c1auto.c1_m3 import cache_file_path_for_period as c1_m3_cache

        paths.append(c1_m3_cache(ref_y, ref_m))
    if kid in _c1auto_kpi_views.C1AUTO_TURNOVER_KPI_IDS:
        from getkpi.c1auto.it_q5_tekuchest import cache_file_path_for_period as c1_q5_cache

        paths.append(c1_q5_cache(ref_y, ref_m))
    if kid in _sup_kpi_views.SUP_KPI_IDS:
        from sup import hrd_m1, hrd_m2, hrd_m3, hrd_m4, hrd_q4

        if kid == 'HRD-M1':
            paths.append(hrd_m1.cache_file_path_for_period(ref_y, ref_m))
        elif kid == 'HRD-M2':
            paths.extend([
                hrd_m2.cache_file_path_for_period(ref_y, ref_m),
                hrd_m2.monthly_cache_path(ref_y, ref_m),
            ])
        elif kid == 'HRD-M3':
            paths.extend([
                hrd_m3.cache_file_path_for_period(ref_y, ref_m),
                hrd_m3.monthly_cache_path(ref_y, ref_m),
            ])
        elif kid == 'HRD-M4':
            paths.append(hrd_m4.cache_file_path_for_period(ref_y, ref_m))
        elif kid == 'HRD-Q4':
            paths.append(hrd_q4.cache_file_path_for_period(ref_y, ref_m))

    paths.extend(techdir_dashboard.cache_stamp_paths(kid, ref_y, ref_m))
    return list(dict.fromkeys(paths))


def _manual_tile_refresh_delete_cache_files(kpi_id: str, ref_y: int | None, ref_m: int | None) -> list[str]:
    deleted: list[str] = []
    for path in _manual_tile_refresh_cache_files(kpi_id, ref_y, ref_m):
        try:
            if path.exists() and path.is_file():
                path.unlink()
                deleted.append(str(path))
        except OSError:
            logger.exception("manual tile refresh: failed to delete cache file %s", path)
    return deleted


def _manual_tile_refresh_find_kpis(department: str) -> tuple[list[dict], str, str | None]:
    ck = commercial_kpi_key(department)
    if isinstance(ck, str):
        return _get_kpi_dicts(ck), 'komdir', dept_guid_for_kpi_key(ck)
    if ck is None:
        return [], 'missing', None
    if _is_chief_metrolog_department(department):
        return _lookup_kpi_data(department), 'chief_metrolog', None
    return _lookup_kpi_data(department), 'universal', None


def _manual_tile_refresh_worker(
    *,
    key: str,
    department: str,
    kpi_id: str,
    ref_y: int | None,
    ref_m: int | None,
    kpis: list[dict],
    payload_kind: str,
    dept_guid: str | None,
) -> None:
    try:
        deleted: list[str] = []
        refresh_paths = _manual_tile_refresh_cache_files(kpi_id, ref_y, ref_m)
        with cache_manager.mark_paths_refreshing(key, refresh_paths), cache_manager.force_compute():
            if payload_kind == 'komdir':
                if not komdir_dashboard.refresh_komdir_tile_cache(
                    kpi_id,
                    month=ref_m,
                    year=ref_y,
                    dept_guid=dept_guid,
                ):
                    komdir_dashboard.build_komdir_payload(
                        kpis,
                        month=ref_m,
                        year=ref_y,
                        dept_guid=dept_guid,
                    )
            elif payload_kind == 'chief_metrolog':
                _build_chief_metrolog_payload_fresh(
                    department,
                    kpis,
                    month=ref_m,
                    year=ref_y,
                    include_debug=True,
                )
            else:
                _build_universal_payload(
                    department,
                    kpis,
                    month=ref_m,
                    year=ref_y,
                    include_debug=True,
                )
        cache_manager.clear_memoized_dashboard_payload()
        _manual_tile_refresh_update_state(
            key,
            status='succeeded',
            finished_at=_manual_tile_refresh_now(),
            error='',
            deleted_files=deleted,
            cache_updated_at=_tile_cache_updated_at(kpi_id, ref_y, ref_m),
        )
    except Exception as exc:
        logger.exception("manual tile refresh failed: %s %s", department, kpi_id)
        _manual_tile_refresh_update_state(
            key,
            status='failed',
            finished_at=_manual_tile_refresh_now(),
            error=str(exc),
        )


def _manual_tile_refresh_response_payload(
    *,
    key: str,
    department: str,
    kpi_id: str,
    ref_y: int | None,
    ref_m: int | None,
    entry: dict | None = None,
) -> dict:
    current = dict(entry or _manual_tile_refresh_state_for_key(key))
    cache_files = _manual_tile_refresh_cache_files(kpi_id, ref_y, ref_m)
    is_refreshing = cache_manager.is_any_cache_path_refreshing(cache_files)
    started = _manual_tile_refresh_parse_dt(current.get('started_at'))
    if (
        current.get('status') == 'running'
        and not is_refreshing
        and started is not None
        and (datetime.now() - started).total_seconds() > 60
    ):
        current['status'] = 'failed'
        current['finished_at'] = current.get('finished_at') or _manual_tile_refresh_now()
        current['error'] = current.get('error') or 'Пересчёт был прерван перезапуском сервера'
        _manual_tile_refresh_update_state(key, **current)
    next_allowed = (
        started + timedelta(seconds=MANUAL_TILE_REFRESH_COOLDOWN_SECONDS)
        if started is not None
        else None
    )
    current.update({
        'department': department,
        'kpi_id': kpi_id,
        'year': ref_y,
        'month': ref_m,
        'cooldown_seconds': MANUAL_TILE_REFRESH_COOLDOWN_SECONDS,
        'next_allowed_at': next_allowed.isoformat(timespec='seconds') if next_allowed else None,
        'cache_updated_at': current.get('cache_updated_at') or _tile_cache_updated_at(kpi_id, ref_y, ref_m),
    })
    if is_refreshing:
        current['status'] = 'running'
    if 'status' not in current:
        current['status'] = 'idle'
    return current


def _build_tile_item(
    kpi: dict,
    pct: float | None,
    color: str,
    entry: dict,
    *,
    ref_y: int | None = None,
    ref_m: int | None = None,
) -> dict:
    tile = {
        'kpi_id': kpi['kpi_id'],
        'name': kpi['name'],
        'goal': kpi.get('goal'),
        'kpi_pct': pct,
        'color': color,
        'period': _period_label_from_kpi(kpi),
        'thresholds': _thresholds_block(kpi),
        'formula': kpi.get('formula'),
        'unit': kpi.get('unit'),
        'source': kpi.get('source'),
        'description': kpi.get('description'),
        'frequency': kpi.get('frequency'),
    }
    tile.update(_extract_tile_plan_fact(entry))
    if entry.get('data_granularity'):
        tile['data_granularity'] = entry.get('data_granularity')
    if kpi.get('kpi_id') in {'OD-Q1', 'OD-Q2', 'QD-Q1'} and entry.get('data_granularity') == 'monthly':
        tile['period'] = 'ежемесячно'
        tile['frequency'] = 'ежемесячно'
    _kid_gspp = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
    if _gspp_kpi_views.gspp_q4_kpi_id_matches(_kid_gspp):
        tile['period'] = 'ежемесячно'
        tile['frequency'] = 'ежемесячно'
        tile['periodicity'] = 'ежемесячно'
    if (
        _is_gspp_m1_tile(kpi) or _is_gspp_m2_tile(kpi)
        or _is_gspp_m3_tile(kpi) or _is_gspp_m5_tile(kpi)
    ):
        tile['period'] = 'ежемесячно'
        tile['frequency'] = 'ежемесячно'
        tile['periodicity'] = 'ежемесячно'
        if _is_gspp_m3_tile(kpi) or _is_gspp_m5_tile(kpi):
            tile['pct_lower_is_better'] = True
        if _normalize_dashboard_kpi_id(kpi.get('kpi_id')) in _sup_kpi_views.SUP_FOT_LIMIT_KPI_IDS:
            tile['pct_lower_is_better'] = True
        if _normalize_dashboard_kpi_id(kpi.get('kpi_id')) in _sup_kpi_views.SUP_BUDGET_LIMIT_KPI_IDS:
            tile['pct_lower_is_better'] = True
    if _is_servhead_tile(kpi):
        tile['period'] = 'ежемесячно'
        tile['frequency'] = 'ежемесячно'
        tile['periodicity'] = 'ежемесячно'
        if _is_servhead_lower_better_tile(kpi):
            tile['pct_lower_is_better'] = True
    if _kid_gspp in _devdir_kpi_views.DEVDIR_PLAN_FACT_COLOR_IDS:
        tile['pct_higher_is_better'] = True
        tile['rag_direction'] = 'higher_better'
    elif _kid_gspp in _devdir_kpi_views.DEVDIR_RUB_UNIT_KPI_IDS:
        tile['pct_lower_is_better'] = True
        tile['rag_direction'] = 'lower_better'
    elif _kid_gspp in komdir_dashboard.LOWER_IS_BETTER_IDS:
        tile['pct_lower_is_better'] = True
    elif _kid_gspp in komdir_dashboard.HIGHER_IS_BETTER_IDS:
        tile['pct_higher_is_better'] = True
    if _kid_gspp in (
        _autoit_kpi_views.AUTOIT_FOT_LIMIT_KPI_IDS
        | _autoit_kpi_views.AUTOIT_BUDGET_LIMIT_KPI_IDS
        | _c1auto_kpi_views.C1AUTO_FOT_LIMIT_KPI_IDS
        | _c1auto_kpi_views.C1AUTO_BUDGET_LIMIT_KPI_IDS
        | _sup_kpi_views.SUP_FOT_LIMIT_KPI_IDS
        | _sup_kpi_views.SUP_BUDGET_LIMIT_KPI_IDS
        | _qualdir_kpi_views.TILE_COLOR_TD_M4_LIMIT_IDS
    ):
        # Бюджет/ФОТ «в пределах лимита»: факт ниже плана — хорошо.
        tile['pct_lower_is_better'] = True
        tile['rag_direction'] = 'lower_better'
    if entry.get('kpi_period'):
        tile['kpi_period'] = entry.get('kpi_period')
    if entry.get('generated_data'):
        tile['generated_data'] = True
    if ref_y and ref_m and tile.get('data_granularity') == 'monthly':
        tile['plan_fact_period_label'] = f"{MONTH_NAMES[ref_m].capitalize()} {ref_y}"
    tile['cache_updated_at'] = _tile_cache_updated_at(kpi.get('kpi_id'), ref_y, ref_m)
    if entry.get('cache_refresh_status'):
        tile['cache_refresh_status'] = entry.get('cache_refresh_status')
    if cache_manager.is_any_cache_path_refreshing(
        _manual_tile_refresh_cache_files(kpi.get('kpi_id'), ref_y, ref_m),
    ):
        tile['cache_refresh_status'] = 'running'
    if entry.get('last_full_month_row'):
        lfr = entry['last_full_month_row']
        if isinstance(lfr, dict):
            if _is_gspp_m1_tile(kpi) or _is_gspp_m2_tile(kpi):
                if lfr.get('kpi_pct') is not None:
                    lfr = {
                        **lfr,
                        'color': _gspp_kpi_views.rag_gspp_m1_m2_pct(float(lfr['kpi_pct'])),
                    }
            elif _gspp_kpi_views.gspp_q4_kpi_id_matches(_kid_gspp):
                if lfr.get('kpi_pct') is not None:
                    lfr = {
                        **lfr,
                        'color': _gspp_kpi_views.rag_gspp_q4_pct(float(lfr['kpi_pct'])),
                    }
            elif _is_servhead_higher_better_tile(kpi):
                if lfr.get('kpi_pct') is not None:
                    lfr = {
                        **lfr,
                        'color': _servhead_kpi_views.rag_servhead_m1_pct(float(lfr['kpi_pct'])),
                    }
            elif _is_servhead_lower_better_tile(kpi):
                if lfr.get('kpi_pct') is not None:
                    lfr = {
                        **lfr,
                        'color': _servhead_kpi_views.rag_servhead_lower_better_pct(float(lfr['kpi_pct'])),
                    }
            elif _kid_gspp in _sup_kpi_views.SUP_TURNOVER_FACT_RAG_IDS:
                if lfr.get('fact') is not None:
                    lfr = {
                        **lfr,
                        'color': _sup_kpi_views.rag_hrd_turnover_fact_pct(
                            float(lfr['fact']),
                            kpi_id=_kid_gspp,
                        ),
                    }
            elif _kid_gspp in _devdir_kpi_views.DEVDIR_PLAN_FACT_COLOR_IDS:
                pct_lfr = _devdir_kpi_views.kpi_pct_from_plan_fact(lfr.get('plan'), lfr.get('fact'))
                if pct_lfr is None and lfr.get('kpi_pct') is not None:
                    pct_lfr = float(lfr['kpi_pct'])
                if pct_lfr is not None:
                    lfr = {
                        **lfr,
                        'kpi_pct': pct_lfr,
                        'color': _devdir_kpi_views.rag_devdir_plan_fact_pct(pct_lfr),
                    }
            elif _kid_gspp in _qualdir_kpi_views.TILE_COLOR_PLAN_FACT_IDS:
                lfr = _qualdir_kpi_views.enrich_qualdir_plan_fact_row(lfr)
        tile['last_full_month_row'] = _public_unit_row(lfr)
        if isinstance(lfr, dict):
            if 'project_deviation_rows' in lfr:
                tile['project_deviation_rows'] = lfr.get('project_deviation_rows')
            if 'max_allowed_delay_workdays' in lfr:
                tile['max_allowed_delay_workdays'] = lfr.get('max_allowed_delay_workdays')
    if entry.get('monthly_data') is not None:
        raw_rows = entry.get('monthly_data') or []
        if _kid_gspp in _qualdir_kpi_views.TILE_COLOR_PLAN_FACT_IDS:
            raw_rows = [
                _qualdir_kpi_views.enrich_qualdir_plan_fact_row(row)
                if isinstance(row, dict)
                else row
                for row in raw_rows
            ]
        elif _is_gspp_m1_tile(kpi) or _is_gspp_m2_tile(kpi):
            raw_rows = [
                {
                    **row,
                    'color': _gspp_kpi_views.rag_gspp_m1_m2_pct(
                        float(row['kpi_pct']) if row.get('kpi_pct') is not None else None
                    ),
                }
                if isinstance(row, dict)
                else row
                for row in raw_rows
            ]
        elif _gspp_kpi_views.gspp_q4_kpi_id_matches(_kid_gspp):
            raw_rows = [
                {
                    **row,
                    'color': _gspp_kpi_views.rag_gspp_q4_pct(
                        float(row['kpi_pct']) if row.get('kpi_pct') is not None else None
                    ),
                }
                if isinstance(row, dict)
                else row
                for row in raw_rows
            ]
        elif _is_servhead_higher_better_tile(kpi):
            raw_rows = [
                {
                    **row,
                    'color': _servhead_kpi_views.rag_servhead_m1_pct(
                        float(row['kpi_pct']) if row.get('kpi_pct') is not None else None
                    ),
                }
                if isinstance(row, dict)
                else row
                for row in raw_rows
            ]
        elif _is_servhead_lower_better_tile(kpi):
            raw_rows = [
                {
                    **row,
                    'color': _servhead_kpi_views.rag_servhead_lower_better_pct(
                        float(row['kpi_pct']) if row.get('kpi_pct') is not None else None
                    ),
                }
                if isinstance(row, dict)
                else row
                for row in raw_rows
            ]
        elif _kid_gspp in _sup_kpi_views.SUP_TURNOVER_FACT_RAG_IDS:
            raw_rows = [
                {
                    **row,
                    'color': _sup_kpi_views.rag_hrd_turnover_fact_pct(
                        float(row['fact']) if row.get('fact') is not None else None,
                        kpi_id=_kid_gspp,
                    ),
                }
                if isinstance(row, dict)
                else row
                for row in raw_rows
            ]
        elif _kid_gspp in _devdir_kpi_views.DEVDIR_PLAN_FACT_COLOR_IDS:
            colored_rows: list[dict[str, Any] | Any] = []
            for row in raw_rows:
                if not isinstance(row, dict):
                    colored_rows.append(row)
                    continue
                pct = _devdir_kpi_views.kpi_pct_from_plan_fact(row.get('plan'), row.get('fact'))
                if pct is None and row.get('kpi_pct') is not None:
                    pct = float(row['kpi_pct'])
                colored_rows.append({
                    **row,
                    **({'kpi_pct': pct} if pct is not None else {}),
                    **({'color': _devdir_kpi_views.rag_devdir_plan_fact_pct(pct)} if pct is not None else {}),
                })
            raw_rows = colored_rows
        tile['monthly_data'] = [_public_unit_row(row) for row in raw_rows]
    if entry.get('quarterly_data') is not None:
        tile['quarterly_data'] = [_public_unit_row(row) for row in entry.get('quarterly_data') or []]
    if entry.get('yearly_data') is not None:
        tile['yearly_data'] = [_public_unit_row(row) for row in entry.get('yearly_data') or []]
    if kpi.get('kpi_id') in {'QD-M1', 'QD-M5', 'QD-M8'}:
        tile['departments'] = entry.get('departments')
        if entry.get('departments_by_month') is not None:
            tile['departments_by_month'] = entry.get('departments_by_month')
        if kpi.get('kpi_id') == 'QD-M8':
            tile['kinds'] = entry.get('kinds')
            if entry.get('breakdown_by_month') is not None:
                tile['breakdown_by_month'] = entry.get('breakdown_by_month')
            lfr = tile.get('last_full_month_row') or {}
            if lfr.get('kinds') is not None:
                tile['kinds'] = lfr.get('kinds')
        lfr = tile.get('last_full_month_row') or {}
        if lfr.get('significant') is not None:
            tile['significant'] = lfr.get('significant')
    if kpi.get('kpi_id') in _qualdir_kpi_views.OTK_INCOMING_TILE_IDS:
        lfr = tile.get('last_full_month_row') or {}
        for extra_key in ('in_work_today', 'rejected_items_count'):
            if extra_key in lfr:
                tile[extra_key] = lfr.get(extra_key)
    if kpi.get('kpi_id') == 'QD-M7':
        lfr = tile.get('last_full_month_row') or {}
        for extra_key in ('accepted_to_work_today', 'checked_otk_today'):
            if extra_key in lfr:
                tile[extra_key] = lfr.get(extra_key)
    if entry.get('reference_analytics') is not None:
        tile['reference_analytics'] = entry.get('reference_analytics')
    if entry.get('period_aggregates') is not None:
        tile['period_aggregates'] = entry.get('period_aggregates')
    if entry.get('frontend_aggregation') is not None:
        tile['frontend_aggregation'] = entry.get('frontend_aggregation')
    return tile


def _plan_fact_period_label_from_kpi_period(period: dict | None) -> str | None:
    if not period or not isinstance(period, dict):
        return None
    ptype = period.get('type')
    year = period.get('year')
    if ptype in {'last_full_month', 'current_month'}:
        month_name = period.get('month_name')
        if month_name and year is not None:
            name = str(month_name)
            return f"{name[:1].upper()}{name[1:]} {year}"
    if ptype in {'last_full_quarter', 'selected_quarter'}:
        quarter = period.get('quarter')
        if quarter is not None and year is not None:
            return f"Q{quarter} {year}"
    if ptype == 'last_full_year' and year is not None:
        return str(year)
    if ptype == 'last_week':
        label = period.get('label')
        if label:
            return str(label)
    if ptype == 'ytd' and year is not None:
        month = period.get('month')
        if month:
            try:
                return f"С начала {year} г. по {MONTH_NAMES[int(month)]}"
            except Exception:
                return f"С начала {year} г."
        return f"С начала {year} г."
    return None


def _build_monthly_points_from_entry(entry: dict) -> list[dict]:
    monthly = entry.get('monthly_data') or []
    points: list[dict] = []
    for row in monthly:
        if not isinstance(row, dict):
            continue
        points.append({
            'month': row.get('month'),
            'month_name': row.get('month_name'),
            'year': row.get('year'),
            'plan': row.get('plan'),
            'fact': row.get('fact'),
            'kpi_pct': row.get('kpi_pct'),
            'has_data': row.get('has_data'),
        })
    return points


def _extract_numeric_target(value) -> float | None:
    if value is None:
        return None
    text = str(value)
    digits = re.findall(r'\d+', text)
    if not digits:
        return None
    try:
        return float(''.join(digits))
    except ValueError:
        return None


def _build_generated_profit_points(ref_y: int, ref_m: int, meta: dict | None = None) -> list[dict]:
    annual_target = _extract_numeric_target((meta or {}).get('yearly_target')) or 333_683_848.0
    monthly_plan = round(annual_target / 12.0, 2)
    points: list[dict] = []
    random.seed(hash(('OD-M2-chart-generated', ref_y, ref_m)))
    for mm in range(1, max(1, ref_m) + 1):
        fact = round(random.uniform(monthly_plan * 0.75, monthly_plan * 1.15), 2)
        points.append({
            'month': mm,
            'month_name': MONTH_NAMES.get(mm, str(mm)),
            'year': ref_y,
            'plan': monthly_plan,
            'fact': fact,
            'kpi_pct': round(fact / monthly_plan * 100, 1) if monthly_plan > 0 else None,
            'has_data': False,
        })
    return points


def _build_opdir_charts(
    tiles_meta: list[dict],
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    by_id = {k['kpi_id']: k for k in tiles_meta}
    revenue_entry = entries_by_id.get('OD-M1') or {}
    revenue_points = _build_monthly_points_from_entry(revenue_entry)
    profit_points = _build_generated_profit_points(ref_y, ref_m, by_id.get('OD-M2'))
    line_series = []
    if revenue_points:
        line_series.append({
            'kpi_id': 'OD-M1',
            'name': (by_id.get('OD-M1') or {}).get('name', 'Выручка (без НДС) - выполнение плана'),
            'chart_type': 'line_plan_fact_monthly',
            'chart_type_label': 'План/факт по месяцам',
            'points': revenue_points,
        })
    if profit_points:
        line_series.append({
            'kpi_id': 'OD-M2-CHART',
            'name': (by_id.get('OD-M2') or {}).get('name', 'Чистая прибыль - выполнение плана'),
            'chart_type': 'line_plan_fact_monthly',
            'chart_type_label': 'Сгенерированный план/факт по месяцам',
            'has_data': False,
            'points': profit_points,
        })

    monthly_meta = [
        ('OD-M1', (by_id.get('OD-M1') or {}).get('name', 'Выручка (без НДС) - выполнение плана'), revenue_points),
        ('OD-M2-CHART', (by_id.get('OD-M2') or {}).get('name', 'Чистая прибыль - выполнение плана'), profit_points),
    ]
    categories: list[str] = []
    plan_values: list[float | None] = []
    fact_values: list[float | None] = []
    bar_points: list[dict] = []
    for kid, name, points in monthly_meta:
        point = pick_monthly_row_for_period(points, ref_y, ref_m)
        categories.append(name)
        plan_values.append(point.get('plan') if point else None)
        fact_values.append(point.get('fact') if point else None)
        bar_points.append({
            'kpi_id': kid,
            'name': name,
            'month': ref_m,
            'year': ref_y,
            'plan': point.get('plan') if point else None,
            'fact': point.get('fact') if point else None,
            'kpi_pct': point.get('kpi_pct') if point else None,
            'has_data': point.get('has_data') if point else False,
        })

    charts = {}
    if line_series:
        charts['OD-C1'] = {
            'kpi_id': 'OD-C1',
            'name': 'Динамика выручки и чистой прибыли',
            'periodicity': 'ежемесячно',
            'chart_type': 'multi_line_plan_fact_monthly',
            'chart_type_label': 'Линейный тренд по месяцам',
            'series': line_series,
        }
    charts['OD-C2'] = {
        'kpi_id': 'OD-C2',
        'name': 'План/факт за выбранный месяц',
        'periodicity': 'ежемесячно',
        'chart_type': 'column_plan_fact_monthly',
        'chart_type_label': 'Столбцы: план/факт за месяц',
        'series': [{
            'kpi_id': 'OD-C2',
            'name': 'План/факт за месяц',
            'chart_type': 'column_plan_fact_monthly',
            'chart_type_label': 'Столбцы',
            'categories': categories,
            'plan': plan_values,
            'fact': fact_values,
            'points': bar_points,
        }],
    }
    return charts


def _line_values_from_points(points: list[dict], key: str) -> list[float | None]:
    values: list[float | None] = []
    for point in points:
        value = point.get(key)
        try:
            values.append(float(value) if value is not None else None)
        except (TypeError, ValueError):
            values.append(None)
    return values


def _build_prod_deputy_charts(entries_by_id: dict[str, dict], ref_y: int, ref_m: int) -> dict:
    shops = [
        ("pc1", "Турбулентность-Дон", "PD-M3.B1", "PD-M3.F1"),
        ("pc2", "Алмаз", "PD-M3.B2", "PD-M3.F2"),
    ]
    series: list[dict] = []
    bar_series: list[dict] = []

    for shop, label, budget_id, fot_id in shops:
        budget_points = _build_monthly_points_from_entry(entries_by_id.get(budget_id) or {})
        fot_points = _build_monthly_points_from_entry(entries_by_id.get(fot_id) or {})
        points_by_month: dict[tuple[int | None, int | None], dict] = {}

        for point in budget_points:
            key = (point.get("year"), point.get("month"))
            row = points_by_month.setdefault(key, {
                "year": point.get("year"),
                "month": point.get("month"),
                "month_name": point.get("month_name"),
            })
            row["budget_plan"] = point.get("plan")
            row["budget_fact"] = point.get("fact")

        for point in fot_points:
            key = (point.get("year"), point.get("month"))
            row = points_by_month.setdefault(key, {
                "year": point.get("year"),
                "month": point.get("month"),
                "month_name": point.get("month_name"),
            })
            row["fot_plan"] = point.get("plan")
            row["fot_fact"] = point.get("fact")

        points = sorted(
            points_by_month.values(),
            key=lambda row: (int(row.get("year") or 0), int(row.get("month") or 0)),
        )
        if not points:
            continue

        series.append({
            "kpi_id": f"PD-C1-{shop.upper()}",
            "name": label,
            "option_label": label,
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": "Бюджет и ФОТ: план/факт по месяцам",
            "points": points,
            "line_series": [
                {
                    "name": "Бюджет факт",
                    "data": _line_values_from_points(points, "budget_fact"),
                    "color": "#2563eb",
                    "value_role": "fact",
                    "metric": "budget",
                },
                {
                    "name": "Бюджет план",
                    "data": _line_values_from_points(points, "budget_plan"),
                    "color": "#2563eb",
                    "dashStyle": "Dash",
                    "value_role": "plan",
                    "metric": "budget",
                },
                {
                    "name": "ФОТ факт",
                    "data": _line_values_from_points(points, "fot_fact"),
                    "color": "#16a34a",
                    "value_role": "fact",
                    "metric": "fot",
                },
                {
                    "name": "ФОТ план",
                    "data": _line_values_from_points(points, "fot_plan"),
                    "color": "#16a34a",
                    "dashStyle": "Dash",
                    "value_role": "plan",
                    "metric": "fot",
                },
            ],
            "disable_all_option": True,
        })

    if any(kpi_id in entries_by_id for kpi_id in PROD_DEPUTY_OUTPUT_PERIOD_BY_ID):
        try:
            from . import calc_prod_deputy_output
            for shop, label, _budget_id, _fot_id in shops:
                chart_data = cache_manager.locked_call(
                    f'pd_m1_output_{shop}_{ref_y}_{ref_m}',
                    calc_prod_deputy_output.get_prod_deputy_output_monthly,
                    shop,
                    year=ref_y,
                    month=ref_m,
                )
                points = [
                    point for point in chart_data.get("months") or []
                    if (
                        int(point.get("year") or 0) == int(ref_y)
                        and point.get("fact") is not None
                        and abs(float(point.get("fact") or 0)) > 0
                    )
                ]
                points = sorted(points, key=lambda point: int(point.get("month") or 0))
                if not points:
                    points = [
                        point for point in chart_data.get("months") or []
                        if int(point.get("year") or 0) == int(ref_y) and point.get("fact") is not None
                    ]
                    points = sorted(points, key=lambda point: int(point.get("month") or 0))
                unit = (
                    chart_data.get("ytd", {}).get("values_unit")
                    or calc_prod_deputy_output.VALUES_UNIT.get(shop)
                )
                if not points:
                    continue
                bar_series.append({
                    "kpi_id": f"PD-C2-{shop.upper()}",
                    "name": label,
                    "option_label": label,
                    "chart_type": "column_plan_fact_monthly",
                    "chart_type_label": "План/факт выпуска по месяцам",
                    "categories": [
                        str(p.get("month_name") or p.get("month") or "").capitalize()
                        for p in points
                    ],
                    "plan": [p.get("plan") for p in points],
                    "fact": [p.get("fact") for p in points],
                    "points": points,
                    "unit": unit,
                    "x_axis_title": "Месяцы",
                    "y_axis_title": unit or "Значение",
                    "single_indicator": True,
                    "disable_all_option": True,
                })
        except Exception:
            logger.exception("Не удалось собрать помесячный график выполнения производственного плана")

    if not series and not bar_series:
        return {}

    charts = {}
    if series:
        charts["PD-C1"] = {
            "kpi_id": "PD-C1",
            "name": "Турбулентность-Дон / Алмаз: ФОТ и бюджет",
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "План пунктиром, факт сплошной линией",
            "series": series,
        }
    if bar_series:
        charts["PD-C2"] = {
            "kpi_id": "PD-C2",
            "name": "Выполнение производственного плана",
            "periodicity": "ежемесячно",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "План/факт по месяцам за год",
            "series": bar_series,
        }
    return charts


def _build_qualdir_charts(
    tiles_meta: list[dict],
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """
    Графики дашборда «директор по качеству»: линия — ФОТ (QD-M4) и бюджет (QD-M3),
    столбцы — факт (и план) по месяцам для форм 03-17 / 03-18 / 03-19.
    """
    by_id = {k['kpi_id']: k for k in tiles_meta}
    charts: dict = {}

    line_kpis = ['QD-M4', 'QD-M3']
    display_line = {'QD-M3': 'Бюджет', 'QD-M4': 'ФОТ'}
    series: list[dict] = []
    for kid in line_kpis:
        kpi_meta = by_id.get(kid, {})
        entry = _entry_by_kpi_id(entries_by_id, kid)
        monthly = entry.get('monthly_data') or []
        points = [
            {
                'month': row.get('month'),
                'month_name': row.get('month_name'),
                'year': row.get('year'),
                'plan': row.get('plan'),
                'fact': row.get('fact'),
            }
            for row in monthly
        ]
        if not points:
            continue
        if not any((p.get('plan') is not None or p.get('fact') is not None) for p in points):
            continue
        series.append({
            'kpi_id': kid,
            'name': display_line.get(kid, kpi_meta.get('name', kid)),
            'chart_type': 'line_plan_fact_monthly',
            'chart_type_label': (
                f"План/Факт по месяцам: {display_line.get(kid, kpi_meta.get('name', kid))}"
            ),
            'points': points,
        })

    if series:
        charts['QD-C1'] = {
            'kpi_id': 'QD-C1',
            'name': 'Динамика ФОТ и бюджета',
            'periodicity': 'ежемесячно',
            'chart_type': 'multi_line_plan_fact_monthly',
            'chart_type_label': 'Линейный тренд по месяцам (план/факт)',
            'series': series,
        }

    bar_specs: list[tuple[str, str]] = [
        ('QD-M8', 'Форма 03-17'),
        ('QD-M5', 'Форма 03-18'),
        ('QD-M1', 'Форма 03-19'),
    ]
    month_names = {
        1: 'январь', 2: 'февраль', 3: 'март', 4: 'апрель',
        5: 'май', 6: 'июнь', 7: 'июль', 8: 'август',
        9: 'сентябрь', 10: 'октябрь', 11: 'ноябрь', 12: 'декабрь',
    }
    bar_series: list[dict] = []
    for kid, form_label in bar_specs:
        entry = _entry_by_kpi_id(entries_by_id, kid)
        monthly_rows = _qualdir_monthly_rows_for_chart(entry, ref_y, ref_m)
        categories = [
            str(row.get('month_name') or month_names.get(int(row.get('month', 0)), ''))
            for row in monthly_rows
        ]
        plan_values = [row.get('plan') for row in monthly_rows]
        fact_values = [row.get('fact') for row in monthly_rows]
        points = [
            {
                'kpi_id': kid,
                'name': form_label,
                'form': form_label,
                'month': row.get('month'),
                'year': row.get('year'),
                'month_name': row.get('month_name'),
                'plan': row.get('plan'),
                'fact': row.get('fact'),
                'kpi_pct': row.get('kpi_pct'),
                'has_data': row.get('has_data'),
                'values_unit': 'шт.',
            }
            for row in monthly_rows
        ]
        bar_series.append({
            'kpi_id': kid,
            'name': form_label,
            'form': form_label,
            'chart_type': 'column_plan_fact_monthly',
            'chart_type_label': 'Столбцы',
            'categories': categories,
            'plan': plan_values,
            'fact': fact_values,
            'points': points,
        })

    charts['QD-C2'] = {
        'kpi_id': 'QD-C2',
        'name': 'Формы 03-17, 03-18, 03-19',
        'periodicity': 'ежемесячно',
        'chart_type': 'column_plan_fact_monthly',
        'chart_type_label': 'Столбцы: факт по формам по месяцам',
        'period': {
            'year': ref_y,
            'month': ref_m,
            'month_name': month_names[ref_m],
        },
        'series': bar_series,
    }

    return charts


def _build_gspp_charts(
    tiles_meta: list[dict],
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """Графики ГСПП: линия и столбцы по ФОТ подразделения (M3) и бюджету (M5)."""
    chart_specs = [
        ("GSPP-M3", "ФОТ", _is_gspp_m3_tile),
        ("GSPP-M5", "Бюджет", _is_gspp_m5_tile),
    ]
    sources: list[tuple[str, str, dict]] = []
    for fallback_id, display_name, predicate in chart_specs:
        meta = next((item for item in tiles_meta if predicate(item)), {})
        entry = entries_by_id.get(meta.get("kpi_id")) if meta else None
        sources.append((fallback_id, display_name, entry or {}))

    series: list[dict] = []
    for fallback_id, display_name, entry in sources:
        monthly = entry.get("monthly_data") or []
        points = [
            {
                "month": row.get("month"),
                "month_name": row.get("month_name"),
                "year": row.get("year"),
                "plan": row.get("plan"),
                "fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "has_data": row.get("has_data"),
                "values_unit": row.get("values_unit"),
            }
            for row in monthly
            if isinstance(row, dict)
        ]
        if not any((p.get("plan") is not None or p.get("fact") is not None) for p in points):
            continue
        series.append({
            "kpi_id": fallback_id,
            "name": display_name,
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": f"План/Факт по месяцам: {display_name}",
            "points": points,
        })

    charts: dict = {}
    if series:
        charts["GSPP-C1"] = {
            "kpi_id": "GSPP-C1",
            "name": "Динамика ФОТ и бюджета ГСПП",
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "Линейный тренд по месяцам (план/факт)",
            "series": series,
        }

    bar_categories: list[str] = []
    bar_plan_values: list[float | None] = []
    bar_fact_values: list[float | None] = []
    bar_points: list[dict] = []
    for fallback_id, display_name, entry in sources:
        kper = entry.get("kpi_period") or {}
        point_y, point_m = ref_y, ref_m
        if isinstance(kper, dict) and kper.get("year") is not None and kper.get("month") is not None:
            point_y = int(kper["year"])
            point_m = max(1, min(12, int(kper["month"])))
        point = (
            pick_monthly_row_for_period(entry.get("monthly_data") or [], point_y, point_m)
            or entry.get("last_full_month_row")
            or {}
        )
        bar_categories.append(display_name)
        bar_plan_values.append(point.get("plan"))
        bar_fact_values.append(point.get("fact"))
        bar_points.append({
            "kpi_id": fallback_id,
            "name": display_name,
            "month": point.get("month", point_m),
            "year": point.get("year", point_y),
            "plan": point.get("plan"),
            "fact": point.get("fact"),
            "kpi_pct": point.get("kpi_pct"),
            "has_data": point.get("has_data"),
            "values_unit": point.get("values_unit"),
        })

    if any(v is not None for v in bar_plan_values) or any(v is not None for v in bar_fact_values):
        charts["GSPP-C2"] = {
            "kpi_id": "GSPP-C2",
            "name": "ФОТ и бюджет ГСПП за выбранный месяц",
            "periodicity": "ежемесячно",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "Столбцы: план/факт за месяц",
            "series": [{
                "kpi_id": "GSPP-C2",
                "name": "План/факт за месяц",
                "chart_type": "column_plan_fact_monthly",
                "chart_type_label": "Столбцы",
                "categories": bar_categories,
                "plan": bar_plan_values,
                "fact": bar_fact_values,
                "points": bar_points,
            }],
        }

    return charts


def _build_dual_plan_fact_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
    *,
    sources: list[tuple[str, str]],
    line_chart_id: str,
    bar_chart_id: str,
    line_name: str,
    bar_name: str,
    default_unit: str = "руб.",
) -> dict:
    """Линия (C1) + столбцы (C2) по двум помесячным KPI (план/факт)."""
    resolved: list[tuple[str, str, dict]] = [
        (kid, display_name, _entry_by_kpi_id(entries_by_id, kid))
        for kid, display_name in sources
    ]

    series: list[dict] = []
    for kid, display_name, entry in resolved:
        monthly = entry.get("monthly_data") or []
        points = [
            {
                "month": row.get("month"),
                "month_name": row.get("month_name"),
                "year": row.get("year"),
                "plan": row.get("plan"),
                "fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "has_data": row.get("has_data"),
                "values_unit": row.get("values_unit") or default_unit,
            }
            for row in monthly
            if isinstance(row, dict)
        ]
        if not any((p.get("plan") is not None or p.get("fact") is not None) for p in points):
            continue
        series.append({
            "kpi_id": kid,
            "name": display_name,
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": f"План/Факт по месяцам: {display_name}",
            "points": points,
        })

    charts: dict = {}
    if series:
        charts[line_chart_id] = {
            "kpi_id": line_chart_id,
            "name": line_name,
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "Линейный тренд по месяцам (план/факт)",
            "series": series,
        }

    bar_categories: list[str] = []
    bar_plan_values: list[float | None] = []
    bar_fact_values: list[float | None] = []
    bar_points: list[dict] = []
    for kid, display_name, entry in resolved:
        kper = entry.get("kpi_period") or {}
        point_y, point_m = ref_y, ref_m
        if isinstance(kper, dict) and kper.get("year") is not None and kper.get("month") is not None:
            point_y = int(kper["year"])
            point_m = max(1, min(12, int(kper["month"])))
        point = (
            entry.get("last_full_month_row")
            or pick_monthly_row_for_period(entry.get("monthly_data") or [], point_y, point_m)
            or {}
        )
        bar_categories.append(display_name)
        bar_plan_values.append(point.get("plan"))
        bar_fact_values.append(point.get("fact"))
        bar_points.append({
            "kpi_id": kid,
            "name": display_name,
            "month": point.get("month", point_m),
            "year": point.get("year", point_y),
            "plan": point.get("plan"),
            "fact": point.get("fact"),
            "kpi_pct": point.get("kpi_pct"),
            "has_data": point.get("has_data"),
            "values_unit": point.get("values_unit") or default_unit,
        })

    if any(v is not None for v in bar_plan_values) or any(v is not None for v in bar_fact_values):
        charts[bar_chart_id] = {
            "kpi_id": bar_chart_id,
            "name": bar_name,
            "periodicity": "ежемесячно",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "Столбцы: план/факт за месяц",
            "series": [{
                "kpi_id": bar_chart_id,
                "name": "План/факт за месяц",
                "chart_type": "column_plan_fact_monthly",
                "chart_type_label": "Столбцы",
                "categories": bar_categories,
                "plan": bar_plan_values,
                "fact": bar_fact_values,
                "points": bar_points,
            }],
        }

    return charts


def _build_sup_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """Графики SUP: линия и столбцы по ФОТ (HRD-M2) и бюджету (HRD-M3)."""
    return _build_dual_plan_fact_charts(
        entries_by_id,
        ref_y,
        ref_m,
        sources=[("HRD-M2", "ФОТ"), ("HRD-M3", "Бюджет")],
        line_chart_id="HRD-C1",
        bar_chart_id="HRD-C2",
        line_name="Динамика ФОТ и бюджета",
        bar_name="ФОТ и бюджет за выбранный месяц",
    )


def _build_autoit_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """Графики autoit: ФОТ (IT-M4) и бюджет (IT-M3)."""
    return _build_dual_plan_fact_charts(
        entries_by_id,
        ref_y,
        ref_m,
        sources=[("IT-M4", "ФОТ"), ("IT-M3", "Бюджет")],
        line_chart_id="IT-C1",
        bar_chart_id="IT-C2",
        line_name="Динамика ФОТ и бюджета",
        bar_name="ФОТ и бюджет за выбранный месяц",
    )


def _build_c1auto_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """Графики 1С: ФОТ (1C-M4) и бюджет (1C-M3)."""
    return _build_dual_plan_fact_charts(
        entries_by_id,
        ref_y,
        ref_m,
        sources=[("1C-M4", "ФОТ"), ("1C-M3", "Бюджет")],
        line_chart_id="1C-C1",
        bar_chart_id="1C-C2",
        line_name="Динамика ФОТ и бюджета",
        bar_name="ФОТ и бюджет за выбранный месяц",
    )


def _build_servhead_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """
    Графики servhead.

    Если есть плитки ФОТ/бюджет (СЕР-M3-1 / СЕР-M3-2) — как у SUP.
    Иначе на дашборде «Начальник сервисной службы» только обращения:
    SH-M1 (удовлетворённые) и SH-M4 (в срок).
    """
    fot_entry = (
        _entry_by_kpi_id(entries_by_id, "СЕР-M3-1")
        or _entry_by_kpi_id(entries_by_id, "CEP-M3-1")
        or _entry_by_kpi_id(entries_by_id, "SER-M3-1")
    )
    budget_entry = (
        _entry_by_kpi_id(entries_by_id, "СЕР-M3-2")
        or _entry_by_kpi_id(entries_by_id, "CEP-M3-2")
        or _entry_by_kpi_id(entries_by_id, "SER-M3-2")
    )
    has_fot_budget = bool(
        (fot_entry.get("monthly_data") or fot_entry.get("last_full_month_row"))
        or (budget_entry.get("monthly_data") or budget_entry.get("last_full_month_row"))
    )
    if has_fot_budget:
        return _build_dual_plan_fact_charts(
            entries_by_id,
            ref_y,
            ref_m,
            sources=[("СЕР-M3-1", "ФОТ"), ("СЕР-M3-2", "Бюджет")],
            line_chart_id="СЕР-C1",
            bar_chart_id="СЕР-C2",
            line_name="Динамика ФОТ и бюджета",
            bar_name="ФОТ и бюджет за выбранный месяц",
        )
    return _build_dual_plan_fact_charts(
        entries_by_id,
        ref_y,
        ref_m,
        sources=[("SH-M1", "Удовлетворённые"), ("SH-M4", "В срок")],
        line_chart_id="SH-C1",
        bar_chart_id="SH-C2",
        line_name="Динамика обращений",
        bar_name="Обращения за выбранный месяц",
        default_unit="шт.",
    )


def _build_devdir_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """Графики devdir: линия — RD-M4/RD-M3; столбцы RD-C2 — RD-M3-1 и RD-M2-1."""
    line_sources = [
        ("RD-M4", "ФОТ", entries_by_id.get("RD-M4") or {}),
        ("RD-M3", "Бюджет", entries_by_id.get("RD-M3") or {}),
    ]
    bar_sources = [
        ("RD-M3-1", entries_by_id.get("RD-M3-1") or {}),
        ("RD-M2-1", entries_by_id.get("RD-M2-1") or {}),
    ]

    series: list[dict] = []
    for kid, display_name, entry in line_sources:
        monthly = entry.get("monthly_data") or []
        points = [
            {
                "month": row.get("month"),
                "month_name": row.get("month_name"),
                "year": row.get("year"),
                "plan": row.get("plan"),
                "fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "has_data": row.get("has_data"),
                "values_unit": row.get("values_unit"),
            }
            for row in monthly
            if isinstance(row, dict)
        ]
        if not any((p.get("plan") is not None or p.get("fact") is not None) for p in points):
            continue
        series.append({
            "kpi_id": kid,
            "name": display_name,
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": f"План/Факт по месяцам: {display_name}",
            "points": points,
        })

    charts: dict = {}
    if series:
        charts["RD-C1"] = {
            "kpi_id": "RD-C1",
            "name": "Динамика ФОТ и бюджета",
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "Линейный тренд по месяцам (план/факт)",
            "series": series,
        }

    bar_categories: list[str] = []
    bar_plan_values: list[float | None] = []
    bar_fact_values: list[float | None] = []
    bar_points: list[dict] = []
    for kid, entry in bar_sources:
        kper = entry.get("kpi_period") or {}
        point_y, point_m = ref_y, ref_m
        if isinstance(kper, dict) and kper.get("year") is not None and kper.get("month") is not None:
            point_y = int(kper["year"])
            point_m = max(1, min(12, int(kper["month"])))
        point = (
            pick_monthly_row_for_period(entry.get("monthly_data") or [], point_y, point_m)
            or entry.get("last_full_month_row")
            or {}
        )
        display_name = (entry.get("name") or kid).strip()
        bar_categories.append(display_name)
        bar_plan_values.append(point.get("plan"))
        bar_fact_values.append(point.get("fact"))
        bar_points.append({
            "kpi_id": kid,
            "name": display_name,
            "month": point.get("month", point_m),
            "year": point.get("year", point_y),
            "plan": point.get("plan"),
            "fact": point.get("fact"),
            "kpi_pct": point.get("kpi_pct"),
            "has_data": point.get("has_data"),
            "values_unit": point.get("values_unit"),
        })

    if any(v is not None for v in bar_plan_values) or any(v is not None for v in bar_fact_values):
        charts["RD-C2"] = {
            "kpi_id": "RD-C2",
            "name": "План/факт за выбранный месяц (RD-M3-1, RD-M2-1)",
            "periodicity": "ежемесячно",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "Столбцы: план/факт за месяц",
            "series": [{
                "kpi_id": "RD-C2",
                "name": "План/факт за месяц",
                "chart_type": "column_plan_fact_monthly",
                "chart_type_label": "Столбцы",
                "categories": bar_categories,
                "plan": bar_plan_values,
                "fact": bar_fact_values,
                "points": bar_points,
            }],
        }

    return charts


def _build_chief_metrolog_charts(
    entries_by_id: dict[str, dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """Графики главного метролога: ФОТ и бюджет, план/факт по месяцам."""
    sources = [
        ("METD-M3.F", "ФОТ", entries_by_id.get("METD-M3.F") or {}),
        ("METD-M3.B", "Бюджет", entries_by_id.get("METD-M3.B") or {}),
    ]

    series: list[dict] = []
    for kid, display_name, entry in sources:
        points = [
            {
                "month": row.get("month"),
                "month_name": row.get("month_name"),
                "year": row.get("year"),
                "plan": row.get("plan"),
                "fact": row.get("fact"),
                "kpi_pct": row.get("kpi_pct"),
                "has_data": row.get("has_data"),
                "values_unit": row.get("values_unit"),
            }
            for row in entry.get("monthly_data") or []
            if isinstance(row, dict)
        ]
        if not any((p.get("plan") is not None or p.get("fact") is not None) for p in points):
            continue
        series.append({
            "kpi_id": kid,
            "name": display_name,
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": f"План/Факт по месяцам: {display_name}",
            "points": points,
        })

    charts: dict = {}
    if series:
        charts["METD-C1"] = {
            "kpi_id": "METD-C1",
            "name": "Динамика ФОТ и бюджета",
            "periodicity": "ежемесячно",
            "chart_type": "multi_line_plan_fact_monthly",
            "chart_type_label": "Линейный тренд по месяцам (план/факт)",
            "series": series,
        }

    bar_categories: list[str] = []
    bar_plan_values: list[float | None] = []
    bar_fact_values: list[float | None] = []
    bar_points: list[dict] = []
    for kid, display_name, entry in sources:
        kper = entry.get("kpi_period") or {}
        point_y, point_m = ref_y, ref_m
        if isinstance(kper, dict) and kper.get("year") is not None and kper.get("month") is not None:
            point_y = int(kper["year"])
            point_m = max(1, min(12, int(kper["month"])))
        point = (
            pick_monthly_row_for_period(entry.get("monthly_data") or [], point_y, point_m)
            or entry.get("last_full_month_row")
            or {}
        )
        bar_categories.append(display_name)
        bar_plan_values.append(point.get("plan"))
        bar_fact_values.append(point.get("fact"))
        bar_points.append({
            "kpi_id": kid,
            "name": display_name,
            "month": point.get("month", point_m),
            "year": point.get("year", point_y),
            "plan": point.get("plan"),
            "fact": point.get("fact"),
            "kpi_pct": point.get("kpi_pct"),
            "has_data": point.get("has_data"),
            "values_unit": point.get("values_unit"),
        })

    if any(v is not None for v in bar_plan_values) or any(v is not None for v in bar_fact_values):
        charts["METD-C2"] = {
            "kpi_id": "METD-C2",
            "name": "ФОТ и бюджет за выбранный месяц",
            "periodicity": "ежемесячно",
            "chart_type": "column_plan_fact_monthly",
            "chart_type_label": "Столбцы: план/факт за месяц",
            "series": [{
                "kpi_id": "METD-C2",
                "name": "План/факт за месяц",
                "chart_type": "column_plan_fact_monthly",
                "chart_type_label": "Столбцы",
                "categories": bar_categories,
                "plan": bar_plan_values,
                "fact": bar_fact_values,
                "points": bar_points,
            }],
        }

    return charts


def _wants_tile_debug(request) -> bool:
    """Параметр ``?include_debug=1`` — добавить в элементы плиток поле ``debug`` (диагностика)."""
    v = (request.GET.get('include_debug') or '').strip().lower()
    return v in ('1', 'true', 'yes', 'on')


def _request_aggregation_params(request) -> tuple[str | None, list[int]]:
    mode = (request.GET.get('aggregation_mode') or '').strip().lower() or None
    raw_quarters = request.GET.get('selected_quarters') or ''
    quarters: list[int] = []
    for part in str(raw_quarters).replace(';', ',').split(','):
        try:
            q = int(part.strip())
        except (TypeError, ValueError):
            continue
        if 1 <= q <= 4:
            quarters.append(q)
    return mode, quarters


def _build_universal_payload(
    dept: str,
    all_kpis: list[dict],
    *,
    month: int | None = None,
    year: int | None = None,
    include_debug: bool = False,
    aggregation_mode: str | None = None,
    selected_quarters: list[int] | None = None,
    _skip_disk_cache: bool = False,
) -> dict:
    """
    Универсальный билдер: Плитки, Графики, Таблицы.
    Таблицы — активные претензии из 1С (Catalog_Претензии) по разрешенным статусам.
    """
    from .komdir_claims import fetch_claims_for_month
    from .kpi_periods import last_full_month as _lfm

    tiles_meta = [k for k in all_kpis if k.get('block', 'плитка') == 'плитка']
    charts_meta = [k for k in all_kpis if k.get('block') == 'график']

    plitki_items: list[dict] = []
    entries_by_id: dict[str, dict] = {}

    # Нельзя писать «if month and year»: при ?month=5 без year второй аргумент
    # игнорировался, ref становился «сегодня», а не май.
    today = date.today()
    if year is not None and month is not None:
        ref_y, ref_m = int(year), max(1, min(12, int(month)))
    elif (
        techdir_dashboard.is_techdir_department(dept)
        or str(dept).strip().lower() == 'операционный директор'
        or _is_prod_deputy_department(dept)
        or _is_qualdir_department(dept)
        or _is_chief_constructor_department(dept)
        or _is_chief_metrolog_department(dept)
        or logistics_views.is_logistics_head_department(dept)
        or _is_qualdir_dashboard(dept, all_kpis)
        or _is_devdir_department(dept)
        or _is_gspp_department(dept)
        or _is_sup_department(dept)
        or _servhead_kpi_views.is_servhead_department(dept)
        or _is_autoit_department(dept)
        or _is_c1auto_department(dept)
    ):
        if year is not None and month is None:
            ref_y = int(year)
            if _is_qualdir_dashboard(dept, all_kpis):
                # Не ставить december по умолчанию при ref_y≠today.year —
                # электрон может слать только year без month: «висящий» 12-й тянул «Декабрь» в середине календаря.
                if ref_y <= today.year:
                    ref_m = today.month if ref_y == today.year else 12
                else:
                    ref_y, ref_m = today.year, today.month
            else:
                ref_m = today.month if ref_y == today.year else 12
        elif month is not None and year is None:
            ref_y, ref_m = today.year, max(1, min(12, int(month)))
        else:
            ref_y, ref_m = today.year, today.month
    else:
        ref_y, ref_m = _lfm(today)

    gspp_memo_key: str | None = None
    techdir_memo_key: str | None = None
    qualdir_memo_key: str | None = None
    sup_memo_key: str | None = None
    autoit_memo_key: str | None = None
    c1auto_memo_key: str | None = None
    servhead_memo_key: str | None = None
    devdir_memo_key: str | None = None
    if _is_gspp_department(dept) and not include_debug:
        gspp_memo_key = f"gspp_dashboard:v4:{dept.strip().lower()}:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(gspp_memo_key)
        if cached_payload is not None:
            return cached_payload
    if techdir_dashboard.is_techdir_department(dept) and not include_debug:
        techdir_memo_key = f"techdir_dashboard:v1:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(techdir_memo_key)
        if cached_payload is not None:
            return cached_payload
    if _is_qualdir_dashboard(dept, all_kpis) and not include_debug:
        qualdir_memo_key = f"qualdir_dashboard:v2:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(qualdir_memo_key)
        if cached_payload is not None:
            return cached_payload
    if _is_sup_department(dept) and not include_debug:
        # v5: trim monthly_data до опорного месяца (HRD-M3: не смешивать июль с цветом июня).
        sup_memo_key = f"sup_dashboard:v6:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(sup_memo_key)
        if cached_payload is not None:
            return cached_payload
    if _is_autoit_department(dept) and not include_debug:
        autoit_memo_key = f"autoit_dashboard:v7:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(autoit_memo_key)
        if cached_payload is not None:
            return cached_payload
    if _is_c1auto_department(dept) and not include_debug:
        c1auto_memo_key = f"c1auto_dashboard:v4:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(c1auto_memo_key)
        if cached_payload is not None:
            return cached_payload
    if _servhead_kpi_views.is_servhead_department(dept) and not include_debug:
        servhead_memo_key = f"servhead_dashboard:v5:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(servhead_memo_key)
        if cached_payload is not None:
            return cached_payload
    if _is_devdir_department(dept) and not include_debug:
        # v2: сброс memo после ручной инвалидации RD-M3-1 / SQL-планов текучести
        devdir_memo_key = f"devdir_dashboard:v2:{ref_y}:{ref_m:02d}"
        cached_payload = cache_manager.get_memoized_dashboard_payload(devdir_memo_key)
        if cached_payload is not None:
            logger.info("cache_manager: devdir dashboard memo hit %s", devdir_memo_key)
            return cached_payload

    dashboard_disk_key: str | None = None
    dashboard_mem_key: str | None = None
    if not _skip_disk_cache and not include_debug:
        if gspp_memo_key:
            dashboard_disk_key = f"gspp_v2_{dept.strip().lower()}_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = gspp_memo_key
        elif techdir_memo_key:
            dashboard_disk_key = f"techdir_v1_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = techdir_memo_key
        elif qualdir_memo_key:
            dashboard_disk_key = f"qualdir_v2_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = qualdir_memo_key
        elif sup_memo_key:
            dashboard_disk_key = f"sup_v6_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = sup_memo_key
        elif autoit_memo_key:
            dashboard_disk_key = f"autoit_v7_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = autoit_memo_key
        elif c1auto_memo_key:
            dashboard_disk_key = f"c1auto_v4_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = c1auto_memo_key
        elif servhead_memo_key:
            dashboard_disk_key = f"servhead_v5_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = servhead_memo_key
        elif devdir_memo_key:
            dashboard_disk_key = f"devdir_v2_{ref_y}_{ref_m:02d}"
            dashboard_mem_key = devdir_memo_key

    if dashboard_disk_key and dashboard_mem_key:
        disk_cached = cache_manager.try_serve_dashboard_disk_cache(
            dashboard_disk_key,
            dashboard_mem_key,
            refresh_fn=lambda: _build_universal_payload(
                dept,
                all_kpis,
                month=month,
                year=year,
                include_debug=include_debug,
                aggregation_mode=aggregation_mode,
                selected_quarters=selected_quarters,
                _skip_disk_cache=True,
            ),
        )
        if disk_cached is not None:
            return disk_cached

    for kpi in tiles_meta:
        entry = _build_kpi_entry(kpi, 'плитка', dept_key=dept, year=ref_y, month=ref_m)
        entries_by_id[kpi['kpi_id']] = entry
        pct, color = _tile_color(kpi, entry)
        # QD-M3 / QD-M4 и devdir RD-*: месяц в расчёте нормализуется внутри get_*_ytd —
        # строка плитки должна браться из kpi_period иначе план/факт за «чужой» месяц.
        tile_lm_y, tile_lm_m = ref_y, ref_m
        _kid_tile = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
        if _kid_tile in (
            _qualdir_kpi_views.KPI_IDS_USE_BUILDER_KP_PERIOD
            | _devdir_kpi_views.DEVDIR_KPI_IDS
            | _gspp_kpi_views.GSPP_KPI_IDS_USE_BUILDER_KP_PERIOD
            | _sup_kpi_views.SUP_KPI_IDS_USE_BUILDER_KP_PERIOD
            | _servhead_kpi_views.SERVHEAD_KPI_IDS_USE_BUILDER_KP_PERIOD
            | _autoit_kpi_views.AUTOIT_KPI_IDS_USE_BUILDER_KP_PERIOD
            | _c1auto_kpi_views.C1AUTO_KPI_IDS_USE_BUILDER_KP_PERIOD
        ) or _kid_tile == 'TD-M6':
            kper = entry.get('kpi_period')
            if (
                isinstance(kper, dict)
                and kper.get('type') in {'last_full_month', 'current_month'}
                and kper.get('year') is not None
                and kper.get('month') is not None
            ):
                tile_lm_y = int(kper['year'])
                tile_lm_m = max(1, min(12, int(kper['month'])))
        tile = _build_tile_item(
            kpi, pct, color, entry, ref_y=tile_lm_y, ref_m=tile_lm_m,
        )

        monthly_data = entry.get('monthly_data')
        if _kid_tile in PROD_DEPUTY_OUTPUT_PERIOD_BY_ID:
            # Эти плитки не месячная точка графика: week/month/total берутся из
            # одного выбранного документа ТД_ПроизводственныйПлан.
            lm = entry.get('last_full_month_row') or {}
        else:
            lm = (
                pick_monthly_row_for_period(monthly_data, tile_lm_y, tile_lm_m)
                if monthly_data
                else {}
            )
            if not lm:
                lm = {
                    'year': tile_lm_y,
                    'month': tile_lm_m,
                    'month_name': MONTH_NAMES.get(tile_lm_m, str(tile_lm_m)),
                    'plan': None,
                    'fact': None,
                    'kpi_pct': None,
                    'has_data': False,
                }
        if kpi.get('kpi_id') in {'QD-M1', 'QD-M5', 'QD-M6', 'QD-M8', 'QD-M9', 'QD-M10', 'QD-Q1'}:
            lfr = entry.get('last_full_month_row') or {}
            if lfr.get('plan') is not None and (
                not lm or (lm.get('plan') is None and lm.get('fact') is not None)
            ):
                lm = lfr
            elif lm.get('plan') is None and lm.get('fact') is not None:
                ytd_vals = entry.get('ytd') or {}
                if ytd_vals.get('total_plan') is not None:
                    lm = {
                        **lm,
                        'plan': ytd_vals.get('total_plan'),
                        'fact': ytd_vals.get('total_fact'),
                        'kpi_pct': ytd_vals.get('kpi_pct'),
                    }
        if lm:
            tile['plan'] = lm.get('plan')
            tile['fact'] = lm.get('fact')
            if 'has_data' in lm:
                tile['has_data'] = lm.get('has_data')
            if 'plan_by_dept' in lm:
                tile['plan_by_dept'] = lm.get('plan_by_dept')
            if 'fact_by_dept' in lm:
                tile['fact_by_dept'] = lm.get('fact_by_dept')
            if 'production_plan_rows' in lm:
                tile['production_plan_rows'] = lm.get('production_plan_rows')
            if 'project_deviation_rows' in lm:
                tile['project_deviation_rows'] = lm.get('project_deviation_rows')
            if 'max_allowed_delay_workdays' in lm:
                tile['max_allowed_delay_workdays'] = lm.get('max_allowed_delay_workdays')
            if kpi.get('kpi_id') in {'QD-M1', 'QD-M5', 'QD-M8'}:
                if 'departments' in lm:
                    tile['departments'] = lm.get('departments')
                if kpi.get('kpi_id') == 'QD-M8' and 'kinds' in lm:
                    tile['kinds'] = lm.get('kinds')
                if lm.get('significant') is not None:
                    tile['significant'] = lm.get('significant')
            if kpi.get('kpi_id') in {'QD-M1', 'QD-M5', 'QD-M6', 'QD-M8', 'QD-M9', 'QD-M10', 'QD-Q1'}:
                if lm.get('kpi_pct') is not None:
                    tile['kpi_pct'] = lm.get('kpi_pct')
                    tile['color'] = _qualdir_kpi_views.rag_plan_fact_pct(float(lm['kpi_pct']))
            if _is_gspp_m1_tile(kpi) or _is_gspp_m2_tile(kpi):
                if lm.get('kpi_pct') is not None:
                    tile['kpi_pct'] = lm.get('kpi_pct')
                    tile['color'] = _gspp_kpi_views.rag_gspp_m1_m2_pct(float(lm['kpi_pct']))
            elif _gspp_kpi_views.gspp_q4_kpi_id_matches(_kid_tile):
                if lm.get('kpi_pct') is not None:
                    tile['kpi_pct'] = lm.get('kpi_pct')
                    tile['color'] = _gspp_kpi_views.rag_gspp_q4_pct(float(lm['kpi_pct']))
            elif _is_servhead_higher_better_tile(kpi):
                if lm.get('kpi_pct') is not None:
                    tile['kpi_pct'] = lm.get('kpi_pct')
                    tile['color'] = _servhead_kpi_views.rag_servhead_m1_pct(float(lm['kpi_pct']))
            elif _is_servhead_lower_better_tile(kpi):
                if lm.get('kpi_pct') is not None:
                    tile['kpi_pct'] = lm.get('kpi_pct')
                    tile['color'] = _servhead_kpi_views.rag_servhead_lower_better_pct(float(lm['kpi_pct']))
            elif _kid_tile in _devdir_kpi_views.DEVDIR_PLAN_FACT_COLOR_IDS:
                sync_pct = _devdir_kpi_views.kpi_pct_from_plan_fact(
                    lm.get('plan'), lm.get('fact'),
                )
                if sync_pct is not None:
                    tile['kpi_pct'] = sync_pct
                    tile['color'] = _devdir_kpi_views.rag_devdir_plan_fact_pct(sync_pct)
                    tile['status_color'] = tile['color']
                elif lm.get('kpi_pct') is not None:
                    tile['kpi_pct'] = lm.get('kpi_pct')
                    tile['color'] = _devdir_kpi_views.rag_devdir_plan_fact_pct(float(lm['kpi_pct']))
                    tile['status_color'] = tile['color']
                tile['pct_higher_is_better'] = True
                tile['rag_direction'] = 'higher_better'
            elif _kid_tile in (
                _autoit_kpi_views.AUTOIT_FOT_LIMIT_KPI_IDS
                | _autoit_kpi_views.AUTOIT_BUDGET_LIMIT_KPI_IDS
                | _c1auto_kpi_views.C1AUTO_FOT_LIMIT_KPI_IDS
                | _c1auto_kpi_views.C1AUTO_BUDGET_LIMIT_KPI_IDS
                | _sup_kpi_views.SUP_FOT_LIMIT_KPI_IDS
                | _sup_kpi_views.SUP_BUDGET_LIMIT_KPI_IDS
            ):
                try:
                    p_lm = float(lm['plan']) if lm.get('plan') is not None else None
                    f_lm = float(lm['fact']) if lm.get('fact') is not None else None
                except (TypeError, ValueError):
                    p_lm, f_lm = None, None
                if p_lm is not None and f_lm is not None and p_lm > 0:
                    sync_pct = round(f_lm / p_lm * 100, 1)
                    tile['kpi_pct'] = sync_pct
                    tile['color'] = _rag_td_m4_limit(sync_pct)
                    tile['status_color'] = tile['color']
                elif lm.get('kpi_pct') is not None:
                    sync_pct = float(lm['kpi_pct'])
                    tile['kpi_pct'] = sync_pct
                    tile['color'] = _rag_td_m4_limit(sync_pct)
                    tile['status_color'] = tile['color']
                tile['pct_lower_is_better'] = True
                tile['rag_direction'] = 'lower_better'
                # Не отдавать на плитку месяцы после опоры: фронт берёт последний
                # ненулевой факт и смешивает его с цветом закрытого месяца.
                md_tile = tile.get('monthly_data') or []
                if md_tile and isinstance(lm, dict) and lm.get('month') is not None:
                    try:
                        cut_y = int(lm.get('year') or tile_lm_y)
                        cut_m = int(lm['month'])
                    except (TypeError, ValueError):
                        cut_y, cut_m = tile_lm_y, tile_lm_m
                    trimmed_md: list[dict] = []
                    for row in md_tile:
                        if not isinstance(row, dict):
                            continue
                        try:
                            ry = int(row['year']) if row.get('year') is not None else cut_y
                            rm = int(row['month'])
                        except (KeyError, TypeError, ValueError):
                            continue
                        if (ry, rm) > (cut_y, cut_m):
                            continue
                        try:
                            p_r = float(row['plan']) if row.get('plan') is not None else None
                            f_r = float(row['fact']) if row.get('fact') is not None else None
                        except (TypeError, ValueError):
                            p_r, f_r = None, None
                        row_out = dict(row)
                        if p_r is not None and f_r is not None and p_r > 0:
                            pct_r = round(f_r / p_r * 100, 1)
                            row_out['kpi_pct'] = pct_r
                            row_out['color'] = _rag_td_m4_limit(pct_r)
                        trimmed_md.append(row_out)
                    if trimmed_md:
                        tile['monthly_data'] = trimmed_md
            if kpi.get('kpi_id') in _qualdir_kpi_views.OTK_INCOMING_TILE_IDS:
                for extra_key in ('in_work_today', 'rejected_items_count'):
                    if extra_key in lm:
                        tile[extra_key] = lm.get(extra_key)
            if kpi.get('kpi_id') == 'QD-M7':
                for extra_key in ('accepted_to_work_today', 'checked_otk_today'):
                    if extra_key in lm:
                        tile[extra_key] = lm.get(extra_key)
        # Не подменять tile['monthly_data'] сырым entry: _build_tile_item уже положил
        # нормализованные строки (QD-Q2 — пересчёт kpi_pct); иначе фронт может снова
        # окрасить плитку по «сырым» kpi_pct и правилу «чем выше % — тем лучше».

        if kpi.get('kpi_id') == 'QD-Q2':
            p_fin = tile.get('plan')
            f_fin = tile.get('fact')
            tpct = _qd_q2_kpi_pct(p_fin, f_fin)
            if tpct is not None:
                tile['kpi_pct'] = tpct
            tile['color'] = _rag_lower_turnover(float(tpct) if tpct is not None else None)
            # Подсказка фронту: kpi_pct = факт/план×100, зелёный при <90%, жёлтый до 100%.
            tile['pct_lower_is_better'] = True

        if kpi.get('kpi_id') == 'QD-Q1':
            p_fin = tile.get('plan')
            f_fin = tile.get('fact')
            tpct = _qd_q2_kpi_pct(p_fin, f_fin)
            if tpct is not None:
                tile['kpi_pct'] = tpct
            tile['color'] = _rag_higher_better(float(tpct) if tpct is not None else None)

        if kpi.get('kpi_id') in {'OD-M3.1', 'OD-M3.2'}:
            tpct = _budget_fact_div_plan_pct({'last_full_month_row': tile})
            if tpct is not None:
                tile['kpi_pct'] = tpct
            tile['color'] = _rag_budget_fact_div_plan(float(tpct) if tpct is not None else None)
            tile['pct_lower_is_better'] = True

        logistics_views.apply_tile_overrides(kpi, tile)
        logistics_views.apply_tile_value_overrides(kpi, tile, entry)

        if kpi.get('kpi_id') in {
            'OD-M1', 'OD-M3.1', 'OD-M3.2',
            'PD-M1.1', 'PD-M1.1.M', 'PD-M1.1.W', 'PD-M1.1.T',
            'PD-M3.B1', 'PD-M3.B2', 'PD-M3.F1', 'PD-M3.F2',
            'PD-M3.1', 'PD-M3.2', 'METD-M3.B', 'METD-M3.F',
        }:
            tile['unit'] = 'руб.'
        elif (
            kpi.get('kpi_id') in {'PD-M1.2', 'PD-M1.2.M', 'PD-M1.2.W', 'PD-M1.2.T'}
            or _kid_tile in {'HRD-M1', 'METD-M1', 'МЕТ-M1'}
        ):
            tile['unit'] = 'шт.'
        elif _kid_tile == 'HRD-M2':
            tile['unit'] = 'руб.'
        elif _kid_tile == 'HRD-M3':
            tile['unit'] = 'руб.'
        elif _kid_tile in {'HRD-M4', 'HRD-Q4'}:
            tile['unit'] = '%'
        elif _kid_tile == 'METD-Q2':
            tile['unit'] = '%'
        elif kpi.get('kpi_id') == 'KD-M11':
            tile['unit'] = 'чел.'
        elif kpi.get('kpi_id') in {'OD-Q2', 'PD-Q2.1', 'PD-Q2.2'}:
            tile['unit'] = 'чел.'
        elif _kid_tile == 'TD-Q2':
            tile['unit'] = 'чел.'
        elif _is_gspp_q5_tile(kpi):
            tile['unit'] = 'чел.'
        elif _is_turnover_style_tile(kpi):
            tile['unit'] = '%'
            # Текучесть: рост факта над планом — плохо (бейдж/RAG на фронте).
            tile['pct_lower_is_better'] = True
            tile['rag_direction'] = 'lower_better'
            if _kid_tile in _c1auto_kpi_views.C1AUTO_TURNOVER_KPI_IDS | {'IT-Q2'}:
                tile['period'] = 'ежемесячно'
                tile['frequency'] = 'ежемесячно'
                if entry.get('data_granularity') == 'monthly':
                    tile['data_granularity'] = 'monthly'
        elif kpi.get('kpi_id') in {'PD-M2', 'GK-M1', 'GK-Q1'} or _kid_tile in {'MET-Q4-1', 'METD-Q1', 'METD-Q3'}:
            tile['unit'] = 'шт.'
        elif kpi.get('kpi_id') in {'TD-M1', 'TD-M2', 'TD-Q1', 'QD-Q1', 'QD-M6', 'QD-M7', 'QD-M8', 'QD-M9', 'QD-M10'}:
            tile['unit'] = 'шт.'
        elif _gspp_kpi_views.gspp_q4_kpi_id_matches(_kid_tile):
            tile['unit'] = 'шт.'
        elif _is_gspp_m1_tile(kpi) or _is_gspp_m2_tile(kpi):
            tile['unit'] = 'шт.'
        elif _is_servhead_tile(kpi):
            tile['unit'] = 'шт.'
        elif _is_gspp_m3_tile(kpi) or _is_gspp_m5_tile(kpi):
            tile['unit'] = 'руб.'
        elif _kid_tile in _devdir_kpi_views.DEVDIR_PIECE_UNIT_KPI_IDS:
            tile['unit'] = 'шт.'
        elif _kid_tile in _autoit_kpi_views.AUTOIT_SLA_KPI_IDS | _c1auto_kpi_views.C1AUTO_SLA_KPI_IDS:
            # План/факт — число заявок (шт.); % выполнения — в kpi_pct, не в unit.
            tile['unit'] = 'шт.'
        elif _kid_tile in _autoit_kpi_views.AUTOIT_RUB_KPI_IDS | _c1auto_kpi_views.C1AUTO_RUB_KPI_IDS:
            tile['unit'] = 'руб.'
        elif _kid_tile in techdir_dashboard.TECHDIR_RUB_UNIT_KPI_IDS | _qualdir_kpi_views.RUB_UNIT_KPI_IDS | _devdir_kpi_views.DEVDIR_RUB_UNIT_KPI_IDS:
            tile['unit'] = 'руб.'

        if _kid_tile == 'TD-Q2' or _is_gspp_q5_tile(kpi):
            tile['unit'] = '%'

        period_label = _plan_fact_period_label_from_kpi_period(entry.get('kpi_period'))
        if period_label:
            tile['plan_fact_period_label'] = period_label

        if include_debug and entry.get('debug') is not None:
            tile['debug'] = entry['debug']

        _devdir_kpi_views.sync_devdir_piece_tile_color(tile)
        _qualdir_kpi_views.sync_qualdir_plan_fact_tile_color(tile)
        _qualdir_kpi_views.clear_qualdir_fact_only_tile_rag(tile)

        plitki_items.append(tile)

    grafiki = {}
    for chart_kpi in charts_meta:
        cid = chart_kpi['kpi_id']
        grafiki[cid] = {
            'kpi_id': cid,
            'name': chart_kpi['name'],
            'periodicity': _period_label_from_kpi(chart_kpi),
            'chart_type': chart_kpi.get('chart_type', ''),
            'chart_type_label': chart_kpi.get('chart_type_label', ''),
            'formula': chart_kpi.get('formula'),
        }

    if str(dept).strip().lower() == 'операционный директор':
        grafiki.update(_build_opdir_charts(tiles_meta, entries_by_id, ref_y, ref_m))
    if techdir_dashboard.is_techdir_department(dept):
        techdir_tile_values = {
            item['kpi_id']: {
                'plan': item.get('plan'),
                'fact': item.get('fact'),
                'kpi_pct': item.get('kpi_pct'),
            }
            for item in plitki_items
            if item.get('kpi_id') in techdir_dashboard.TECHDIR_CHART_TILE_IDS
        }
        grafiki.update(
            techdir_dashboard.build_charts(tiles_meta, entries_by_id, techdir_tile_values, ref_y, ref_m),
        )
    if logistics_views.is_logistics_head_department(dept):
        grafiki.update(logistics_views.build_charts(tiles_meta, entries_by_id, ref_y, ref_m))
    if _is_prod_deputy_department(dept):
        grafiki.update(_build_prod_deputy_charts(entries_by_id, ref_y, ref_m))
    if _is_qualdir_department(dept):
        qualdir_tile_values = {
            item['kpi_id']: {
                'plan': item.get('plan'),
                'fact': item.get('fact'),
                'kpi_pct': item.get('kpi_pct'),
            }
            for item in plitki_items
            if item.get('kpi_id') in {'QD-M1', 'QD-Q1'}
        }
    if _is_qualdir_dashboard(dept, all_kpis):
        grafiki.update(
            _build_qualdir_charts(tiles_meta, entries_by_id, ref_y, ref_m),
        )
        techdir_dashboard.strip_external_orders_budget_from_grafiki(grafiki)
    if _is_devdir_department(dept):
        grafiki.update(_build_devdir_charts(entries_by_id, ref_y, ref_m))
    if _is_gspp_department(dept):
        grafiki.update(_build_gspp_charts(tiles_meta, entries_by_id, ref_y, ref_m))
    if _is_chief_metrolog_department(dept):
        grafiki.update(_build_chief_metrolog_charts(entries_by_id, ref_y, ref_m))
    if _is_sup_department(dept):
        grafiki.update(_build_sup_charts(entries_by_id, ref_y, ref_m))
    if _is_autoit_department(dept):
        grafiki.update(_build_autoit_charts(entries_by_id, ref_y, ref_m))
    if _is_c1auto_department(dept):
        grafiki.update(_build_c1auto_charts(entries_by_id, ref_y, ref_m))
    if _servhead_kpi_views.is_servhead_department(dept):
        grafiki.update(_build_servhead_charts(entries_by_id, ref_y, ref_m))
    month_names = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
    }

    tablitsy = {}

    def _period_end_month_for_snapshot() -> int:
        mode = (aggregation_mode or '').strip().lower()
        if mode == 'quarter':
            quarters = [
                q for q in (selected_quarters or [])
                if isinstance(q, int) and 1 <= q <= 4
            ]
            quarter = max(quarters) if quarters else ((ref_m - 1) // 3) + 1
            return max(1, min(ref_m, quarter * 3))
        return ref_m

    def _snapshot_date_for_selected_period() -> date:
        snap_m = _period_end_month_for_snapshot()
        snap = date(ref_y, snap_m, calendar.monthrange(ref_y, snap_m)[1])
        today = date.today()
        if snap > today:
            snap = today
        return snap

    include_generic_tables = (
        not techdir_dashboard.is_techdir_department(dept)
        and not _is_prod_deputy_department(dept)
        and not _is_chief_constructor_department(dept)
        and not _is_chief_metrolog_department(dept)
        and not _is_devdir_department(dept)
        and not _is_gspp_department(dept)
        and not _is_sup_department(dept)
        and not _is_qualdir_dashboard(dept, all_kpis)
        and not _is_autoit_department(dept)
        and not _is_c1auto_department(dept)
        and not _servhead_kpi_views.is_servhead_department(dept)
    )

    if include_generic_tables:
        claims_table_key = "KD-T-CLAIMS"
        claims_table_name = "Активные претензии"
        claims_table_description = "Претензии из 1С со статусами: Зарегистрирована, Обрабатывается, На контроле"
        try:
            if logistics_views.is_logistics_head_department(dept):
                from .logistics_claims import fetch_logistics_claims_for_month

                rows = fetch_logistics_claims_for_month(ref_y, ref_m)
                claims_table_key = "LOG-T-CLAIMS"
                claims_table_name = "Претензии поставщиков"
                claims_table_description = (
                    "Строки табличной части Несоответствия из "
                    "Document_ТД_АктОНесоответствиеПриборовИКомплектующих: "
                    "КатегорияПоПричинеВозникновения = Поставщик, "
                    "ВозможностьУстраненияНесоответствия = Окончательный"
                )
            else:
                rows = _fetch_claims_rows_for_department(ref_y, ref_m, dept)
        except Exception:
            rows = []

        try:
            lawsuit_rows = _fetch_lawsuits_rows_for_department(ref_y, ref_m, dept)
        except Exception:
            lawsuit_rows = []

        tablitsy.update({
            claims_table_key: {
                "name": claims_table_name,
                "periodicity": "ежемесячно",
                "description": claims_table_description,
                "period": {
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": month_names[ref_m],
                },
                "rows": rows,
            },
            "KD-T-LAWSUITS": {
                "name": f"Суды на {month_names[ref_m]} {ref_y}",
                "periodicity": "ежемесячно",
                "description": (
                    "Активные судебные споры и исковая работа из 1С "
                    "(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) "
                    "на конец выбранного месяца (статус ≠ Закрыта)"
                ),
                "period": {
                    "year": ref_y,
                    "month": ref_m,
                    "month_name": month_names[ref_m],
                },
                "columns": [
                    "Тип документа", "Контрагент", "Предмет спора",
                    "Роль ГК в споре", "Юр. лицо", "Подразделение",
                    "Дата SLA", "Краткое описание ситуации",
                    "Сумма требований, руб.",
                ],
                "rows": lawsuit_rows,
            },
        })

        if logistics_views.is_logistics_head_department(dept):
            try:
                supplier_dz_date = _snapshot_date_for_selected_period()
                supplier_dz_detail = cache_manager.locked_call(
                    f"log_supplier_dz_detail_{supplier_dz_date.isoformat()}",
                    calc_postavshchiki.get_supplier_dz_detail,
                    supplier_dz_date,
                )
            except Exception:
                supplier_dz_detail = {"rows": [], "total_dolg_regl": 0, "na_datu": ""}
                supplier_dz_date = _snapshot_date_for_selected_period()
            tablitsy["LOG-T-SUPPLIER-DZ"] = {
                "name": f"Дебиторская задолженность на {supplier_dz_detail.get('na_datu') or supplier_dz_date.isoformat()}",
                "periodicity": "ежемесячно",
                "description": (
                    "Поставщики с нашим долгом > 0 (ДолгРеглBalance) на дату среза; "
                    "предоплата не включается; за два полных календарных месяца до даты среза "
                    "оборот по ДолгРегл = 0 и остаток не менялся"
                ),
                "period": {
                    "year": ref_y,
                    "month": supplier_dz_date.month,
                    "month_name": month_names.get(supplier_dz_date.month, str(supplier_dz_date.month)),
                    "as_of_date": supplier_dz_detail.get("na_datu") or supplier_dz_date.isoformat(),
                    "aggregation_mode": aggregation_mode or "current",
                },
                "total": supplier_dz_detail.get("total_dolg_regl", 0),
                "columns": ["№ объекта расчетов", "Дата", "Объект расчетов", "Поставщик", "Сумма"],
                "verification": supplier_dz_detail.get("verification") or {},
                "query_protocol": supplier_dz_detail.get("query_protocol") or {},
                "rows": supplier_dz_detail.get("rows") or [],
            }

    if _is_qualdir_dashboard(dept, all_kpis):
        qualdir_tables.merge_qualdir_brak_tables(tablitsy, year=ref_y, month=ref_m)

    if techdir_dashboard.is_techdir_department(dept):
        techdir_dashboard.merge_deviation_tables(tablitsy, ref_y, ref_m)

    if _is_devdir_department(dept):
        try:
            rd_m3_1_period = (entries_by_id.get('RD-M3-1') or {}).get('kpi_period') or {}
            table_y, table_m = ref_y, ref_m
            if (
                isinstance(rd_m3_1_period, dict)
                and rd_m3_1_period.get('year') is not None
                and rd_m3_1_period.get('month') is not None
            ):
                table_y = int(rd_m3_1_period['year'])
                table_m = max(1, min(12, int(rd_m3_1_period['month'])))
            devdir_table = _devdir_turboproject_projects.get_projects_deviation_table(
                year=table_y,
                month=table_m,
            )
        except Exception:
            devdir_table = None
        # Таблицу отдаём всегда (даже пустую): иначе при пустом stale-кэше
        # фронт вообще не видит блок, хотя на плитке уже plan≠fact.
        if devdir_table:
            tablitsy['DEVDIR-T-PROJECTS-DEVIATIONS'] = devdir_table

        try:
            rd_m2_1_period = (entries_by_id.get('RD-M2-1') or {}).get('kpi_period') or {}
            ope_table_y, ope_table_m = ref_y, ref_m
            if (
                isinstance(rd_m2_1_period, dict)
                and rd_m2_1_period.get('year') is not None
                and rd_m2_1_period.get('month') is not None
            ):
                ope_table_y = int(rd_m2_1_period['year'])
                ope_table_m = max(1, min(12, int(rd_m2_1_period['month'])))
            ope_table = _devdir_turboproject_ope.get_ope_projects_deviation_table(
                year=ope_table_y,
                month=ope_table_m,
            )
        except Exception:
            ope_table = None
        if ope_table and (ope_table.get('rows') or []):
            tablitsy['DEVDIR-T-OPE-PROJECTS-DEVIATIONS'] = ope_table

    if _is_gspp_department(dept):
        _gspp_kpi_views.merge_gspp_tables_into_universal_payload(tablitsy, ref_y, ref_m)

    if _is_sup_department(dept):
        _sup_kpi_views.merge_sup_tables_into_universal_payload(tablitsy, entries_by_id, ref_y, ref_m)

    # servhead: SH-T1 вместо KD-T-CLAIMS (претензии по клиентам, не построчный журнал).
    if _servhead_kpi_views.is_servhead_department(dept):
        _servhead_kpi_views.merge_servhead_tables_into_universal_payload(tablitsy, ref_y, ref_m)

    if str(dept).strip().lower() == 'операционный директор':
        try:
            od_q1_table = techdir_projects.get_od_q1_deviation_table(month=ref_m, year=ref_y)
        except Exception:
            od_q1_table = None
        if od_q1_table:
            tablitsy['OD-T-Q1-DEVIATIONS'] = od_q1_table

    if _is_prod_deputy_department(dept) or 'PD-Q1' in entries_by_id:
        def _prod_claim_months() -> list[int]:
            mode = (aggregation_mode or '').strip().lower()
            if mode == 'ytd':
                return list(range(1, ref_m + 1))
            if mode == 'quarter':
                quarters = [
                    q for q in (selected_quarters or [])
                    if isinstance(q, int) and 1 <= q <= 4
                ]
                if not quarters:
                    quarters = [((ref_m - 1) // 3) + 1]
                months: list[int] = []
                for q in sorted(set(quarters)):
                    start_m = (q - 1) * 3 + 1
                    end_m = min(q * 3, ref_m)
                    months.extend(range(start_m, end_m + 1))
                return months
            return [ref_m]

        try:
            from .logistics_claims import fetch_production_claims_for_period

            claim_months = _prod_claim_months()
            prod_claims = fetch_production_claims_for_period(ref_y, claim_months)
        except Exception:
            prod_claims = []
            claim_months = [ref_m]
        tablitsy['PD-T-PROD-CLAIMS'] = {
            'name': 'Претензии на стороне производства',
            'periodicity': 'ежемесячно',
            'description': (
                'Строки табличной части Несоответствия из '
                'Document_ТД_АктОНесоответствиеПриборовИКомплектующих, где '
                'ПодразделениеВиновник = Производственный цех №1 или Производственный цех №2'
            ),
            'period': {
                'year': ref_y,
                'month': ref_m,
                'month_name': month_names[ref_m],
                'aggregation_mode': aggregation_mode or 'current',
                'months': claim_months,
            },
            'columns': [
                'Номер', 'Дата', 'Подразделение-виновник', 'Статус',
                'Номенклатура', 'Описание', 'Расчетное кол-во брака',
            ],
            'rows': prod_claims,
        }
        try:
            from . import calc_prod_deputy_projects

            pd_q1_table = calc_prod_deputy_projects.get_pd_q1_deviation_table(month=ref_m, year=ref_y)
        except Exception:
            pd_q1_table = None
        if pd_q1_table:
            tablitsy['PD-T-Q1-DEVIATIONS'] = pd_q1_table

    if _is_production_director_department(dept) or 'PD-Q3' in entries_by_id:
        try:
            from . import calc_prod_deputy_projects

            pd_q3_table = calc_prod_deputy_projects.get_pd_q3_improvement_table(month=ref_m, year=ref_y)
        except Exception:
            pd_q3_table = None
        if pd_q3_table:
            tablitsy['PD-T-Q3-IMPROVEMENTS'] = pd_q3_table

    if _is_chief_constructor_department(dept) or 'GK-M1' in entries_by_id:
        try:
            from . import calc_chief_constructor_projects

            gk_m1_table = calc_chief_constructor_projects.get_gk_m1_deviation_table(month=ref_m, year=ref_y)
        except Exception:
            gk_m1_table = None
        if gk_m1_table:
            tablitsy['GK-T-M1-DEVIATIONS'] = gk_m1_table

    if (
        not _is_chief_metrolog_department(dept)
        and ('METD-Q1' in entries_by_id or 'МЕТ-Q4-1' in entries_by_id)
    ):
        try:
            from . import calc_metrolog_projects

            metrolog_table = calc_metrolog_projects.get_metrolog_project_deviation_table(month=ref_m, year=ref_y)
        except Exception:
            metrolog_table = None
        if metrolog_table:
            # Reuse the already supported project-deviation table key so older loaded frontend code
            # renders metrologist project rows without waiting for a static JS refresh.
            tablitsy['GK-T-M1-DEVIATIONS'] = metrolog_table
        try:
            from . import calc_metrolog_turnover

            turnover_table = calc_metrolog_turnover.get_metrolog_turnover_table(year=ref_y, month=ref_m)
        except Exception:
            turnover_table = None
        if turnover_table:
            tablitsy['METD-T-Q2-TURNOVER'] = turnover_table
        try:
            from . import calc_metrolog_fot

            fot_table = calc_metrolog_fot.get_metrolog_fot_table(year=ref_y, month=ref_m)
        except Exception:
            fot_table = None
        if fot_table:
            tablitsy['METD-T-M3-FOT'] = fot_table
        try:
            from . import calc_metrolog_budget

            budget_table = calc_metrolog_budget.get_metrolog_budget_table(year=ref_y, month=ref_m)
        except Exception:
            budget_table = None
        if budget_table:
            tablitsy['METD-T-M3-BUDGET'] = budget_table
    if _is_chief_metrolog_department(dept):
        try:
            from .komdir_dashboard import _build_overdue_table

            tablitsy['KD-T-OVERDUE'] = _build_overdue_table(ref_y, ref_m, dept_guid=None)
        except Exception:
            pass
        try:
            from . import calc_metrolog_production_plan

            tablitsy['METD-T-M1-LATE-STAGES'] = calc_metrolog_production_plan.get_metrolog_late_stage_table(
                year=ref_y,
                month=ref_m,
            )
        except Exception:
            logger.exception("Не удалось построить таблицу просроченных этапов метролога")
    else:
        dept_protocol_tables.merge_protocol_overdue_table(tablitsy, dept, year=ref_y, month=ref_m)

    if _is_chief_metrolog_department(dept):
        _enrich_chief_metrolog_table_cache_metadata(tablitsy, ref_y, ref_m)
    if _is_prod_deputy_department(dept):
        _enrich_prod_deputy_table_cache_metadata(tablitsy, ref_y, ref_m)

    result = {
        'month': ref_m,
        'year': ref_y,
        'kpi_ref_month': ref_m,
        'Плитки': {'count': len(plitki_items), 'items': plitki_items},
        'Графики': grafiki,
        'Таблицы': tablitsy,
    }
    if gspp_memo_key:
        cache_manager.set_memoized_dashboard_payload(gspp_memo_key, result)
    if techdir_memo_key:
        cache_manager.set_memoized_dashboard_payload(techdir_memo_key, result)
    if qualdir_memo_key:
        cache_manager.set_memoized_dashboard_payload(qualdir_memo_key, result)
    if sup_memo_key:
        cache_manager.set_memoized_dashboard_payload(sup_memo_key, result)
    if autoit_memo_key:
        cache_manager.set_memoized_dashboard_payload(autoit_memo_key, result)
    if c1auto_memo_key:
        cache_manager.set_memoized_dashboard_payload(c1auto_memo_key, result)
    if servhead_memo_key:
        cache_manager.set_memoized_dashboard_payload(servhead_memo_key, result)
    if devdir_memo_key:
        cache_manager.set_memoized_dashboard_payload(devdir_memo_key, result)
        logger.info(
            "cache_manager: devdir dashboard built and memoized %s (%d tiles)",
            devdir_memo_key,
            len(plitki_items),
        )
    if dashboard_disk_key:
        cache_manager.save_dashboard_disk(dashboard_disk_key, result)
    return result


CHIEF_METROLOG_PAYLOAD_CACHE_VERSION = 2
PROD_DEPUTY_PAYLOAD_CACHE_VERSION = 1


CHIEF_METROLOG_TABLE_CACHE_KPI_IDS = {
    'KD-T-OVERDUE': 'KD-T-OVERDUE',
    'METD-T-M1-LATE-STAGES': 'METD-M1',
    'GK-T-M1-DEVIATIONS': 'METD-Q1',
    'METD-T-Q2-TURNOVER': 'METD-Q2',
    'METD-T-M3-FOT': 'METD-M3.F',
    'METD-T-M3-BUDGET': 'METD-M3.B',
}


def _enrich_chief_metrolog_table_cache_metadata(tables: dict, ref_y: int, ref_m: int) -> None:
    if not isinstance(tables, dict):
        return
    for table_key, table in tables.items():
        if not isinstance(table, dict):
            continue
        kpi_id = CHIEF_METROLOG_TABLE_CACHE_KPI_IDS.get(str(table_key).strip())
        if not kpi_id:
            continue
        table['cache_refresh_kpi_id'] = kpi_id
        table['cache_updated_at'] = _tile_cache_updated_at(kpi_id, ref_y, ref_m)
        if cache_manager.is_any_cache_path_refreshing(
            _manual_tile_refresh_cache_files(kpi_id, ref_y, ref_m),
        ):
            table['cache_refresh_status'] = 'running'


def _chief_metrolog_payload_cache_path(ref_y: int, ref_m: int) -> Path:
    cache_manager.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return cache_manager.CACHE_DIR / f"chief_metrolog_payload_{int(ref_y)}_{int(ref_m):02d}.json"


def _prod_deputy_payload_cache_path(ref_y: int, ref_m: int) -> Path:
    cache_manager.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return cache_manager.CACHE_DIR / f"prod_deputy_payload_{int(ref_y)}_{int(ref_m):02d}.json"


def _mark_payload_cache_refreshing(payload: dict) -> dict:
    payload = dict(payload)
    payload['cache_refresh_status'] = 'running'
    for tile in (payload.get('Плитки') or {}).get('items') or []:
        if isinstance(tile, dict):
            tile['cache_refresh_status'] = 'running'
    for table in (payload.get('Таблицы') or {}).values():
        if isinstance(table, dict):
            table['cache_refresh_status'] = 'running'
    return payload


def _payload_without_cache_refresh_status(payload: dict) -> dict:
    """Убрать transient running-статусы перед сохранением snapshot в файл."""
    clean = copy.deepcopy(payload)
    if isinstance(clean, dict):
        clean.pop('cache_refresh_status', None)
        for tile in (clean.get('Плитки') or {}).get('items') or []:
            if isinstance(tile, dict):
                tile.pop('cache_refresh_status', None)
        for table in (clean.get('Таблицы') or {}).values():
            if isinstance(table, dict):
                table.pop('cache_refresh_status', None)
    return clean


def _unwrap_chief_metrolog_payload_cache(raw: dict) -> dict:
    if (
        isinstance(raw, dict)
        and isinstance(raw.get('payload'), dict)
    ):
        # locked_call может вернуть stale snapshot старой версии, пока новый payload
        # пересчитывается в фоне. Для ответа разворачиваем его, но fresh-loader
        # всё равно принимает только актуальную CHIEF_METROLOG_PAYLOAD_CACHE_VERSION.
        return _mark_payload_cache_refreshing(raw['payload'])
    return raw


def _prod_deputy_cache_files_for_kpi(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    kid = _normalize_dashboard_kpi_id(kpi_id)
    cd = cache_manager.CACHE_DIR
    paths: list[Path] = []
    if kid.startswith('PD-M1.1'):
        from . import calc_prod_deputy_output
        paths.append(calc_prod_deputy_output.cache_path('pc1', ref_y, ref_m))
    elif kid.startswith('PD-M1.2'):
        from . import calc_prod_deputy_output
        paths.append(calc_prod_deputy_output.cache_path('pc2', ref_y, ref_m))
    elif kid == 'PD-M2':
        paths.append(cd / f"otif_vypusk_prod_monthly_{ref_y}_{ref_m:02d}.json")
    elif kid == 'PD-M3.B1':
        from .calc_prod_deputy_pc_common import cache_path as pc_cache_path
        paths.append(pc_cache_path('budget', 'pc1', ref_y, ref_m))
    elif kid == 'PD-M3.B2':
        from .calc_prod_deputy_pc_common import cache_path as pc_cache_path
        paths.append(pc_cache_path('budget', 'pc2', ref_y, ref_m))
    elif kid == 'PD-M3.F1':
        from .calc_prod_deputy_pc_common import cache_path as pc_cache_path
        paths.append(pc_cache_path('fot', 'pc1', ref_y, ref_m))
    elif kid == 'PD-M3.F2':
        from .calc_prod_deputy_pc_common import cache_path as pc_cache_path
        paths.append(pc_cache_path('fot', 'pc2', ref_y, ref_m))
    elif kid == 'PD-Q1':
        from . import calc_prod_deputy_projects
        paths.append(calc_prod_deputy_projects.CACHE_PATH)
    elif kid == 'PD-Q3':
        from . import calc_prod_deputy_projects
        paths.append(calc_prod_deputy_projects.CACHE_PATH)
    elif kid == 'PD-Q2.1':
        from . import calc_prod_deputy_turnover
        paths.append(calc_prod_deputy_turnover.cache_path('pc1', ref_y, ref_m))
    elif kid == 'PD-Q2.2':
        from . import calc_prod_deputy_turnover
        paths.append(calc_prod_deputy_turnover.cache_path('pc2', ref_y, ref_m))
    return paths


PROD_DEPUTY_TABLE_CACHE_KPI_IDS = {
    'PD-T-Q1-DEVIATIONS': 'PD-Q1',
    'PD-T-Q3-IMPROVEMENTS': 'PD-Q3',
}


def _enrich_prod_deputy_table_cache_metadata(tables: dict, ref_y: int, ref_m: int) -> None:
    if not isinstance(tables, dict):
        return
    for table_key, table in tables.items():
        if not isinstance(table, dict):
            continue
        kpi_id = PROD_DEPUTY_TABLE_CACHE_KPI_IDS.get(str(table_key).strip())
        if not kpi_id:
            continue
        table['cache_refresh_kpi_id'] = kpi_id
        table['cache_updated_at'] = _tile_cache_updated_at(kpi_id, ref_y, ref_m)
        if cache_manager.is_any_cache_path_refreshing(
            _prod_deputy_cache_files_for_kpi(kpi_id, ref_y, ref_m),
        ):
            table['cache_refresh_status'] = 'running'


def _prod_deputy_payload_with_active_refresh_status(payload: dict, ref_y: int, ref_m: int) -> dict:
    if not isinstance(payload, dict):
        return payload
    cache_path = _prod_deputy_payload_cache_path(ref_y, ref_m)
    payload_refreshing = cache_manager.is_cache_path_refreshing(cache_path)
    next_payload = dict(payload)
    any_refreshing = payload_refreshing
    for tile in (next_payload.get('Плитки') or {}).get('items') or []:
        if not isinstance(tile, dict):
            continue
        kpi_id = str(tile.get('kpi_id') or '').strip()
        tile_refreshing = payload_refreshing or cache_manager.is_any_cache_path_refreshing(
            _prod_deputy_cache_files_for_kpi(kpi_id, ref_y, ref_m),
        )
        if tile_refreshing:
            tile['cache_refresh_status'] = 'running'
            any_refreshing = True
    if payload_refreshing:
        for table in (next_payload.get('Таблицы') or {}).values():
            if isinstance(table, dict):
                table['cache_refresh_status'] = 'running'
    _enrich_prod_deputy_table_cache_metadata(next_payload.get('Таблицы') or {}, ref_y, ref_m)
    if any_refreshing:
        next_payload['cache_refresh_status'] = 'running'
    return next_payload


def _load_fresh_chief_metrolog_payload_cache(ref_y: int, ref_m: int) -> dict | None:
    path = _chief_metrolog_payload_cache_path(ref_y, ref_m)
    if not path.exists():
        return None
    try:
        with path.open('r', encoding='utf-8') as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if (
        raw.get('cache_version') == CHIEF_METROLOG_PAYLOAD_CACHE_VERSION
        and raw.get('cache_date') == date.today().isoformat()
        and isinstance(raw.get('payload'), dict)
    ):
        return raw['payload']
    return None


def _load_fresh_prod_deputy_payload_cache(ref_y: int, ref_m: int) -> dict | None:
    path = _prod_deputy_payload_cache_path(ref_y, ref_m)
    if not path.exists():
        return None
    try:
        with path.open('r', encoding='utf-8') as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if (
        raw.get('cache_version') == PROD_DEPUTY_PAYLOAD_CACHE_VERSION
        and raw.get('cache_date') == date.today().isoformat()
        and isinstance(raw.get('payload'), dict)
    ):
        return _prod_deputy_payload_with_active_refresh_status(raw['payload'], ref_y, ref_m)
    return None


def _save_chief_metrolog_payload_cache(ref_y: int, ref_m: int, payload: dict) -> None:
    try:
        clean_payload = _payload_without_cache_refresh_status(payload)
        _chief_metrolog_payload_cache_path(ref_y, ref_m).write_text(
            json.dumps(
                {
                    'cache_version': CHIEF_METROLOG_PAYLOAD_CACHE_VERSION,
                    'cache_date': date.today().isoformat(),
                    'payload': clean_payload,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding='utf-8',
        )
    except OSError:
        logger.exception("Не удалось сохранить snapshot payload главного метролога")


def _save_prod_deputy_payload_cache(ref_y: int, ref_m: int, payload: dict) -> None:
    try:
        clean_payload = _payload_without_cache_refresh_status(payload)
        _prod_deputy_payload_cache_path(ref_y, ref_m).write_text(
            json.dumps(
                {
                    'cache_version': PROD_DEPUTY_PAYLOAD_CACHE_VERSION,
                    'cache_date': date.today().isoformat(),
                    'payload': clean_payload,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding='utf-8',
        )
    except OSError:
        logger.exception("Не удалось сохранить snapshot payload заместителя операционного директора")


def _chief_metrolog_ref_period(month: int | None, year: int | None) -> tuple[int, int]:
    today = date.today()
    if year is not None and month is not None:
        return int(year), max(1, min(12, int(month)))
    if year is not None:
        ref_y = int(year)
        return ref_y, today.month if ref_y == today.year else 12
    if month is not None:
        return today.year, max(1, min(12, int(month)))
    return today.year, today.month


def _prod_deputy_ref_period(month: int | None, year: int | None) -> tuple[int, int]:
    today = date.today()
    if year is not None and month is not None:
        return int(year), max(1, min(12, int(month)))
    if year is not None:
        ref_y = int(year)
        return ref_y, today.month if ref_y == today.year else 12
    if month is not None:
        return today.year, max(1, min(12, int(month)))
    return today.year, today.month


def _build_chief_metrolog_payload_fresh(
    requested_dept: str,
    kpis: list[dict],
    *,
    month: int | None = None,
    year: int | None = None,
    include_debug: bool = False,
    aggregation_mode: str | None = None,
    selected_quarters: list[int] | None = None,
) -> dict:
    payload = _build_universal_payload(
        requested_dept,
        kpis,
        month=month,
        year=year,
        include_debug=include_debug,
        aggregation_mode=aggregation_mode,
        selected_quarters=selected_quarters,
    )
    ref_y = int(payload.get('year') or _chief_metrolog_ref_period(month, year)[0])
    ref_m = int(payload.get('month') or _chief_metrolog_ref_period(month, year)[1])
    clean_payload = _payload_without_cache_refresh_status(payload)
    _save_chief_metrolog_payload_cache(ref_y, ref_m, clean_payload)
    return clean_payload


def _build_prod_deputy_payload_fresh(
    requested_dept: str,
    kpis: list[dict],
    *,
    month: int | None = None,
    year: int | None = None,
    include_debug: bool = False,
    aggregation_mode: str | None = None,
    selected_quarters: list[int] | None = None,
) -> dict:
    payload = _build_universal_payload(
        requested_dept,
        kpis,
        month=month,
        year=year,
        include_debug=include_debug,
        aggregation_mode=aggregation_mode,
        selected_quarters=selected_quarters,
    )
    ref_y = int(payload.get('year') or _prod_deputy_ref_period(month, year)[0])
    ref_m = int(payload.get('month') or _prod_deputy_ref_period(month, year)[1])
    clean_payload = _payload_without_cache_refresh_status(payload)
    _save_prod_deputy_payload_cache(ref_y, ref_m, clean_payload)
    return clean_payload


def _build_prod_deputy_payload(
    requested_dept: str,
    kpis: list[dict],
    *,
    month: int | None = None,
    year: int | None = None,
    include_debug: bool = False,
    aggregation_mode: str | None = None,
    selected_quarters: list[int] | None = None,
) -> dict:
    ref_y, ref_m = _prod_deputy_ref_period(month, year)
    cache_key = f"prod_deputy_payload_{ref_y}_{ref_m:02d}"
    cache_path = _prod_deputy_payload_cache_path(ref_y, ref_m)
    cache_manager.register_cache_path(cache_key, cache_path)
    if not cache_manager.is_force_compute_context():
        cached_payload = _load_fresh_prod_deputy_payload_cache(ref_y, ref_m)
        if cached_payload is not None:
            return cached_payload
    raw = cache_manager.locked_call(
        cache_key,
        _build_prod_deputy_payload_fresh,
        requested_dept,
        kpis,
        month=month,
        year=year,
        include_debug=include_debug,
        aggregation_mode=aggregation_mode,
        selected_quarters=selected_quarters,
    )
    if (
        isinstance(raw, dict)
        and raw.get('cache_version') == PROD_DEPUTY_PAYLOAD_CACHE_VERSION
        and isinstance(raw.get('payload'), dict)
    ):
        return _mark_payload_cache_refreshing(raw['payload'])
    return raw


def _build_chief_metrolog_payload(
    requested_dept: str,
    kpis: list[dict],
    *,
    month: int | None = None,
    year: int | None = None,
    include_debug: bool = False,
    aggregation_mode: str | None = None,
    selected_quarters: list[int] | None = None,
) -> dict:
    ref_y, ref_m = _chief_metrolog_ref_period(month, year)
    cache_key = f"chief_metrolog_payload_{ref_y}_{ref_m:02d}"
    cache_path = _chief_metrolog_payload_cache_path(ref_y, ref_m)
    cache_manager.register_cache_path(cache_key, cache_path)
    if not cache_manager.is_force_compute_context():
        cached_payload = _load_fresh_chief_metrolog_payload_cache(ref_y, ref_m)
        if cached_payload is not None:
            return cached_payload
    raw = cache_manager.locked_call(
        cache_key,
        _build_chief_metrolog_payload_fresh,
        requested_dept,
        kpis,
        month=month,
        year=year,
        include_debug=include_debug,
        aggregation_mode=aggregation_mode,
        selected_quarters=selected_quarters,
    )
    return _unwrap_chief_metrolog_payload_cache(raw)


MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _dept_guid_for_universal(dept_key: str | None) -> str | None:
    """GUID 1С для KPI дочернего отдела; для агрегата комдира — None."""
    if not dept_key:
        return None
    ck = commercial_kpi_key(dept_key)
    if not isinstance(ck, str):
        return None
    return dept_guid_for_kpi_key(ck)


def _normalize_commercial_context_department(department: str | None) -> tuple[str, str | None]:
    raw = str(department or '').strip()
    if not raw:
        return '', None
    ck = commercial_kpi_key(raw)
    if isinstance(ck, str):
        return ck, dept_guid_for_kpi_key(ck)
    lowered = raw.lower()
    if 'коммерческий' in lowered and 'директор' in lowered:
        return 'коммерческий директор', None
    return raw, None


def _psd_claim_order_sum(row: dict) -> float:
    try:
        return float(row.get('order_sum') or 0)
    except (TypeError, ValueError):
        return 0.0


def _filter_psd_claim_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if row.get('reason_key') == PSD_CLAIM_REASON_PRETENSION_KEY
        and _psd_claim_order_sum(row) > PSD_CLAIM_MIN_ORDER_SUM
    ]


def _fetch_claims_rows_for_department(
    year: int,
    month: int,
    department: str,
    *,
    psd_filters: bool = False,
) -> list[dict]:
    from .komdir_claims import VED_DEPT_KEY, fetch_claims_for_month, _is_uztransgaz_partner

    canonical_dept, dept_guid = _normalize_commercial_context_department(department)
    include_all = (
        not isinstance(commercial_kpi_key(canonical_dept), str)
        and dept_guid is None
    ) or dept_guid == VED_DEPT_KEY
    rows = fetch_claims_for_month(year, month, include_all=include_all)
    if dept_guid:
        rows = [
            r for r in rows
            if (
                (r.get('normalized_order_dept_key') or r.get('order_dept_key')) == dept_guid
                or (dept_guid == VED_DEPT_KEY and _is_uztransgaz_partner(r.get('partner', '')))
            )
        ]
    if psd_filters:
        rows = _filter_psd_claim_rows(rows)
    return rows


def _fetch_lawsuits_rows_for_department(year: int, month: int, department: str) -> list[dict]:
    from .komdir_lawsuits import fetch_lawsuits_for_month

    canonical_dept, dept_guid = _normalize_commercial_context_department(department)
    # Суды: коммерческий директор и ПСД (в коммерческом блоке) видят ВСЕ суды
    # компании, а не только инициированные сотрудниками коммерческих отделов.
    # Конкретное подразделение видит только свои (отфильтруем ниже по initiator_dept_key).
    rows = fetch_lawsuits_for_month(year, month, include_all=True)
    if dept_guid:
        rows = [r for r in rows if r.get('initiator_dept_key') == dept_guid]
    return rows


def _generate_monthly_data(
    plan: float,
    *,
    include_current_month: bool = False,
    ref_year: int | None = None,
    ref_month: int | None = None,
) -> list[dict]:
    """Помесячные точки: до ref_year/ref_month (если заданы), иначе до текущего или до последнего полного."""
    today = date.today()
    if ref_year is not None and ref_month is not None:
        ref_y, ref_m = int(ref_year), max(1, min(12, int(ref_month)))
    elif include_current_month:
        ref_y, ref_m = today.year, today.month
    else:
        ref_y, ref_m = last_full_month(today)
    if ref_y == today.year:
        pairs = [(today.year, mm) for mm in range(1, ref_m + 1)]
    else:
        pairs = [(ref_y, ref_m)]
    result = []
    for y, m in pairs:
        fact = round(random.uniform(plan * 0.8, plan * 1.2), 2)
        pct = round(fact / plan * 100, 1) if plan else None
        result.append({
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": plan,
            "fact": round(fact, 2),
            "kpi_pct": pct,
            "has_data": False,
        })
    return result


def _build_kpi_entry(
    kpi: dict,
    block: str,
    *,
    dept_key: str | None = None,
    year: int | None = None,
    month: int | None = None,
) -> dict:
    freq = kpi['frequency']
    entry = {
        'kpi_id': kpi['kpi_id'],
        'name': kpi['name'],
        'block': block,
        'frequency': freq,
        'perspective': kpi['perspective'],
        'goal': kpi['goal'],
        'formula': kpi['formula'],
        'unit': kpi['unit'],
        'source': kpi['source'],
        'description': kpi.get('description'),
        'monthly_target': kpi['monthly_target'],
        'quarterly_target': kpi['quarterly_target'],
        'yearly_target': kpi['yearly_target'],
        'green_threshold': kpi['green_threshold'],
        'yellow_threshold': kpi['yellow_threshold'],
        'red_threshold': kpi['red_threshold'],
        'weight_pct': kpi['weight_pct'],
    }

    # Нормализация кода KPI: см. _normalize_dashboard_kpi_id (иначе уходит в синтетику).
    kpi_id = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
    dg = _dept_guid_for_universal(dept_key)

    logistics_entry = logistics_views.build_kpi_entry(kpi_id, entry, year=year, month=month)
    if logistics_entry is not None:
        return logistics_entry

    chief_accountant_entry = calc_chief_accountant.build_kpi_entry(kpi_id, year=year, month=month)
    if chief_accountant_entry is not None:
        entry.update(chief_accountant_entry)
        return entry

    # ИТ-* / 1С-*: склейка по коду KPI с учётом department (IT-M3 — autoit vs c1auto).
    if _autoit_kpi_views.merge_kpi_entry_if_applicable(
        kpi_id, entry, year=year, month=month, department=dept_key,
    ):
        return entry
    if _c1auto_kpi_views.merge_kpi_entry_if_applicable(
        kpi_id, entry, year=year, month=month, department=dept_key,
    ):
        return entry

    if dept_key and dept_dz.is_dz_kpi(kpi_id):
        dz = dept_dz.get_dept_dz_ytd(dept_key)
        if dz is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = dz['months']
            entry['last_full_month_row'] = dz.get('last_full_month_row')
            entry['ytd'] = dz['ytd']
            entry['kpi_period'] = dz.get('kpi_period')
            return entry

    if kpi_id == 'METD-M3.B':
        from . import calc_metrolog_budget

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'metrolog_budget_{ref_y}_{ref_m}',
            calc_metrolog_budget.get_metrolog_budget_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry['data_granularity'] = data.get('data_granularity', 'monthly')
        entry['monthly_data'] = data.get('monthly_data') or []
        entry['last_full_month_row'] = data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        entry['kpi_period'] = data.get('kpi_period')
        entry['debug'] = data.get('debug')
        if data.get('cache_refresh_status'):
            entry['cache_refresh_status'] = data.get('cache_refresh_status')
        return entry

    if dept_key and _is_budget_limit_m3_kpi(kpi_id) and not _is_prod_deputy_pc_m3_kpi(kpi_id):
        bm = dept_budget_m3.get_dept_budget_m3_ytd(dept_key)
        if bm is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = bm['months']
            entry['last_full_month_row'] = bm.get('last_full_month_row')
            entry['ytd'] = bm['ytd']
            entry['kpi_period'] = bm.get('kpi_period')
            return entry

    if kpi_id == 'METD-M3.F':
        from . import calc_metrolog_fot

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'metrolog_fot_{ref_y}_{ref_m}',
            calc_metrolog_fot.get_metrolog_fot_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry['data_granularity'] = data.get('data_granularity', 'monthly')
        entry['monthly_data'] = data.get('monthly_data') or []
        entry['last_full_month_row'] = data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        entry['kpi_period'] = data.get('kpi_period')
        entry['debug'] = data.get('debug')
        if data.get('cache_refresh_status'):
            entry['cache_refresh_status'] = data.get('cache_refresh_status')
        return entry

    if dept_key and dept_turnover_q5.is_turnover_q5_kpi(kpi_id):
        tq = dept_turnover_q5.build_turnover_q5_entry(dept_key)
        if tq is not None:
            entry['data_granularity'] = tq['data_granularity']
            entry['quarterly_data'] = tq['quarterly_data']
            entry['ytd'] = tq['ytd']
            entry['kpi_period'] = tq['kpi_period']
            return entry

    if kpi_id == 'OD-M1':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'vyruchka_opdir_{ref_y}_{ref_m}',
            calc_vyruchka_opdir.get_vyruchka_opdir_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = data.get('months') or []
        entry['quarterly_data'] = data.get('quarterly_data') or []
        entry['yearly_data'] = data.get('yearly_data') or []
        selected_row = pick_monthly_row_for_period(entry['monthly_data'], ref_y, ref_m)
        entry['last_full_month_row'] = selected_row or data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        if selected_row:
            entry['kpi_period'] = {
                'type': 'current_month',
                'year': selected_row.get('year', ref_y),
                'month': selected_row.get('month', ref_m),
                'month_name': selected_row.get('month_name'),
            }
        else:
            entry['kpi_period'] = data.get('kpi_period')
        return entry

    if kpi_id == 'GK-M1':
        from . import calc_chief_constructor_projects

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'gk_m1_projects_{ref_y}_{ref_m}',
            calc_chief_constructor_projects.get_gk_m1_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            if data.get('cache_refresh_status'):
                entry['cache_refresh_status'] = data.get('cache_refresh_status')
            return entry

    if kpi_id == 'GK-Q1':
        from . import calc_chief_constructor_projects

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'gk_q1_rnd_projects_{ref_y}_{ref_m}',
            calc_chief_constructor_projects.get_gk_q1_rnd_quarterly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['quarterly_data'] = data.get('quarterly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['last_full_quarter_row'] = data.get('last_full_quarter_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            if data.get('cache_refresh_status'):
                entry['cache_refresh_status'] = data.get('cache_refresh_status')
            return entry

    if kpi_id in {'MET-Q4-1', 'METD-Q1'}:
        from . import calc_metrolog_projects

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'metrolog_projects_hozyan_{ref_y}_{ref_m}',
            calc_metrolog_projects.get_metrolog_projects_without_major_deviation_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            if data.get('cache_refresh_status'):
                entry['cache_refresh_status'] = data.get('cache_refresh_status')
            return entry

    if kpi_id in {'МЕТ-M1', 'METD-M1'}:
        from . import calc_metrolog_production_plan

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'metrolog_production_plan_{ref_y}_{ref_m}',
            calc_metrolog_production_plan.get_metrolog_production_plan_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            if data.get('cache_refresh_status'):
                entry['cache_refresh_status'] = data.get('cache_refresh_status')
            return entry

    if kpi_id == 'METD-Q2':
        from . import calc_metrolog_turnover

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'metrolog_turnover_{ref_y}_{ref_m}',
            calc_metrolog_turnover.get_metrolog_turnover_ytd,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'quarterly')
            entry['quarterly_data'] = data.get('quarterly_data') or []
            entry['last_full_quarter_row'] = data.get('last_full_quarter_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            if data.get('cache_refresh_status'):
                entry['cache_refresh_status'] = data.get('cache_refresh_status')
            return entry

    if kpi_id == 'METD-Q3':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        from . import calc_metrolog_projects

        data = cache_manager.locked_call(
            f'metrolog_certification_projects_{ref_y}_{ref_m}',
            calc_metrolog_projects.get_certification_projects_without_major_deviation_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            if data.get('cache_refresh_status'):
                entry['cache_refresh_status'] = data.get('cache_refresh_status')
            return entry

    if kpi_id == 'METD-Q4':
        plan = 100.0
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = _generate_monthly_data(
            plan,
            ref_year=year,
            ref_month=month,
        )
        entry['last_full_month_row'] = entry['monthly_data'][-1] if entry['monthly_data'] else None
        if entry['last_full_month_row']:
            lm = entry['last_full_month_row']
            entry['ytd'] = {
                'total_plan': lm.get('plan'),
                'total_fact': lm.get('fact'),
                'kpi_pct': lm.get('kpi_pct'),
                'months_with_data': len(entry['monthly_data']),
                'months_total': len(entry['monthly_data']),
            }
            entry['kpi_period'] = {
                'type': 'last_full_month',
                'year': lm.get('year'),
                'month': lm.get('month'),
                'month_name': lm.get('month_name'),
            }
        return entry

    if kpi_id in {'GK-M3.B', 'GK-M3.F'}:
        plan = 100.0
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = _generate_monthly_data(
            plan,
            ref_year=year,
            ref_month=month,
        )
        entry['last_full_month_row'] = entry['monthly_data'][-1] if entry['monthly_data'] else None
        if entry['last_full_month_row']:
            lm = entry['last_full_month_row']
            entry['ytd'] = {
                'total_plan': lm.get('plan'),
                'total_fact': lm.get('fact'),
                'kpi_pct': lm.get('kpi_pct'),
                'months_with_data': len(entry['monthly_data']),
                'months_total': len(entry['monthly_data']),
            }
            entry['kpi_period'] = {
                'type': 'last_full_month',
                'year': lm.get('year'),
                'month': lm.get('month'),
                'month_name': lm.get('month_name'),
            }
        return entry

    if kpi_id == 'OD-M3.1':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'budget_limit_opdir_{ref_y}_{ref_m}',
            calc_budget_limit.get_budget_limit_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = data.get('months') or []
        entry['quarterly_data'] = data.get('quarterly_data') or []
        entry['yearly_data'] = data.get('yearly_data') or []
        selected_row = pick_monthly_row_for_period(entry['monthly_data'], ref_y, ref_m)
        entry['last_full_month_row'] = selected_row or data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        if selected_row:
            entry['kpi_period'] = {
                'type': 'current_month',
                'year': selected_row.get('year', ref_y),
                'month': selected_row.get('month', ref_m),
                'month_name': selected_row.get('month_name'),
            }
        else:
            entry['kpi_period'] = data.get('kpi_period')
        return entry

    if kpi_id == 'OD-M3.2':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'fot_management_opdir_{ref_y}_{ref_m}',
            calc_fot_management.get_fot_management_monthly,
            year=ref_y,
            month=ref_m,
        )
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = data.get('months') or []
        entry['quarterly_data'] = data.get('quarterly_data') or []
        entry['yearly_data'] = data.get('yearly_data') or []
        entry['last_full_month_row'] = data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        entry['kpi_period'] = data.get('kpi_period')

        return entry

    if kpi_id == 'OD-Q1':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'od_q1_projects_{ref_y}_{ref_m}',
            techdir_projects.get_od_q1_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            return entry

    if kpi_id in {'PD-M3.B1', 'PD-M3.B2'}:
        from . import calc_prod_deputy_pc

        shop = 'pc1' if kpi_id.endswith('B1') else 'pc2'
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_pc_budget_{shop}_{ref_y}_{ref_m}',
            calc_prod_deputy_pc.get_pc_budget_monthly,
            shop,
            ref_y,
            ref_m,
        )
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = data.get('months') or []
        entry['quarterly_data'] = data.get('quarterly_data') or []
        entry['yearly_data'] = data.get('yearly_data') or []
        entry['last_full_month_row'] = data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        entry['kpi_period'] = data.get('kpi_period')
        return entry

    if kpi_id in {'PD-M3.F1', 'PD-M3.F2'}:
        from . import calc_prod_deputy_pc

        shop = 'pc1' if kpi_id.endswith('F1') else 'pc2'
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_pc_fot_{shop}_{ref_y}_{ref_m}',
            calc_prod_deputy_pc.get_pc_fot_monthly,
            shop,
            ref_y,
            ref_m,
        )
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = data.get('months') or []
        entry['quarterly_data'] = data.get('quarterly_data') or []
        entry['yearly_data'] = data.get('yearly_data') or []
        selected_row = pick_monthly_row_for_period(entry['monthly_data'], ref_y, ref_m)
        entry['last_full_month_row'] = selected_row or data.get('last_full_month_row')
        entry['ytd'] = data.get('ytd') or {}
        if selected_row:
            entry['kpi_period'] = {
                'type': 'current_month',
                'year': selected_row.get('year', ref_y),
                'month': selected_row.get('month', ref_m),
                'month_name': selected_row.get('month_name'),
            }
        else:
            entry['kpi_period'] = data.get('kpi_period')
        return entry

    if kpi_id == 'PD-Q1':
        from . import calc_prod_deputy_projects

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_q1_projects_{ref_y}_{ref_m}',
            calc_prod_deputy_projects.get_pd_q1_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            return entry

    if kpi_id == 'PD-Q3':
        from . import calc_prod_deputy_projects

        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_q3_improvement_projects_{ref_y}_{ref_m}',
            calc_prod_deputy_projects.get_pd_q3_improvement_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = data.get('data_granularity', 'monthly')
            entry['monthly_data'] = data.get('monthly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            return entry

    if kpi_id in {'PD-M1.1', 'PD-M1.2'} or kpi_id in PROD_DEPUTY_OUTPUT_PERIOD_BY_ID:
        from . import calc_prod_deputy_output

        if kpi_id in PROD_DEPUTY_OUTPUT_PERIOD_BY_ID:
            shop, output_period = PROD_DEPUTY_OUTPUT_PERIOD_BY_ID[kpi_id]
        else:
            shop, output_period = ('pc1' if kpi_id.endswith('.1') else 'pc2'), 'month'
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_m1_output_{shop}_{ref_y}_{ref_m}',
            calc_prod_deputy_output.get_prod_deputy_output_period,
            shop,
            output_period,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = 'fixed_period' if kpi_id in PROD_DEPUTY_OUTPUT_PERIOD_BY_ID else 'monthly'
            entry['monthly_data'] = data.get('monthly_data') if 'monthly_data' in data else data.get('months') or []
            entry['quarterly_data'] = data.get('quarterly_data') or []
            entry['yearly_data'] = data.get('yearly_data') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            return entry

    if kpi_id in {'PD-Q2.1', 'PD-Q2.2'}:
        from . import calc_prod_deputy_turnover

        shop = 'pc1' if kpi_id.endswith('.1') else 'pc2'
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_q2_turnover_{shop}_{ref_y}_{ref_m}',
            calc_prod_deputy_turnover.get_prod_deputy_turnover_monthly,
            shop,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = data.get('months') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            entry['debug'] = data.get('debug')
            return entry

    if kpi_id == 'OD-Q2':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'od_q2_turnover_{ref_y}_{ref_m}',
            calc_tekuchest_opdir.get_tekuchest_opdir_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = data.get('months') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            return entry

    if kpi_id == 'PD-M2':
        if year and month:
            ref_y, ref_m = year, month
        else:
            today = date.today()
            ref_y, ref_m = today.year, today.month
        data = cache_manager.locked_call(
            f'pd_m2_otif_{ref_y}_{ref_m}',
            calc_otif_vypusk_zam_proizvodstva.get_otif_vypusk_prod_monthly,
            year=ref_y,
            month=ref_m,
        )
        if data is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = data.get('months') or []
            entry['last_full_month_row'] = data.get('last_full_month_row')
            entry['ytd'] = data.get('ytd') or {}
            entry['kpi_period'] = data.get('kpi_period')
            return entry

    if techdir_kpi_entry.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
        return entry

    if _devdir_kpi_views.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
        return entry

    if _qualdir_kpi_views.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
        return entry

    if _gspp_kpi_views.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
        return entry

    if _sup_kpi_views.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
        return entry

    if _servhead_kpi_views.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
        return entry

    if _autoit_kpi_views.merge_kpi_entry_if_applicable(
        kpi_id, entry, year=year, month=month, department=dept_key,
    ):
        return entry

    if kpi_id == 'KD-M1':
        today = date.today()
        if year is not None and month is not None:
            ref_y, ref_m = int(year), max(1, min(12, int(month)))
        else:
            _, ref_y, ref_m = komdir_dashboard._get_monthly_pairs()
        series_m = komdir_dashboard._series_through_month(today, ref_y, ref_m)
        dengi = calc_dengi_fact.get_dengi_monthly(
            year=ref_y, month=series_m, dept_guid=dg,
        )
        plans_payload = calc_plan.get_plans_monthly(
            year=ref_y, month=series_m, dept_guid=dg,
        )
        plans_months = (plans_payload or {}).get('months', [])
        plans_by_month = {r['month']: (r.get('dengi') or 0) for r in plans_months}
        expected_by_month = {r['month']: (r.get('dengi_expected') or 0) for r in plans_months}
        tile = komdir_dashboard._build_plan_fact_tile(
            dengi.get('months', []), plans_by_month, expected_by_month, ref_y, ref_m,
        )
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = tile['monthly_data']
        entry['last_full_month_row'] = tile.get('last_full_month_row')
        entry['ytd'] = tile['ytd']
        entry['kpi_period'] = tile.get('kpi_period')
    elif kpi_id == 'KD-M6':
        vp_data = valovaya_pribyl.get_vp_ytd(dept_guid=dg)
        if year is not None and month is not None:
            ry, rm = int(year), max(1, min(12, int(month)))
        else:
            ry, rm = last_full_month(date.today())
        lm = komdir_dashboard._vp_row_for_period(vp_data, ry, rm) or vp_data.get('last_full_month_row')
        pct = lm.get('kpi_pct') if lm else None
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = vp_data.get('months_calendar') or vp_data.get('months') or []
        entry['last_full_month_row'] = lm
        entry['months_calendar'] = vp_data.get('months_calendar')
        entry['calendar_year'] = vp_data.get('calendar_year')
        entry['plans_apply_to_year'] = vp_data.get('plans_apply_to_year')
        entry['plans_by_month'] = vp_data.get('plans_by_month')
        entry['ytd'] = {
            'total_plan': lm.get('plan') if lm else None,
            'total_fact': lm.get('fact') if lm else None,
            'kpi_pct': pct,
            'months_with_data': 1 if lm and lm.get('fact') is not None else 0,
            'months_total': 1,
        }
        entry['kpi_period'] = {
            'type': 'last_full_month',
            'year': ry,
            'month': rm,
            'month_name': MONTH_NAMES[rm],
        }
    elif kpi_id == 'KD-M2':
        m2 = denzhi_dz.get_kd_m2_ytd()
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = m2['months']
        entry['ytd'] = m2['ytd']
        entry['kpi_period'] = m2.get('kpi_period')
    elif kpi_id == 'KD-M3':
        qd = komdir_quarterly.quarterly_m3()
        entry['data_granularity'] = 'quarterly'
        entry['quarterly_data'] = qd['quarterly_data']
        entry['ytd'] = qd['ytd']
        entry['kpi_period'] = qd.get('kpi_period')
    elif kpi_id == 'KD-Q1':
        vp_data = valovaya_pribyl.get_vp_ytd(dept_guid=dg)
        qd = komdir_quarterly.quarterly_q1(vp_data['months'])
        entry['data_granularity'] = 'quarterly'
        entry['quarterly_data'] = qd['quarterly_data']
        entry['ytd'] = qd['ytd']
        entry['kpi_period'] = qd.get('kpi_period')
    elif kpi_id == 'KD-Q2':
        qd = komdir_quarterly.quarterly_q2()
        entry['data_granularity'] = 'quarterly'
        entry['quarterly_data'] = qd['quarterly_data']
        entry['ytd'] = qd['ytd']
        entry['kpi_period'] = qd.get('kpi_period')
    else:
        freq_l = (freq or '').lower()
        if 'квартал' in freq_l:
            _kid_syn = _normalize_dashboard_kpi_id(kpi.get('kpi_id'))
            if (
                (_is_c1auto_department(dept_key) and _kid_syn in _c1auto_kpi_views.C1AUTO_TURNOVER_KPI_IDS)
                or (_is_autoit_department(dept_key) and _kid_syn in {'IT-Q2', 'ИТ-Q2'})
            ):
                entry['data_granularity'] = 'monthly'
                entry['monthly_data'] = []
                entry['ytd'] = {
                    'total_plan': None,
                    'total_fact': None,
                    'kpi_pct': None,
                    'months_with_data': 0,
                    'months_total': 0,
                }
                return entry
            qrow, kper = _synthetic_quarter_row_for_tile(kpi)
            entry['data_granularity'] = 'quarterly'
            entry['quarterly_data'] = [qrow]
            entry['kpi_period'] = kper
            if kpi_id in DEVDIR_GENERATED_TILE_IDS:
                entry['generated_data'] = True
            fp = qrow.get('fact_turnover_pct')
            if fp is not None:
                entry['ytd'] = {
                    'total_plan': qrow.get('plan_max_turnover_pct'),
                    'total_fact': fp,
                    'kpi_pct': qrow.get('kpi_pct'),
                    'quarters_with_data': 1,
                    'quarters_total': 1,
                }
            else:
                entry['ytd'] = {
                    'total_plan': qrow.get('plan'),
                    'total_fact': qrow.get('fact'),
                    'kpi_pct': qrow.get('kpi_pct'),
                    'quarters_with_data': 1,
                    'quarters_total': 1,
                }
        elif 'год' in freq_l or 'ежегодн' in freq_l:
            yrow, kper = _synthetic_year_row_for_tile(kpi)
            entry['data_granularity'] = 'yearly'
            entry['yearly_data'] = [yrow]
            entry['kpi_period'] = kper
            if kpi_id in DEVDIR_GENERATED_TILE_IDS:
                entry['generated_data'] = True
            entry['ytd'] = {
                'total_plan': yrow['plan'],
                'total_fact': yrow['fact'],
                'kpi_pct': yrow['kpi_pct'],
                'years_with_data': 1,
                'years_total': 1,
            }
        else:
            plan = 100.0
            entry['data_granularity'] = 'monthly'
            is_opdir_monthly = (
                str(dept_key or '').strip().lower() in {
                    'операционный директор',
                    'заместитель операционного директора-директор по производству',
                }
                and 'месяч' in freq_l
                and (str(kpi_id).startswith('OD-M') or str(kpi_id).startswith('PD-M'))
            )
            entry['monthly_data'] = _generate_monthly_data(
                plan,
                include_current_month=is_opdir_monthly,
                ref_year=year,
                ref_month=month,
            )
            if kpi_id in DEVDIR_GENERATED_TILE_IDS:
                entry['generated_data'] = True
            months = entry['monthly_data']
            with_data = [r for r in months if r.get('kpi_pct') is not None]
            last = months[-1] if months else None
            if with_data:
                total_plan = sum(r['plan'] for r in with_data)
                total_fact = sum(r['fact'] for r in with_data)
                avg_kpi = round(
                    sum(r['kpi_pct'] for r in with_data) / len(with_data), 1
                )
                entry['ytd'] = {
                    'total_plan': total_plan,
                    'total_fact': total_fact,
                    'kpi_pct': avg_kpi,
                    'months_with_data': len(with_data),
                    'months_total': len(months),
                }
                entry['kpi_period'] = {
                    'type': 'last_full_month',
                    'year': last['year'],
                    'month': last['month'],
                    'month_name': last['month_name'],
                }
            else:
                entry['ytd'] = {
                    'total_plan': None,
                    'total_fact': None,
                    'kpi_pct': None,
                    'months_with_data': 0,
                    'months_total': 0,
                }

    if _is_gspp_m5_tile(kpi):
        if _gspp_kpi_views.merge_kpi_entry_if_applicable(kpi_id, entry, year=year, month=month):
            return entry

    return entry


@require_GET
@login_required
def get_kpi(request):
    user_department = request.current_user.department

    if not user_department:
        return JsonResponse({'error': 'User has no department assigned'}, status=400)

    requested_dept = request.GET.get('department', user_department)
    for_raw = request.GET.get('for')
    # Фронт иногда шлёт label блока («Коммерческий блок») как department → 403.
    requested_dept, for_from_label = chairman_data.resolve_virtual_block_department(
        requested_dept,
        user_department=user_department,
        for_raw=for_raw,
    )
    # Label-as-department всегда задаёт блок; явный for= имеет приоритет.
    if for_from_label and not (for_raw and str(for_raw).strip()):
        for_raw = for_from_label

    allowed = _get_allowed_departments(user_department)

    if requested_dept not in allowed:
        return JsonResponse({'error': 'Permission denied'}, status=403)

    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    req_month = int(month_param) if month_param else None
    req_year = int(year_param) if year_param else None
    aggregation_mode, selected_quarters = _request_aggregation_params(request)
    is_own_dashboard = str(requested_dept).strip() == str(user_department).strip()

    ck = commercial_kpi_key(requested_dept)
    if ck is None:
        return JsonResponse(
            {
                'department': requested_dept,
                'kpi_count': 0,
                'message': 'Информация по KPI для этого подразделения не найдена',
                'Плитки': {'count': 0, 'items': []},
                'Графики': {},
                'Таблицы': {'месяц': [], 'квартал': []},
            },
            json_dumps_params={'ensure_ascii': False},
        )

    if isinstance(ck, str):
        kpis = _get_kpi_dicts(ck)
        if not kpis:
            return JsonResponse({
                'error': f'No KPIs configured for department key "{ck}"',
            }, status=404)
        dg = dept_guid_for_kpi_key(ck)
        if is_own_dashboard:
            try:
                commercial_cache_scheduler.start_first_access_refresh_if_stale(
                    month=req_month,
                    year=req_year,
                    payload_departments=[ck],
                )
            except Exception:
                logger.exception("commercial first-access cache refresh failed [%s]", requested_dept)
        payload = komdir_dashboard.build_komdir_payload(
            kpis, month=req_month, year=req_year, dept_guid=dg,
        )
        dept_protocol_tables.enrich_payload_tables(payload, requested_dept)
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    kpis = _lookup_kpi_data(requested_dept)
    if kpis is None:
        return JsonResponse({
            'error': f'Department "{requested_dept}" not found in KPI database',
            'available_departments': _get_departments(),
        }, status=404)

    if is_own_dashboard and not _is_komdir_department(requested_dept):
        # Глобальный прогрев всех модулей (GSPP/servhead/…) на минуты блокирует API.
        # Для виртуальных блоков ПСД (commerce) — только коммерческий warm.
        for_norm = chairman_data.normalize_chairman_for_param(for_raw) if for_raw else None
        is_commerce_block = (
            chairman_data.is_chairman_department(requested_dept)
            and for_norm == chairman_data.CHAIRMAN_BLOCK_COMMERCE
        )
        try:
            if is_commerce_block:
                commercial_cache_scheduler.start_first_access_refresh_if_stale(
                    month=req_month,
                    year=req_year,
                    payload_departments=["коммерческий директор"],
                )
            else:
                today = date.today()
                cache_manager.start_period_warming_if_stale(
                    req_year if req_year is not None else today.year,
                    req_month if req_month is not None else today.month,
                )
        except Exception:
            logger.exception("first-access cache warm failed [%s]", requested_dept)

    if _is_komdir_department(requested_dept):
        if is_own_dashboard:
            try:
                commercial_cache_scheduler.start_first_access_refresh_if_stale(
                    month=req_month,
                    year=req_year,
                    payload_departments=["коммерческий директор"],
                )
            except Exception:
                logger.exception("commercial first-access cache refresh failed [%s]", requested_dept)
        payload = komdir_dashboard.build_komdir_payload(kpis, month=req_month, year=req_year)
        dept_protocol_tables.enrich_payload_tables(payload, requested_dept)
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    if chairman_data.is_chairman_department(requested_dept):
        payload, for_block = chairman_data.build_chairman_payload_by_for(
            kpis, month=req_month, year=req_year, for_raw=for_raw,
        )
        # Коммерческий блок уже кладёт претензии/суды/ДЗ в payload — не дублируем запросы в 1С.
        if for_block != chairman_data.CHAIRMAN_BLOCK_COMMERCE:
            if req_month and req_year:
                ref_y, ref_m = req_year, req_month
            else:
                _t = date.today()
                ref_y, ref_m = _t.year, _t.month
            tables = payload.get('Таблицы') or {}
            target_dept = requested_dept
            try:
                claims_rows = _fetch_claims_rows_for_department(ref_y, ref_m, target_dept, psd_filters=True)
            except Exception:
                claims_rows = []
            try:
                lawsuits_rows = _fetch_lawsuits_rows_for_department(ref_y, ref_m, target_dept)
            except Exception:
                lawsuits_rows = []
            month_name = MONTH_NAMES.get(ref_m, str(ref_m))
            tables.update({
                'KD-T-CLAIMS': {
                    'name': 'Активные претензии',
                    'periodicity': 'ежемесячно',
                    'description': (
                        'Претензии из 1С со статусами: Зарегистрирована, Обрабатывается, '
                        'На контроле; причина возникновения = Претензия; сумма заказа > 1 млн'
                    ),
                    'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
                    'rows': claims_rows,
                },
                'KD-T-LAWSUITS': {
                    'name': f'Суды на {month_name} {ref_y}',
                    'periodicity': 'ежемесячно',
                    'description': (
                        'Активные судебные споры и исковая работа из 1С '
                        '(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) '
                        'на конец выбранного месяца (статус ≠ Закрыта)'
                    ),
                    'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
                    'columns': [
                        'Тип документа', 'Контрагент', 'Предмет спора',
                        'Роль ГК в споре', 'Юр. лицо', 'Подразделение',
                        'Дата SLA', 'Краткое описание ситуации',
                        'Сумма требований, руб.',
                    ],
                    'rows': lawsuits_rows,
                },
            })
            payload['Таблицы'] = tables
        dept_protocol_tables.enrich_payload_tables(payload, requested_dept)
        # У председателя СД нет расшифровки просроченной ДЗ / «ТОП-10 решений».
        payload = chairman_data.strip_chairman_overdue_table(payload)
        return JsonResponse(
            {
                'department': requested_dept,
                'for': for_block,
                'kpi_count': payload['Плитки']['count'],
                **payload,
            },
            json_dumps_params={'ensure_ascii': False},
        )

    if _is_chief_metrolog_department(requested_dept):
        payload = _build_chief_metrolog_payload(
            requested_dept,
            kpis,
            month=req_month,
            year=req_year,
            include_debug=_wants_tile_debug(request),
            aggregation_mode=aggregation_mode,
            selected_quarters=selected_quarters,
        )
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    if _is_prod_deputy_department(requested_dept):
        payload = _build_prod_deputy_payload(
            requested_dept,
            kpis,
            month=req_month,
            year=req_year,
            include_debug=_wants_tile_debug(request),
            aggregation_mode=aggregation_mode,
            selected_quarters=selected_quarters,
        )
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    payload = _build_universal_payload(
        requested_dept,
        kpis,
        month=req_month,
        year=req_year,
        include_debug=_wants_tile_debug(request),
        aggregation_mode=aggregation_mode,
        selected_quarters=selected_quarters,
    )
    return JsonResponse(
        {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
        json_dumps_params={'ensure_ascii': False},
    )


def _build_komdir_style_payload(dept: str, kpis: list[dict], request,
                                dept_guid: str | None = None) -> dict:
    """Payload в стиле коммерческого директора (10 плиток + графики + претензии)."""
    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    req_month = int(month_param) if month_param else None
    req_year = int(year_param) if year_param else None
    return komdir_dashboard.build_komdir_payload(
        kpis, month=req_month, year=req_year, dept_guid=dept_guid,
    )


@require_GET
@login_required
def get_all_departments(request):
    user_department = request.current_user.department

    if not user_department:
        return JsonResponse({'error': 'User has no department assigned'}, status=400)

    allowed = _get_allowed_departments(user_department)
    requested_dept = request.GET.get('department')

    if requested_dept:
        if requested_dept not in allowed:
            return JsonResponse({'error': 'Permission denied'}, status=403)

        ck = commercial_kpi_key(requested_dept)
        if ck is None:
            return JsonResponse(
                {
                    'department': requested_dept,
                    'kpi_count': 0,
                    'message': 'Информация по KPI для этого подразделения не найдена',
                    'Плитки': {'count': 0, 'items': []},
                    'Графики': {},
                    'Таблицы': {'месяц': [], 'квартал': []},
                },
                json_dumps_params={'ensure_ascii': False},
            )

        if isinstance(ck, str):
            kpis = _get_kpi_dicts(ck)
            if not kpis:
                return JsonResponse({
                    'error': f'No KPIs configured for department key "{ck}"',
                }, status=404)
            dg = dept_guid_for_kpi_key(ck)
            payload = _build_komdir_style_payload(ck, kpis, request, dept_guid=dg)
            dept_protocol_tables.enrich_payload_tables(payload, requested_dept)
            return JsonResponse(
                {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
                json_dumps_params={'ensure_ascii': False},
            )

        kpis = _lookup_kpi_data(requested_dept)
        if kpis is None:
            return JsonResponse(
                {
                    'department': requested_dept,
                    'kpi_count': 0,
                    'message': 'Информация по KPI для этого подразделения не найдена',
                    'Плитки': {'count': 0, 'items': []},
                    'Графики': {},
                    'Таблицы': {'месяц': [], 'квартал': []},
                },
                json_dumps_params={'ensure_ascii': False},
            )

        if _is_komdir_department(requested_dept):
            payload = _build_komdir_style_payload(requested_dept, kpis, request)
            dept_protocol_tables.enrich_payload_tables(payload, requested_dept)
            return JsonResponse(
                {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
                json_dumps_params={'ensure_ascii': False},
            )

        if chairman_data.is_chairman_department(requested_dept):
            month_param = request.GET.get('month')
            year_param = request.GET.get('year')
            req_m = int(month_param) if month_param else None
            req_yr = int(year_param) if year_param else None
            for_raw = request.GET.get('for')
            payload, for_block = chairman_data.build_chairman_payload_by_for(
                kpis, month=req_m, year=req_yr, for_raw=for_raw,
            )
            if for_block != chairman_data.CHAIRMAN_BLOCK_COMMERCE:
                if req_m and req_yr:
                    ref_y, ref_m = req_yr, req_m
                else:
                    _t = date.today()
                    ref_y, ref_m = _t.year, _t.month
                tables = payload.get('Таблицы') or {}
                target_dept = requested_dept
                try:
                    claims_rows = _fetch_claims_rows_for_department(ref_y, ref_m, target_dept, psd_filters=True)
                except Exception:
                    claims_rows = []
                try:
                    lawsuits_rows = _fetch_lawsuits_rows_for_department(ref_y, ref_m, target_dept)
                except Exception:
                    lawsuits_rows = []
                month_name = MONTH_NAMES.get(ref_m, str(ref_m))
                tables.update({
                    'KD-T-CLAIMS': {
                        'name': 'Активные претензии',
                        'periodicity': 'ежемесячно',
                        'description': (
                            'Претензии из 1С со статусами: Зарегистрирована, Обрабатывается, '
                            'На контроле; причина возникновения = Претензия; сумма заказа > 1 млн'
                        ),
                        'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
                        'rows': claims_rows,
                    },
                    'KD-T-LAWSUITS': {
                        'name': f'Суды на {month_name} {ref_y}',
                        'periodicity': 'ежемесячно',
                        'description': (
                            'Активные судебные споры и исковая работа из 1С '
                            '(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) '
                            'на конец выбранного месяца (статус ≠ Закрыта)'
                        ),
                        'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
                        'columns': [
                            'Тип документа', 'Контрагент', 'Предмет спора',
                            'Роль ГК в споре', 'Юр. лицо', 'Подразделение',
                            'Дата SLA', 'Краткое описание ситуации',
                            'Сумма требований, руб.',
                        ],
                        'rows': lawsuits_rows,
                    },
                })
                payload['Таблицы'] = tables
            dept_protocol_tables.enrich_payload_tables(payload, requested_dept)
            payload = chairman_data.strip_chairman_overdue_table(payload)
            return JsonResponse(
                {
                    'department': requested_dept,
                    'for': for_block,
                    'kpi_count': payload['Плитки']['count'],
                    **payload,
                },
                json_dumps_params={'ensure_ascii': False},
            )

        month_param = request.GET.get('month')
        year_param = request.GET.get('year')
        req_month_all = int(month_param) if month_param else None
        req_year_all = int(year_param) if year_param else None
        aggregation_mode, selected_quarters = _request_aggregation_params(request)
        if _is_prod_deputy_department(requested_dept):
            payload = _build_prod_deputy_payload(
                requested_dept,
                kpis,
                month=req_month_all,
                year=req_year_all,
                include_debug=_wants_tile_debug(request),
                aggregation_mode=aggregation_mode,
                selected_quarters=selected_quarters,
            )
            return JsonResponse(
                {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
                json_dumps_params={'ensure_ascii': False},
            )
        payload = _build_universal_payload(
            requested_dept,
            kpis,
            month=req_month_all,
            year=req_year_all,
            include_debug=_wants_tile_debug(request),
            aggregation_mode=aggregation_mode,
            selected_quarters=selected_quarters,
        )
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    req_month_all = int(month_param) if month_param else None
    req_year_all = int(year_param) if year_param else None
    aggregation_mode_all, selected_quarters_all = _request_aggregation_params(request)
    chairman_for_raw = request.GET.get('for')
    chairman_for_norm = chairman_data.normalize_chairman_for_param(chairman_for_raw)
    include_debug_all = _wants_tile_debug(request)

    db_depts_lower = {d.lower(): d for d in _get_departments()}

    def _build_one(dept: str) -> dict:
        """Payload для одного подразделения (как в основном цикле ниже)."""
        kpis = _get_kpi_dicts(dept)
        if _is_komdir_department(dept):
            payload = _build_komdir_style_payload(dept, kpis, request)
            dept_protocol_tables.enrich_payload_tables(payload, dept)
            return {'department': dept, 'kpi_count': payload['Плитки']['count'], **payload}
        if chairman_data.is_chairman_department(dept):
            payload, for_block = chairman_data.build_chairman_payload_by_for(
                kpis, month=req_month_all, year=req_year_all, for_raw=chairman_for_raw,
            )
            if for_block != chairman_data.CHAIRMAN_BLOCK_COMMERCE:
                if req_month_all and req_year_all:
                    ref_y, ref_m = req_year_all, req_month_all
                else:
                    _t = date.today()
                    ref_y, ref_m = _t.year, _t.month
                tables = payload.get('Таблицы') or {}
                target_dept = dept
                try:
                    claims_rows = _fetch_claims_rows_for_department(ref_y, ref_m, target_dept, psd_filters=True)
                except Exception:
                    claims_rows = []
                try:
                    lawsuits_rows = _fetch_lawsuits_rows_for_department(ref_y, ref_m, target_dept)
                except Exception:
                    lawsuits_rows = []
                month_name = MONTH_NAMES.get(ref_m, str(ref_m))
                tables.update({
                    'KD-T-CLAIMS': {
                        'name': 'Активные претензии',
                        'periodicity': 'ежемесячно',
                        'description': (
                            'Претензии из 1С со статусами: Зарегистрирована, Обрабатывается, '
                            'На контроле; причина возникновения = Претензия; сумма заказа > 1 млн'
                        ),
                        'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
                        'rows': claims_rows,
                    },
                    'KD-T-LAWSUITS': {
                        'name': f'Суды на {month_name} {ref_y}',
                        'periodicity': 'ежемесячно',
                        'description': (
                            'Активные судебные споры и исковая работа из 1С '
                            '(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) '
                            'на конец выбранного месяца (статус ≠ Закрыта)'
                        ),
                        'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
                        'columns': [
                            'Тип документа', 'Контрагент', 'Предмет спора',
                            'Роль ГК в споре', 'Юр. лицо', 'Подразделение',
                            'Дата SLA', 'Краткое описание ситуации',
                            'Сумма требований, руб.',
                        ],
                        'rows': lawsuits_rows,
                    },
                })
                payload['Таблицы'] = tables
            dept_protocol_tables.enrich_payload_tables(payload, dept)
            payload = chairman_data.strip_chairman_overdue_table(payload)
            return {
                'department': dept,
                'for': for_block,
                'kpi_count': payload['Плитки']['count'],
                **payload,
            }
        if is_komdir_child(dept):
            dg = dept_guid_for_kpi_key(commercial_kpi_key(dept))
            payload = _build_komdir_style_payload(dept, kpis, request, dept_guid=dg)
            dept_protocol_tables.enrich_payload_tables(payload, dept)
            return {'department': dept, 'kpi_count': payload['Плитки']['count'], **payload}
        if isinstance((ck := commercial_kpi_key(dept)), str):
            ck_kpis = _get_kpi_dicts(ck)
            dg = dept_guid_for_kpi_key(ck)
            payload = _build_komdir_style_payload(ck, ck_kpis, request, dept_guid=dg)
            dept_protocol_tables.enrich_payload_tables(payload, dept)
            return {'department': dept, 'kpi_count': payload['Плитки']['count'], **payload}
        if _is_prod_deputy_department(dept):
            payload = _build_prod_deputy_payload(
                dept,
                kpis,
                month=req_month_all,
                year=req_year_all,
                include_debug=include_debug_all,
                aggregation_mode=aggregation_mode_all,
                selected_quarters=selected_quarters_all,
            )
            return {'department': dept, 'kpi_count': payload['Плитки']['count'], **payload}
        payload = _build_universal_payload(
            dept,
            kpis,
            month=req_month_all,
            year=req_year_all,
            include_debug=include_debug_all,
            aggregation_mode=aggregation_mode_all,
            selected_quarters=selected_quarters_all,
        )
        return {'department': dept, 'kpi_count': payload['Плитки']['count'], **payload}

    def _empty_entry(dept: str) -> dict:
        """Заглушка для подразделения из structure.json, у которого нет KPI в БД."""
        return {
            'department': dept,
            'kpi_count': 0,
            'message': 'Информация по KPI для этого подразделения не найдена',
            'Плитки': {'count': 0, 'items': []},
            'Графики': {},
            'Таблицы': {'месяц': [], 'квартал': []},
        }

    summary: list[dict] = []
    chairman_in_allowed = any(chairman_data.is_chairman_department(d) for d in allowed)

    if (
        chairman_for_norm == chairman_data.CHAIRMAN_BLOCK_COMMERCE
        and chairman_in_allowed
    ):
        chairman_tree_list = _chairman_and_immediate_children()

        for struct_name in chairman_tree_list:
            if struct_name not in allowed:
                continue
            db_name = db_depts_lower.get(struct_name.lower())
            if db_name is not None:
                summary.append(_build_one(db_name))
            else:
                summary.append(_empty_entry(struct_name))
    else:
        for dept in _get_departments():
            if dept not in allowed:
                continue
            summary.append(_build_one(dept))

    return JsonResponse(
        {'departments': summary},
        json_dumps_params={'ensure_ascii': False},
    )


@require_GET
@login_required
def get_departments_list(request):
    return JsonResponse({'departments': _get_departments()})


@require_GET
@login_required
def get_chairman_for_catalog(request):
    """
    Справочник значений query-параметра `for` для дашборда председателя совета директоров.

    Ответ: ``items`` (упорядоченный список блоков), ``labels`` (id → подпись для UI).
    Расширяется правкой ``CHAIRMAN_FOR_BLOCKS`` в ``chairman_data``.
    """
    cat = chairman_data.get_chairman_for_catalog()
    return JsonResponse(cat, json_dumps_params={'ensure_ascii': False})


@require_GET
@login_required
def get_structure(request):
    structure = get_structure_data()
    payload = {'structure': structure}
    include_headcount = str(request.GET.get('include_headcount') or '').strip().lower()
    if include_headcount in {'1', 'true', 'yes', 'y'}:
        payload['headcount'] = employee_headcount.get_employee_headcount(structure=structure)
    return JsonResponse(payload, json_dumps_params={'ensure_ascii': False})


@require_GET
@login_required
def get_immediate_subordinates(request):
    """
    GET ?department=<название>[&for=<блок>] — непосредственные дочерние подразделения
    (только один уровень вниз по structure.json).

    Если запрос пришёл от ПСД и указан виртуальный блок `for` (например, commerce),
    возвращаем детей соответствующего «реального» подразделения (коммерческого директора),
    а не самого ПСД: именно эту ветку ПСД сейчас просматривает.
    """
    raw = request.GET.get('department', '').strip()
    if not raw:
        return JsonResponse({'error': 'department query parameter is required'}, status=400)

    for_raw = request.GET.get('for')
    effective = raw
    user_dept = getattr(request.current_user, 'department', '') or ''
    if chairman_data.is_chairman_department(user_dept) and for_raw:
        target = chairman_data.chairman_for_target_department(for_raw)
        if target:
            effective = target

    found = _find_immediate_children(get_structure_data(), effective)
    if found is None:
        return JsonResponse(
            {'error': f'Department "{effective}" not found in structure'},
            status=404,
        )

    canonical, children = found
    return JsonResponse(
        {
            'department': canonical,
            'immediate_children': children,
            'count': len(children),
            'for': chairman_data.normalize_chairman_for_param(for_raw) if for_raw else None,
        },
        json_dumps_params={'ensure_ascii': False},
    )


@require_GET
def get_users_departments(request):
    """Список пользователей и их подразделений (без авторизации)."""
    from User.models import User as AppUser

    users = list(
        AppUser.objects.values('nickname', 'department')
        .order_by('department', 'nickname')
    )
    return JsonResponse(
        {'users': users, 'count': len(users)},
        json_dumps_params={'ensure_ascii': False},
    )


@require_http_methods(["GET", "POST"])
@login_required
def refresh_kpi_tile_cache(request):
    """Запустить или проверить ручной пересчёт кэша одной KPI-плитки."""
    user_department = request.current_user.department
    if not user_department:
        return JsonResponse({'error': 'User has no department assigned'}, status=400)

    body: dict[str, Any] = {}
    if request.method == 'POST' and request.body:
        try:
            parsed = json.loads(request.body.decode('utf-8') or '{}')
            body = parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON body'}, status=400)

    def _param(name: str, default: object = None) -> object:
        return body.get(name, request.GET.get(name, default))

    requested_dept = str(_param('department', user_department) or '').strip()
    kpi_id = str(_param('kpi_id', '') or '').strip()
    if not requested_dept:
        return JsonResponse({'error': 'department is required'}, status=400)
    if not kpi_id:
        return JsonResponse({'error': 'kpi_id is required'}, status=400)

    allowed = _get_allowed_departments(user_department)
    if requested_dept not in allowed:
        return JsonResponse({'error': 'Permission denied'}, status=403)

    try:
        ref_m = int(_param('month')) if _param('month') not in (None, '') else None
        ref_y = int(_param('year')) if _param('year') not in (None, '') else None
    except (TypeError, ValueError):
        return JsonResponse({'error': 'month/year must be integers'}, status=400)

    if ref_m is not None and not (1 <= ref_m <= 12):
        return JsonResponse({'error': 'month must be between 1 and 12'}, status=400)

    key = _manual_tile_refresh_key(requested_dept, kpi_id, ref_y, ref_m)
    if request.method == 'GET':
        return JsonResponse(
            _manual_tile_refresh_response_payload(
                key=key,
                department=requested_dept,
                kpi_id=kpi_id,
                ref_y=ref_y,
                ref_m=ref_m,
            ),
            json_dumps_params={'ensure_ascii': False},
        )

    kpis, payload_kind, dept_guid = _manual_tile_refresh_find_kpis(requested_dept)
    if not kpis:
        return JsonResponse({'error': 'No KPIs configured for department'}, status=404)

    normalized_request_kpi_id = _normalize_dashboard_kpi_id(kpi_id)
    is_allowed_table_refresh = (
        payload_kind == 'chief_metrolog'
        and normalized_request_kpi_id == 'KD-T-OVERDUE'
    )
    if not is_allowed_table_refresh and not any(_normalize_dashboard_kpi_id(row.get('kpi_id')) == normalized_request_kpi_id for row in kpis):
        return JsonResponse({'error': 'KPI not found in department'}, status=404)

    with _manual_tile_refresh_state_lock:
        state = _manual_tile_refresh_read_state()
        current = dict(state.get(key) or {})
        started = _manual_tile_refresh_parse_dt(current.get('started_at'))
        if current.get('status') == 'running':
            cache_files = _manual_tile_refresh_cache_files(kpi_id, ref_y, ref_m)
            is_refreshing = cache_manager.is_any_cache_path_refreshing(cache_files)
            if (
                not is_refreshing
                and started is not None
                and (datetime.now() - started).total_seconds() > 60
            ):
                current['status'] = 'failed'
                current['finished_at'] = current.get('finished_at') or _manual_tile_refresh_now()
                current['error'] = current.get('error') or 'Пересчёт был прерван перезапуском сервера'
                state[key] = current
                _manual_tile_refresh_write_state(state)
        if current.get('status') == 'running':
            return JsonResponse(
                _manual_tile_refresh_response_payload(
                    key=key,
                    department=requested_dept,
                    kpi_id=kpi_id,
                    ref_y=ref_y,
                    ref_m=ref_m,
                    entry=current,
                ),
                status=202,
                json_dumps_params={'ensure_ascii': False},
            )

        if started is not None and current.get('status') != 'failed':
            next_allowed = started + timedelta(seconds=MANUAL_TILE_REFRESH_COOLDOWN_SECONDS)
            if datetime.now() < next_allowed:
                payload = _manual_tile_refresh_response_payload(
                    key=key,
                    department=requested_dept,
                    kpi_id=kpi_id,
                    ref_y=ref_y,
                    ref_m=ref_m,
                    entry=current,
                )
                payload['status'] = 'cooldown'
                return JsonResponse(payload, status=429, json_dumps_params={'ensure_ascii': False})

        entry = {
            **current,
            'status': 'running',
            'department': requested_dept,
            'kpi_id': kpi_id,
            'year': ref_y,
            'month': ref_m,
            'started_at': _manual_tile_refresh_now(),
            'finished_at': None,
            'error': '',
        }
        state[key] = entry
        _manual_tile_refresh_write_state(state)

    thread = threading.Thread(
        target=_manual_tile_refresh_worker,
        kwargs={
            'key': key,
            'department': requested_dept,
            'kpi_id': kpi_id,
            'ref_y': ref_y,
            'ref_m': ref_m,
            'kpis': kpis,
            'payload_kind': payload_kind,
            'dept_guid': dept_guid,
        },
        name=f'manual-cache-refresh-{normalized_request_kpi_id}',
        daemon=True,
    )
    thread.start()

    return JsonResponse(
        _manual_tile_refresh_response_payload(
            key=key,
            department=requested_dept,
            kpi_id=kpi_id,
            ref_y=ref_y,
            ref_m=ref_m,
            entry=entry,
        ),
        status=202,
        json_dumps_params={'ensure_ascii': False},
    )


@require_GET
@login_required
def get_cache_status(request):
    """Статус кэшей: когда обновлялся кэш по каждой плитке коммерческого директора."""
    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    ref_month = int(month_param) if month_param else None
    ref_year = int(year_param) if year_param else None

    status = komdir_dashboard.get_tiles_cache_status(
        ref_y=ref_year, ref_m=ref_month,
    )
    return JsonResponse(status, json_dumps_params={'ensure_ascii': False})


@require_GET
@login_required
def get_lawsuits_table(request):
    requested_dept = (request.GET.get('department') or '').strip()
    if not requested_dept:
        return JsonResponse({'error': 'department query parameter is required'}, status=400)

    user_department = request.current_user.department
    if not user_department:
        return JsonResponse({'error': 'User has no department assigned'}, status=400)

    allowed = _get_allowed_departments(user_department)
    ck = commercial_kpi_key(requested_dept)
    canonical_dept = ck if isinstance(ck, str) else requested_dept
    lowered = requested_dept.lower()
    if "коммерческий" in lowered and "директор" in lowered:
        canonical_dept = requested_dept

    if requested_dept not in allowed and canonical_dept not in allowed and canonical_dept != 'коммерческий директор':
        return JsonResponse({'error': 'Permission denied'}, status=403)

    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    if not month_param or not year_param:
        return JsonResponse({'error': 'month and year query parameters are required'}, status=400)
    try:
        req_month = int(month_param)
        req_year = int(year_param)
    except (TypeError, ValueError):
        return JsonResponse({'error': 'month and year must be integers'}, status=400)
    if req_month < 1 or req_month > 12:
        return JsonResponse({'error': 'month must be in range 1..12'}, status=400)

    data = get_sudy_by_department(req_year, req_month, requested_dept)
    month_name = MONTH_NAMES.get(req_month, str(req_month))
    return JsonResponse(
        {
            'department': data['department'],
            'name': f"Суды на {month_name} {req_year}",
            'periodicity': 'ежемесячно',
            'description': (
                'Активные судебные споры и исковая работа из 1С '
                '(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) '
                'на конец выбранного месяца (статус ≠ Закрыта)'
            ),
            'period': {
                'year': req_year,
                'month': req_month,
                'month_name': month_name,
            },
            'columns': [
                'Тип документа', 'Контрагент', 'Предмет спора',
                'Роль ГК в споре', 'Юр. лицо', 'Подразделение',
                'Дата SLA', 'Краткое описание ситуации',
                'Сумма требований, руб.',
            ],
            'count': data['count'],
            'rows': data['rows'],
        },
        json_dumps_params={'ensure_ascii': False},
    )
