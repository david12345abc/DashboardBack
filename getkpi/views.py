import json
import random
from datetime import date
from pathlib import Path

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from User.views import login_required
from . import denzhi_dz, komdir_dashboard, komdir_quarterly, valovaya_pribyl
from .kpi_periods import last_full_month

# Только плитки дашборда коммерческого директора (KD-Y1 дублирует KD-M1)
KOMDIR_TILE_IDS = frozenset({'KD-M1', 'KD-M2', 'KD-M3', 'KD-Q1', 'KD-Q2'})

_KPI_FILE = Path(__file__).resolve().parent / 'kpi_data.json'
with open(_KPI_FILE, encoding='utf-8') as _f:
    KPI_DATA: dict[str, list[dict]] = json.load(_f)

_STRUCTURE_FILE = Path(__file__).resolve().parent / 'structure.json'
_structure_cache: dict | None = None
_structure_mtime: float | None = None


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

DEPARTMENTS = sorted(KPI_DATA.keys())
_KPI_LOWER_MAP = {k.lower(): k for k in KPI_DATA.keys()}


def _lookup_kpi_data(department: str) -> list[dict] | None:
    """Case-insensitive lookup in KPI_DATA."""
    kpis = KPI_DATA.get(department)
    if kpis is not None:
        return kpis
    real_key = _KPI_LOWER_MAP.get(department.lower())
    if real_key:
        return KPI_DATA[real_key]
    return None


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


def _find_subordinates(tree, target: str) -> set[str] | None:
    """
    Находит target в дереве (case-insensitive) и возвращает множество всех подразделений
    ниже по иерархии (включая сам target). Возвращает None если не найден.
    """
    target_lower = target.lower()
    if isinstance(tree, dict):
        for key, children in tree.items():
            if key.lower() == target_lower:
                subs = {key}
                subs.update(_collect_all_keys(children))
                return subs
            found = _find_subordinates(children, target)
            if found is not None:
                return found
    elif isinstance(tree, list):
        for item in tree:
            if isinstance(item, str) and item.lower() == target_lower:
                return {item}
            elif isinstance(item, dict):
                found = _find_subordinates(item, target)
                if found is not None:
                    return found
    return None


def _get_allowed_departments(user_department: str) -> set[str]:
    """
    Возвращает множество подразделений, которые пользователь имеет право просматривать.
    Включает как ключи из structure.json, так и соответствующие ключи из KPI_DATA
    (case-insensitive matching).
    """
    subordinates = _find_subordinates(get_structure_data(), user_department)
    if subordinates is None:
        subordinates = {user_department}

    result = set(subordinates)
    result.add(user_department)
    lower_map = {d.lower(): d for d in KPI_DATA.keys()}
    for sub in subordinates:
        kpi_key = lower_map.get(sub.lower())
        if kpi_key:
            result.add(kpi_key)
    user_kpi_key = lower_map.get(user_department.lower())
    if user_kpi_key:
        result.add(user_kpi_key)

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
    target_lower = target.strip().lower()
    if isinstance(tree, dict):
        for key, child_tree in tree.items():
            if key.lower() == target_lower:
                return key, _immediate_children_of_node(child_tree)
            found = _find_immediate_children(child_tree, target)
            if found is not None:
                return found
    elif isinstance(tree, list):
        for item in tree:
            if isinstance(item, str) and item.lower() == target_lower:
                return item, []
            if isinstance(item, dict):
                found = _find_immediate_children(item, target)
                if found is not None:
                    return found
    return None


def _is_komdir_department(dept: str) -> bool:
    d = dept.strip().lower()
    return 'коммерческий' in d and 'директор' in d


def _filter_kpis_for_department(dept: str, kpis: list[dict]) -> list[dict]:
    if _is_komdir_department(dept):
        return [k for k in kpis if k.get('kpi_id') in KOMDIR_TILE_IDS]
    return kpis


MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def _generate_monthly_data(plan: float) -> list[dict]:
    """Помесячные точки только за завершённые месяцы (январь — последний полный месяц)."""
    today = date.today()
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
        })
    return result


def _build_kpi_entry(kpi: dict, block: str) -> dict:
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
        'monthly_target': kpi['monthly_target'],
        'quarterly_target': kpi['quarterly_target'],
        'yearly_target': kpi['yearly_target'],
        'green_threshold': kpi['green_threshold'],
        'yellow_threshold': kpi['yellow_threshold'],
        'red_threshold': kpi['red_threshold'],
        'weight_pct': kpi['weight_pct'],
    }

    kpi_id = kpi['kpi_id']
    if kpi_id == 'KD-M1':
        vp_data = valovaya_pribyl.get_vp_ytd()
        entry['data_granularity'] = 'monthly'
        entry['monthly_data'] = vp_data['months']
        entry['ytd'] = vp_data['ytd']
        entry['kpi_period'] = vp_data.get('kpi_period')
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
        vp_data = valovaya_pribyl.get_vp_ytd()
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
        plan = 100.0
        entry['monthly_data'] = _generate_monthly_data(plan)
        last = entry['monthly_data'][-1] if entry['monthly_data'] else None
        if last:
            entry['ytd'] = {
                'total_plan': last['plan'],
                'total_fact': last['fact'],
                'kpi_pct': last['kpi_pct'],
                'months_with_data': 1,
                'months_total': 1,
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

    return entry


@require_GET
@login_required
def get_kpi(request):
    user_department = request.current_user.department

    if not user_department:
        return JsonResponse({'error': 'User has no department assigned'}, status=400)

    requested_dept = request.GET.get('department', user_department)
    allowed = _get_allowed_departments(user_department)

    if requested_dept not in allowed:
        return JsonResponse({'error': 'Permission denied'}, status=403)

    kpis = _lookup_kpi_data(requested_dept)
    if kpis is None:
        return JsonResponse({
            'error': f'Department "{requested_dept}" not found in KPI database',
            'available_departments': DEPARTMENTS,
        }, status=404)

    if _is_komdir_department(requested_dept):
        payload = komdir_dashboard.build_komdir_payload(kpis)
        return JsonResponse(
            {
                'department': requested_dept,
                'kpi_count': payload['Плитки']['count'],
                **payload,
            },
            json_dumps_params={'ensure_ascii': False},
        )

    kpis = _filter_kpis_for_department(requested_dept, kpis)
    result = []
    for kpi in kpis:
        block = kpi.get('block', 'плитка')
        entry = _build_kpi_entry(kpi, block)
        result.append(entry)

    return JsonResponse({
        'department': requested_dept,
        'kpi_count': len(result),
        'kpis': result,
    })


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

        kpis = _lookup_kpi_data(requested_dept)
        if kpis is None:
            return JsonResponse({
                'error': f'Department "{requested_dept}" not found in KPI database',
            }, status=404)

        if _is_komdir_department(requested_dept):
            payload = komdir_dashboard.build_komdir_payload(kpis)
            return JsonResponse(
                {
                    'department': requested_dept,
                    'kpi_count': payload['Плитки']['count'],
                    **payload,
                },
                json_dumps_params={'ensure_ascii': False},
            )

        kpis = _filter_kpis_for_department(requested_dept, kpis)
        dept_kpis = [_build_kpi_entry(kpi, kpi.get('block', 'плитка')) for kpi in kpis]
        return JsonResponse({
            'department': requested_dept,
            'kpi_count': len(dept_kpis),
            'kpis': dept_kpis,
        })

    summary = []
    for dept, kpis in KPI_DATA.items():
        if dept not in allowed:
            continue
        if _is_komdir_department(dept):
            payload = komdir_dashboard.build_komdir_payload(kpis)
            summary.append({
                'department': dept,
                'kpi_count': payload['Плитки']['count'],
                **payload,
            })
        else:
            fkpis = _filter_kpis_for_department(dept, kpis)
            dept_kpis = [_build_kpi_entry(kpi, kpi.get('block', 'плитка')) for kpi in fkpis]
            summary.append({
                'department': dept,
                'kpi_count': len(dept_kpis),
                'kpis': dept_kpis,
            })

    return JsonResponse(
        {'departments': summary},
        json_dumps_params={'ensure_ascii': False},
    )


@require_GET
@login_required
def get_departments_list(request):
    return JsonResponse({'departments': DEPARTMENTS})


@require_GET
@login_required
def get_structure(request):
    return JsonResponse({'structure': get_structure_data()})


@require_GET
@login_required
def get_immediate_subordinates(request):
    """
    GET ?department=<название> — непосредственные дочерние подразделения
    (только один уровень вниз по structure.json).
    """
    raw = request.GET.get('department', '').strip()
    if not raw:
        return JsonResponse({'error': 'department query parameter is required'}, status=400)

    found = _find_immediate_children(get_structure_data(), raw)
    if found is None:
        return JsonResponse(
            {'error': f'Department "{raw}" not found in structure'},
            status=404,
        )

    canonical, children = found
    return JsonResponse(
        {
            'department': canonical,
            'immediate_children': children,
            'count': len(children),
        },
        json_dumps_params={'ensure_ascii': False},
    )
