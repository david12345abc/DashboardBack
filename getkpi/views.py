import json
import random
from pathlib import Path

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from User.views import login_required
from . import valovaya_pribyl

_KPI_FILE = Path(__file__).resolve().parent / 'kpi_data.json'
with open(_KPI_FILE, encoding='utf-8') as _f:
    KPI_DATA: dict[str, list[dict]] = json.load(_f)

_STRUCTURE_FILE = Path(__file__).resolve().parent / 'structure.json'
with open(_STRUCTURE_FILE, encoding='utf-8') as _f:
    STRUCTURE: dict = json.load(_f)

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
    subordinates = _find_subordinates(STRUCTURE, user_department)
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


def _generate_tile_value() -> float:
    return round(random.uniform(80, 120), 1)


def _generate_chart_values(frequency: str) -> list[float]:
    count = 4 if 'квартал' in frequency.lower() else 12
    return [round(random.uniform(80, 120), 1) for _ in range(count)]


def _success_percentage(values: list[float], target: float = 100.0) -> float:
    hit = sum(1 for v in values if v >= target)
    return round(hit / len(values) * 100, 1)


_TABLE_STATUSES = ['Зелёный', 'Жёлтый', 'Красный']
_TABLE_ACTIONS = [
    'Скорректировать план',
    'Провести анализ причин',
    'Назначить ответственного',
    'Усилить контроль',
    'Запросить данные у владельца',
]


def _generate_table_rows() -> list[dict]:
    count = random.randint(3, 5)
    rows = []
    for i in range(1, count + 1):
        rows.append({
            'rank': i,
            'status': random.choice(_TABLE_STATUSES),
            'deviation_pct': round(random.uniform(-15, -1), 1),
            'action': random.choice(_TABLE_ACTIONS),
        })
    return rows


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
    if kpi_id in ('KD-M1', 'KD-Y1'):
        vp_data = valovaya_pribyl.get_vp_ytd()
        entry['valovaya_pribyl'] = vp_data
        ytd = vp_data['ytd']
        if ytd['kpi_pct'] is not None:
            entry['value'] = ytd['kpi_pct']
        else:
            entry['value'] = _generate_tile_value()
    elif block == 'плитка':
        entry['value'] = _generate_tile_value()
    elif block == 'график':
        values = _generate_chart_values(freq)
        entry['period_count'] = len(values)
        entry['values'] = values
        entry['success_pct'] = _success_percentage(values)
    elif block == 'таблица':
        entry['rows'] = _generate_table_rows()
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
        dept_kpis = [_build_kpi_entry(kpi, kpi.get('block', 'плитка')) for kpi in kpis]
        summary.append({
            'department': dept,
            'kpi_count': len(dept_kpis),
            'kpis': dept_kpis,
        })

    return JsonResponse({'departments': summary})


@require_GET
@login_required
def get_departments_list(request):
    return JsonResponse({'departments': DEPARTMENTS})


@require_GET
@login_required
def get_structure(request):
    return JsonResponse({'structure': STRUCTURE})
