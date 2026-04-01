import json
import random
from pathlib import Path

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from User.views import login_required

_KPI_FILE = Path(__file__).resolve().parent / 'kpi_data.json'
with open(_KPI_FILE, encoding='utf-8') as _f:
    KPI_DATA: dict[str, list[dict]] = json.load(_f)

DEPARTMENTS = sorted(KPI_DATA.keys())


def _generate_values(frequency: str) -> list[float]:
    if 'квартал' in frequency.lower():
        return [round(random.uniform(70, 115), 1) for _ in range(4)]
    return [round(random.uniform(70, 115), 1) for _ in range(12)]


def _success_percentage(values: list[float], target: float = 100.0) -> float:
    hit = sum(1 for v in values if v >= target)
    return round(hit / len(values) * 100, 1)


@require_GET
@login_required
def get_kpi(request):
    department = request.current_user.department

    if not department:
        return JsonResponse({'error': 'User has no department assigned'}, status=400)

    kpis = KPI_DATA.get(department)
    if kpis is None:
        return JsonResponse({
            'error': f'Department "{department}" not found in KPI database',
            'available_departments': DEPARTMENTS,
        }, status=404)

    result = []
    for kpi in kpis:
        freq = kpi['frequency']
        values = _generate_values(freq)
        success = _success_percentage(values)

        result.append({
            'kpi_id': kpi['kpi_id'],
            'name': kpi['name'],
            'frequency': freq,
            'period_count': len(values),
            'values': values,
            'success_pct': success,
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
        })

    return JsonResponse({
        'department': department,
        'kpi_count': len(result),
        'kpis': result,
    })


@require_GET
@login_required
def get_all_departments(request):
    summary = []
    for dept, kpis in KPI_DATA.items():
        dept_kpis = []
        for kpi in kpis:
            values = _generate_values(kpi['frequency'])
            dept_kpis.append({
                'kpi_id': kpi['kpi_id'],
                'name': kpi['name'],
                'frequency': kpi['frequency'],
                'period_count': len(values),
                'values': values,
                'success_pct': _success_percentage(values),
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
            })
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
