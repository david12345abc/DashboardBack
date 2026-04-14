import json
import random
from datetime import date
from pathlib import Path

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from User.views import login_required
from . import (
    denzhi_dz,
    dept_budget_m3,
    dept_dz,
    dept_turnover_q5,
    komdir_dashboard,
    komdir_quarterly,
    valovaya_pribyl,
)
from .commercial_tiles import commercial_kpi_key, is_komdir_child
from .kpi_periods import last_full_month, last_full_quarter
from .models import KpiDefinition

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

def _get_departments() -> list[str]:
    return list(
        KpiDefinition.objects.values_list('department', flat=True)
        .distinct().order_by('department')
    )


def _get_kpi_dicts(department: str) -> list[dict]:
    """Все KPI подразделения из БД в формате dict (как был kpi_data.json)."""
    return [obj.to_dict() for obj in KpiDefinition.objects.filter(department=department)]


def _lookup_kpi_data(department: str) -> list[dict] | None:
    """Case-insensitive lookup в таблице kpi_definition."""
    qs = KpiDefinition.objects.filter(department=department)
    if not qs.exists():
        qs = KpiDefinition.objects.filter(department__iexact=department)
    if not qs.exists():
        return None
    return [obj.to_dict() for obj in qs]


def _all_department_names() -> set[str]:
    """Множество всех уникальных department из БД."""
    return set(
        KpiDefinition.objects.values_list('department', flat=True).distinct()
    )


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
    if fact_pct is None:
        return 'unknown'
    if fact_pct <= 5:
        return 'green'
    if fact_pct <= 7:
        return 'yellow'
    return 'red'


def _is_budget_limit_m3_kpi(kpi_id: str) -> bool:
    """Плитки «бюджет в пределах лимита» (*-M3-1): пороги <=100% как у ДЗ."""
    return kpi_id.endswith('-M3-1')


def _is_turnover_style_tile(kpi: dict) -> bool:
    kid = kpi.get('kpi_id') or ''
    nm = (kpi.get('name') or '').lower()
    if 'текучесть' in nm:
        return True
    if kid.endswith('-Q5') or kid == 'ZKD-Q2':
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


def _synthetic_quarter_row_for_tile(kpi: dict) -> tuple[dict, dict]:
    ly, lq = last_full_quarter(date.today())
    random.seed(hash((kpi.get('kpi_id'), ly, lq)))
    kid = kpi.get('kpi_id') or ''
    nm = (kpi.get('name') or '').lower()
    period = {'type': 'last_full_quarter', 'year': ly, 'quarter': lq}
    if 'текучесть' in nm or kid.endswith('-Q5') or kid == 'ZKD-Q2':
        fact = round(random.uniform(2.0, 8.0), 2)
        target = 5.0
        # На плитке — сама текучесть (%); не «выполнение плана» до 100.
        row = {
            'quarter': lq,
            'year': ly,
            'label': f'Q{lq} {ly}',
            'plan_max_turnover_pct': target,
            'fact_turnover_pct': fact,
            'kpi_pct': fact,
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
    }
    return row, period


def _synthetic_year_row_for_tile(kpi: dict) -> tuple[dict, dict]:
    ref_year = date.today().year - 1
    random.seed(hash((kpi.get('kpi_id'), ref_year)))
    plan = 100.0
    fact = round(random.uniform(90, 118), 1)
    kpi_pct = round(fact / plan * 100, 1)
    period = {'type': 'last_full_year', 'year': ref_year}
    row = {'year': ref_year, 'plan': plan, 'fact': fact, 'kpi_pct': kpi_pct}
    return row, period


def _tile_color(kpi: dict, entry: dict) -> tuple[float | None, str]:
    """Вычислить kpi_pct и RAG-цвет для плитки."""
    ytd = entry.get('ytd') or {}
    pct = ytd.get('kpi_pct')
    if pct is not None:
        pct = float(pct)
    kid = kpi.get('kpi_id', '')
    if _is_turnover_style_tile(kpi):
        qd = entry.get('quarterly_data') or []
        turnover = qd[-1].get('fact_turnover_pct') if qd else None
        color_src = pct if pct is not None else turnover
        color = _rag_lower_turnover(float(color_src) if color_src is not None else None)
    elif dept_dz.is_dz_kpi(kid):
        color = _rag_dz_lower_better(pct)
    elif _is_budget_limit_m3_kpi(kid):
        color = _rag_dz_lower_better(pct)
    else:
        color = _rag_higher_better(pct)
    return pct, color


def _build_tile_item(kpi: dict, pct: float | None, color: str) -> dict:
    return {
        'kpi_id': kpi['kpi_id'],
        'name': kpi['name'],
        'kpi_pct': pct,
        'color': color,
        'period': _period_label_from_kpi(kpi),
        'thresholds': _thresholds_block(kpi),
        'formula': kpi.get('formula'),
        'unit': kpi.get('unit'),
        'source': kpi.get('source'),
        'frequency': kpi.get('frequency'),
    }


def _build_universal_payload(dept: str, all_kpis: list[dict],
                             *, month: int | None = None,
                             year: int | None = None) -> dict:
    """
    Универсальный билдер: Плитки (+ AVG), Графики, Таблицы.
    Таблицы — претензии из 1С (Catalog_Претензии) за выбранный месяц.
    """
    from .komdir_claims import fetch_claims_for_month
    from .kpi_periods import last_full_month as _lfm

    tiles_meta = [k for k in all_kpis if k.get('block', 'плитка') == 'плитка']
    charts_meta = [k for k in all_kpis if k.get('block') == 'график']

    plitki_items: list[dict] = []
    numeric_for_avg: list[float] = []

    for kpi in tiles_meta:
        entry = _build_kpi_entry(kpi, 'плитка', dept_key=dept)
        pct, color = _tile_color(kpi, entry)
        if pct is not None:
            numeric_for_avg.append(pct)
        plitki_items.append(_build_tile_item(kpi, pct, color))

    prefix = tiles_meta[0]['kpi_id'].split('-')[0] if tiles_meta else 'KPI'
    avg_pct = round(sum(numeric_for_avg) / len(numeric_for_avg), 1) if numeric_for_avg else None
    plitki_items.append({
        'kpi_id': f'{prefix}-AVG',
        'name': 'Среднее по плиткам KPI',
        'kpi_pct': avg_pct,
        'color': _rag_higher_better(avg_pct),
        'period': 'агрегат',
        'thresholds': {'green': '≥100%', 'yellow': '90–99,9%', 'red': '<90%'},
        'formula': 'Среднее арифметическое kpi_pct всех плиток',
        'unit': '%',
        'source': 'Расчётный показатель',
        'frequency': 'агрегат',
    })

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

    if month and year:
        ref_y, ref_m = year, month
    else:
        ref_y, ref_m = _lfm(date.today())

    month_names = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
    }

    try:
        rows = fetch_claims_for_month(ref_y, ref_m)
    except Exception:
        rows = []

    tablitsy = {
        "KD-T-CLAIMS": {
            "name": f"Претензии за {month_names[ref_m]} {ref_y}",
            "periodicity": "ежемесячно",
            "description": "Претензии из 1С (Catalog_Претензии) за выбранный месяц",
            "period": {
                "year": ref_y,
                "month": ref_m,
                "month_name": month_names[ref_m],
            },
            "rows": rows,
        },
    }

    return {
        'Плитки': {'count': len(plitki_items), 'items': plitki_items},
        'Графики': grafiki,
        'Таблицы': tablitsy,
    }


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


def _build_kpi_entry(kpi: dict, block: str, *, dept_key: str | None = None) -> dict:
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

    if dept_key and dept_dz.is_dz_kpi(kpi_id):
        dz = dept_dz.get_dept_dz_ytd(dept_key)
        if dz is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = dz['months']
            entry['last_full_month_row'] = dz.get('last_full_month_row')
            entry['ytd'] = dz['ytd']
            entry['kpi_period'] = dz.get('kpi_period')
            return entry

    if dept_key and _is_budget_limit_m3_kpi(kpi_id):
        bm = dept_budget_m3.get_dept_budget_m3_ytd(dept_key)
        if bm is not None:
            entry['data_granularity'] = 'monthly'
            entry['monthly_data'] = bm['months']
            entry['last_full_month_row'] = bm.get('last_full_month_row')
            entry['ytd'] = bm['ytd']
            entry['kpi_period'] = bm.get('kpi_period')
            return entry

    if dept_key and dept_turnover_q5.is_turnover_q5_kpi(kpi_id):
        tq = dept_turnover_q5.build_turnover_q5_entry(dept_key)
        if tq is not None:
            entry['data_granularity'] = tq['data_granularity']
            entry['quarterly_data'] = tq['quarterly_data']
            entry['ytd'] = tq['ytd']
            entry['kpi_period'] = tq['kpi_period']
            return entry

    if kpi_id == 'KD-M1':
        vp_data = valovaya_pribyl.get_vp_ytd()
        entry['data_granularity'] = 'monthly'
        lm = vp_data.get('last_full_month_row')
        entry['monthly_data'] = [lm] if lm else []
        entry['months_calendar'] = vp_data.get('months_calendar')
        entry['calendar_year'] = vp_data.get('calendar_year')
        entry['plans_apply_to_year'] = vp_data.get('plans_apply_to_year')
        entry['plans_by_month'] = vp_data.get('plans_by_month')
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
        freq_l = (freq or '').lower()
        if 'квартал' in freq_l:
            qrow, kper = _synthetic_quarter_row_for_tile(kpi)
            entry['data_granularity'] = 'quarterly'
            entry['quarterly_data'] = [qrow]
            entry['kpi_period'] = kper
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
            entry['monthly_data'] = _generate_monthly_data(plan)
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

    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    req_month = int(month_param) if month_param else None
    req_year = int(year_param) if year_param else None

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
        payload = komdir_dashboard.build_komdir_payload(kpis, month=req_month, year=req_year)
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

    if _is_komdir_department(requested_dept):
        payload = komdir_dashboard.build_komdir_payload(kpis, month=req_month, year=req_year)
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    payload = _build_universal_payload(requested_dept, kpis, month=req_month, year=req_year)
    return JsonResponse(
        {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
        json_dumps_params={'ensure_ascii': False},
    )


def _build_komdir_style_payload(dept: str, kpis: list[dict], request) -> dict:
    """Payload в стиле коммерческого директора (10 плиток + графики + претензии)."""
    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    req_month = int(month_param) if month_param else None
    req_year = int(year_param) if year_param else None
    return komdir_dashboard.build_komdir_payload(kpis, month=req_month, year=req_year)


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
            payload = _build_komdir_style_payload(ck, kpis, request)
            return JsonResponse(
                {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
                json_dumps_params={'ensure_ascii': False},
            )

        kpis = _lookup_kpi_data(requested_dept)
        if kpis is None:
            return JsonResponse({
                'error': f'Department "{requested_dept}" not found in KPI database',
            }, status=404)

        if _is_komdir_department(requested_dept):
            payload = _build_komdir_style_payload(requested_dept, kpis, request)
            return JsonResponse(
                {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
                json_dumps_params={'ensure_ascii': False},
            )

        month_param = request.GET.get('month')
        year_param = request.GET.get('year')
        req_month_all = int(month_param) if month_param else None
        req_year_all = int(year_param) if year_param else None
        payload = _build_universal_payload(requested_dept, kpis, month=req_month_all, year=req_year_all)
        return JsonResponse(
            {'department': requested_dept, 'kpi_count': payload['Плитки']['count'], **payload},
            json_dumps_params={'ensure_ascii': False},
        )

    month_param = request.GET.get('month')
    year_param = request.GET.get('year')
    req_month_all = int(month_param) if month_param else None
    req_year_all = int(year_param) if year_param else None

    summary = []
    all_depts = _get_departments()
    for dept in all_depts:
        if dept not in allowed:
            continue
        kpis = _get_kpi_dicts(dept)
        if _is_komdir_department(dept) or is_komdir_child(dept):
            payload = _build_komdir_style_payload(dept, kpis, request)
            summary.append({'department': dept, 'kpi_count': payload['Плитки']['count'], **payload})
        elif isinstance((ck := commercial_kpi_key(dept)), str):
            ck_kpis = _get_kpi_dicts(ck)
            payload = _build_komdir_style_payload(ck, ck_kpis, request)
            summary.append({'department': dept, 'kpi_count': payload['Плитки']['count'], **payload})
        else:
            payload = _build_universal_payload(dept, kpis, month=req_month_all, year=req_year_all)
            summary.append({'department': dept, 'kpi_count': payload['Плитки']['count'], **payload})

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
