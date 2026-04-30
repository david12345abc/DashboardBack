"""
Дашборд «Председатель совета директоров».

Полный payload: Плитки, Графики (3), Таблицы.
Формат ответа идентичен komdir_dashboard.build_komdir_payload.
"""
from __future__ import annotations

from datetime import date

from . import (
    cache_manager,
    calc_debitorka,
    calc_dengi_fact,
    calc_otgruzki_fact,
    calc_plan,
    calc_psd_portfolio,
    calc_psd_vipusk_plan,
    calc_postavshchiki,
    calc_reclamations,
    calc_shipment_share_bmi_gazprom,
    calc_svoevremennaya_otgruzka,
    calc_tenders_bmi,
)
from .kpi_periods import last_full_month
from .komdir_dashboard import (
    MONTH_NAMES_RU,
    _build_line_chart,
    _get_monthly_pairs,
    _get_tile_data as _komdir_get_tile_data,
    _series_through_month,
    _tile_rag,
)

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

LOWER_IS_BETTER = frozenset({"FND-T6", "FND-T7"})

# ═══════════════════════════════════════════════════════════════
#  Захардкоженные данные по месяцам
# ═══════════════════════════════════════════════════════════════

# FND-T1  Выручка / план-факт  (план + факт)
_T1_PLAN = {1: 201_300_000, 2: 201_300_000, 3: 201_300_000}
_T1_FACT = {1: 185_420_000, 2: 210_750_000, 3: 198_600_000}

# FND-T2  Чистая прибыль / план-факт  (план + факт)
_T2_PLAN = {1: 27_800_000, 2: 27_800_000, 3: 27_800_000}
_T2_FACT = {1: 22_150_000, 2: 31_420_000, 3: 26_980_000}

# FND-T3 «Соотношение ДЗ и КЗ» рассчитывается из 1С
# (calc_debitorka + calc_postavshchiki) в `_build_fnd_t3_dz_kz_rows`.

# FND-T4  Своевременная отгрузка  (среднее по всем месяцам)
_T4_FACT = {1: 94.2, 2: 96.8, 3: 93.5}

# FND-T5  Качество: рекламации и САРА  (reclamations + capa_overdue)
_T5_DATA: dict[int, tuple[int, int]] = {1: (3, 1), 2: (2, 0), 3: (4, 2)}

# FND-T7  Дебиторская задолженность  (план + факт)
_T7_PLAN = {1: 120_000_000, 2: 120_000_000, 3: 120_000_000}
_T7_FACT = {1: 98_500_000, 2: 115_200_000, 3: 132_400_000}

# FND-T9  Выпуск / план-факт, руб. План задан двумя строками ПСД и суммируется по месяцу.
_T9_PLAN_ROWS = (
    {
        1: 51_850_261, 2: 40_528_324, 3: 112_879_583, 4: 131_788_552,
        5: 147_474_990, 6: 187_746_649, 7: 158_217_075, 8: 133_320_522,
        9: 168_974_477, 10: 111_959_640, 11: 102_591_730, 12: 203_883_850,
    },
    {
        1: 3_964_943, 2: 7_641_156, 3: 19_801_269, 4: 23_760_543,
        5: 25_857_206, 6: 34_007_643, 7: 42_543_313, 8: 45_794_767,
        9: 42_258_602, 10: 38_213_686, 11: 20_511_236, 12: 15_363_074,
    },
)
_T9_PLAN = {
    month: sum(row.get(month, 0) for row in _T9_PLAN_ROWS)
    for month in range(1, 13)
}
_T9_THRESHOLDS = {
    "green": "≥100%",
    "yellow": "90–99,9%",
    "red": "<90%",
}


# ═══════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ═══════════════════════════════════════════════════════════════

def _kpi_pct(fact, plan) -> float | None:
    if plan is None or plan == 0 or fact is None:
        return None
    return round(fact / plan * 100, 2)


def _rag(kpi_id: str, pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if kpi_id in LOWER_IS_BETTER:
        if pct < 100:
            return "green"
        if pct <= 110:
            return "yellow"
        return "red"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _rag_higher_better(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 100:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _month_pairs(ref_y: int, ref_m: int) -> list[int]:
    today = date.today()
    if ref_y == today.year:
        return list(range(1, ref_m + 1))
    return [ref_m]


def _period_label(kpi: dict) -> str:
    f = (kpi.get("frequency") or "").lower()
    if "квартал" in f:
        return "ежеквартально"
    if "месяц" in f or "ежемесячно" in f:
        return "ежемесячно"
    if "год" in f:
        return "ежегодно"
    return kpi.get("frequency") or ""


def _thresholds(kpi: dict) -> dict:
    return {
        "green": kpi.get("green_threshold"),
        "yellow": kpi.get("yellow_threshold"),
        "red": kpi.get("red_threshold"),
    }


# ═══════════════════════════════════════════════════════════════
#  Генераторы помесячных данных по KPI
# ═══════════════════════════════════════════════════════════════

def _months_plan_fact(plan_dict, fact_dict, months):
    rows = []
    for m in months:
        plan = plan_dict.get(m)
        fact = fact_dict.get(m)
        has = plan is not None and fact is not None
        rows.append({
            "month": m, "year": 2026, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if has else None,
            "has_data": has,
        })
    return rows


def _months_fact_only(fact_dict, months):
    rows = []
    for m in months:
        fact = fact_dict.get(m)
        has = fact is not None
        rows.append({
            "month": m, "year": 2026, "month_name": MONTH_NAMES[m],
            "plan": None, "fact": fact,
            "kpi_pct": None,
            "has_data": has,
        })
    return rows


def _build_fnd_t1_revenue_rows(months: list[int], ref_y: int) -> list[dict]:
    """FND-T1 «Выручка» = деньги ПЛАН (из планов коммерческого блока) + деньги
    ФАКТ (из calc_dengi_fact). Используем те же источники, что в KD-M3 у
    коммерческого директора.
    """
    if not months:
        return []
    max_m = max(months)

    plans_payload = cache_manager.locked_call(
        f"plans_{ref_y}_{max_m}",
        calc_plan.get_plans_monthly,
        year=ref_y, month=max_m, dept_guid=None,
    )
    dengi_payload = cache_manager.locked_call(
        f"dengi_{ref_y}_{max_m}",
        calc_dengi_fact.get_dengi_monthly,
        year=ref_y, month=max_m,
    )

    plan_by_m: dict[int, float | None] = {}
    for row in plans_payload.get("months", []) or []:
        m = int(row.get("month") or 0)
        if 1 <= m <= 12:
            # calc_plan.get_plans_monthly возвращает поле 'dengi' (план по ДС).
            plan_by_m[m] = row.get("dengi")

    fact_by_m: dict[int, float | None] = {}
    for row in dengi_payload.get("months", []) or []:
        m = int(row.get("month") or 0)
        if 1 <= m <= 12:
            # calc_dengi_fact.get_dengi_monthly возвращает поле 'fact'.
            fact_by_m[m] = row.get("fact")

    rows: list[dict] = []
    for m in months:
        plan = plan_by_m.get(m)
        fact = fact_by_m.get(m)
        has = plan is not None or fact is not None
        pct = _kpi_pct(fact, plan) if (plan is not None and fact is not None) else None
        rows.append({
            "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": pct, "has_data": has,
        })
    return rows


def _build_fnd_t4_svoevremennaya_rows(months: list[int], ref_y: int, ref_m: int) -> list[dict]:
    """FND-T4 «Своевременная отгрузка» = уникальные заказы из
    ТД_КонтрольныеДатыИсполненияДоговора по ДатаОкончанияЮридическихОбязательствПоДоговору
    (план); факт = план - срывы.
    """
    if not months:
        return []

    payload = cache_manager.locked_call(
        f"svoevremennaya_monthly_{ref_y}_{max(months)}",
        calc_svoevremennaya_otgruzka.get_svoevremennaya_monthly,
        year=ref_y, month=max(months),
    )
    by_m: dict[int, dict] = {}
    for row in payload.get("months", []) or []:
        m = int(row.get("month") or 0)
        if 1 <= m <= 12:
            by_m[m] = row

    rows: list[dict] = []
    for m in months:
        d = by_m.get(m)
        plan = d.get("plan") if d else None
        fact = d.get("fact") if d else None
        pct = d.get("kpi_pct") if d else None
        has = (plan or 0) > 0
        rows.append({
            "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": pct, "has_data": has,
        })
    return rows


def _build_fnd_t5_reclamations_rows(months: list[int], ref_y: int) -> list[dict]:
    """FND-T5 «Качество рекламаций» = количество претензий из
    Справочник.Претензии по ТД_ДатаОкончанияПлан (план) и ДатаОкончания (факт).
    """
    if not months:
        return []

    payload = cache_manager.locked_call(
        f"reclamations_monthly_{ref_y}_{max(months)}",
        calc_reclamations.get_reclamations_monthly,
        year=ref_y, month=max(months),
    )
    by_m: dict[int, dict] = {}
    for row in payload.get("months", []) or []:
        m = int(row.get("month") or 0)
        if 1 <= m <= 12:
            by_m[m] = row

    rows: list[dict] = []
    for m in months:
        d = by_m.get(m)
        plan = d.get("plan") if d else None
        fact = d.get("fact") if d else None
        pct = d.get("kpi_pct") if d else None
        has = bool(d and d.get("has_data"))
        rows.append({
            "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": pct, "has_data": has,
        })
    return rows


def _build_fnd_t3_dz_kz_rows(months: list[int], ref_y: int) -> list[dict]:
    """FND-T3 «Соотношение ДЗ и КЗ» (ПСД, «Мой дашборд»).

    Помесячно для каждого месяца вычисляются 4 значения (в рублях):
      - `dz_client`    — долг клиентов (ДЗ) = Σ положительных нетто-остатков
                          по регистру РасчетыСКлиентамиПоСрокам.
      - `kz_client`    — наш долг клиентам (КЗ) = Σ |отрицательных| нетто-остатков
                          по тому же регистру (авансы / переплаты клиентов).
      - `dz_supplier`  — дельта ДЗ поставщиков за месяц.
      - `kz_supplier`  — дельта КЗ поставщикам за месяц.

    Для поставщиков месячные числа нужны фронту именно как помесячные
    значения, чтобы он мог суммировать выбранный диапазон. При этом
    `pct_supplier` по-прежнему считаем по закрывающему остатку месяца,
    а не по дельте, чтобы не ломать текущий KPI плитки.

    Проценты:
      - `pct_client`   = dz_client   / kz_client   × 100  (higher_better)
      - `pct_supplier` = dz_supplier / kz_supplier × 100  (higher_better)
      - `pct_total`    = (dz_client + dz_supplier) / (kz_client + kz_supplier) × 100

    Основной `kpi_pct` плитки — именно `pct_total`.
    Отдельные поля клиентов/поставщиков сохраняем для детализации.
    """
    if not months:
        return []

    max_m = max(months)
    dz_payload = cache_manager.locked_call(
        f"debitorka_{ref_y}_{max_m}",
        calc_debitorka.get_komdir_dz_monthly,
        year=ref_y, month=max_m, dept_name=None,
    )
    supplier_payload = cache_manager.locked_call(
        f"postavshchiki_{ref_y}_{max_m}",
        calc_postavshchiki.get_supplier_monthly,
        year=ref_y, ref_month=max_m,
    )

    dz_by_m: dict[int, dict] = {}
    for row in dz_payload.get("months", []) or []:
        mm = int(row.get("month") or 0)
        if 1 <= mm <= 12:
            dz_by_m[mm] = row

    sup_by_m: dict[int, dict] = {}
    for row in supplier_payload.get("months", []) or []:
        mm = int(row.get("month") or 0)
        if 1 <= mm <= 12:
            sup_by_m[mm] = row

    rows: list[dict] = []
    for m in months:
        dz_row = dz_by_m.get(m) or {}
        sup_row = sup_by_m.get(m) or {}
        dz_client = float(dz_row.get("dz_fact") or 0)
        kz_client = float(dz_row.get("kz_fact") or 0)
        dz_supplier = float(sup_row.get("predoplata_regl") or 0)
        kz_supplier = float(sup_row.get("dolg_regl") or 0)
        dz_supplier_closing = float(
            sup_row.get("closing_predoplata_regl") or dz_supplier
        )
        kz_supplier_closing = float(
            sup_row.get("closing_dolg_regl") or kz_supplier
        )

        pct_client = round(dz_client / kz_client * 100, 1) if kz_client > 0 else None
        pct_supplier = (
            round(dz_supplier_closing / kz_supplier_closing * 100, 1)
            if kz_supplier_closing > 0 else None
        )
        dz_total = dz_client + dz_supplier
        kz_total = kz_client + kz_supplier
        pct_total = round(dz_total / kz_total * 100, 1) if kz_total > 0 else None

        has_data = (
            abs(dz_client) > 0 or abs(kz_client) > 0
            or abs(dz_supplier) > 0 or abs(kz_supplier) > 0
        )

        rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": round(kz_total, 2),
            "fact": round(dz_total, 2),
            "kpi_pct": pct_total,
            "has_data": has_data,
            "dz_client": round(dz_client, 2),
            "kz_client": round(kz_client, 2),
            "dz_supplier": round(dz_supplier, 2),
            "kz_supplier": round(kz_supplier, 2),
            "dz_total": round(dz_total, 2),
            "kz_total": round(kz_total, 2),
            "pct_client": pct_client,
            "pct_supplier": pct_supplier,
            "pct_total": pct_total,
        })
    return rows


def _build_fnd_t7_debitorka_rows(months: list[int], ref_y: int) -> list[dict]:
    """FND-T7 «Дебиторская задолженность» — те же данные, что и у коммерческого
    директора (KD-M4): агрегат по всей компании из calc_debitorka.
    План — фиксированный (100 млн руб.), как у коммерческого директора.
    """
    if not months:
        return []

    max_m = max(months)
    dz_payload = cache_manager.locked_call(
        f"debitorka_{ref_y}_{max_m}",
        calc_debitorka.get_komdir_dz_monthly,
        year=ref_y, month=max_m, dept_name=None,
    )
    by_m: dict[int, dict] = {}
    for row in dz_payload.get("months", []) or []:
        mm = int(row.get("month") or 0)
        if 1 <= mm <= 12:
            by_m[mm] = row

    plan = 100_000_000.0
    rows: list[dict] = []
    for m in months:
        d = by_m.get(m)
        fact = float(d.get("dz_fact")) if d and d.get("dz_fact") is not None else None
        pct = round(fact / plan * 100, 1) if (plan and fact is not None) else None
        rows.append({
            "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
            "plan": plan, "fact": fact,
            "kpi_pct": pct, "has_data": fact is not None,
        })
    return rows


def _build_fnd_t6_portfolio_rows(months: list[int], ref_y: int) -> list[dict]:
    """FND-T6 «Портфель проектов».

    plan = количество проектов из TurboProject, пришедших из 1С (`has_1c=true`)
           и имеющих хотя бы одну веху в выбранном месяце.
    fact = количество проектов из этого портфеля со сдвигом по baseline.
    kpi_pct = fact / plan × 100.

    Для фронта важно, что plan/fact помесячные и дальше могут суммироваться
    за квартал / YTD с повторным пересчётом процента.
    """
    if not months:
        return []

    max_m = max(months)
    payload = cache_manager.locked_call(
        f"psd_portfolio_{ref_y}_{max_m}",
        calc_psd_portfolio.get_psd_portfolio_monthly,
        year=ref_y,
        ref_month=max_m,
    )

    by_m: dict[int, dict] = {}
    for row in payload.get("months", []) or []:
        mm = int(row.get("month") or 0)
        if 1 <= mm <= 12:
            by_m[mm] = row

    rows: list[dict] = []
    for m in months:
        row = by_m.get(m) or {}
        plan = float(row.get("portfolio_count") or 0)
        fact = float(row.get("deviation_count") or 0)
        pct = round(fact / plan * 100, 1) if plan > 0 else None
        rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": round(plan, 2),
            "fact": round(fact, 2),
            "kpi_pct": pct,
            "has_data": plan > 0 or fact > 0,
            "portfolio_count": round(plan, 2),
            "deviation_count": round(fact, 2),
        })
    return rows


def _build_fnd_t9_vipusk_rows(months: list[int], ref_y: int) -> list[dict]:
    """FND-T9 «Выпуск — план/факт».

    Факт берём из `calc_psd_vipusk_plan.py` как помесячный выпуск в рублях.
    План задан пользователем по месяцам 2026 года. KPI = факт / план × 100.
    """
    if not months:
        return []

    max_m = max(months)
    payload = cache_manager.locked_call(
        f"psd_vipusk_plan_{ref_y}_{max_m}",
        calc_psd_vipusk_plan.get_psd_vipusk_plan_monthly,
        year=ref_y,
        ref_month=max_m,
    )

    by_m: dict[int, dict] = {}
    for row in payload.get("months", []) or []:
        mm = int(row.get("month") or 0)
        if 1 <= mm <= 12:
            by_m[mm] = row

    rows: list[dict] = []
    for m in months:
        row = by_m.get(m) or {}
        plan = float(_T9_PLAN.get(m, 0) or 0) if ref_y == 2026 else 0.0
        fact = float(row.get("fact_rub_total") or 0)
        pct = round(fact / plan * 100, 1) if plan > 0 else None
        rows.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES[m],
            "plan": round(plan, 2),
            "fact": round(fact, 2),
            "kpi_pct": pct,
            "has_data": plan > 0 or fact > 0,
            "plan_rub_total": round(plan, 2),
            "fact_rub_total": round(fact, 2),
            "fact_qty_total": round(float(row.get("fact_qty_total") or 0), 2),
        })
    return rows


def _get_tile_data(kpi_id: str, months: list[int], ref_y: int, ref_m: int) -> dict:
    """Вернуть monthly_data + ytd + kpi_period для одного KPI."""

    if kpi_id == "FND-T1":
        rows = _build_fnd_t1_revenue_rows(months, ref_y)
    elif kpi_id == "FND-T2":
        rows = _months_plan_fact(_T2_PLAN, _T2_FACT, months)
    elif kpi_id == "FND-T3":
        rows = _build_fnd_t3_dz_kz_rows(months, ref_y)
    elif kpi_id == "FND-T4":
        rows = _build_fnd_t4_svoevremennaya_rows(months, ref_y, ref_m)
    elif kpi_id == "FND-T5":
        rows = _build_fnd_t5_reclamations_rows(months, ref_y)
    elif kpi_id == "FND-T6":
        rows = _build_fnd_t6_portfolio_rows(months, ref_y)
    elif kpi_id == "FND-T7":
        rows = _build_fnd_t7_debitorka_rows(months, ref_y)
    elif kpi_id == "FND-T9":
        rows = _build_fnd_t9_vipusk_rows(months, ref_y)
    else:
        rows = []

    ref_row = None
    for r in rows:
        if r["year"] == ref_y and r["month"] == ref_m:
            ref_row = r
            break

    with_data = [r for r in rows if r.get("has_data")]
    plans = [r["plan"] for r in with_data if r.get("plan") is not None]
    facts = [r["fact"] for r in with_data if r.get("fact") is not None]
    kpis = [r["kpi_pct"] for r in with_data if r.get("kpi_pct") is not None]
    ytd_pct = round(sum(kpis) / len(kpis), 2) if kpis else None

    return {
        "monthly_data": rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "ytd": {
            "total_plan": sum(plans) if plans else None,
            "total_fact": sum(facts) if facts else None,
            "kpi_pct": ytd_pct,
            "months_with_data": len(with_data),
            "months_total": len(rows),
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
    }


# ═══════════════════════════════════════════════════════════════
#  Графики
# ═══════════════════════════════════════════════════════════════

def _build_chart_c1(by_id: dict, tiles_data: dict) -> dict:
    """FND-C1: Тренд 12 мес — Выручка, EBITDA (чистая прибыль), Выпуск."""
    meta = by_id.get("FND-C1", {})
    series = []
    for kid in ["FND-T1", "FND-T2", "FND-T9"]:
        kpi_meta = by_id.get(kid, {})
        td = tiles_data.get(kid, {})
        monthly = td.get("monthly_data") or []
        points = []
        for row in monthly:
            points.append({
                "month": row.get("month"),
                "month_name": row.get("month_name"),
                "year": row.get("year"),
                "plan": row.get("plan"),
                "fact": row.get("fact"),
            })
        series.append({
            "kpi_id": kid,
            "name": kpi_meta.get("name", kid),
            "chart_type": "line_plan_fact_monthly",
            "chart_type_label": f"План/Факт по месяцам: {kpi_meta.get('name', kid)}",
            "points": points,
        })
    return {
        "kpi_id": "FND-C1",
        "name": meta.get("name", "Тренд 12 месяцев: выручка / EBITDA / валовая маржа"),
        "periodicity": "ежемесячно",
        "chart_type": meta.get("chart_type", "combo_bar_line_monthly"),
        "chart_type_label": "Линейный тренд по месяцам (план/факт)",
        "series": series,
    }


def _build_chart_c2(by_id: dict, tiles_data: dict, ref_y: int, ref_m: int) -> dict:
    """FND-C2: Waterfall EBITDA — разложение отклонения на драйверы."""
    meta = by_id.get("FND-C2", {})
    t2 = tiles_data.get("FND-T2", {})
    lm = t2.get("last_full_month_row") or {}
    plan_val = lm.get("plan", 27_800_000)
    fact_val = lm.get("fact", 26_980_000)
    delta = fact_val - plan_val

    drivers = [
        {"name": "План EBITDA", "value": plan_val, "type": "total"},
        {"name": "Выручка", "value": round(delta * 0.45), "type": "driver"},
        {"name": "Маржа", "value": round(delta * 0.25), "type": "driver"},
        {"name": "Скидки", "value": round(delta * -0.10), "type": "driver"},
        {"name": "Постоянные расходы", "value": round(delta * 0.30), "type": "driver"},
        {"name": "Разовые эффекты", "value": round(delta * 0.10), "type": "driver"},
        {"name": "Факт EBITDA", "value": fact_val, "type": "total"},
    ]
    return {
        "kpi_id": "FND-C2",
        "name": meta.get("name", "Waterfall EBITDA: план → факт"),
        "periodicity": "ежемесячно",
        "chart_type": "waterfall",
        "chart_type_label": "Waterfall EBITDA",
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
        "drivers": drivers,
    }


def _build_chart_c3(by_id: dict, tiles_data: dict, ref_y: int, ref_m: int) -> dict:
    """FND-C3: Heatmap по директорам/контурам."""
    meta = by_id.get("FND-C3", {})
    directors = [
        "Коммерческий директор",
        "Финансовый директор",
        "Технический директор",
        "Директор по производству",
        "Директор по качеству",
        "Директор по персоналу",
    ]
    kpi_columns = ["Выручка", "Прибыль", "Отгрузка", "Качество", "Кадры"]
    heatmap_rows = []
    statuses = ["green", "yellow", "red", "green", "yellow"]
    for i, d in enumerate(directors):
        cells = []
        for j, col in enumerate(kpi_columns):
            idx = (i + j) % len(statuses)
            cells.append({"kpi": col, "status": statuses[idx]})
        worst = "red" if any(c["status"] == "red" for c in cells) else \
                "yellow" if any(c["status"] == "yellow" for c in cells) else "green"
        heatmap_rows.append({"director": d, "overall_status": worst, "cells": cells})

    return {
        "kpi_id": "FND-C3",
        "name": meta.get("name", "Heatmap по директорам / контурам"),
        "periodicity": "ежемесячно",
        "chart_type": "heatmap_rag",
        "chart_type_label": "Heatmap RAG",
        "period": {"year": ref_y, "month": ref_m, "month_name": MONTH_NAMES[ref_m]},
        "kpi_columns": kpi_columns,
        "rows": heatmap_rows,
    }


# ═══════════════════════════════════════════════════════════════
#  Главная сборка payload
# ═══════════════════════════════════════════════════════════════

TILE_IDS = [
    "FND-T1", "FND-T2", "FND-T3", "FND-T4", "FND-T5",
    "FND-T6", "FND-T7", "FND-T9",
]

# Блок «Председатель / коммерция» (плитки MRK-*)
CHAIRMAN_BLOCK_COMMERCE = "commerce"
CHAIRMAN_BLOCK_MY_DASHBOARD = "my_dashboard"

# Реестр значений query-параметра `for` (порядок — для UI; дополняйте новыми dict).
CHAIRMAN_FOR_BLOCKS: tuple[dict[str, str | tuple[str, ...]], ...] = (
    {
        "id": CHAIRMAN_BLOCK_MY_DASHBOARD,
        "label": "",
        "aliases": (
            "my_dashboard",
            "mydashboard",
            "dashboard",
            "мой_дашборд",
            "мойдашборд",
        ),
    },
    {
        "id": CHAIRMAN_BLOCK_COMMERCE,
        "label": "Коммерческий блок",
        "aliases": ("commerce", "коммерция", "commercial"),
    },
)

_ALIAS_TO_FOR_ID: dict[str, str] = {}
for _blk in CHAIRMAN_FOR_BLOCKS:
    bid = str(_blk["id"])
    _ALIAS_TO_FOR_ID[bid.lower()] = bid
    for _al in _blk["aliases"]:
        _ALIAS_TO_FOR_ID[str(_al).lower()] = bid


# Виртуальный блок ПСД → реальное подразделение, чью «ветку структуры» ПСД видит.
# my_dashboard не маппится (смотрит свою собственную ветку, сам ПСД).
CHAIRMAN_FOR_TARGET_DEPT: dict[str, str] = {
    CHAIRMAN_BLOCK_COMMERCE: "коммерческий директор",
}


def chairman_for_target_department(for_raw: str | None) -> str | None:
    """
    Подразделение, ПО которому ПСД «виртуально» смотрит при заданном for.
    Возвращает None, если for пустой или соответствует my_dashboard (своя ветка).
    """
    if for_raw is None or not str(for_raw).strip():
        return None
    block = normalize_chairman_for_param(for_raw)
    return CHAIRMAN_FOR_TARGET_DEPT.get(block)


def get_chairman_for_catalog() -> dict:
    """
    Каталог параметра for для председателя: id, подпись, алиасы, словарь id→label.
    Новые блоки — только дописать в CHAIRMAN_FOR_BLOCKS.
    """
    items = [
        {
            "id": b["id"],
            "label": b["label"],
            "aliases": list(b["aliases"]),
        }
        for b in CHAIRMAN_FOR_BLOCKS
    ]
    labels = {str(b["id"]): b["label"] for b in CHAIRMAN_FOR_BLOCKS}
    return {"items": items, "labels": labels}


COMMERCE_TILE_IDS = [f"MRK-{i:02d}" for i in range(1, 10)]

# Демо-факт на опорный месяц (значения с макета дашборда).
# ВАЖНО: MRK-01/02/03 считаются по данным КомДира, MRK-04 — из _mrk04_shipment_growth_yoy,
# MRK-06 — из calc_shipment_share_bmi_gazprom, MRK-09 — из calc_tenders_bmi.
# Числа ниже остаются только для плиток, у которых ещё нет реальных калькуляторов
# (MRK-10) — fallback-ветка в конце цикла.
_COMMERCE_FACT: dict[str, float | int] = {
    "MRK-10": 4,
}

_COMMERCE_RAG: dict[str, str] = {
    "MRK-01": "green", "MRK-02": "green", "MRK-03": "green",
    "MRK-04": "yellow", "MRK-05": "red", "MRK-06": "red",
    "MRK-07": "yellow", "MRK-08": "green", "MRK-09": "red",
    "MRK-10": "red",
}

_COMMERCE_PCT_IDS = frozenset({"MRK-04", "MRK-06", "MRK-07", "MRK-08", "MRK-09"})


def normalize_chairman_for_param(raw: str | None) -> str:
    """Нормализация query-параметра for для председателя (алиасы из CHAIRMAN_FOR_BLOCKS)."""
    if raw is None or not str(raw).strip():
        return CHAIRMAN_BLOCK_MY_DASHBOARD
    s = str(raw).strip().lower().replace(" ", "_").replace("-", "_")
    return _ALIAS_TO_FOR_ID.get(s, CHAIRMAN_BLOCK_MY_DASHBOARD)


def _komdir_commerce_context(
    month: int | None,
    year: int | None,
) -> tuple[int, int, list[tuple[int, int]], int]:
    """Те же ref_y, ref_m, pairs, series_m, что в komdir_dashboard.build_komdir_payload (агрегат комдира)."""
    today = date.today()
    if month and year:
        ref_y, ref_m = year, month
        pairs = [(year, mm) for mm in range(1, month + 1)]
        series_m = month
    else:
        pairs_lm, ref_y, ref_m = _get_monthly_pairs()
        series_m = _series_through_month(today, ref_y, ref_m)
        if ref_y == today.year:
            pairs = [(ref_y, mm) for mm in range(1, series_m + 1)]
        else:
            pairs = pairs_lm
    return ref_y, ref_m, pairs, series_m


def _to_int_or_none(x) -> int | None:
    """Приводит рублёвую сумму к целому числу (rub, без масштабирования и «млн»)."""
    if x is None:
        return None
    try:
        return int(round(float(x)))
    except (TypeError, ValueError):
        return None


def _ytd_sum_plan_fact(monthly_data: list[dict]) -> dict[str, float | None]:
    """Накопительно: сумма плана и факта по строкам monthly_data (янв.–опорный месяц)."""
    plans = [r.get("plan") for r in monthly_data if r.get("plan") is not None]
    facts = [r.get("fact") for r in monthly_data if r.get("fact") is not None]
    sp = float(sum(plans)) if plans else None
    sf = float(sum(facts)) if facts else None
    kpi_pct = round(sf / sp * 100, 1) if sp and sf is not None else None
    return {"total_plan": sp, "total_fact": sf, "kpi_pct": kpi_pct}


def _to_float_or_none(x) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _mrk_conversion_rag(pct: float | None) -> str:
    """RAG для MRK-07/08: ≥95 — зелёный, 90–94.9 — жёлтый, <90 — красный."""
    if pct is None:
        return "unknown"
    if pct >= 95:
        return "green"
    if pct >= 90:
        return "yellow"
    return "red"


def _mrk05_cash_gap_rag(value: float | None) -> str:
    """RAG для MRK-05: неотрицательный разрыв — зелёный, отрицательный — красный."""
    if value is None:
        return "unknown"
    return "green" if value >= 0 else "red"


def _build_cash_gap_detail(
    money_monthly: list[dict],
    shipments_monthly: list[dict],
    ref_y: int,
    ref_m: int,
) -> dict:
    """MRK-05: кассовый разрыв по месяцам = деньги факт - отгрузки факт."""
    months = _month_pairs(ref_y, ref_m)
    money_by_month = {
        int(row.get("month")): _to_float_or_none(row.get("fact"))
        for row in (money_monthly or [])
        if row.get("month") is not None
    }
    ship_by_month = {
        int(row.get("month")): _to_float_or_none(row.get("fact"))
        for row in (shipments_monthly or [])
        if row.get("month") is not None
    }

    monthly_rows: list[dict] = []
    ref_row = None
    months_with_data = 0
    for mm in months:
        money = money_by_month.get(mm)
        ship = ship_by_month.get(mm)
        has_data = money is not None or ship is not None
        gap = None if (money is None and ship is None) else (money or 0.0) - (ship or 0.0)
        row = {
            "month": mm,
            "year": ref_y,
            "month_name": MONTH_NAMES_RU[mm],
            "plan": None,
            "fact": _to_int_or_none(gap),
            "kpi_pct": None,
            "has_data": has_data,
            "money_fact": _to_int_or_none(money),
            "shipments_fact": _to_int_or_none(ship),
        }
        monthly_rows.append(row)
        if has_data:
            months_with_data += 1
        if mm == ref_m:
            ref_row = row

    return {
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "ytd": {
            "total_plan": None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": None,
            "months_with_data": months_with_data,
            "months_total": len(monthly_rows),
        },
        "cash_gap_detail": {
            "money_label": "Деньги полученные",
            "shipments_label": "Отгрузки произведенные",
            "period_start": f"{ref_y}-01-01",
            "period_end": f"{ref_y}-{ref_m:02d}-01",
        },
    }


def _build_conversion_monthly_detail(
    numerator_monthly: list[dict],
    denominator_monthly: list[dict],
    ref_y: int,
    ref_m: int,
    *,
    numerator_label: str,
    denominator_label: str,
) -> dict:
    """
    Помесячная конверсия: Отгрузки (факт) / Договоры (факт) * 100 за каждый месяц.
    plan  = знаменатель месяца (Договоры / Деньги),
    fact  = числитель  месяца (Отгрузки),
    kpi_pct = fact / plan * 100 за этот месяц.

    На фронтенде при агрегации (квартал / YTD) plan и fact складываются
    по правилу «план с планом, факт с фактом», а процент пересчитывается
    как sum(fact) / sum(plan) * 100 — это обеспечивает корректное
    нарастающее значение.

    Используется для MRK-07 и MRK-08 в коммерческом блоке ПСД.
    """
    months = _month_pairs(ref_y, ref_m)
    num_by_month = {
        int(row.get("month")): _to_float_or_none(row.get("fact"))
        for row in (numerator_monthly or [])
        if row.get("month") is not None
    }
    den_by_month = {
        int(row.get("month")): _to_float_or_none(row.get("fact"))
        for row in (denominator_monthly or [])
        if row.get("month") is not None
    }

    monthly_rows: list[dict] = []
    ytd_num = 0.0
    ytd_den = 0.0
    ref_row = None
    months_with_data = 0

    for mm in months:
        num = num_by_month.get(mm)
        den = den_by_month.get(mm)
        has_data = num is not None or den is not None
        if num is not None:
            ytd_num += num
        if den is not None:
            ytd_den += den
        plan_m = _to_int_or_none(den)
        fact_m = _to_int_or_none(num)
        pct_m = (
            round(num / den * 100, 1)
            if (num is not None and den is not None and den > 0)
            else None
        )
        row = {
            "month": mm,
            "year": ref_y,
            "month_name": MONTH_NAMES_RU[mm],
            "plan": plan_m,
            "fact": fact_m,
            "kpi_pct": pct_m,
            "has_data": has_data,
            "numerator_fact": fact_m,
            "denominator_fact": plan_m,
            "numerator_label": numerator_label,
            "denominator_label": denominator_label,
        }
        monthly_rows.append(row)
        if has_data:
            months_with_data += 1
        if mm == ref_m:
            ref_row = row

    ref_pct = ref_row.get("kpi_pct") if ref_row else None
    return {
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "ref": {
            "plan": ref_row.get("plan") if ref_row else None,
            "fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_pct,
            "months_with_data": months_with_data,
            "months_total": len(monthly_rows),
        },
        "conversion_detail": {
            "numerator_label": numerator_label,
            "denominator_label": denominator_label,
            "ytd_numerator_fact": _to_int_or_none(ytd_num),
            "ytd_denominator_fact": _to_int_or_none(ytd_den),
            "period_start": f"{ref_y}-01-01",
            "period_end": f"{ref_y}-{ref_m:02d}-01",
        },
    }


def _mrk04_shipment_growth_yoy(ref_y: int, ref_m: int, series_m: int) -> tuple[float | None, dict]:
    """
    Рост отгрузок текущего года к предыдущему: отношение факт/план, где
      план = сумма отгрузок за янв..m предыдущего года,
      факт = сумма отгрузок за янв..m текущего года,
      m — «текущий период» (в контексте коммерции берём series_m из komdir_dashboard:
           не раньше последнего полного месяца и не позже текущего календарного).
    Возвращает одну цифру kpi_pct = факт / план * 100 (%).
    Источники и dept_guid=None — те же, что у плитки KD-M2 коммерческого директора.

    Дополнительно в detail кладётся помесячная разбивка отгрузок по предыдущему
    и текущему году за янв..m.
    """
    today = date.today()
    current_y = int(ref_y or today.year)
    previous_y = current_y - 1
    # Сравниваем одинаковое количество месяцев в обоих годах (янв..m).
    m = max(1, min(12, int(series_m) if series_m else int(ref_m) if ref_m else today.month))

    prev_payload = cache_manager.locked_call(
        f"otgruzki_{previous_y}_{m}",
        calc_otgruzki_fact.get_otgruzki_monthly,
        year=previous_y,
        month=m,
        dept_guid=None,
    )
    current_payload = cache_manager.locked_call(
        f"otgruzki_{current_y}_{m}",
        calc_otgruzki_fact.get_otgruzki_monthly,
        year=current_y,
        month=m,
        dept_guid=None,
    )

    def _by_month(rows: list[dict]) -> dict[int, float]:
        out: dict[int, float] = {}
        for r in rows or []:
            mm = int(r.get("month") or 0)
            if 1 <= mm <= 12:
                out[mm] = float(r.get("fact") or 0)
        return out

    prev_by_month = _by_month(prev_payload.get("months") or [])
    current_by_month = _by_month(current_payload.get("months") or [])

    monthly_rows: list[dict] = []
    for mm in range(1, m + 1):
        prev_value = prev_by_month.get(mm, 0.0)
        current_value = current_by_month.get(mm, 0.0)
        monthly_rows.append({
            "month": mm,
            "year": current_y,
            "month_name": MONTH_NAMES_RU[mm],
            # Для агрегации председательских плиток:
            # plan = прошлый год, fact = текущий год.
            "plan": _to_int_or_none(prev_value),
            "fact": _to_int_or_none(current_value),
            "previous_year_value": _to_int_or_none(prev_value),
            "current_year_value": _to_int_or_none(current_value),
            "kpi_pct": round(current_value / prev_value * 100, 1) if prev_value > 0 else None,
            "has_data": abs(prev_value) > 0 or abs(current_value) > 0,
        })

    prev_sum = sum((r.get("plan") or 0) for r in monthly_rows)
    current_sum = sum((r.get("fact") or 0) for r in monthly_rows)
    detail = {
        "compare_months": m,
        "previous_year": previous_y,
        "current_year": current_y,
        "months_previous_year": m,
        "months_current_year": m,
        "sum_shipments_rub_previous_year": prev_sum,
        "sum_shipments_rub_current_year": current_sum,
        # Оставляем старые ключи для обратной совместимости.
        "sum_shipments_rub_2025": prev_sum,
        "sum_shipments_rub_2026": current_sum,
        "label": f"Янв.–{MONTH_NAMES_RU[m]} {current_y} к янв.–{MONTH_NAMES_RU[m]} {previous_y}",
        "monthly_comparison": monthly_rows,
    }
    if prev_sum <= 0:
        return None, detail
    pct = round(current_sum / prev_sum * 100, 1)
    return pct, detail


def _mrk04_rag(growth_pct: float | None) -> str:
    """RAG для факт/план·100 (%): ≥100 — зелёный, 90–99.9 — жёлтый, <90 — красный."""
    if growth_pct is None:
        return "unknown"
    if growth_pct >= 100:
        return "green"
    if growth_pct >= 90:
        return "yellow"
    return "red"


def _mrk_plan_fact_rag(kpi_pct: float | None) -> str:
    """
    RAG для MRK-01/02/03 (коммерческий блок ПСД): исполнение плана в %.
    ≥100 — зелёный, 90–100 — жёлтый, <90 — красный.
    """
    if kpi_pct is None:
        return "unknown"
    if kpi_pct >= 100:
        return "green"
    if kpi_pct >= 90:
        return "yellow"
    return "red"


def _mrk09_monthly_ytd(ref_y: int, ref_m: int) -> list[dict]:
    """
    Помесячный ряд для MRK-09: для каждого месяца m ∈ [1..12] считается
    процент выигранных тендеров БМИ **за этот конкретный месяц**
    (от 1 по последний день месяца), не накопительно.

    Для будущих месяцев текущего года — None (данных ещё нет).

    Возвращает список точек для графика/агрегации:
      [{"month", "year", "month_name", "plan", "fact", "kpi_pct", "has_data"}]
    """
    today = date.today()
    points: list[dict] = []
    for m in range(1, 13):
        is_future = (ref_y == today.year and m > today.month)
        if is_future:
            points.append({
                "month": m,
                "year": ref_y,
                "month_name": MONTH_NAMES_RU[m],
                "plan": None,
                "fact": None,
                "kpi_pct": None,
                "has_data": False,
            })
            continue
        # Только за этот месяц, без накопления с начала года.
        data = cache_manager.locked_call(
            f"tenders_bmi_monthly_{ref_y}_{m:02d}",
            calc_tenders_bmi.get_tenders_bmi,
            year=ref_y,
            month=m,
            cumulative=False,
        )
        plan = int(data.get("plan") or 0)
        fact = int(data.get("fact") or 0)
        pct = data.get("pct")
        points.append({
            "month": m,
            "year": ref_y,
            "month_name": MONTH_NAMES_RU[m],
            "plan": plan,
            "fact": fact,
            "found": int(data.get("found") or plan),
            "won": int(data.get("won") or fact),
            "not_participating": int(data.get("not_participating") or 0),
            "status_counts": data.get("status_counts") or {},
            "kpi_pct": pct,
            "has_data": plan > 0,
        })
    return points


def _mrk09_rag(pct: float | None) -> str:
    """RAG для MRK-09 по порогам из kpi-справочника: ≥25 — зелёный, 15–24.9 — жёлтый, <15 — красный."""
    if pct is None:
        return "unknown"
    if pct >= 25:
        return "green"
    if pct >= 15:
        return "yellow"
    return "red"


def _mrk06_share_bmi_gazprom(ref_y: int, ref_m: int) -> dict:
    """
    Данные плитки MRK-06 «Доля Газпром + БМИ в отгрузке».
    Окно «с 01.01 ref_y по сегодня» для текущего года, «весь год» — для прошлых
    (плитка не помесячная, поэтому не ограничиваем по ref_m).
    """
    today = date.today()
    if ref_y == today.year:
        return cache_manager.locked_call(
            f"share_bmi_gp_{ref_y}_{today.isoformat()}",
            calc_shipment_share_bmi_gazprom.get_shipment_share_bmi_gazprom,
            year=ref_y,
            month=today.month,
        )
    return cache_manager.locked_call(
        f"share_bmi_gp_{ref_y}_12",
        calc_shipment_share_bmi_gazprom.get_shipment_share_bmi_gazprom,
        year=ref_y,
        month=12,
    )


def _mrk06_share_bmi_gazprom_monthly(ref_y: int, ref_m: int) -> dict:
    """
    Помесячная разбивка MRK-06 «Доля Газпром + БМИ в отгрузке» за янв..ref_m.
    Возвращает dict с ключом 'months' (см. calc_shipment_share_bmi_gazprom_monthly).
    """
    today = date.today()
    end_m = today.month if ref_y == today.year else 12
    return cache_manager.locked_call(
        f"share_bmi_gp_monthly_{ref_y}_{end_m:02d}",
        calc_shipment_share_bmi_gazprom.get_shipment_share_bmi_gazprom_monthly,
        year=ref_y,
        month=end_m,
    )


def _mrk06_rag(pct: float | None) -> str:
    """RAG для MRK-06 (меньше = лучше): ≤70 — зелёный, 70.1–75 — жёлтый, >75 — красный."""
    if pct is None:
        return "unknown"
    if pct <= 70:
        return "green"
    if pct <= 75:
        return "yellow"
    return "red"


def _get_commerce_tile_data(kpi_id: str, months: list[int], ref_y: int, ref_m: int) -> dict:
    fact = _COMMERCE_FACT.get(kpi_id)
    rows = []
    for m in months:
        rows.append({
            "month": m, "year": ref_y, "month_name": MONTH_NAMES[m],
            "plan": None, "fact": fact,
            "kpi_pct": fact if kpi_id in _COMMERCE_PCT_IDS else None,
            "has_data": fact is not None,
        })
    ref_row = None
    for r in rows:
        if r["year"] == ref_y and r["month"] == ref_m:
            ref_row = r
            break
    with_data = [r for r in rows if r.get("has_data")]
    facts = [r["fact"] for r in with_data if r.get("fact") is not None]
    return {
        "monthly_data": rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "ytd": {
            "total_plan": None,
            "total_fact": sum(facts) if facts else None,
            "kpi_pct": None,
            "months_with_data": len(with_data),
            "months_total": len(rows),
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
    }


def build_chairman_commerce_payload(
    kpi_list: list[dict],
    month: int | None = None,
    year: int | None = None,
) -> dict:
    """
    Блок «Председатель / коммерция»: MRK-01…03 из тех же данных, что KD-M2/M3/M1 у коммерческого директора;
    на плитках — план/факт за опорный месяц; в monthly_data — помесячно для графиков.
    MRK-04 — рост отгрузок текущего года к предыдущему + помесячные отгрузки по обоим годам.
    MRK-05…10 — заглушки.
    """
    by_id = {k["kpi_id"]: k for k in kpi_list}

    ref_y, ref_m, pairs, series_m = _komdir_commerce_context(month, year)
    plans_payload = cache_manager.locked_call(
        f"plans_{ref_y}_{series_m}",
        calc_plan.get_plans_monthly,
        year=ref_y,
        month=series_m,
        dept_guid=None,
    )

    td_m1 = _komdir_get_tile_data(
        "KD-M1", pairs, ref_y, ref_m, series_m,
        dz_payload=None, dept_guid=None, plans_payload=plans_payload,
    )
    td_m2 = _komdir_get_tile_data(
        "KD-M2", pairs, ref_y, ref_m, series_m,
        dz_payload=None, dept_guid=None, plans_payload=plans_payload,
    )
    td_m3 = _komdir_get_tile_data(
        "KD-M3", pairs, ref_y, ref_m, series_m,
        dz_payload=None, dept_guid=None, plans_payload=plans_payload,
    )

    komdir_for_chart = {"KD-M1": td_m1, "KD-M2": td_m2, "KD-M3": td_m3}
    mrk_from_komdir: dict[str, tuple[str, dict]] = {
        "MRK-01": ("KD-M2", td_m2),
        "MRK-02": ("KD-M3", td_m3),
        "MRK-03": ("KD-M1", td_m1),
    }

    month_label = f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}"

    plitki_items: list[dict] = []
    months_stub = _month_pairs(ref_y, ref_m)
    tile_ids = [kid for kid in COMMERCE_TILE_IDS if kid in by_id]

    for kid in tile_ids:
        meta = by_id.get(kid)
        if not meta:
            continue

        if kid in mrk_from_komdir:
            kd_id, _td = mrk_from_komdir[kid]
            lm = _td.get("last_full_month_row") or {}
            kpi_pct = lm.get("kpi_pct")
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": kpi_pct,
                "color": _mrk_plan_fact_rag(kpi_pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": "руб.",
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": _to_int_or_none(lm.get("plan")),
                "fact": _to_int_or_none(lm.get("fact")),
                "has_data": bool(lm.get("has_data")),
                "plan_fact_period_label": month_label,
                "monthly_data": _td.get("monthly_data"),
            })
            continue

        if kid == "MRK-04":
            growth_pct, growth_detail = _mrk04_shipment_growth_yoy(ref_y, ref_m, series_m)
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": growth_pct,
                "color": _mrk04_rag(growth_pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": _to_int_or_none(growth_detail.get("sum_shipments_rub_previous_year")),
                "fact": _to_int_or_none(growth_detail.get("sum_shipments_rub_current_year")),
                "has_data": bool(
                    growth_pct is not None
                    or growth_detail.get("sum_shipments_rub_previous_year")
                    or growth_detail.get("sum_shipments_rub_current_year")
                ),
                "plan_fact_period_label": growth_detail.get("label", ""),
                "monthly_data": growth_detail.get("monthly_comparison") or [],
                "yoy_detail": growth_detail,
            })
            continue

        if kid == "MRK-05":
            cash_gap = _build_cash_gap_detail(
                td_m1.get("monthly_data") or [],
                td_m2.get("monthly_data") or [],
                ref_y,
                ref_m,
            )
            lm = cash_gap.get("last_full_month_row") or {}
            fact = lm.get("fact")
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": None,
                "color": _mrk05_cash_gap_rag(_to_float_or_none(fact)),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": "Деньги полученные (факт) − Отгрузки произведенные (факт)",
                "unit": "руб.",
                "source": "1С / коммерция",
                "frequency": meta.get("frequency"),
                "plan": None,
                "fact": fact,
                "has_data": fact is not None,
                "plan_fact_period_label": month_label,
                "monthly_data": cash_gap.get("monthly_data") or [],
                "cash_gap_detail": cash_gap.get("cash_gap_detail") or {},
            })
            continue

        if kid == "MRK-06":
            share_monthly = _mrk06_share_bmi_gazprom_monthly(ref_y, ref_m)
            months_raw = share_monthly.get("months") or []
            monthly_data: list[dict] = []
            ref_row = None
            for row in months_raw:
                mm = int(row.get("month") or 0)
                if mm < 1 or mm > 12:
                    continue
                total_m = _to_float_or_none(row.get("total"))
                pair_m = _to_float_or_none(row.get("pair"))
                pct_m = _to_float_or_none(row.get("pct_pair"))
                has_data_row = (total_m is not None and abs(total_m) > 0) or (
                    pair_m is not None and abs(pair_m) > 0
                )
                md_row = {
                    "month": mm,
                    "year": ref_y,
                    "month_name": MONTH_NAMES_RU[mm],
                    # plan = общая отгрузка, fact = БМИ+Газпром.
                    # При агрегации фронт складывает plan с планом, fact с фактом
                    # и пересчитывает процент как sum(fact)/sum(plan)*100.
                    "plan": round(total_m, 2) if total_m is not None else None,
                    "fact": round(pair_m, 2) if pair_m is not None else None,
                    "kpi_pct": pct_m,
                    "has_data": has_data_row,
                }
                monthly_data.append(md_row)
                if mm == ref_m:
                    ref_row = {
                        "plan": md_row["plan"],
                        "fact": md_row["fact"],
                        "kpi_pct": md_row["kpi_pct"],
                        "has_data": md_row["has_data"],
                        "by_dept": row.get("by_dept") or {},
                        "bmi": row.get("bmi"),
                        "gp": row.get("gp"),
                    }

            ref_pct = ref_row.get("kpi_pct") if ref_row else None
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": ref_pct,
                "color": _mrk06_rag(ref_pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": ref_row.get("plan") if ref_row else None,
                "fact": ref_row.get("fact") if ref_row else None,
                "has_data": bool(ref_row and ref_row.get("has_data")),
                "plan_fact_period_label": f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}",
                "monthly_data": monthly_data,
                "share_detail": {
                    "by_dept": ref_row.get("by_dept") if ref_row else {},
                    "bmi": ref_row.get("bmi") if ref_row else None,
                    "gp": ref_row.get("gp") if ref_row else None,
                    "period_year": ref_y,
                    "period_month": ref_m,
                },
            })
            continue

        if kid == "MRK-07":
            conv = _build_conversion_monthly_detail(
                td_m2.get("monthly_data") or [],
                td_m3.get("monthly_data") or [],
                ref_y,
                ref_m,
                numerator_label="Отгрузки произведенные",
                denominator_label="Договоры заключенные",
            )
            ref_info = conv.get("ref") or {}
            pct = ref_info.get("kpi_pct")
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": pct,
                "color": _mrk_conversion_rag(pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": "Отгрузки произведенные (факт) / Договоры заключенные (факт) * 100",
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": ref_info.get("plan"),
                "fact": ref_info.get("fact"),
                "has_data": pct is not None,
                "plan_fact_period_label": f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}",
                "monthly_data": conv.get("monthly_data") or [],
                "conversion_detail": conv.get("conversion_detail") or {},
            })
            continue

        if kid == "MRK-08":
            conv = _build_conversion_monthly_detail(
                td_m2.get("monthly_data") or [],
                td_m1.get("monthly_data") or [],
                ref_y,
                ref_m,
                numerator_label="Отгрузки произведенные",
                denominator_label="Деньги полученные",
            )
            ref_info = conv.get("ref") or {}
            pct = ref_info.get("kpi_pct")
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": pct,
                "color": _mrk_conversion_rag(pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": "Отгрузки произведенные (факт) / Деньги полученные (факт) * 100",
                "unit": meta.get("unit"),
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": ref_info.get("plan"),
                "fact": ref_info.get("fact"),
                "has_data": pct is not None,
                "plan_fact_period_label": f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}",
                "monthly_data": conv.get("monthly_data") or [],
                "conversion_detail": conv.get("conversion_detail") or {},
            })
            continue

        if kid == "MRK-09":
            # Плитка считается ТОЛЬКО по выбранному месяцу ref_m (не нарастающим итогом).
            tenders = cache_manager.locked_call(
                f"tenders_bmi_monthly_{ref_y}_{ref_m:02d}",
                calc_tenders_bmi.get_tenders_bmi,
                year=ref_y,
                month=ref_m,
                cumulative=False,
            )
            plan_n = int(tenders.get("plan") or 0)
            fact_n = int(tenders.get("fact") or 0)
            not_part_n = int(tenders.get("not_participating") or 0)
            pct = tenders.get("pct")
            monthly_ytd = _mrk09_monthly_ytd(ref_y, ref_m)
            plitki_items.append({
                "kpi_id": kid,
                "name": meta["name"],
                "goal": meta.get("goal"),
                "kpi_pct": pct,
                "color": _mrk09_rag(pct),
                "period": _period_label(meta),
                "thresholds": _thresholds(meta),
                "formula": meta.get("formula"),
                "unit": "шт",
                "source": meta.get("source"),
                "frequency": meta.get("frequency"),
                "plan": plan_n,
                "fact": fact_n,
                "found": plan_n,
                "won": fact_n,
                "not_participating": not_part_n,
                "status_counts": tenders.get("status_counts") or {},
                "has_data": plan_n > 0,
                "plan_fact_period_label": f"{MONTH_NAMES_RU[ref_m].capitalize()} {ref_y}",
                "monthly_data": monthly_ytd,
                "tenders_detail": {
                    "distribution": tenders.get("distribution") or {},
                    "status_counts": tenders.get("status_counts") or {},
                    "samples": tenders.get("samples") or [],
                    "year": tenders.get("year"),
                    "month": tenders.get("month"),
                    "period_start": tenders.get("period_start"),
                    "period_end": tenders.get("period_end"),
                },
            })
            continue

        td_stub = _get_commerce_tile_data(kid, months_stub, ref_y, ref_m)
        lm = td_stub.get("last_full_month_row")
        unit = (meta.get("unit") or "").lower()
        is_pct = "%" in unit or "процент" in unit
        fact_val = lm.get("fact") if lm else None
        kpi_pct = float(fact_val) if (is_pct and fact_val is not None) else None
        plan_raw = lm.get("plan") if lm else None
        fact_raw = lm.get("fact") if lm else None
        plitki_items.append({
            "kpi_id": kid,
            "name": meta["name"],
            "goal": meta.get("goal"),
            "kpi_pct": kpi_pct,
            "color": _COMMERCE_RAG.get(kid, "unknown"),
            "period": _period_label(meta),
            "thresholds": _thresholds(meta),
            "formula": meta.get("formula"),
            "unit": meta.get("unit"),
            "source": meta.get("source"),
            "frequency": meta.get("frequency"),
            "plan": (plan_raw if is_pct else _to_int_or_none(plan_raw)),
            "fact": (fact_raw if is_pct else _to_int_or_none(fact_raw)),
            "has_data": lm.get("has_data", True) if lm else False,
            "plan_fact_period_label": f"{MONTH_NAMES[ref_m].capitalize()} {ref_y}",
            "monthly_data": td_stub.get("monthly_data"),
        })

    by_id_chart = {
        "KD-C1": {"name": "Динамика: деньги, отгрузки, договоры"},
        "KD-M1": {"name": (by_id.get("MRK-03") or {}).get("name", "Деньги")},
        "KD-M2": {"name": (by_id.get("MRK-01") or {}).get("name", "Отгрузки")},
        "KD-M3": {"name": (by_id.get("MRK-02") or {}).get("name", "Договоры")},
    }
    chart = _build_line_chart(by_id_chart, komdir_for_chart)
    chart["kpi_id"] = "MRK-C1"
    chart["name"] = "План/факт по месяцам (отгрузки, договоры, деньги)"
    # Порядок серий как у плиток MRK-01 → MRK-02 → MRK-03 (в KD-C1 было M1, M2, M3)
    ser = chart.get("series") or []
    if len(ser) == 3:
        chart["series"] = [ser[1], ser[2], ser[0]]

    from .views import _fetch_claims_rows_for_department, _fetch_lawsuits_rows_for_department
    try:
        claims_rows = _fetch_claims_rows_for_department(ref_y, ref_m, 'коммерческий директор')
    except Exception:
        claims_rows = []
    try:
        lawsuits_rows = _fetch_lawsuits_rows_for_department(ref_y, ref_m, 'коммерческий директор')
    except Exception:
        lawsuits_rows = []

    # KD-T-OVERDUE — та же таблица просроченной ДЗ, что и у коммерческого директора
    try:
        from .komdir_dashboard import _build_overdue_table
        overdue_table = _build_overdue_table(ref_y, ref_m, dept_guid=None)
    except Exception:
        overdue_table = None

    month_name = MONTH_NAMES.get(ref_m, str(ref_m))

    tables: dict = {
        'KD-T-CLAIMS': {
            'name': f'Претензии за {month_name} {ref_y}',
            'periodicity': 'ежемесячно',
            'description': 'Претензии из 1С (Catalog_Претензии) за выбранный месяц',
            'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
            'rows': claims_rows,
        },
        'KD-T-LAWSUITS': {
            'name': f'Суды за {month_name} {ref_y}',
            'periodicity': 'ежемесячно',
            'description': (
                'Судебные споры и исковая работа из 1С '
                '(Document_ТД_ПретензииСудебныеСпорыИсковаяРабота) за выбранный месяц'
            ),
            'period': {'year': ref_y, 'month': ref_m, 'month_name': month_name},
            'columns': [
                'Номер', 'Статус', 'Тип документа', 'Контрагент',
                'Предмет спора', 'Сумма требований',
                'Роль ГК в споре', 'Площадка (юрлицо ГК)',
                'Подразделение инициатора',
            ],
            'rows': lawsuits_rows,
        },
    }
    if overdue_table is not None:
        tables['KD-T-OVERDUE'] = overdue_table

    grafiki: dict = {"MRK-C1": chart}

    # Планы блока «КС развитие» — помесячные круговые диаграммы по паре
    # (отдел × показатель). Для коммерческого блока ПСД показываем все
    # документы по всем подразделениям.
    try:
        from . import calc_ks_razvitie
        ks_plans = cache_manager.locked_call(
            f"ks_razvitie_{ref_y}",
            calc_ks_razvitie.get_ks_razvitie_plans,
            year=ref_y,
        )
        grafiki["KS-RAZVITIE"] = {
            "kpi_id": "KS-RAZVITIE",
            "name": "КС развитие — планы по месяцам",
            "periodicity": "ежемесячно",
            "chart_type": "donut_multiple_monthly",
            "chart_type_label": "Круговые диаграммы по месяцам (КС развитие)",
            "period": {"year": ref_y, "month": ref_m, "month_name": month_name},
            "indicators": ks_plans.get("indicators") or [],
            "indicator_units": ks_plans.get("indicator_units") or {},
            "months": ks_plans.get("months") or {},
            "by_dept": ks_plans.get("by_dept") or {},
            "by_dept_guid": ks_plans.get("by_dept_guid") or {},
            "dept_indicators": ks_plans.get("dept_indicators") or {},
            "charts": ks_plans.get("charts") or [],
        }
    except Exception:
        pass

    return {
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": grafiki,
        "Таблицы": tables,
    }


def build_chairman_payload_by_for(
    kpi_list: list[dict],
    month: int | None = None,
    year: int | None = None,
    for_raw: str | None = None,
) -> tuple[dict, str]:
    """
    Сборка payload председателя по блоку просмотра.
    Возвращает (payload, нормализованный for).
    """
    block = normalize_chairman_for_param(for_raw)
    if block == CHAIRMAN_BLOCK_COMMERCE:
        mkpis = [k for k in kpi_list if str(k.get("kpi_id", "")).startswith("MRK-")]
        return build_chairman_commerce_payload(mkpis, month=month, year=year), block
    return build_chairman_payload(kpi_list, month=month, year=year), block


def build_chairman_payload(
    kpi_list: list[dict],
    month: int | None = None,
    year: int | None = None,
) -> dict:
    by_id = {k["kpi_id"]: k for k in kpi_list}

    if month and year:
        ref_y, ref_m = year, month
    else:
        today = date.today()
        ref_y, ref_m = today.year, today.month

    months = _month_pairs(ref_y, ref_m)

    tile_ids = [kid for kid in TILE_IDS if kid in by_id]
    tiles_data: dict[str, dict] = {}
    for kid in tile_ids:
        tiles_data[kid] = _get_tile_data(kid, months, ref_y, ref_m)

    plitki_items: list[dict] = []

    for kid in tile_ids:
        meta = by_id.get(kid)
        if not meta:
            continue
        td = tiles_data[kid]
        lm = td.get("last_full_month_row")

        # Процент за ТЕКУЩИЙ месяц (для пилюли на лицевой стороне).
        month_pct = lm.get("kpi_pct") if lm else None
        if month_pct is not None:
            month_pct = float(month_pct)

        ytd_pct = td["ytd"].get("kpi_pct")
        if ytd_pct is not None:
            ytd_pct = float(ytd_pct)

        # Цвет RAG считаем по месячному проценту, а если его нет — по YTD.
        pct_for_rag = month_pct if month_pct is not None else ytd_pct
        color = _rag(kid, pct_for_rag)
        tile = {
            "kpi_id": kid,
            "name": meta["name"],
            "kpi_pct": month_pct,
            "ytd_pct": ytd_pct,
            "color": color,
            "period": _period_label(meta),
            "thresholds": _thresholds(meta),
            "formula": meta.get("formula"),
            "unit": meta.get("unit"),
            "source": meta.get("source"),
            "frequency": meta.get("frequency"),
            "plan": lm.get("plan") if lm else None,
            "fact": lm.get("fact") if lm else None,
            "has_data": lm.get("has_data", True) if lm else False,
            "plan_fact_period_label": f"{MONTH_NAMES[ref_m].capitalize()} {ref_y}",
            "monthly_data": td.get("monthly_data"),
        }

        # FND-T3 «Соотношение ДЗ и КЗ» — прокидываем custom-поля
        # ref-месяца: детализацию по клиентам/поставщикам и общий итог.
        if kid == "FND-T3" and lm:
            for extra_key in (
                "dz_client", "kz_client", "dz_supplier", "kz_supplier",
                "dz_total", "kz_total",
                "pct_client", "pct_supplier", "pct_total",
            ):
                if extra_key in lm:
                    tile[extra_key] = lm[extra_key]
        if kid == "FND-T6" and lm:
            for extra_key in ("portfolio_count", "deviation_count"):
                if extra_key in lm:
                    tile[extra_key] = lm[extra_key]
        if kid == "FND-T9":
            tile["unit"] = "руб."
            tile["thresholds"] = dict(_T9_THRESHOLDS)
            tile["green_threshold"] = _T9_THRESHOLDS["green"]
            tile["yellow_threshold"] = _T9_THRESHOLDS["yellow"]
            tile["red_threshold"] = _T9_THRESHOLDS["red"]
            if lm:
                for extra_key in ("plan_rub_total", "fact_rub_total", "fact_qty_total"):
                    if extra_key in lm:
                        tile[extra_key] = lm[extra_key]

        plitki_items.append(tile)

    grafiki = {
        "FND-C1": _build_chart_c1(by_id, tiles_data),
        "FND-C2": _build_chart_c2(by_id, tiles_data, ref_y, ref_m),
        "FND-C3": _build_chart_c3(by_id, tiles_data, ref_y, ref_m),
    }

    return {
        "Плитки": {"count": len(plitki_items), "items": plitki_items},
        "Графики": grafiki,
        "Таблицы": {},
    }


def is_chairman_department(dept: str) -> bool:
    d = dept.strip().lower()
    return "председатель" in d and "совет" in d and "директор" in d
