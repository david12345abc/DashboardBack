"""
YTD API коммерческого блока на SQL-эталонах comdir/.

Каждый get_*_ytd(year, month, dept_guid=None) возвращает серию январь..ref_month
в формате, совместимом с komdir_dashboard._get_tile_data / _build_plan_fact_tile.
"""
from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from comdir import (  # noqa: E402
    calc_plan_fact_cena as cena_mod,
    calc_plan_fact_dengi as dengi_mod,
    calc_plan_fact_dogovory as dog_mod,
    calc_plan_fact_fot as fot_mod,
    calc_plan_fact_otgruzki as otg_mod,
    calc_plan_fact_rashody as rash_mod,
    calc_plan_fact_tkp_sla as sla_mod,
    calc_plan_fact_vp as vp_mod,
)
from comdir.common import (  # noqa: E402
    MONTH_RU,
    aggregate_by_odata_bytes,
    aggregate_by_odata_name,
    connect_ctx,
    empty_error_payload,
    period_bounds,
    slice_months_by_dept,
)
from comdir.sql_tile_cache import get_ytd_via_cache, normalize_period  # noqa: E402
from getkpi.calc_fot import get_fot_plan  # noqa: E402
from getkpi.calc_rashody import get_rashody_plan  # noqa: E402
from getkpi.valovaya_pribyl import vp_plan_for_month  # noqa: E402

logger = logging.getLogger(__name__)

# v13: KD-M3 договоры — план без закрытых объектов, ожидаемо SQL (не HTTP).
CACHE_VERSION = 13


def _kpi_pct(fact, plan) -> float | None:
    try:
        p = float(plan)
        f = float(fact)
    except (TypeError, ValueError):
        return None
    if not p:
        return None
    return round(f / p * 100, 1)


def _build_ytd_payload(
    ref_y: int,
    ref_m: int,
    months: list[dict],
    *,
    kpi_id: str,
) -> dict[str, Any]:
    ref_row = None
    for row in months:
        if row.get("year") == ref_y and row.get("month") == ref_m:
            ref_row = row
            break
    plan = (ref_row or {}).get("plan")
    fact = (ref_row or {}).get("fact")
    calc = (ref_row or {}).get("calc")
    # KD-M9: plan = calc
    if calc is not None and plan is None:
        plan = calc
    pct = _kpi_pct(fact, plan if plan is not None else calc)
    return {
        "year": ref_y,
        "ref_month": ref_m,
        "months": months,
        "monthly_data": months,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "ytd": {
            "total_plan": plan,
            "total_fact": fact,
            "total_expected_plan": (ref_row or {}).get("expected"),
            "kpi_pct": pct,
            "months_with_data": sum(1 for r in months if r.get("fact") is not None),
            "months_total": len(months),
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_RU.get(ref_m, str(ref_m)),
            "data_complete": True,
        },
        "debug": {"status": "ok", "kpi_id": kpi_id, "source": "comdir.sql"},
    }


def _overlay_expected_from_1c(
    months: list[dict[str, Any]],
    year: int,
    ref_month: int,
    metric: str,
) -> list[dict[str, Any]]:
    """Подставить «ожидаемо» из кэша/сервиса 1С (calc_plan).

    Берём помесячные ``plans_{y}_{m}.json`` (там эталон HTTP-сервиса), а не
    устаревший агрегат ``plans_monthly_*.json``.

    Для прошлых лет не дергаем get_plans_monthly / OData: «ожидаемо» там не
    нужно для MRK-04 (YoY по факту), а fallback OData при 404 сервиса
    блокирует коммерческий блок ПСД на минуты.
    """
    if metric not in {"dengi", "otgruzki", "dogovory"}:
        return months
    key = f"{metric}_expected"
    try:
        from getkpi.calc_plan import _load_cache, get_plans_monthly
    except Exception as exc:
        logger.warning("comdir expected 1C overlay import failed for %s: %s", metric, exc)
        return months

    by_m: dict[int, dict[str, Any]] = {}
    for m in range(1, ref_month + 1):
        cd = _load_cache(year, m)
        if isinstance(cd, dict) and key in cd:
            by_m[m] = cd

    today = date.today()
    is_past_year = int(year) < int(today.year)
    if len(by_m) < ref_month and not is_past_year:
        # Достроить недостающие месяцы через get_plans_monthly (может сходить в 1С).
        try:
            plans = get_plans_monthly(year=year, month=ref_month)
            for r in plans.get("months") or []:
                mm = int(r.get("month") or 0)
                if mm and mm not in by_m:
                    by_m[mm] = r
        except Exception as exc:
            logger.warning("comdir expected 1C overlay fallback failed for %s: %s", metric, exc)
    elif len(by_m) < ref_month and is_past_year:
        logger.info(
            "comdir expected 1C overlay: skip live plans fetch for past year %s (%s)",
            year, metric,
        )

    if not by_m:
        logger.warning(
            "comdir expected 1C overlay: empty months for %s %s-%02d",
            metric, year, ref_month,
        )
        return months

    out: list[dict[str, Any]] = []
    for row in months:
        m = int(row.get("month") or 0)
        src = by_m.get(m) or {}
        exp_total = float(src.get(key) or 0)
        exp_by_dept = {
            str(g).lower(): float((v or {}).get(key) or 0)
            for g, v in (src.get("by_dept") or {}).items()
        }
        new_row = dict(row)
        new_row["expected"] = round(exp_total, 2)
        bd_in = new_row.get("by_dept") or {}
        bd_norm: dict[str, dict[str, float]] = {}
        for g, prev in bd_in.items():
            gl = str(g).lower()
            bucket = dict(prev) if isinstance(prev, dict) else {"fact": float(prev or 0)}
            bd_norm[gl] = bucket
        for g, amt in exp_by_dept.items():
            bucket = bd_norm.setdefault(g, {})
            bucket["expected"] = round(amt, 2)
        for g, bucket in bd_norm.items():
            bucket.setdefault("expected", 0.0)
        new_row["by_dept"] = bd_norm
        out.append(new_row)
    return out


def _merge_by_dept_maps(
    guids: set[str],
    *,
    fact_map: dict[str, float] | None = None,
    plan_map: dict[str, float] | None = None,
    expected_map: dict[str, float] | None = None,
    calc_map: dict[str, float] | None = None,
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for raw_g in guids:
        g = str(raw_g).lower()
        bucket: dict[str, float] = {}
        if fact_map is not None:
            bucket["fact"] = float(
                fact_map.get(g, fact_map.get(raw_g, 0)) or 0
            )
        if plan_map is not None:
            bucket["plan"] = float(
                plan_map.get(g, plan_map.get(raw_g, 0)) or 0
            )
        if expected_map is not None:
            bucket["expected"] = float(
                expected_map.get(g, expected_map.get(raw_g, 0)) or 0
            )
        if calc_map is not None:
            bucket["calc"] = float(
                calc_map.get(g, calc_map.get(raw_g, 0)) or 0
            )
        out[g] = bucket
    return out


# ── KD-M1 Деньги ──────────────────────────────────────────────

def compute_dengi_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    today = date.today()
    fact_next = p_next
    if year == today.year and month == today.month:
        fact_next = dengi_mod.to_1c_dt(today + timedelta(days=1))
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        plan_by_name = dengi_mod.calc_plan(cur, p0, p_next)
        fact_by_name = dengi_mod.calc_fact(cur, p0, fact_next)
        expected_by_name = dengi_mod.calc_expected(cur, p0, p_next)
    plan_map = aggregate_by_odata_name(plan_by_name)
    fact_map = aggregate_by_odata_name(fact_by_name)
    expected_map = aggregate_by_odata_name(expected_by_name)
    guids = set(plan_map) | set(fact_map) | set(expected_map)
    by_dept = _merge_by_dept_maps(
        guids,
        fact_map=fact_map,
        plan_map=plan_map,
        expected_map=expected_map,
    )
    return {
        "year": year,
        "month": month,
        "fact": round(sum(fact_map.values()), 2),
        "plan": round(sum(plan_map.values()), 2),
        "expected": round(sum(expected_map.values()), 2),
        "by_dept": by_dept,
    }


def build_dengi_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_dengi_month(year, m) for m in range(1, month + 1)]
    payload = _build_ytd_payload(year, month, months, kpi_id="KD-M1")
    payload["debug"] = {
        **(payload.get("debug") or {}),
        "source": "comdir.sql",
    }
    return payload


def get_dengi_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    def _compute(y: int, m: int) -> dict[str, Any]:
        return build_dengi_payload(y, m)

    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m1_ytd",
        source_tag="comdir_kd_m1_ytd_sql_v12",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m1",
        compute_fn=_compute,
        kpi_id="KD-M1",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M1", e),
    )
    if dept_guid:
        payload = dict(payload)
        payload["months"] = slice_months_by_dept(payload.get("months") or [], dept_guid)
        payload["monthly_data"] = payload["months"]
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, payload["months"], kpi_id="KD-M1")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M2 Отгрузки ────────────────────────────────────────────

def compute_otgruzki_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        plan_by_name = otg_mod.calc_mp_plan(cur, p0, p_next)
        fact_by_name = otg_mod.calc_fact(cur, p0, p_next)
    plan_map = aggregate_by_odata_name(plan_by_name)
    fact_map = aggregate_by_odata_name(fact_by_name)
    guids = set(plan_map) | set(fact_map)
    by_dept = _merge_by_dept_maps(
        guids, fact_map=fact_map, plan_map=plan_map, expected_map={g: 0.0 for g in guids},
    )
    return {
        "year": year,
        "month": month,
        "fact": round(sum(fact_map.values()), 2),
        "plan": round(sum(plan_map.values()), 2),
        "expected": 0.0,
        "by_dept": by_dept,
    }


def build_otgruzki_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_otgruzki_month(year, m) for m in range(1, month + 1)]
    months = _overlay_expected_from_1c(months, year, month, "otgruzki")
    payload = _build_ytd_payload(year, month, months, kpi_id="KD-M2")
    payload["debug"] = {
        **(payload.get("debug") or {}),
        "source": "comdir.sql+1c_expected",
    }
    return payload


def get_otgruzki_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m2_ytd",
        source_tag="comdir_kd_m2_ytd_sql_v7",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m2",
        compute_fn=build_otgruzki_payload,
        kpi_id="KD-M2",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M2", e),
    )
    if dept_guid:
        payload = dict(payload)
        payload["months"] = slice_months_by_dept(payload.get("months") or [], dept_guid)
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, payload["months"], kpi_id="KD-M2")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M3 Договоры ────────────────────────────────────────────

def compute_dogovory_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    today = date.today()
    # В текущем месяце факт и даты «ожидаемо» — по сегодня (как период Excel).
    asof_next = p_next
    if year == today.year and month == today.month:
        asof_next = dog_mod.to_1c_dt(today + timedelta(days=1))
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        plan_by_name = dog_mod.calc_mp_plan(cur, p0, p_next)
        fact_by_name = dog_mod.calc_fact(cur, p0, asof_next)
        expected_by_name = dog_mod.calc_expected(cur, p0, p_next, p_asof=asof_next)
    plan_map = aggregate_by_odata_name(plan_by_name)
    fact_map = aggregate_by_odata_name(fact_by_name)
    expected_map = aggregate_by_odata_name(expected_by_name)
    guids = set(plan_map) | set(fact_map) | set(expected_map)
    by_dept = _merge_by_dept_maps(
        guids,
        fact_map=fact_map,
        plan_map=plan_map,
        expected_map=expected_map,
    )
    return {
        "year": year,
        "month": month,
        "fact": round(sum(fact_map.values()), 2),
        "plan": round(sum(plan_map.values()), 2),
        "expected": round(sum(expected_map.values()), 2),
        "by_dept": by_dept,
    }


def build_dogovory_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_dogovory_month(year, m) for m in range(1, month + 1)]
    payload = _build_ytd_payload(year, month, months, kpi_id="KD-M3")
    payload["debug"] = {
        **(payload.get("debug") or {}),
        "source": "comdir.sql",
    }
    return payload


def get_dogovory_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m3_ytd",
        source_tag="comdir_kd_m3_ytd_sql_v13",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m3",
        compute_fn=build_dogovory_payload,
        kpi_id="KD-M3",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M3", e),
    )
    if dept_guid:
        payload = dict(payload)
        payload["months"] = slice_months_by_dept(payload.get("months") or [], dept_guid)
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, payload["months"], kpi_id="KD-M3")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M6 Валовая прибыль ─────────────────────────────────────

def compute_vp_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        by_bytes = vp_mod.calc_vp_by_dept(cur, p0, p_next)
    fact_map = aggregate_by_odata_bytes(by_bytes)
    plan_total = float(vp_plan_for_month(month, year))
    # план только агрегат; по отделам план = 0 (как раньше у дочерних — из valovaya)
    plan_map = {g: 0.0 for g in fact_map}
    if not fact_map:
        # всё равно показать план на плитке комдира
        pass
    guids = set(fact_map) | set(plan_map)
    by_dept = _merge_by_dept_maps(guids, fact_map=fact_map, plan_map=plan_map)
    return {
        "year": year,
        "month": month,
        "fact": round(sum(fact_map.values()), 2),
        "plan": plan_total,
        "by_dept": by_dept,
        "kpi_pct": _kpi_pct(sum(fact_map.values()), plan_total),
    }


def build_vp_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_vp_month(year, m) for m in range(1, month + 1)]
    # months_calendar на 12 месяцев года (как valovaya_pribyl) — для графиков
    cal = []
    today = date.today()
    for m in range(1, 13):
        if m <= month and year == (year):
            existing = next((r for r in months if r["month"] == m), None)
            if existing:
                cal.append(existing)
                continue
        plan = float(vp_plan_for_month(m, year))
        fact = None
        if (year, m) < (today.year, today.month) or (
            year == today.year and m < today.month
        ):
            fact = 0.0
        cal.append({
            "year": year,
            "month": m,
            "plan": plan,
            "fact": fact,
            "kpi_pct": _kpi_pct(fact, plan) if fact is not None else None,
            "by_dept": {},
        })
    # overwrite Jan..ref with computed
    by_m = {r["month"]: r for r in months}
    for i, row in enumerate(cal):
        if row["month"] in by_m:
            cal[i] = by_m[row["month"]]
    payload = _build_ytd_payload(year, month, months, kpi_id="KD-M6")
    payload["months_calendar"] = cal
    return payload


def get_vp_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m6_ytd",
        source_tag="comdir_kd_m6_ytd_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m6",
        compute_fn=build_vp_payload,
        kpi_id="KD-M6",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M6", e),
    )
    if dept_guid:
        payload = dict(payload)
        months = slice_months_by_dept(payload.get("months") or [], dept_guid)
        # для отдела план = 0 (нет разреза); оставляем fact
        for row in months:
            row["plan"] = row.get("plan") or 0
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, months, kpi_id="KD-M6")
        rebuilt["months_calendar"] = months
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M7 Расходы ─────────────────────────────────────────────

def compute_rashody_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        non_vrt = rash_mod.calc_44_by_dept(cur, p0, p_next, rash_mod.ARTS_44_NON_VRT)
        vrt = rash_mod.calc_44_by_dept(cur, p0, p_next, rash_mod.ARTS_44_VRT)
        sds = rash_mod.calc_sds_by_dept(cur, p0, p_next)
        dt51 = rash_mod.calc_dt51_by_dept(cur, p0, p_next)
        bal71 = rash_mod.calc_71_debit_balance(cur, p_next)
    by_name: dict[str, float] = {}
    names = set(non_vrt) | set(vrt) | set(sds) | set(dt51) | set(bal71)
    for name in names:
        a = non_vrt.get(name, 0.0)
        b = vrt.get(name, 0.0)
        c = sds.get(name, 0.0)
        d = dt51.get(name, 0.0)
        e = bal71.get(name, 0.0)
        by_name[name] = a + max(b, c) + d + e
    fact_map = aggregate_by_odata_name(by_name)
    plan_map = {
        g: float(get_rashody_plan(month, g)) for g in fact_map
    }
    # план по отделам из RASHODY_PLAN; агрегат включает KOMDIR_OWN
    plan_total = float(get_rashody_plan(month, None))
    guids = set(fact_map) | set(plan_map)
    by_dept = _merge_by_dept_maps(guids, fact_map=fact_map, plan_map=plan_map)
    return {
        "year": year,
        "month": month,
        "fact": round(sum(fact_map.values()), 2),
        "plan": plan_total,
        "by_dept": by_dept,
    }


def build_rashody_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_rashody_month(year, m) for m in range(1, month + 1)]
    return _build_ytd_payload(year, month, months, kpi_id="KD-M7")


def get_rashody_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m7_ytd",
        source_tag="comdir_kd_m7_ytd_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m7",
        compute_fn=build_rashody_payload,
        kpi_id="KD-M7",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M7", e),
    )
    if dept_guid:
        payload = dict(payload)
        months = slice_months_by_dept(payload.get("months") or [], dept_guid)
        for row in months:
            row["plan"] = float(get_rashody_plan(row["month"], dept_guid))
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, months, kpi_id="KD-M7")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M8 ФОТ ─────────────────────────────────────────────────

def compute_fot_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        by_name = fot_mod.calc_fot_by_dept(cur, p0, p_next)
    fact_map = aggregate_by_odata_name(by_name)
    plan_map = {g: float(get_fot_plan(month, g)) for g in fact_map}
    plan_total = float(get_fot_plan(month, None))
    guids = set(fact_map) | set(plan_map)
    by_dept = _merge_by_dept_maps(guids, fact_map=fact_map, plan_map=plan_map)
    return {
        "year": year,
        "month": month,
        "fact": round(sum(fact_map.values()), 2),
        "plan": plan_total,
        "by_dept": by_dept,
    }


def build_fot_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_fot_month(year, m) for m in range(1, month + 1)]
    return _build_ytd_payload(year, month, months, kpi_id="KD-M8")


def get_fot_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m8_ytd",
        source_tag="comdir_kd_m8_ytd_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m8",
        compute_fn=build_fot_payload,
        kpi_id="KD-M8",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M8", e),
    )
    if dept_guid:
        payload = dict(payload)
        months = slice_months_by_dept(payload.get("months") or [], dept_guid)
        for row in months:
            row["plan"] = float(get_fot_plan(row["month"], dept_guid))
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, months, kpi_id="KD-M8")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M9 Цена факт/расчёт ────────────────────────────────────

def compute_cena_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        by_bytes = cena_mod.calc_by_dept(cur, p0, p_next)
    # value = (fact, calc, n)
    fact_map = aggregate_by_odata_bytes(by_bytes, value_fn=lambda t: float(t[0] or 0))
    calc_map = aggregate_by_odata_bytes(by_bytes, value_fn=lambda t: float(t[1] or 0))
    guids = set(fact_map) | set(calc_map)
    by_dept = _merge_by_dept_maps(guids, fact_map=fact_map, calc_map=calc_map)
    fact_total = round(sum(fact_map.values()), 2)
    calc_total = round(sum(calc_map.values()), 2)
    return {
        "year": year,
        "month": month,
        "fact": fact_total,
        "calc": calc_total,
        "plan": calc_total,  # на плитке plan = расчётная
        "by_dept": by_dept,
    }


def build_cena_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_cena_month(year, m) for m in range(1, month + 1)]
    return _build_ytd_payload(year, month, months, kpi_id="KD-M9")


def get_cena_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m9_ytd",
        source_tag="comdir_kd_m9_ytd_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m9",
        compute_fn=build_cena_payload,
        kpi_id="KD-M9",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M9", e),
    )
    if dept_guid:
        payload = dict(payload)
        months = slice_months_by_dept(payload.get("months") or [], dept_guid)
        for row in months:
            if row.get("calc") is not None:
                row["plan"] = row["calc"]
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, months, kpi_id="KD-M9")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# alias for callers expecting kp_price naming
get_kp_price_ytd = get_cena_ytd


# ── KD-M10 ТКП SLA ────────────────────────────────────────────

def compute_tkp_sla_month(year: int, month: int) -> dict[str, Any]:
    p0, p_next = period_bounds(year, month)
    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        by_name = sla_mod.calc_by_dept(cur, p0, p_next)
    plan_map: dict[str, float] = {}
    fact_map: dict[str, float] = {}
    for name, (plan_v, fact_v) in by_name.items():
        from comdir.common import name_to_odata
        g = name_to_odata(name)
        if not g:
            continue
        plan_map[g] = plan_map.get(g, 0) + float(plan_v or 0)
        fact_map[g] = fact_map.get(g, 0) + float(fact_v or 0)
    guids = set(plan_map) | set(fact_map)
    by_dept = _merge_by_dept_maps(guids, fact_map=fact_map, plan_map=plan_map)
    plan_total = sum(plan_map.values())
    fact_total = sum(fact_map.values())
    pct = round(fact_total / plan_total * 100, 1) if plan_total else None
    return {
        "year": year,
        "month": month,
        "fact": fact_total,
        "plan": plan_total,
        "pct": pct,
        "by_dept": by_dept,
    }


def build_tkp_sla_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_tkp_sla_month(year, m) for m in range(1, month + 1)]
    return _build_ytd_payload(year, month, months, kpi_id="KD-M10")


def get_tkp_sla_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m10_ytd",
        source_tag="comdir_kd_m10_ytd_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m10",
        compute_fn=build_tkp_sla_payload,
        kpi_id="KD-M10",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M10", e),
    )
    if dept_guid:
        payload = dict(payload)
        months = slice_months_by_dept(payload.get("months") or [], dept_guid)
        for row in months:
            row["pct"] = _kpi_pct(row.get("fact"), row.get("plan"))
        ref_y, ref_m = normalize_period(year, month)
        rebuilt = _build_ytd_payload(ref_y, ref_m, months, kpi_id="KD-M10")
        rebuilt["debug"] = payload.get("debug") or rebuilt["debug"]
        return rebuilt
    return payload


# ── KD-M4 / KD-M5 ДЗ и просроченная ДЗ ─────────────────────────

def compute_debitorka_month(year: int, month: int) -> dict[str, Any]:
    from comdir.calc_debitorka import month_end, snapshot_on_date

    today = date.today()
    na = month_end(year, month)
    if na > today:
        na = today
    snap = snapshot_on_date(na)
    by_guid = snap.get("by_dept_guid") or {}
    by_dept = {
        g: {
            "fact": float(v.get("dz") or 0),
            "overdue": float(v.get("overdue") or 0),
        }
        for g, v in by_guid.items()
    }
    return {
        "year": year,
        "month": month,
        "na_datu": snap.get("na_datu"),
        "dz_fact": float(snap.get("total_dz") or 0),
        "kz_fact": float(snap.get("total_kz") or 0),
        "overdue_fact": float(snap.get("total_overdue") or 0),
        "fact": float(snap.get("total_dz") or 0),  # для stamp / debug
        "plan": None,
        "by_dept": by_dept,
        "by_dept_names": snap.get("by_dept") or {},
        "source": snap.get("source"),
    }


def build_debitorka_payload(year: int, month: int) -> dict[str, Any]:
    months = [compute_debitorka_month(year, m) for m in range(1, month + 1)]
    payload = _build_ytd_payload(year, month, months, kpi_id="KD-M4")
    # Совместимость с get_komdir_dz_monthly / плитками M4/M5
    payload["months"] = months
    payload["monthly_data"] = months
    payload["kz_source"] = "predoplata"
    payload["dept_alias_source"] = "debitorka_sql_v1"
    return payload


def get_debitorka_ytd(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,
) -> dict[str, Any]:
    """Помесячные ДЗ/просрочка (январь..ref_month). dept_guid — срез отдела."""
    payload = get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix="comdir_kd_m4_ytd",
        source_tag="comdir_kd_m4_ytd_sql_v1",
        version=CACHE_VERSION,
        lock_key_prefix="comdir_kd_m4",
        compute_fn=build_debitorka_payload,
        kpi_id="KD-M4",
        error_factory=lambda y, m, e: empty_error_payload(y, m, "KD-M4", e),
    )
    if not dept_guid:
        return payload

    g = dept_guid.lower()
    months_out = []
    for row in payload.get("months") or []:
        dept_bucket = (row.get("by_dept") or {}).get(g) or {}
        months_out.append({
            **row,
            "dz_fact": float(dept_bucket.get("fact") or 0),
            "overdue_fact": float(dept_bucket.get("overdue") or 0),
            "kz_fact": 0.0,
            "fact": float(dept_bucket.get("fact") or 0),
        })
    ref_y, ref_m = normalize_period(year, month)
    rebuilt = _build_ytd_payload(ref_y, ref_m, months_out, kpi_id="KD-M4")
    rebuilt["months"] = months_out
    rebuilt["monthly_data"] = months_out
    rebuilt["kz_source"] = "predoplata"
    rebuilt["dept_alias_source"] = "debitorka_sql_v1"
    rebuilt["debug"] = payload.get("debug") or rebuilt.get("debug")
    return rebuilt


def cache_stamp_paths(kpi_id: str, ref_y: int, ref_m: int) -> list[Path]:
    """Файлы кэша для штампа «Обновлено» на плитке."""
    from pathlib import Path as P
    dash = P(__file__).resolve().parent.parent / "getkpi" / "dashboard"
    mapping = {
        "KD-M1": f"comdir_kd_m1_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M2": f"comdir_kd_m2_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M3": f"comdir_kd_m3_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M4": f"comdir_kd_m4_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M5": f"comdir_kd_m4_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M6": f"comdir_kd_m6_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M7": f"comdir_kd_m7_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M8": f"comdir_kd_m8_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M9": f"comdir_kd_m9_ytd_{ref_y}_{ref_m:02d}.json",
        "KD-M10": f"comdir_kd_m10_ytd_{ref_y}_{ref_m:02d}.json",
    }
    name = mapping.get(kpi_id)
    return [dash / name] if name else []
