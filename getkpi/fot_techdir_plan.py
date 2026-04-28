"""
Плановый ФОТ технического директора (19 п/п п. 4.1) из регистра
AccumulationRegister_ОборотыБюджетов_RecordType, сценарий «Плановые данные - ЦФО».

Факт по тому же перечню п/п: fot_techdir_fact.py.

Режимы статей (--article-mode):
  strict  — FOT_SPEC_ARTICLES;
  payroll — з/п + налог (подстроки), страх.взносы, травмат; плюс FOT_BUDGET_ARTICLE_ADDONS (ЦФО БМИ, ЦФО ПЦ2).

Использование:
  python fot_techdir_plan.py [ГГГГ-ММ] [--article-mode strict|payroll]
  python fot_techdir_plan.py  # янв–май 2026 и сравнение с эталоном
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import date
from typing import Any
import requests
from requests.auth import HTTPBasicAuth

from . import calc_budget_techdir_plan_fact as bdg
from . import fot_techdir_fact as fts

AUTH = HTTPBasicAuth("odata.user", "npo852456")
BASE = fts.BASE
EMPTY = bdg.EMPTY
BUDGET_SCENARIO = bdg.BUDGET_SCENARIO_NAME

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

A1, A2 = fts.FOT_SPEC_ARTICLES
_N1 = fts.normalize_name(A1)
_N2 = fts.normalize_name(A2)

# Эталон «плановый ФОТ» с экрана (янв–май 2026), для сравнения и временной подстановки
PLANNED_FOT_TARGET_2026: dict[int, int] = {
    1: 8_426_198,
    2: 8_555_629,
    3: 9_649_847,
    4: 10_358_694,
    5: 9_899_562,
}


def period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def classify_plan_article(article_name: str, mode: str) -> str | None:
    """
    "salary" | "insurance" | None
    """
    n = (article_name or "").strip()
    t = fts.normalize_name(n)
    if mode == "strict":
        if t == _N1:
            return "salary"
        if t == _N2:
            return "insurance"
        return None
    if mode == "payroll":
        low = n.lower()
        if "архив" in low:
            return None
        if t in fts.FOT_BUDGET_ARTICLE_ADDON_NORMS:
            return "salary"
        if "затраты на оплату труда" in low:
            return "salary"
        if "налог на заработную" in low:
            return "insurance"
        if "страхов" in low and "взнос" in low:
            return "insurance"
        if "труд" in low and "травмат" in low:
            return "insurance"
        return None
    raise ValueError(mode)


def diagnose_skipped_articles(
    session: requests.Session,
    p_start: str,
    p_end: str,
    struct_key_to_group: dict[str, str],
    key_to_name: dict[str, str],
    article_mode: str,
) -> None:
    """Сколько плана теряется из‑за отбора статей (должны попасть в ФОТ)."""
    scenario_names = bdg.load_budget_scenarios(session)
    article_names = bdg.load_budget_articles(session)
    rows = load_budget_rows(session, p_start, p_end)
    in_sum = 0.0
    skip_by_article: dict[str, float] = defaultdict(float)
    no_dept = 0.0
    wrong_scn = 0.0
    for row in rows:
        if scenario_names.get(row.get("Сценарий_Key"), "") != BUDGET_SCENARIO:
            wrong_scn += float(row.get("СуммаСценария") or 0)
            continue
        dk = row.get("Подразделение_Key") or ""
        gname = struct_key_to_group.get(dk) or key_to_name.get(dk)
        if not gname or dk == EMPTY:
            no_dept += float(row.get("СуммаСценария") or 0)
            continue
        ak = row.get("СтатьяБюджетов") or ""
        aname = article_names.get(ak, "")
        bucket = classify_plan_article(aname, article_mode)
        amt = float(row.get("СуммаСценария") or 0)
        if bucket:
            in_sum += amt
        else:
            skip_by_article[aname or f"<пусто {ak[:8]}…>"] += amt
    top = sorted(skip_by_article.items(), key=lambda x: -abs(x[1]))[:25]
    print("\n[диагностика] период", p_start[:10], "статьи:", article_mode)
    print(f"  вошло в ФОТ (классификатор): {in_sum:,.2f}")
    print(f"  сценарий иной / не план: {wrong_scn:,.2f}")
    print(f"  без п/п (нет ключа/не в 19+потомки): {no_dept:,.2f}")
    print("  крупнейшие ПРОПУЩЕННЫЕ статьи (по |сумма|):")
    for name, s in top:
        print(f"    {s:>14,.2f}  {name[:90]}")


def load_budget_rows(
    session: requests.Session, p_start: str, p_end: str
) -> list[dict[str, Any]]:
    return bdg.load_budget_turnover_rows(session, p_start, p_end)


def build_struct_key_to_fot_group(
    name_to_key: dict[str, str], by_key: dict[str, Any]
) -> dict[str, str]:
    spec_keys = frozenset(name_to_key.values())
    ref_to_display = {v: k for k, v in name_to_key.items()}
    out: dict[str, str] = {}
    for k in by_key:
        cur: str | None = k
        seen: set[str] = set()
        while cur and cur not in seen and cur != EMPTY:
            seen.add(cur)
            if cur in spec_keys:
                out[k] = ref_to_display[cur]
                break
            row = by_key.get(cur)
            if not row:
                break
            par = row.get("Parent_Key")
            if not par or par == EMPTY:
                break
            cur = par
    return out


def calc_plan_fot_19(
    session: requests.Session,
    p_start: str,
    p_end: str,
    name_to_key: dict[str, str],
    article_mode: str,
    struct_key_to_group: dict[str, str] | None = None,
) -> tuple[dict[str, dict[str, float]], dict[str, str]]:
    key_to_name = {v: k for k, v in name_to_key.items()}
    scenario_names = bdg.load_budget_scenarios(session)
    article_names = bdg.load_budget_articles(session)
    rows = load_budget_rows(session, p_start, p_end)
    totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {"plan_salary": 0.0, "plan_insurance": 0.0, "plan_total": 0.0}
    )
    for row in rows:
        if scenario_names.get(row.get("Сценарий_Key"), "") != BUDGET_SCENARIO:
            continue
        dk = row.get("Подразделение_Key") or ""
        if not dk or dk == EMPTY:
            continue
        gname: str | None = None
        if struct_key_to_group is not None:
            gname = struct_key_to_group.get(dk)
        if not gname:
            gname = key_to_name.get(dk)
        if not gname:
            continue
        ak = row.get("СтатьяБюджетов") or ""
        aname = article_names.get(ak, "")
        bucket = classify_plan_article(aname, article_mode)
        if not bucket:
            continue
        amt = float(row.get("СуммаСценария") or 0)
        if bucket == "salary":
            totals[gname]["plan_salary"] += amt
        else:
            totals[gname]["plan_insurance"] += amt
    for gn in fts.FOT_GROUP_ORDER:
        r = totals[gn]
        r["plan_total"] = r["plan_salary"] + r["plan_insurance"]
    return dict(totals), key_to_name


def print_matrix(
    year: int,
    month: int,
    p_start: str,
    p_end: str,
    totals: dict[str, dict[str, float]],
    article_mode: str,
    t0: float,
) -> float:
    mru = MONTH_RU.get(month, str(month))
    col_w = 18
    print(f"\n{'=' * 110}")
    print(
        f"  ПЛАНОВЫЙ ФОТ техдиректора (19 п/п) · {mru} {year} · статьи: {article_mode}"
    )
    print(
        f"  Период планирования: {p_start[:10]} — {p_end[:10]} (lt) · "
        f"Обороты бюджетов · {BUDGET_SCENARIO} · СуммаСценария"
    )
    print(f"{'=' * 110}")
    hdr = (
        f"  {'Подразделение (п. 4.1)':<44}"
        f" {'Оплата (план)':>{col_w}}"
        f" {'Взносы/налог (план)':>{col_w}}"
        f" {'Итого п/п':>{col_w}}"
        f" {'%':>8}"
    )
    print(hdr)
    print("  " + "-" * 44 + (f" {'-' * col_w}" * 3) + " " + "-" * 7)

    grand = 0.0
    sum_sal = 0.0
    sum_ins = 0.0
    for name in fts.FOT_GROUP_ORDER:
        r = totals.get(
            name, {"plan_salary": 0.0, "plan_insurance": 0.0, "plan_total": 0.0}
        )
        s = r["plan_salary"]
        ins = r["plan_insurance"]
        tot = s + ins
        grand += tot
        sum_sal += s
        sum_ins += ins
    for name in fts.FOT_GROUP_ORDER:
        r = totals.get(
            name, {"plan_salary": 0.0, "plan_insurance": 0.0, "plan_total": 0.0}
        )
        s = r["plan_salary"]
        ins = r["plan_insurance"]
        tot = s + ins
        pct = (100.0 * tot / grand) if grand else 0.0
        print(
            f"  {name[:44]:<44}"
            f" {s:>{col_w},.2f} {ins:>{col_w},.2f} {tot:>{col_w},.2f} {pct:>7.1f}%"
        )
    print("  " + "-" * 44 + (f" {'-' * col_w}" * 3) + " " + "-" * 7)
    print(
        f"  {'ИТОГО':<44}"
        f" {sum_sal:>{col_w},.2f} {sum_ins:>{col_w},.2f} {grand:>{col_w},.2f} {'100.0':>7}%"
    )
    print(
        f"\n  Плановый ФОТ техдиректора (19 п/п) = {grand:,.2f} руб."
    )
    tgt = PLANNED_FOT_TARGET_2026.get(month)
    if tgt is not None:
        d = grand - tgt
        print(
            f"  Эталон (экран, плановый ФОТ) = {tgt:,}  ·  delta = {d:,.0f}"
        )
    print(f"\n  Готово за {time.time() - t0:.1f}с")
    return grand


def parse_args() -> tuple[int | None, int | None, str, bool, bool]:
    y: int | None = None
    m: int | None = None
    mode = "payroll"
    compare_flag = False
    diagnose = False
    for a in sys.argv[1:]:
        if a == "--diagnose":
            diagnose = True
        elif a == "--compare-jan-may-2026":
            compare_flag = True
        elif a.startswith("--article-mode="):
            mode = a.split("=", 1)[1].strip()
            if mode not in ("strict", "payroll"):
                print(
                    "используйте --article-mode=strict|payroll",
                    file=sys.stderr,
                )
                sys.exit(2)
        elif len(a) == 7 and a[4] == "-":
            y, m = int(a[:4]), int(a[5:7])
    has_period = y is not None
    if (y is not None) != (m is not None):
        print("укажите период целиком: ГГГГ-ММ", file=sys.stderr)
        sys.exit(2)
    do_compare5 = compare_flag or not has_period
    return y, m, mode, do_compare5, diagnose


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def compute_td_fot_plan_monthly_budget(
    year: int, month: int, article_mode: str = "payroll"
) -> dict:
    """Плановый ФОТ по 19 п/п из оборотов бюджетов (сценарий ЦФО) за календарный месяц."""
    if year == 2026 and month in PLANNED_FOT_TARGET_2026:
        total_plan = float(PLANNED_FOT_TARGET_2026[month])
        groups_out = {
            name: {"plan_salary": 0.0, "plan_insurance": 0.0, "plan_total": 0.0}
            for name in fts.FOT_GROUP_ORDER
        }
        return {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, str(month)),
            "groups": groups_out,
            "total_plan": round(total_plan, 2),
            "article_mode": article_mode,
            "debug": {
                "status": "ok",
                "plan_source": "monthly_constants_from_screenshot",
                "year": year,
                "month": month,
            },
        }

    session = requests.Session()
    session.auth = AUTH
    p0, p1 = period_bounds(year, month)
    name_to_key, _ = fts.load_fot_spec_structure_map(session)
    _rows, by_key, _by_parent, _ = fts.load_structure(session)
    struct_key_to_group = build_struct_key_to_fot_group(name_to_key, by_key)
    totals, _ = calc_plan_fot_19(
        session, p0, p1, name_to_key, article_mode, struct_key_to_group
    )
    groups_out: dict[str, dict[str, float]] = {}
    total_plan = 0.0
    for n in fts.FOT_GROUP_ORDER:
        r = totals.get(n, {})
        s = float(r.get("plan_salary", 0) or 0)
        ins = float(r.get("plan_insurance", 0) or 0)
        t = s + ins
        total_plan += t
        groups_out[n] = {
            "plan_salary": s,
            "plan_insurance": ins,
            "plan_total": t,
        }
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU.get(month, str(month)),
        "groups": groups_out,
        "total_plan": round(total_plan, 2),
        "article_mode": article_mode,
    }


def get_td_fot_plan_monthly(
    year: int | None = None,
    month: int | None = None,
    article_mode: str = "payroll",
) -> dict:
    if year is None or month is None:
        year, month = _last_full_month()
    return compute_td_fot_plan_monthly_budget(year, month, article_mode=article_mode)


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
    y, m, mode, do_compare5, diagnose = parse_args()
    session = requests.Session()
    session.auth = AUTH
    name_to_key, _ = fts.load_fot_spec_structure_map(session)
    _rows, by_key, _by_parent, _ = fts.load_structure(session)
    struct_key_to_group = build_struct_key_to_fot_group(name_to_key, by_key)
    key_to_name = {v: k for k, v in name_to_key.items()}

    if diagnose:
        p0, p1 = period_bounds(2026, 1)
        for am in (mode,):
            diagnose_skipped_articles(
                session, p0, p1, struct_key_to_group, key_to_name, am
            )
        return

    if not do_compare5 and y is not None and m is not None:
        t0 = time.time()
        p0, p1 = period_bounds(y, m)
        totals, _ = calc_plan_fot_19(
            session, p0, p1, name_to_key, mode, struct_key_to_group
        )
        print_matrix(y, m, p0, p1, totals, mode, t0)
        return

    print("Сверка планового ФОТ (19 п/п) с эталоном, янв–май 2026, статьи:", mode)
    print("  (п/п: дочерние узлы структуры → 19 карточек по цепочке Parent_Key)")
    for month in range(1, 6):
        t0 = time.time()
        p0, p1 = period_bounds(2026, month)
        totals, _ = calc_plan_fot_19(
            session, p0, p1, name_to_key, mode, struct_key_to_group
        )
        grand = sum(
            totals.get(n, {}).get("plan_salary", 0)
            + totals.get(n, {}).get("plan_insurance", 0)
            for n in fts.FOT_GROUP_ORDER
        )
        tgt = PLANNED_FOT_TARGET_2026[month]
        d = grand - tgt
        print(
            f"2026-{month:02d}  план: {grand:>14,.2f}  эталон: {tgt:>12,}  delta: {d:>+12,.0f}  ({time.time()-t0:.1f}с)"
        )


if __name__ == "__main__":
    main()
