"""
calc_fot_management.py — ПЛИТКА «ФОТ компании управленческого контура
в пределах лимита».

Методика:
  Источник факта   : AccountingRegister_Хозрасчетный / RecordsWithExtDimensions
  Счета            : 44, 44.01, 44.02
  Статьи затрат    : 4 статьи ФОТ (оплата труда возм./ТД, страх.взносы возм./ТД)
  Организации      : ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО + Турбулентность-Дон ООО.
                     Другие дочерние юр.лица (СКТБ, АЛМАЗ, МГС) не входят.
  Подразделения    : ВСЕ — это периметр «управленческого контура».

Показатели:
  • ФАКТ  — Дт-оборот 44.xx за месяц по 4 статьям ФОТ.
  • ПЛАН  — берётся тем же способом, что и для коммерческого директора
            (`calc_fot.get_fot_plan`), с последующей пропорцией на текущий месяц.

Запуск:
  python calc_fot_management.py 2026-03           # один месяц
  python calc_fot_management.py 2026              # весь год
  python calc_fot_management.py 2026-03 --json    # + fot_mgmt_2026-03.json
  python calc_fot_management.py 2026     --json   # + fot_mgmt_2026.json
"""
import requests, sys, time, os, json
from collections import defaultdict
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

sys.stdout.reconfigure(encoding="utf-8")

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

TURB_ORGS = {
    "fbca2148-6cfd-11e7-812d-001e67112509": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
    "fbca2143-6cfd-11e7-812d-001e67112509": "Турбулентность-Дон ООО",
}

ACCOUNTS_44 = {
    "fb2bde54-6250-11e7-812d-001e67112509",  # 44
    "fb2bde55-6250-11e7-812d-001e67112509",  # 44.01
    "fb2bde56-6250-11e7-812d-001e67112509",  # 44.02
}

COST_ARTICLES = {
    "992a514f-782a-11eb-854d-ac1f6b05524d": "Оплата труда (44 сч) возмещение НПО!",
    "cc7c4aa6-3767-11ea-82f3-ac1f6b05524d": "Оплата труда (44 сч) ТД НПО!",
    "a04a98f8-782a-11eb-854d-ac1f6b05524d": "Страховые взносы (44.01) возмещение НПО!",
    "0360bf95-3768-11ea-82f3-ac1f6b05524d": "Страховые взносы (44.01) ТД НПО!",
}
ARTICLE_SET = frozenset(COST_ARTICLES)
ARTICLE_ORDER = list(COST_ARTICLES.keys())
ARTICLE_SHORT = {
    "992a514f-782a-11eb-854d-ac1f6b05524d": "Опл.тр. возм.",
    "cc7c4aa6-3767-11ea-82f3-ac1f6b05524d": "Опл.тр. ТД",
    "a04a98f8-782a-11eb-854d-ac1f6b05524d": "Страх.вз. возм.",
    "0360bf95-3768-11ea-82f3-ac1f6b05524d": "Страх.вз. ТД",
}

SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "fot_management_monthly_v3"

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"fot_management_monthly_{year}_{ref_month:02d}.json"


def _normalize_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    if month is None:
        ref_month = today.month if ref_year == today.year else 12
    else:
        ref_month = int(month)
    if ref_month < 1:
        ref_month = 1
    if ref_year == today.year and ref_month > today.month:
        ref_month = today.month
    if ref_month > 12:
        ref_month = 12
    return ref_year, ref_month


def period_bounds(year: int, month: int):
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _get_plan_total(month: int) -> float:
    try:
        from . import calc_fot
    except ImportError:
        import calc_fot
    return float(calc_fot.get_fot_plan(month) or 0.0)


def _prorate_if_current(plan: float | None, year: int, month: int) -> float | None:
    if plan is None:
        return None
    today = date.today()
    if year == today.year and month == today.month:
        total_days = monthrange(year, month)[1]
        return round(float(plan) * today.day / total_days, 2)
    return round(float(plan), 2)


def calc_fact(session: requests.Session, year: int, month: int) -> dict:
    p_start, p_end = period_bounds(year, month)
    acc_or = " or ".join(f"AccountDr_Key eq guid'{a}'" for a in ACCOUNTS_44)
    org_or = " or ".join(f"Организация_Key eq guid'{o}'" for o in TURB_ORGS)
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and ({acc_or}) and ({org_or})"
    )
    sel = ",".join([
        "Period", "AccountDr_Key", "Организация_Key", "ПодразделениеDr_Key",
        "Сумма", "Сторно", "ExtDimensionDr1", "ExtDimensionTypeDr1_Key",
    ])
    url_base = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}/RecordsWithExtDimensions"
        f"?$format=json"
        f"&$filter={quote(flt, safe='')}"
        f"&$select={quote(sel, safe=',_')}"
    )
    rows = []
    skip = 0
    PAGE = 5000
    while True:
        r = session.get(f"{url_base}&$top={PAGE}&$skip={skip}", timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break

    by_art: dict[str, float] = {a: 0.0 for a in ARTICLE_ORDER}
    by_org: dict[str, float] = {o: 0.0 for o in TURB_ORGS}
    by_org_art: dict[str, dict[str, float]] = {
        o: {a: 0.0 for a in ARTICLE_ORDER} for o in TURB_ORGS
    }
    total = 0.0
    taken = 0
    for r in rows:
        amount = r.get("Сумма", 0) or 0
        if r.get("Сторно"):
            amount = -amount
        ext1 = r.get("ExtDimensionDr1")
        typ1 = r.get("ExtDimensionTypeDr1_Key")
        art = ext1 if typ1 == SUBCONTO_TYPE_COST and ext1 else None
        if art not in ARTICLE_SET:
            continue
        org = r.get("Организация_Key")
        if org not in by_org:
            continue
        total += amount
        by_art[art] += amount
        by_org[org] += amount
        by_org_art[org][art] += amount
        taken += 1

    return {
        "total": round(total, 2),
        "by_article": {k: round(v, 2) for k, v in by_art.items()},
        "by_org": {k: round(v, 2) for k, v in by_org.items()},
        "by_org_article": {
            o: {a: round(v, 2) for a, v in cells.items()}
            for o, cells in by_org_art.items()
        },
        "records_total": len(rows),
        "records_taken": taken,
    }


def calc_month(session: requests.Session, year: int, month: int) -> dict:
    t = time.time()
    fact = calc_fact(session, year, month)
    plan_total = _get_plan_total(month)
    return {
        "year": year, "month": month,
        "period": {"start": period_bounds(year, month)[0][:10],
                   "end":   period_bounds(year, month)[1][:10]},
        "fact": fact,
        "plan_total": plan_total,
        "plan_by_article": {},
        "plan_by_org": {},
        "elapsed_sec": round(time.time() - t, 2),
    }


def get_fot_management_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path_monthly(ref_year, ref_month)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    session = requests.Session()
    session.auth = AUTH

    months_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        row = calc_month(session, ref_year, mm)
        fact_payload = row.get("fact") or {}
        plan_total = row.get("plan_total")
        fact_total = float(fact_payload.get("total") or 0)
        plan_numeric = _prorate_if_current(plan_total, ref_year, mm)
        months_out.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan_numeric, 2) if plan_numeric is not None else None,
            "fact": round(fact_total, 2),
            "kpi_pct": round(fact_total / plan_numeric * 100, 1) if plan_numeric and plan_numeric > 0 else None,
            "has_data": (plan_numeric is not None) or abs(fact_total) > 0,
            "values_unit": "руб.",
            "plan_by_org": row.get("plan_by_org") or {},
            "fact_by_org": fact_payload.get("by_org") or {},
            "plan_by_article": row.get("plan_by_article") or {},
            "fact_by_article": fact_payload.get("by_article") or {},
            "cost_articles": COST_ARTICLES,
        })

    with_data = [row for row in months_out if row.get("has_data")]
    last_data_row = with_data[-1] if with_data else (months_out[-1] if months_out else None)
    total_plan = sum(float(row.get("plan") or 0) for row in months_out if row.get("plan") is not None)
    total_fact = sum(float(row.get("fact") or 0) for row in months_out)
    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(last_data_row) if last_data_row else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if months_out else None,
            "total_fact": round(total_fact, 2) if months_out else None,
            "kpi_pct": round(total_fact / total_plan * 100, 1) if total_plan > 0 else None,
            "months_with_data": len(with_data),
            "months_total": len(months_out),
            "values_unit": "руб." if months_out else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": (last_data_row or {}).get("year", ref_year),
            "month": (last_data_row or {}).get("month", ref_month),
            "month_name": (last_data_row or {}).get("month_name", MONTH_RU[ref_month].lower()),
        },
    }
    _save_json(cache_path, payload)
    return payload


def fmt(v) -> str:
    if v is None:
        return "—"
    return f"{v:,.2f}"


def print_month(res: dict) -> None:
    y, m = res["year"], res["month"]
    fact = res["fact"]
    plan = res["plan_total"]
    delta = None if plan is None else (fact["total"] - plan)

    print("=" * 92)
    print(f"  ФОТ управленческого контура в пределах лимита · {y}-{m:02d} ({MONTH_RU[m]})")
    print(f"  Периметр: {', '.join(TURB_ORGS.values())}")
    print("=" * 92)

    print(f"\n  ФАКТ (начислено, Дт 44.xx × 4 статьи ФОТ):")
    print(f"  {'Организация':<32s}", end="")
    for a in ARTICLE_ORDER:
        print(f" {ARTICLE_SHORT[a]:>14s}", end="")
    print(f" {'ИТОГО':>16s}")
    print(f"  {'─'*32}" + f" {'─'*14}" * len(ARTICLE_ORDER) + f" {'─'*16}")
    for og, name in TURB_ORGS.items():
        row = f"  {name[:32]:<32s}"
        for a in ARTICLE_ORDER:
            row += f" {fact['by_org_article'][og][a]:>14,.2f}"
        row += f" {fact['by_org'][og]:>16,.2f}"
        print(row)
    print(f"  {'─'*32}" + f" {'─'*14}" * len(ARTICLE_ORDER) + f" {'─'*16}")
    row = f"  {'ИТОГО':<32s}"
    for a in ARTICLE_ORDER:
        row += f" {fact['by_article'][a]:>14,.2f}"
    row += f" {fact['total']:>16,.2f}"
    print(row)

    print(f"\n  {'Показатель':<48s} {'Значение':>20s}")
    print(f"  {'─'*48} {'─'*20}")
    print(f"  {'ФАКТ начисленный ФОТ (сч.44)':<48s} {fact['total']:>20,.2f}")
    print(f"  {'ПЛАН (как у коммерческого директора)':<48s} {fmt(plan):>20s}")
    if delta is not None:
        print(f"  {'Δ (факт − план)':<48s} {delta:>20,.2f}")
    print(f"  Время: {res['elapsed_sec']:.1f}с")


def main():
    save_json = "--json" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: calc_fot_management.py YYYY-MM | YYYY [--json]")

    arg = args[0]
    today = datetime.now()
    t0 = time.time()

    s = requests.Session(); s.auth = AUTH

    if len(arg) == 7 and arg[4] == "-":
        year, month = int(arg[:4]), int(arg[5:7])
        res = calc_month(s, year, month)
        print_month(res)
        if save_json:
            res["organizations"] = TURB_ORGS
            res["cost_articles"] = COST_ARTICLES
            res["generated"] = datetime.now().isoformat(timespec="seconds")
            out = os.path.join(os.path.dirname(__file__),
                               f"fot_mgmt_{year}-{month:02d}.json")
            with open(out, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            print(f"  → {out}")
        return

    if len(arg) != 4 or not arg.isdigit():
        raise SystemExit("Ожидался YYYY-MM или YYYY")

    year = int(arg)
    last_month = today.month if year == today.year else 12
    months_out = []
    print("=" * 92)
    print(f"  ФОТ управленческого контура · {year} (январь — {MONTH_RU[last_month]})")
    print(f"  Периметр: {', '.join(TURB_ORGS.values())}")
    print("=" * 92)
    print(f"\n  {'Мес':<10s} {'ФАКТ начисл.':>18s} {'ПЛАН':>16s} {'Δ (факт−план)':>18s}")
    print(f"  {'─'*10} {'─'*18} {'─'*16} {'─'*18}")
    for m in range(1, last_month + 1):
        res = calc_month(s, year, m)
        months_out.append(res)
        plan = res["plan_total"]
        delta = None if plan is None else (res["fact"]["total"] - plan)
        print(f"  {MONTH_RU[m]:<10s} {res['fact']['total']:>18,.2f} "
              f"{fmt(plan):>16s} {(fmt(delta)):>18s}")
    print(f"\n  Время: {time.time()-t0:.1f}с")

    if save_json:
        result = {
            "year": year,
            "generated": datetime.now().isoformat(timespec="seconds"),
            "organizations": TURB_ORGS,
            "cost_articles": COST_ARTICLES,
            "metric": "ФОТ управленческого контура в пределах лимита",
            "months": months_out,
        }
        out = os.path.join(os.path.dirname(__file__), f"fot_mgmt_{year}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  → {out}")


if __name__ == "__main__":
    main()
