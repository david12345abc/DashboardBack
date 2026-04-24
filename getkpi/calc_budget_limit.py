"""
calc_budget_limit.py — ПЛИТКА «Бюджет управленческого контура в пределах лимита».

Затраты (план/факт) по казначейскому контуру 1С ERP: регистр
AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент.

Периметр:
  • Организации: ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО + Турбулентность-Дон ООО.
    Другие дочерние юр.лица холдинга (СКТБ, АЛМАЗ, Метрогазсервис)
    в этот контур не входят.

Показатели:
  • ПЛАН = Σ СуммаКВыплатеВРамкахЛимита по заявкам на расход ДС,
           где дата плановой оплаты (= Period регистра) попадает в месяц.
    Источник: Recorder_Type = Document_ЗаявкаНаРасходованиеДенежныхСредств.
  • ФАКТ = Σ СуммаОплаты − Σ СуммаКВыплатеСверхЛимита по фактическим
           расходным документам того же периода (списания, РКО, выдача).
           Т.е. фактически израсходовано «в рамках лимита».

Статьи ДДС разбиваются отдельно в разрезе (топ-15 по сумме).

Запуск:
  python calc_budget_limit.py 2026-04            # один месяц
  python calc_budget_limit.py 2026               # все месяцы года до текущего
  python calc_budget_limit.py 2026-04 --json
  python calc_budget_limit.py 2026      --json   # годовой файл
"""
import requests, sys, time, os, json
from collections import defaultdict
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

PLAN_RECORDER_TYPE = "StandardODATA.Document_ЗаявкаНаРасходованиеДенежныхСредств"

FACT_RECORDER_TYPES = {
    "StandardODATA.Document_СписаниеБезналичныхДенежныхСредств",
    "StandardODATA.Document_РасходныйКассовыйОрдер",
    "StandardODATA.Document_ВыдачаНаличныхДенежныхСредств",
    "StandardODATA.Document_ВыдачаНаличных",
    "StandardODATA.Document_ПлатежноеПоручениеИсходящее",
}

DDS_CACHE = os.path.join(os.path.dirname(__file__), "dds_articles_cache.json")
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "budget_limit_monthly_v1"

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
    return CACHE_DIR / f"budget_limit_monthly_{year}_{ref_month:02d}.json"


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


def load_records(session: requests.Session, p_start: str, p_end: str) -> list:
    """Все записи регистра по двум организациям за период."""
    reg = quote("AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType")
    org_or = " or ".join(f"Организация_Key eq guid'{o}'" for o in TURB_ORGS)
    flt = (
        f"Period ge datetime'{p_start}'"
        f" and Period lt datetime'{p_end}'"
        f" and Active eq true and ({org_or})"
    )
    sel = ",".join([
        "Period", "Recorder", "Recorder_Type", "Сторно",
        "Организация_Key", "Подразделение_Key",
        "СтатьяДвиженияДенежныхСредств_Key", "ХозяйственнаяОперация",
        "ЗаявкаНаРасходованиеДенежныхСредств_Key",
        "СуммаКВыплатеВРамкахЛимита", "СуммаКВыплатеСверхЛимита",
        "СуммаОплаты", "СуммаПостоплаты", "СуммаПредоплаты",
    ])
    rows = []
    skip = 0
    PAGE = 5000
    while True:
        url = (
            f"{BASE}/{reg}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        r = session.get(url, timeout=120)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break
    return rows


class DdsCache:
    def __init__(self, session: requests.Session):
        self.session = session
        self.names: dict[str, str] = {}
        if os.path.exists(DDS_CACHE):
            try:
                self.names = json.load(open(DDS_CACHE, encoding="utf-8"))
            except Exception:
                self.names = {}

    def ensure(self, keys: set[str]):
        missing = [k for k in keys if k and k != EMPTY and k not in self.names]
        BATCH = 25
        for i in range(0, len(missing), BATCH):
            batch = missing[i:i + BATCH]
            flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in batch)
            url = (f"{BASE}/Catalog_"
                   + quote("СтатьиДвиженияДенежныхСредств")
                   + f"?$format=json&$filter={quote(flt, safe='')}"
                   + f"&$select=Ref_Key,Description&$top={BATCH}")
            r = self.session.get(url, timeout=60)
            if r.ok:
                for it in r.json().get("value", []):
                    self.names[it["Ref_Key"]] = (it.get("Description") or "").strip()
        if missing:
            try:
                with open(DDS_CACHE, "w", encoding="utf-8") as f:
                    json.dump(self.names, f, ensure_ascii=False)
            except Exception:
                pass

    def label(self, key: str) -> str:
        return self.names.get(key, f"<{key[:8]}…>") or "<пусто>"


def calc_month(session: requests.Session, year: int, month: int, dds: DdsCache) -> dict:
    p_start, p_end = period_bounds(year, month)
    rows = load_records(session, p_start, p_end)

    plan_total = 0.0
    fact_gross = 0.0
    overlimit_total = 0.0
    plan_by_art: dict[str, float] = defaultdict(float)
    fact_by_art: dict[str, float] = defaultdict(float)
    plan_by_org: dict[str, float] = {k: 0.0 for k in TURB_ORGS}
    fact_by_org: dict[str, float] = {k: 0.0 for k in TURB_ORGS}
    plan_count = 0
    fact_count = 0

    art_keys: set[str] = set()

    for r in rows:
        sign = -1 if r.get("Сторно") else 1
        in_limit = float(r.get("СуммаКВыплатеВРамкахЛимита") or 0) * sign
        over = float(r.get("СуммаКВыплатеСверхЛимита") or 0) * sign
        opl = float(r.get("СуммаОплаты") or 0) * sign
        rt = r.get("Recorder_Type") or ""
        org = r.get("Организация_Key")
        art = r.get("СтатьяДвиженияДенежныхСредств_Key") or EMPTY
        art_keys.add(art)

        if rt == PLAN_RECORDER_TYPE:
            if in_limit != 0:
                plan_total += in_limit
                plan_by_art[art] += in_limit
                if org in plan_by_org:
                    plan_by_org[org] += in_limit
                plan_count += 1
        elif rt in FACT_RECORDER_TYPES:
            if opl != 0:
                fact_gross += opl
                overlimit_total += over
                net = opl - over
                fact_by_art[art] += net
                if org in fact_by_org:
                    fact_by_org[org] += net
                fact_count += 1

    dds.ensure(art_keys)
    fact_total = fact_gross - overlimit_total

    return {
        "year": year, "month": month,
        "period": {"start": p_start[:10], "end": p_end[:10]},
        "plan_total": round(plan_total, 2),
        "fact_total": round(fact_total, 2),
        "fact_gross": round(fact_gross, 2),
        "overlimit_fact": round(overlimit_total, 2),
        "plan_by_org": {o: round(v, 2) for o, v in plan_by_org.items()},
        "fact_by_org": {o: round(v, 2) for o, v in fact_by_org.items()},
        "plan_by_article": {k: round(v, 2) for k, v in plan_by_art.items() if v},
        "fact_by_article": {k: round(v, 2) for k, v in fact_by_art.items() if v},
        "plan_count": plan_count,
        "fact_count": fact_count,
        "records_total": len(rows),
    }


def get_budget_limit_monthly(year: int | None = None, month: int | None = None) -> dict:
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
    dds = DdsCache(session)

    months_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        row = calc_month(session, ref_year, mm, dds)
        plan_total = float(row.get("plan_total") or 0)
        fact_total = float(row.get("fact_total") or 0)
        months_out.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan_total, 2),
            "fact": round(fact_total, 2),
            "kpi_pct": round(fact_total / plan_total * 100, 1) if plan_total > 0 else None,
            "has_data": abs(plan_total) > 0 or abs(fact_total) > 0,
            "values_unit": "руб.",
            "overlimit_fact": round(float(row.get("overlimit_fact") or 0), 2),
            "plan_by_org": row.get("plan_by_org") or {},
            "fact_by_org": row.get("fact_by_org") or {},
            "plan_by_article": row.get("plan_by_article") or {},
            "fact_by_article": row.get("fact_by_article") or {},
            "articles_names": {
                k: dds.label(k)
                for k in set((row.get("plan_by_article") or {}).keys()) | set((row.get("fact_by_article") or {}).keys())
            },
        })

    with_data = [row for row in months_out if row.get("has_data")]
    total_plan = sum(float(row.get("plan") or 0) for row in months_out)
    total_fact = sum(float(row.get("fact") or 0) for row in months_out)
    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(months_out[-1]) if months_out else None,
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
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
    }
    _save_json(cache_path, payload)
    return payload


def print_month(res: dict, dds: DdsCache) -> None:
    year, month = res["year"], res["month"]
    print("=" * 82)
    print(f"  Бюджет управленческого контура в пределах лимита · {year}-{month:02d} ({MONTH_RU[month]})")
    print(f"  Периметр: {', '.join(TURB_ORGS.values())}")
    print("=" * 82)

    print(f"\n  Записей в регистре: {res['records_total']} "
          f"(план={res['plan_count']}, факт={res['fact_count']})")

    print(f"\n  {'Организация':<32s} {'План':>18s} {'Факт (в лимите)':>18s} {'Δ':>14s}")
    print(f"  {'─'*32} {'─'*18} {'─'*18} {'─'*14}")
    for og, name in TURB_ORGS.items():
        p = res["plan_by_org"].get(og, 0.0)
        f = res["fact_by_org"].get(og, 0.0)
        print(f"  {name:<32s} {p:>18,.2f} {f:>18,.2f} {(f-p):>14,.2f}")
    print(f"  {'─'*32} {'─'*18} {'─'*18} {'─'*14}")
    print(f"  {'ИТОГО':<32s} {res['plan_total']:>18,.2f} {res['fact_total']:>18,.2f} "
          f"{(res['fact_total']-res['plan_total']):>14,.2f}")

    if res["overlimit_fact"]:
        print(f"\n  ⚠ Факт СВЕРХ лимита (информационно): {res['overlimit_fact']:>16,.2f} ₽")
        print(f"      Факт всего:                        {res['fact_gross']:>16,.2f} ₽")

    # Топ-15 статей ДДС
    by_art = defaultdict(lambda: [0.0, 0.0])
    for k, v in res["plan_by_article"].items():
        by_art[k][0] = v
    for k, v in res["fact_by_article"].items():
        by_art[k][1] = v

    ordered = sorted(by_art.items(), key=lambda x: max(abs(x[1][0]), abs(x[1][1])), reverse=True)[:15]
    if ordered:
        print(f"\n  Топ-15 статей ДДС:")
        print(f"  {'Статья':<60s} {'План':>14s} {'Факт':>14s}")
        print(f"  {'─'*60} {'─'*14} {'─'*14}")
        for k, (p, f) in ordered:
            print(f"  {dds.label(k)[:60]:<60s} {p:>14,.2f} {f:>14,.2f}")


def main():
    save_json = "--json" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: calc_budget_limit.py YYYY-MM | YYYY [--json]")

    arg = args[0]
    today = datetime.now()
    t0 = time.time()

    session = requests.Session()
    session.auth = AUTH
    dds = DdsCache(session)

    if len(arg) == 7 and arg[4] == "-":
        year, month = int(arg[:4]), int(arg[5:7])
        res = calc_month(session, year, month, dds)
        print_month(res, dds)
        res["articles_names"] = {k: dds.label(k) for k in
                                 set(res["plan_by_article"]) | set(res["fact_by_article"])}
        res["organizations"] = TURB_ORGS
        res["generated"] = datetime.now().isoformat(timespec="seconds")
        print(f"\n  Время: {time.time()-t0:.1f}с")

        if save_json:
            out = os.path.join(os.path.dirname(__file__),
                               f"budget_limit_{year}-{month:02d}.json")
            with open(out, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            print(f"  → {out}")
        return

    if len(arg) != 4 or not arg.isdigit():
        raise SystemExit("Ожидался YYYY-MM или YYYY")

    year = int(arg)
    last_month = today.month if year == today.year else 12
    months_out = []

    print("=" * 82)
    print(f"  Бюджет управленческого контура в пределах лимита · {year}"
          f" (январь — {MONTH_RU[last_month]})")
    print(f"  Периметр: {', '.join(TURB_ORGS.values())}")
    print("=" * 82)
    print(f"\n  {'Мес':<10s} {'План':>18s} {'Факт (в лимите)':>18s} {'Δ':>14s} {'Свех лимита':>14s}")
    print(f"  {'─'*10} {'─'*18} {'─'*18} {'─'*14} {'─'*14}")
    for m in range(1, last_month + 1):
        res = calc_month(session, year, m, dds)
        months_out.append(res)
        print(f"  {MONTH_RU[m]:<10s} {res['plan_total']:>18,.2f} {res['fact_total']:>18,.2f} "
              f"{(res['fact_total']-res['plan_total']):>14,.2f} {res['overlimit_fact']:>14,.2f}")
    print(f"\n  Время: {time.time()-t0:.1f}с")

    if save_json:
        result = {
            "year": year,
            "generated": datetime.now().isoformat(timespec="seconds"),
            "organizations": TURB_ORGS,
            "metric": "Бюджет управленческого контура в пределах лимита",
            "months": months_out,
        }
        out = os.path.join(os.path.dirname(__file__), f"budget_limit_{year}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  → {out}")


if __name__ == "__main__":
    main()
