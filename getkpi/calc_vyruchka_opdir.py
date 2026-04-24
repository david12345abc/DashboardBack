"""
calc_vyruchka_opdir.py — ПЛИТКА «Выручка без НДС» для операционного директора.

Фильтры (ОпДир видит всю компанию Турбулентность-Дон):
  • Организация    ∈ {ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО, Турбулентность-Дон ООО}
  • Подразделение  — любое (в т.ч. непродающие) заказа этих двух орг.
  • Партнёр        — НЕ в {АЛМАЗ ООО (рабочий), СКТБ Турбо-Дон ООО,
                          Метрогазсервис ООО}. Турбулентность-Дон
                          как партнёр ДОПУСКАЕТСЯ (внутр. обороты между
                          НПО и ООО — часть выручки холдинга).
  • Соглашение заполнено.
  • ТД_НеУчитыватьВПланФакте = false.
  • Posted = true, DeletionMark = false, Статус ≠ НеСогласован.

Показатель — выручка без НДС:
  • ПЛАН  : Σ (СуммаСНДС − СуммаНДС) по строкам Document_ЗаказКлиента_Товары
            с ДатаОтгрузки в заданном месяце (с учётом Отменено=false).
  • ФАКТ  : Σ Сумма регистра AccumulationRegister_РаспоряженияНаОтгрузку
            (ВидДвижения = Расход, Active=true, Сторно=false) за месяц,
            умноженная на net_share ЗК = (ΣСуммаСНДС − ΣСуммаНДС) / ΣСуммаСНДС.

Запуск:
  python calc_vyruchka_opdir.py 2026-04               # один месяц (план + факт)
  python calc_vyruchka_opdir.py 2026                  # все месяцы до текущего
  python calc_vyruchka_opdir.py 2026-04 --json        # + vyruchka_opdir_2026-04.json
  python calc_vyruchka_opdir.py 2026      --json      # + vyruchka_opdir_2026.json
"""
import functools
import json
import os
import sys
import time
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

# ═══════════════════════════════════════════════════════
# КОНФИГ
# ═══════════════════════════════════════════════════════

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

# Организации холдинга Турбулентность-Дон (видимая ОпДир зона)
TURB_ORGS = {
    "fbca2148-6cfd-11e7-812d-001e67112509": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
    "fbca2143-6cfd-11e7-812d-001e67112509": "Турбулентность-Дон ООО",
}

# Отделы продаж — только для разбивки отображения (не для фильтрации).
# Для ОпДир подразделение НЕ ограничиваем; то, что не в списке,
# попадёт в «Прочие».
SALES_DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Эталонное оборудование",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Ключевые клиенты",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Дилерские продажи",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "ПАО Газпром",
}

# Внутренние контрагенты холдинга, которых ОпДир исключает
# (Турбулентность-Дон как партнёр — оставляем: это межсегментная выручка).
EXCLUDE_PARTNER_NAMES = {
    "АЛМАЗ ООО (рабочий)",
    "СКТБ Турбо-Дон ООО",
    "Метрогазсервис ООО",
}

ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"
PARTNERS_CACHE = os.path.join(os.path.dirname(__file__), "partners_exclude_cache.json")

PAGE = 5000
BATCH = 15
TIMEOUT = 120
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "vyruchka_opdir_monthly_v1"

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _dept_breakdown_named(raw: dict | None) -> dict[str, float]:
    raw = raw or {}
    out: dict[str, float] = {}
    for key, value in raw.items():
        if key == "_other":
            out["Прочие подразделения"] = round(float(value or 0), 2)
            continue
        out[SALES_DEPARTMENTS.get(key, str(key))] = round(float(value or 0), 2)
    return out


# ═══════════════════════════════════════════════════════
# УТИЛИТЫ
# ═══════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.auth = AUTH
    return s


def fetch_all_paged(session: requests.Session, base_url: str, page: int = PAGE) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    sep = "&" if "?" in base_url else "?"
    while True:
        url = f"{base_url}{sep}$top={page}&$skip={skip}"
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        chunk = r.json().get("value", [])
        rows.extend(chunk)
        if len(chunk) < page:
            break
        skip += page
    return rows


def month_bounds(year: int, month: int) -> tuple[str, str, date, date]:
    """Возвращает period_from (ISO), period_to (ISO, эксклюзивно), date_start, date_end."""
    start = date(year, month, 1)
    last_day = monthrange(year, month)[1]
    end = date(year, month, last_day)
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    return f"{start.isoformat()}T00:00:00", f"{nxt.isoformat()}T00:00:00", start, end


# ═══════════════════════════════════════════════════════
# ПАРТНЁРЫ: кэш/загрузка
# ═══════════════════════════════════════════════════════

class PartnersCache:
    def __init__(self, session: requests.Session):
        self.session = session
        self.partners: dict[str, str] = {}
        self.exclude_keys: set[str] = set()
        if os.path.exists(PARTNERS_CACHE):
            try:
                self.partners = json.load(open(PARTNERS_CACHE, encoding="utf-8"))
            except Exception:
                self.partners = {}

    def ensure(self, keys):
        missing = [k for k in keys if k and k != EMPTY and k not in self.partners]
        for i in range(0, len(missing), BATCH):
            batch = missing[i:i + BATCH]
            flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
            url = (
                f"{BASE}/Catalog_Партнеры?$format=json"
                f"&$filter={flt}&$select=Ref_Key,Description&$top={BATCH}"
            )
            r = self.session.get(url, timeout=30)
            if r.ok:
                for item in r.json().get("value", []):
                    self.partners[item["Ref_Key"]] = (item.get("Description") or "").strip()
        if missing:
            try:
                with open(PARTNERS_CACHE, "w", encoding="utf-8") as f:
                    json.dump(self.partners, f, ensure_ascii=False)
            except Exception:
                pass
        self.exclude_keys = {k for k, v in self.partners.items() if v in EXCLUDE_PARTNER_NAMES}


# ═══════════════════════════════════════════════════════
# ШАПКИ ЗК
# ═══════════════════════════════════════════════════════

def load_zk_headers(session: requests.Session, guids: list[str]) -> dict[str, dict]:
    sel = ",".join([
        "Ref_Key", "Number", "Date", "Организация_Key", "Подразделение_Key",
        "Партнер_Key", "Соглашение_Key", "ТД_СопровождениеПродажи",
        "ТД_НеУчитыватьВПланФакте", "Posted", "DeletionMark", "Статус",
        "ЦенаВключаетНДС",
    ])
    result: dict[str, dict] = {}
    for i in range(0, len(guids), BATCH):
        batch = guids[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{g}'" for g in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента?$format=json"
            f"&$filter={flt}&$select={sel}&$top={BATCH}"
        )
        r = session.get(url, timeout=TIMEOUT)
        if not r.ok:
            continue
        for item in r.json().get("value", []):
            result[item["Ref_Key"]] = item
    return result


def passes_opdir_filter(h: dict, ctx: PartnersCache) -> bool:
    """Фильтр для ОпДир: всё по НПО+ООО кроме явно «чужих» контрагентов."""
    if not h:
        return False
    if h.get("Организация_Key") not in TURB_ORGS:
        return False
    if not h.get("Posted"):
        return False
    if h.get("DeletionMark"):
        return False
    if h.get("ТД_НеУчитыватьВПланФакте"):
        return False
    agreement = h.get("Соглашение_Key", EMPTY)
    if not agreement or agreement == EMPTY:
        return False
    partner = h.get("Партнер_Key", EMPTY)
    if partner in ctx.exclude_keys:
        return False
    return True


# ═══════════════════════════════════════════════════════
# СУММЫ НДС ПО ЗК (агрегат по Товарам)
# ═══════════════════════════════════════════════════════

def load_zk_vat_totals(session: requests.Session, guids: list[str]) -> dict[str, dict]:
    """Для каждого ЗК: {sum_s_nds, sum_nds}. Без учёта отменённых строк."""
    sel = "Ref_Key,Сумма,СуммаНДС,СуммаСНДС,Отменено"
    totals: dict[str, dict] = {g: {"sum_s_nds": 0.0, "sum_nds": 0.0, "sum_bez_vat": 0.0}
                               for g in guids}
    for i in range(0, len(guids), BATCH):
        batch = guids[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{g}'" for g in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента_Товары?$format=json"
            f"&$filter={flt}&$select={sel}"
        )
        rows = fetch_all_paged(session, url)
        for row in rows:
            g = row["Ref_Key"]
            if row.get("Отменено"):
                continue
            s_nds = float(row.get("СуммаСНДС") or 0)
            nds = float(row.get("СуммаНДС") or 0)
            summa = float(row.get("Сумма") or 0)
            t = totals.setdefault(g, {"sum_s_nds": 0.0, "sum_nds": 0.0, "sum_bez_vat": 0.0})
            t["sum_s_nds"] += s_nds
            t["sum_nds"] += nds
            t["sum_bez_vat"] += (s_nds - nds) if s_nds > 0 else max(summa - nds, 0.0)
    return totals


# ═══════════════════════════════════════════════════════
# ПЛАН ПО МЕСЯЦУ (только строки ЗК с ДатаОтгрузки в месяце)
# ═══════════════════════════════════════════════════════

def calc_plan(session: requests.Session, year: int, month: int,
              ctx: PartnersCache) -> dict:
    p_from, p_to, _, _ = month_bounds(year, month)
    flt = quote(
        f"ДатаОтгрузки ge datetime'{p_from}' and "
        f"ДатаОтгрузки lt datetime'{p_to}'",
        safe=""
    )
    sel = ",".join([
        "Ref_Key", "LineNumber", "КодСтроки",
        "Сумма", "СуммаНДС", "СуммаСНДС", "Отменено",
    ])
    url = (
        f"{BASE}/Document_ЗаказКлиента_Товары?$format=json"
        f"&$filter={flt}&$select={sel}"
    )
    lines = fetch_all_paged(session, url)
    zk_guids = sorted({r["Ref_Key"] for r in lines})
    headers = load_zk_headers(session, zk_guids)
    ctx.ensure({h.get("Партнер_Key") for h in headers.values()})

    passed_guids = {g for g, h in headers.items() if passes_opdir_filter(h, ctx)}

    total = 0.0
    by_dept: dict[str, float] = {k: 0.0 for k in SALES_DEPARTMENTS}
    by_dept["_other"] = 0.0
    by_org: dict[str, float] = {k: 0.0 for k in TURB_ORGS}
    for row in lines:
        g = row["Ref_Key"]
        if g not in passed_guids:
            continue
        if row.get("Отменено"):
            continue
        s_nds = float(row.get("СуммаСНДС") or 0)
        nds = float(row.get("СуммаНДС") or 0)
        summa = float(row.get("Сумма") or 0)
        net = (s_nds - nds) if s_nds > 0 else max(summa - nds, 0.0)
        if net <= 0:
            continue
        total += net
        dept = headers[g].get("Подразделение_Key", EMPTY)
        if dept in SALES_DEPARTMENTS:
            by_dept[dept] += net
        else:
            by_dept["_other"] += net
        org = headers[g].get("Организация_Key")
        if org in by_org:
            by_org[org] += net
    return {
        "total": round(total, 2),
        "by_dept": {dept: round(v, 2) for dept, v in by_dept.items()},
        "by_org": {org: round(v, 2) for org, v in by_org.items()},
        "lines_count": sum(1 for r in lines if r["Ref_Key"] in passed_guids and not r.get("Отменено")),
        "zk_passed": len(passed_guids),
    }


# ═══════════════════════════════════════════════════════
# ФАКТ ПО МЕСЯЦУ (РаспоряженияНаОтгрузку, Расход) с вычетом НДС
# ═══════════════════════════════════════════════════════

def calc_fact(session: requests.Session, year: int, month: int,
              ctx: PartnersCache) -> dict:
    p_from, p_to, _, _ = month_bounds(year, month)
    flt = quote(
        f"Period ge datetime'{p_from}' and Period lt datetime'{p_to}' "
        f"and Active eq true",
        safe=""
    )
    sel = "Period,Active,Распоряжение,Распоряжение_Type,ВидДвиженияРегистра,Сумма,Сторно"
    url = (
        f"{BASE}/AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
        f"?$format=json&$filter={flt}&$select={sel}"
    )
    entries = fetch_all_paged(session, url)

    ship_entries = [
        {
            "guid": r["Распоряжение"],
            "summa": float(r.get("Сумма") or 0),
            "vid": r.get("ВидДвиженияРегистра", ""),
        }
        for r in entries
        if r.get("Распоряжение_Type") == ORDER_TYPE and not r.get("Сторно")
    ]
    zk_guids = sorted({e["guid"] for e in ship_entries if e["guid"]})

    headers = load_zk_headers(session, zk_guids)
    ctx.ensure({h.get("Партнер_Key") for h in headers.values()})

    passed_guids = {g for g, h in headers.items() if passes_opdir_filter(h, ctx)}

    # НДС-доля по каждому прошедшему ЗК (по всем товарным строкам, не отмен.)
    vat_totals = load_zk_vat_totals(session, list(passed_guids))

    total = 0.0
    by_dept: dict[str, float] = {k: 0.0 for k in SALES_DEPARTMENTS}
    by_dept["_other"] = 0.0
    by_org: dict[str, float] = {k: 0.0 for k in TURB_ORGS}
    for e in ship_entries:
        g = e["guid"]
        if g not in passed_guids or e["vid"] != "Расход":
            continue
        s = abs(e["summa"])
        vt = vat_totals.get(g)
        if vt and vt["sum_s_nds"] > 0:
            net_share = max(0.0, (vt["sum_s_nds"] - vt["sum_nds"]) / vt["sum_s_nds"])
        else:
            net_share = 1.0
        net = s * net_share
        total += net
        dept = headers[g].get("Подразделение_Key", EMPTY)
        if dept in SALES_DEPARTMENTS:
            by_dept[dept] += net
        else:
            by_dept["_other"] += net
        org = headers[g].get("Организация_Key")
        if org in by_org:
            by_org[org] += net
    return {
        "total": round(total, 2),
        "by_dept": {dept: round(v, 2) for dept, v in by_dept.items()},
        "by_org": {org: round(v, 2) for org, v in by_org.items()},
        "register_entries": len(entries),
        "zk_passed": len(passed_guids),
    }


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def parse_arg(value: str) -> tuple[int, int | None]:
    """'2026' → (2026, None); '2026-04' → (2026, 4)."""
    if "-" in value:
        y, m = value.split("-")
        return int(y), int(m)
    return int(value), None


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"vyruchka_opdir_monthly_{year}_{ref_month:02d}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _save_json(path: Path, data: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def get_vyruchka_opdir_monthly(
    year: int | None = None,
    month: int | None = None,
    dept_guid: str | None = None,  # noqa: ARG001
    force: bool = False,
) -> dict:
    """
    Помесячная выручка без НДС для операционного директора.

    dept_guid игнорируется: плитка всегда считается по всей компании.
    """
    today = date.today()
    ref_year = int(year) if year is not None else today.year
    if month is not None:
        ref_month = max(1, min(12, int(month)))
    else:
        ref_month = today.month if ref_year == today.year else 12

    cache_path = _cache_path_monthly(ref_year, ref_month)
    if not force:
        cached = _load_json(cache_path)
        if cached is not None and cached.get("source") == SOURCE_TAG:
            return cached

    session = make_session()
    ctx = PartnersCache(session)

    months_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        plan = calc_plan(session, ref_year, mm, ctx)
        fact = calc_fact(session, ref_year, mm, ctx)
        plan_total = float(plan.get("total") or 0)
        fact_total = float(fact.get("total") or 0)
        months_out.append({
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(plan_total, 2),
            "fact": round(fact_total, 2),
            "kpi_pct": round(fact_total / plan_total * 100, 1) if plan_total > 0 else None,
            "has_data": abs(plan_total) > 0 or abs(fact_total) > 0,
            "plan_by_dept": _dept_breakdown_named(plan.get("by_dept") or {}),
            "fact_by_dept": _dept_breakdown_named(fact.get("by_dept") or {}),
            "plan_by_org": plan.get("by_org") or {},
            "fact_by_org": fact.get("by_org") or {},
        })

    with_data = [row for row in months_out if row.get("has_data")]
    total_plan = sum(float(row.get("plan") or 0) for row in months_out)
    total_fact = sum(float(row.get("fact") or 0) for row in months_out)
    payload = {
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


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    save_json = "--json" in flags

    today = date.today()
    if args:
        year, month = parse_arg(args[0])
    else:
        year, month = today.year, today.month

    session = make_session()
    ctx = PartnersCache(session)
    t0 = time.time()

    if month is not None:
        print("=" * 78)
        print(f"  Выручка без НДС (ОпДир) · {year}-{month:02d} ({MONTH_RU[month]})")
        print(f"  Организации: {', '.join(TURB_ORGS.values())}")
        print(f"  Исключены контрагенты: {', '.join(sorted(EXCLUDE_PARTNER_NAMES))}")
        print("=" * 78)

        print(f"\n▸ План")
        t = time.time()
        plan = calc_plan(session, year, month, ctx)
        print(f"  ЗК прошло:    {plan['zk_passed']}  · строк: {plan['lines_count']}")
        print(f"  Σ план (без НДС):  {plan['total']:>16,.2f} ₽  · {time.time()-t:.1f}с")

        print(f"\n▸ Факт")
        t = time.time()
        fact = calc_fact(session, year, month, ctx)
        print(f"  Регистр записей: {fact['register_entries']}  · ЗК прошло: {fact['zk_passed']}")
        print(f"  Σ факт (без НДС):  {fact['total']:>16,.2f} ₽  · {time.time()-t:.1f}с")

        print(f"\n  {'Разрез':<30s} {'План':>16s} {'Факт':>16s} {'Δ':>14s}")
        print(f"  {'─'*30} {'─'*16} {'─'*16} {'─'*14}")
        for dk, name in SALES_DEPARTMENTS.items():
            p = plan["by_dept"].get(dk, 0.0)
            f = fact["by_dept"].get(dk, 0.0)
            print(f"  {name:<30s} {p:>16,.2f} {f:>16,.2f} {(f - p):>14,.2f}")
        p_o = plan["by_dept"].get("_other", 0.0)
        f_o = fact["by_dept"].get("_other", 0.0)
        print(f"  {'Прочие подразделения':<30s} {p_o:>16,.2f} {f_o:>16,.2f} {(f_o - p_o):>14,.2f}")
        print(f"  {'─'*30} {'─'*16} {'─'*16} {'─'*14}")
        for og, name in TURB_ORGS.items():
            p = plan["by_org"].get(og, 0.0)
            f = fact["by_org"].get(og, 0.0)
            print(f"  {name:<30s} {p:>16,.2f} {f:>16,.2f} {(f - p):>14,.2f}")
        print(f"  {'─'*30} {'─'*16} {'─'*16} {'─'*14}")
        print(f"  {'ИТОГО':<30s} {plan['total']:>16,.2f} {fact['total']:>16,.2f} "
              f"{(fact['total'] - plan['total']):>14,.2f}")

        print(f"\n  Время: {time.time()-t0:.1f}с")

        if save_json:
            result = {
                "year": year, "month": month,
                "generated": datetime.now().isoformat(timespec="seconds"),
                "organizations": list(TURB_ORGS.values()),
                "excluded_partner_names": sorted(EXCLUDE_PARTNER_NAMES),
                "plan": plan, "fact": fact,
                "sales_departments": SALES_DEPARTMENTS,
                "turb_orgs": TURB_ORGS,
            }
            out = os.path.join(os.path.dirname(__file__),
                               f"vyruchka_opdir_{year}-{month:02d}.json")
            with open(out, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"  → {out}")
        return

    # Годовой режим
    last_month = today.month if year == today.year else 12
    months_out = []
    print("=" * 78)
    print(f"  Выручка без НДС (ОпДир) · {year} (январь — {MONTH_RU[last_month]})")
    print(f"  Организации: {', '.join(TURB_ORGS.values())}")
    print("=" * 78)
    print(f"\n  {'Мес':<10s} {'План':>16s} {'Факт':>16s} {'Δ':>14s}")
    print(f"  {'─'*10} {'─'*16} {'─'*16} {'─'*14}")
    for m in range(1, last_month + 1):
        t = time.time()
        plan = calc_plan(session, year, m, ctx)
        fact = calc_fact(session, year, m, ctx)
        print(f"  {MONTH_RU[m]:<10s} {plan['total']:>16,.2f} {fact['total']:>16,.2f} "
              f"{(fact['total']-plan['total']):>14,.2f}  · {time.time()-t:.0f}с")
        months_out.append({
            "month": m, "label": MONTH_RU[m],
            "plan_total": plan["total"], "fact_total": fact["total"],
            "plan_by_dept": plan["by_dept"], "fact_by_dept": fact["by_dept"],
            "plan_by_org": plan["by_org"], "fact_by_org": fact["by_org"],
        })
    print(f"\n  Время: {time.time()-t0:.0f}с")

    if save_json:
        result = {
            "year": year,
            "generated": datetime.now().isoformat(timespec="seconds"),
            "organizations": list(TURB_ORGS.values()),
            "excluded_partner_names": sorted(EXCLUDE_PARTNER_NAMES),
            "metric": "Выручка без НДС",
            "sales_departments": SALES_DEPARTMENTS,
            "turb_orgs": TURB_ORGS,
            "months": months_out,
        }
        out = os.path.join(os.path.dirname(__file__),
                           f"vyruchka_opdir_{year}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  → {out}")


if __name__ == "__main__":
    main()
