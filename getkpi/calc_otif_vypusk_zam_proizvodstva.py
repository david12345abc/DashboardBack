# -*- coding: utf-8 -*-
"""
OTIF / выполнение плана отгрузок для заместителя директора по производству.

Источники 1С (OData ERP 2.5):
  • План (шт): РегистрСведений InformationRegister_ТД_КонтрольныеДатыИсполненияДоговора.
      Отбор месяца: ДатаОкончанияЮридическихОбязательствПоДоговору ∈ [начало; конец) месяца.
      Количество: из Document_ЗаказКлиента_Товары по Ref_Key=ЗаказКлиента и КодСтроки.
  • Факт отгрузки (шт за месяц): AccumulationRegister_РаспоряженияНаОтгрузку_RecordType,
      ВидДвиженияРегистра = «Расход», Распоряжение_Type = ЗаказКлиента, Period в месяце,
      количество = |Заказано| (в типовой схеме при расходе значение отрицательное).
      В OData поле Распоряжение нельзя использовать в $filter — выборка за период,
      отбор заказов и строк плана на клиенте.
  • OTIF (выполнение к юр. сроку, по количеству):
      для каждой плановой строки — сумма |Заказано| по всем «Расход» с Period ≤ конец дня
      юридического срока; доля min(факт_к_сроку, план) / план по сумме по строкам;
      дополнительно — доля строк, у которых факт_к_сроку ≥ план (полное выполнение строки).

Фильтр заказов (как у операционного директора / «вся Турбулентность-Дон»):
  организации НПО+ООО, проведён, не помечен на удаление, не ТД_НеУчитыватьВПланФакте,
  заполнено соглашение, партнёр не из списка исключений (Алмаз, СКТБ, Метрогазсервис).

Исключения по регистру: позиция с заполненным ПозицияОтменена_Key не входит в план.

Запуск:
  python calc_otif_vypusk_zam_proizvodstva.py 2026-03
  python calc_otif_vypusk_zam_proizvodstva.py 2026-03 --json
  python calc_otif_vypusk_zam_proizvodstva.py 2026 --json
"""
from __future__ import annotations

import functools
import json
import os
import sys
import time
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth
sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

REG_KONTROL = "InformationRegister_ТД_КонтрольныеДатыИсполненияДоговора"
REG_OTGR = "AccumulationRegister_РаспоряженияНаОтгрузку_RecordType"
DOC_ZK = "Document_ЗаказКлиента"
TAB_TOV = "Document_ЗаказКлиента_Товары"
ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"

TURB_ORGS = {
    "fbca2148-6cfd-11e7-812d-001e67112509": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
    "fbca2143-6cfd-11e7-812d-001e67112509": "Турбулентность-Дон ООО",
}

EXCLUDE_PARTNER_NAMES = {
    "АЛМАЗ ООО (рабочий)",
    "СКТБ Турбо-Дон ООО",
    "Метрогазсервис ООО",
}

PARTNERS_CACHE = os.path.join(os.path.dirname(__file__), "partners_exclude_cache.json")
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "otif_vypusk_prod_monthly_v1"
PAGE = 5000
BATCH = 15
TIMEOUT = 120

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
    return CACHE_DIR / f"otif_vypusk_prod_monthly_{year}_{ref_month:02d}.json"


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


def month_bounds(y: int, m: int) -> tuple[str, str]:
    d0 = date(y, m, 1)
    d1 = d0 + timedelta(days=monthrange(y, m)[1])
    return f"{d0.isoformat()}T00:00:00", f"{d1.isoformat()}T00:00:00"


def make_session() -> requests.Session:
    s = requests.Session()
    s.auth = AUTH
    return s


def fetch_paged(session: requests.Session, url: str) -> list[dict]:
    out: list[dict] = []
    skip = 0
    sep = "&" if "?" in url else "?"
    while True:
        r = session.get(f"{url}{sep}$top={PAGE}&$skip={skip}", timeout=TIMEOUT)
        r.raise_for_status()
        batch = r.json().get("value", [])
        if not batch:
            break
        out.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break
    return out


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

    def ensure(self, keys: set[str]) -> None:
        missing = [k for k in keys if k and k != EMPTY and k not in self.partners]
        for i in range(0, len(missing), BATCH):
            batch = missing[i : i + BATCH]
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


def load_zk_headers(session: requests.Session, guids: list[str]) -> dict[str, dict]:
    sel = ",".join([
        "Ref_Key", "Number", "Организация_Key", "Подразделение_Key",
        "Партнер_Key", "Соглашение_Key", "ТД_СопровождениеПродажи",
        "ТД_НеУчитыватьВПланФакте", "Posted", "DeletionMark",
    ])
    result: dict[str, dict] = {}
    for i in range(0, len(guids), BATCH):
        batch = guids[i : i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{g}'" for g in batch), safe="")
        url = f"{BASE}/{DOC_ZK}?$format=json&$filter={flt}&$select={sel}&$top={BATCH}"
        r = session.get(url, timeout=TIMEOUT)
        if not r.ok:
            continue
        for item in r.json().get("value", []):
            result[item["Ref_Key"]] = item
    return result


def passes_filter(h: dict, ctx: PartnersCache) -> bool:
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


def load_control_rows(session: requests.Session, y: int, m: int) -> list[dict]:
    p0, p1 = month_bounds(y, m)
    flt = quote(
        f"ДатаОкончанияЮридическихОбязательствПоДоговору ge datetime'{p0}' "
        f"and ДатаОкончанияЮридическихОбязательствПоДоговору lt datetime'{p1}'",
        safe="",
    )
    sel = quote(
        "ЗаказКлиента,КодСтрокиЗаказаКлиента,"
        "ДатаОкончанияЮридическихОбязательствПоДоговору,ПозицияОтменена_Key",
        safe=",_",
    )
    url = f"{BASE}/{quote(REG_KONTROL)}?$format=json&$filter={flt}&$select={sel}"
    return fetch_paged(session, url)


def legal_deadline_end(d: str) -> datetime:
    """Конец календарного дня юридического срока (для сравнения с Period)."""
    if not d:
        return datetime(1970, 1, 1)
    s = str(d)[:10]
    y, mo, da = int(s[:4]), int(s[5:7]), int(s[8:10])
    return datetime(y, mo, da, 23, 59, 59)


def norm_kod_stroki(v) -> str:
    """Единый ключ строки: в OData КодСтроки — Edm.Int64, не строка."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def load_line_quantities(
    session: requests.Session, pairs: set[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    """pairs: (Ref_Key заказа, КодСтроки)."""
    by_order: dict[str, set[str]] = defaultdict(set)
    for zk, ks in pairs:
        nk = norm_kod_stroki(ks)
        if nk:
            by_order[zk].add(nk)
    qty: dict[tuple[str, str], float] = {}
    for zk, kodes in by_order.items():
        nums: list[int] = []
        for ks in kodes:
            try:
                nums.append(int(ks))
            except ValueError:
                pass
        if not nums:
            continue
        or_lines = " or ".join(f"КодСтроки eq {n}" for n in sorted(set(nums)))
        flt = quote(
            f"Ref_Key eq guid'{zk}' and Отменено eq false and ({or_lines})",
            safe="",
        )
        sel = quote("Ref_Key,КодСтроки,Количество,Отменено", safe=",_")
        url = f"{BASE}/{quote(TAB_TOV)}?$format=json&$filter={flt}&$select={sel}&$top=500"
        r = session.get(url, timeout=90)
        if not r.ok:
            continue
        for row in r.json().get("value", []):
            key = (row["Ref_Key"], norm_kod_stroki(row.get("КодСтроки")))
            if key[1]:
                qty[key] = float(row.get("Количество") or 0)
    return qty


def fetch_shipments_orders(
    session: requests.Session,
    order_guids: list[str],
    dt_from: str,
    dt_to: str,
) -> list[dict]:
    """Расход по заказам за интервал [dt_from; dt_to].

    В публикации OData нельзя ставить Распоряжение в $filter — грузим расход за период
    и отбираем заказы на клиенте.
    """
    out: list[dict] = []
    if not order_guids:
        return out
    allowed = frozenset(order_guids)
    flt = quote(
        f"Active eq true and ВидДвиженияРегистра eq 'Расход' "
        f"and Period ge datetime'{dt_from}' and Period le datetime'{dt_to}'",
        safe="",
    )
    sel = quote(
        "Period,Распоряжение,Распоряжение_Type,КодСтроки,Заказано,Сторно",
        safe=",_",
    )
    url = f"{BASE}/{quote(REG_OTGR)}?$format=json&$filter={flt}&$select={sel}"
    for r in fetch_paged(session, url):
        if (r.get("Распоряжение") or "") not in allowed:
            continue
        if r.get("Распоряжение_Type") != ORDER_TYPE:
            continue
        if r.get("Сторно"):
            continue
        out.append(r)
    return out


def calc_month(session: requests.Session, ctx: PartnersCache, y: int, m: int) -> dict:
    t0 = time.time()
    p0, p1 = month_bounds(y, m)

    ctrl = load_control_rows(session, y, m)
    pairs: set[tuple[str, str]] = set()
    line_deadline: dict[tuple[str, str], datetime] = {}
    skipped_cancel = 0
    bad_date = 0

    for row in ctrl:
        zk = row.get("ЗаказКлиента") or ""
        if not zk or zk == EMPTY:
            continue
        if row.get("ПозицияОтменена_Key") and row.get("ПозицияОтменена_Key") != EMPTY:
            skipped_cancel += 1
            continue
        ks = norm_kod_stroki(row.get("КодСтрокиЗаказаКлиента"))
        if not ks:
            continue
        d = row.get("ДатаОкончанияЮридическихОбязательствПоДоговору") or ""
        if str(d).startswith("0001-01"):
            bad_date += 1
            continue
        key = (zk, ks)
        pairs.add(key)
        line_deadline[key] = legal_deadline_end(str(d))

    order_guids = sorted({p[0] for p in pairs})
    headers = load_zk_headers(session, order_guids)
    ctx.ensure({h.get("Партнер_Key") for h in headers.values()})
    passed = {g for g, h in headers.items() if passes_filter(h, ctx)}

    pairs_ok = {k for k in pairs if k[0] in passed}
    qty_map = load_line_quantities(session, pairs_ok)

    plan_qty = sum(qty_map.get(k, 0.0) for k in pairs_ok)
    plan_lines = len(pairs_ok)

    # Факт за календарный месяц: расход за месяц, отбор строк плана на клиенте.
    fact_month = 0.0
    if pairs_ok and passed:
        flt_m = quote(
            f"Active eq true and ВидДвиженияРегистра eq 'Расход' "
            f"and Period ge datetime'{p0}' and Period lt datetime'{p1}'",
            safe="",
        )
        sel_m = quote(
            "Period,Распоряжение,Распоряжение_Type,КодСтроки,Заказано,Сторно",
            safe=",_",
        )
        url_m = f"{BASE}/{quote(REG_OTGR)}?$format=json&$filter={flt_m}&$select={sel_m}"
        passed_set = frozenset(passed)
        for r in fetch_paged(session, url_m):
            if (r.get("Распоряжение") or "") not in passed_set:
                continue
            if r.get("Распоряжение_Type") != ORDER_TYPE or r.get("Сторно"):
                continue
            key = (r.get("Распоряжение") or "", norm_kod_stroki(r.get("КодСтроки")))
            if not key[1] or key not in pairs_ok:
                continue
            fact_month += abs(float(r.get("Заказано") or 0))

    # OTIF: накопление до конца юр. срока по каждой строке
    if not passed or not pairs_ok:
        ship_hist: list[dict] = []
    else:
        hist_from = f"{y - 5}-01-01T00:00:00"
        dt_to_otif = max(line_deadline[k] for k in pairs_ok)
        dt_to_str = dt_to_otif.strftime("%Y-%m-%dT%H:%M:%S")
        ship_hist = fetch_shipments_orders(session, list(passed), hist_from, dt_to_str)

    ship_by_line: dict[tuple[str, str], list[tuple[datetime, float]]] = defaultdict(list)
    for r in ship_hist:
        key = (r.get("Распоряжение") or "", norm_kod_stroki(r.get("КодСтроки")))
        if not key[1] or key not in pairs_ok:
            continue
        per = r.get("Period")
        if not per:
            continue
        try:
            pdt = datetime.fromisoformat(str(per).replace("Z", "+00:00").split("+")[0])
        except Exception:
            continue
        ship_by_line[key].append((pdt, abs(float(r.get("Заказано") or 0))))

    otif_num = 0.0
    otif_den = 0.0
    lines_full = 0
    lines_partial_info = []

    for key in sorted(pairs_ok):
        pl = qty_map.get(key, 0.0)
        if pl <= 0:
            continue
        dl = line_deadline.get(key, datetime(y, 12, 31))
        cum = 0.0
        for pdt, q in sorted(ship_by_line.get(key, []), key=lambda x: x[0]):
            if pdt <= dl:
                cum += q
        otif_num += min(cum, pl)
        otif_den += pl
        if cum + 1e-9 >= pl:
            lines_full += 1
        lines_partial_info.append({
            "order": key[0], "line": key[1], "plan_qty": pl,
            "shipped_by_deadline": round(cum, 6),
            "deadline": dl.isoformat()[:10],
        })

    pct_qty = (100.0 * otif_num / otif_den) if otif_den > 0 else None
    pct_lines = (100.0 * lines_full / plan_lines) if plan_lines > 0 else None
    exec_month_pct = (100.0 * fact_month / plan_qty) if plan_qty > 0 else None

    return {
        "year": y,
        "month": m,
        "month_name": MONTH_RU[m],
        "period_legal": {"from": p0[:10], "to_exclusive": p1[:10]},
        "scope": {
            "role": "Заместитель директора по производству (метрика по отгрузкам в шт.)",
            "orders_filter": "как у операционного директора (Турбулентность-Дон НПО+ООО)",
            "plan_criterion": "юридический срок окончания обязательств по договору в месяце",
        },
        "register_control_rows": len(ctrl),
        "skipped_cancelled_positions": skipped_cancel,
        "skipped_bad_legal_date": bad_date,
        "plan_lines": plan_lines,
        "plan_qty": round(plan_qty, 6),
        "fact_shipped_qty_in_month": round(fact_month, 6),
        "execution_month_pct": round(exec_month_pct, 2) if exec_month_pct is not None else None,
        "otif_by_qty_pct": round(pct_qty, 2) if pct_qty is not None else None,
        "otif_full_lines": lines_full,
        "otif_lines_pct": round(pct_lines, 2) if pct_lines is not None else None,
        "line_details_top": sorted(
            lines_partial_info,
            key=lambda x: -x["plan_qty"],
        )[:25],
        "elapsed_sec": round(time.time() - t0, 2),
    }


def get_otif_vypusk_prod_monthly(year: int | None = None, month: int | None = None) -> dict:
    today = date.today()
    ref_year, ref_month = _normalize_period(year, month)
    cache_path = _cache_path_monthly(ref_year, ref_month)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    session = make_session()
    ctx = PartnersCache(session)

    months_out: list[dict] = []
    ref_row: dict | None = None
    for mm in range(1, ref_month + 1):
        row = calc_month(session, ctx, ref_year, mm)
        month_row = {
            "year": ref_year,
            "month": mm,
            "month_name": MONTH_RU[mm].lower(),
            "plan": round(float(row.get("plan_qty") or 0), 6),
            "fact": round(float(row.get("fact_shipped_qty_in_month") or 0), 6),
            "kpi_pct": row.get("otif_by_qty_pct")
            if row.get("otif_by_qty_pct") is not None
            else row.get("execution_month_pct"),
            "has_data": abs(float(row.get("plan_qty") or 0)) > 0 or abs(float(row.get("fact_shipped_qty_in_month") or 0)) > 0,
            "values_unit": "шт.",
            "execution_month_pct": row.get("execution_month_pct"),
            "otif_by_qty_pct": row.get("otif_by_qty_pct"),
            "otif_lines_pct": row.get("otif_lines_pct"),
            "plan_lines": row.get("plan_lines"),
            "otif_full_lines": row.get("otif_full_lines"),
        }
        months_out.append(month_row)
        if mm == ref_month:
            ref_row = month_row

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in months_out if row.get("has_data")),
            "months_total": len(months_out),
            "values_unit": "шт." if months_out else None,
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
    save_json = "--json" in sys.argv
    argv = [a for a in sys.argv[1:] if not a.startswith("--")]
    today = datetime.now()

    if not argv:
        y, mo = today.year, today.month
        mode = "one"
    elif len(argv[0]) == 7 and argv[0][4] == "-":
        y, mo = int(argv[0][:4]), int(argv[0][5:7])
        mode = "one"
    elif len(argv[0]) == 4 and argv[0].isdigit():
        y = int(argv[0])
        mode = "year"
    else:
        print("usage: calc_otif_vypusk_zam_proizvodstva.py [YYYY-MM | YYYY] [--json]")
        sys.exit(1)

    session = make_session()
    ctx = PartnersCache(session)

    if mode == "one":
        res = calc_month(session, ctx, y, mo)
        print("\n" + "=" * 76)
        print(f"  OTIF / план отгрузок (юр. даты, шт.) · {res['month_name']} {y}")
        print("=" * 76)
        print(f"\n  План (шт, по строкам ЗК с юр. сроком в месяце):     {res['plan_qty']:,.6g}")
        print(f"  Строк плана:                                      {res['plan_lines']}")
        print(f"  Факт отгрузки за месяц (шт, Расход регистра):     {res['fact_shipped_qty_in_month']:,.6g}")
        print(f"  Выполнение за месяц, %:                           {res['execution_month_pct']}")
        print(f"  OTIF к юр. сроку (Σ min(факт к сроку, план) / Σ план), %: {res['otif_by_qty_pct']}")
        print(f"  Строк выполнено полностью к сроку:                 {res['otif_full_lines']} / {res['plan_lines']}")
        print(f"  OTIF по строкам (полностью), %:                    {res['otif_lines_pct']}")
        print(f"\n  Строк регистра контроля (до фильтра ЗК):           {res['register_control_rows']}")
        print(f"  Время: {res['elapsed_sec']}с")

        if save_json:
            out = os.path.join(
                os.path.dirname(__file__),
                f"otif_vypusk_zam_proizvodstva_{y}-{mo:02d}.json",
            )
            res["generated"] = datetime.now().isoformat(timespec="seconds")
            with open(out, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            print(f"\n  → {out}")
        return

    last_m = today.month if y == today.year else 12
    rows = []
    print("\n" + "=" * 76)
    print(f"  OTIF / план отгрузок · {y} (январь — {MONTH_RU[last_m]})")
    print("=" * 76)
    print(f"\n  {'Мес':<10} {'План шт':>12} {'Факт мес':>12} {'Вып.%':>8} {'OTIF%':>8} {'Строк':>8}")
    print(f"  {'─'*10} {'─'*12} {'─'*12} {'─'*8} {'─'*8} {'─'*8}")
    for mo in range(1, last_m + 1):
        res = calc_month(session, ctx, y, mo)
        rows.append(res)
        print(
            f"  {MONTH_RU[mo]:<10} {res['plan_qty']:>12,.0f} {res['fact_shipped_qty_in_month']:>12,.0f} "
            f"{(res['execution_month_pct'] or 0):>8.1f} {(res['otif_by_qty_pct'] or 0):>8.1f} "
            f"{res['plan_lines']:>8}"
        )
    if save_json:
        out = os.path.join(os.path.dirname(__file__), f"otif_vypusk_zam_proizvodstva_{y}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump({
                "generated": datetime.now().isoformat(timespec="seconds"),
                "year": y,
                "months": rows,
            }, f, ensure_ascii=False, indent=2)
        print(f"\n  → {out}")


if __name__ == "__main__":
    main()
