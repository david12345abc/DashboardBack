"""
calc_psd_vipusk_plan.py — ПСД · Выпуск · ПЛАН отгрузки за месяц.

Алгоритм (зафиксированные допущения, вариант A из ТЗ пользователя):

  • Источник плана    : документ Document_ЗаказКлиента + табличная часть Товары.
  • «За период»       : строка попадает в месяц, если ДатаОтгрузки в строке
                        принадлежит [месяц_начало; месяц_конец] — это
                        плановая дата отгрузки позиции в ЗК.
  • Организации       : ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО и Турбулентность-Дон ООО.
  • Исключаем         : DeletionMark=true, Posted=false,
                        ТД_НеУчитыватьВПланФакте=true, строки с Отменено=true,
                        статус НеСогласован (заказ не подтверждён).
  • Объём             : Количество из строки ЗК (в базовой ед. измерения
                        номенклатуры). Сумма не считается (по запросу).
  • Детализация       : одна цифра на месяц (как в tiles_2026.json).

Запуск:
  python calc_psd_vipusk_plan.py                     # текущий месяц
  python calc_psd_vipusk_plan.py 2026-04             # апрель 2026
  python calc_psd_vipusk_plan.py 2026-04 --json      # + сохранить psd_vipusk_plan_2026-04.json
  python calc_psd_vipusk_plan.py 2026-04 --force     # игнор кэшей
  python calc_psd_vipusk_plan.py 2026-04 --all-stat  # вкл. НаСогласовании / Закрыт / любой статус
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

TURB_ORGS = {
    "fbca2148-6cfd-11e7-812d-001e67112509": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
    "fbca2143-6cfd-11e7-812d-001e67112509": "Турбулентность-Дон ООО",
}

# Статусы 1С ERP 2.5 для ЗаказКлиента, которые НЕ учитываем в плане.
# НеСогласован = отклонён / не подтверждён — в плане его нет.
EXCLUDE_STATUS_DEFAULT = {"НеСогласован"}
PAGE = 5000
BATCH = 15
TIMEOUT = 120
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "psd_vipusk_plan_v1"


# ═══════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНОЕ
# ═══════════════════════════════════════════════════════

def parse_month_arg(value: str) -> tuple[date, date]:
    y, m = value.split("-")
    y_i, m_i = int(y), int(m)
    start = date(y_i, m_i, 1)
    end = date(y_i, m_i, monthrange(y_i, m_i)[1])
    return start, end


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


# ═══════════════════════════════════════════════════════
# ШАГ 1. Строки ЗК с ДатаОтгрузки в месяце
# ═══════════════════════════════════════════════════════

def load_zk_lines(session: requests.Session, m_start: date, m_end: date) -> list[dict]:
    period_from = f"{m_start.isoformat()}T00:00:00"
    period_to = f"{(date(m_end.year + (m_end.month // 12), (m_end.month % 12) + 1, 1)).isoformat()}T00:00:00"
    flt = quote(
        f"ДатаОтгрузки ge datetime'{period_from}' and "
        f"ДатаОтгрузки lt datetime'{period_to}'",
        safe=""
    )
    sel = ",".join([
        "Ref_Key", "LineNumber", "КодСтроки", "ДатаОтгрузки",
        "Номенклатура_Key", "Количество", "Отменено", "Склад_Key",
    ])
    url = (
        f"{BASE}/Document_ЗаказКлиента_Товары"
        f"?$format=json&$filter={flt}&$select={sel}"
    )
    return fetch_all_paged(session, url)


# ═══════════════════════════════════════════════════════
# ШАГ 2. Шапки ЗК по списку GUID-ов (батч по 15)
# ═══════════════════════════════════════════════════════

def load_zk_headers(
    session: requests.Session, guids: list[str]
) -> dict[str, dict]:
    sel = ",".join([
        "Ref_Key", "Number", "Date", "Организация_Key", "Статус",
        "Posted", "DeletionMark", "ТД_НеУчитыватьВПланФакте",
        "Подразделение_Key", "Партнер_Key", "упоПроектнаяЗадача_Key",
    ])
    result: dict[str, dict] = {}
    for i in range(0, len(guids), BATCH):
        batch = guids[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{g}'" for g in batch), safe="")
        url = (
            f"{BASE}/Document_ЗаказКлиента"
            f"?$format=json&$filter={flt}&$select={sel}&$top={BATCH}"
        )
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        for item in r.json().get("value", []):
            result[item["Ref_Key"]] = item
    return result


# ═══════════════════════════════════════════════════════
# ШАГ 3. Имена номенклатуры (для вывода), батчем
# ═══════════════════════════════════════════════════════

def load_nomenclature_names(
    session: requests.Session, keys: list[str]
) -> dict[str, str]:
    result: dict[str, str] = {}
    for i in range(0, len(keys), BATCH):
        batch = keys[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{BASE}/Catalog_Номенклатура"
            f"?$format=json&$filter={flt}&$select=Ref_Key,Description&$top={BATCH}"
        )
        try:
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            for item in r.json().get("value", []):
                result[item["Ref_Key"]] = (item.get("Description") or "").strip()
        except requests.HTTPError:
            continue
    return result


def _cache_path_snapshot(month_arg: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"psd_vipusk_plan_{month_arg}.json"


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"psd_vipusk_plan_monthly_{year}_{ref_month:02d}.json"


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


def _calculate_month_result(month_arg: str, *, all_statuses: bool = False) -> dict:
    m_start, m_end = parse_month_arg(month_arg)
    session = make_session()

    lines = load_zk_lines(session, m_start, m_end)
    uniq_guids = sorted({row["Ref_Key"] for row in lines})
    headers = load_zk_headers(session, uniq_guids)
    exclude_statuses = set() if all_statuses else EXCLUDE_STATUS_DEFAULT

    passed_guids: set[str] = set()
    reject = {
        "not_found": 0, "org": 0, "posted": 0, "deleted": 0,
        "ne_uchit": 0, "status": 0,
    }
    status_breakdown: dict[str, int] = {}

    for g in uniq_guids:
        h = headers.get(g)
        if not h:
            reject["not_found"] += 1
            continue
        if h.get("Организация_Key") not in TURB_ORGS:
            reject["org"] += 1
            continue
        if not h.get("Posted"):
            reject["posted"] += 1
            continue
        if h.get("DeletionMark"):
            reject["deleted"] += 1
            continue
        if h.get("ТД_НеУчитыватьВПланФакте"):
            reject["ne_uchit"] += 1
            continue
        st = h.get("Статус") or "?"
        if st in exclude_statuses:
            reject["status"] += 1
            continue
        passed_guids.add(g)
        status_breakdown[st] = status_breakdown.get(st, 0) + 1

    total_qty = 0.0
    kept_lines: list[dict] = []
    by_org: dict[str, float] = {g: 0.0 for g in TURB_ORGS}

    for row in lines:
        g = row["Ref_Key"]
        if g not in passed_guids:
            continue
        if row.get("Отменено"):
            continue
        qty = float(row.get("Количество") or 0)
        if qty <= 0:
            continue
        h = headers[g]
        org_k = h.get("Организация_Key")
        total_qty += qty
        by_org[org_k] = by_org.get(org_k, 0.0) + qty
        kept_lines.append({
            "zk_guid": g,
            "zk_number": h.get("Number"),
            "zk_date": h.get("Date"),
            "line": row.get("КодСтроки"),
            "ship_date": row.get("ДатаОтгрузки"),
            "nomenclature_key": row.get("Номенклатура_Key"),
            "warehouse_key": row.get("Склад_Key"),
            "qty": qty,
            "org_key": org_k,
            "dept_key": h.get("Подразделение_Key") or EMPTY,
            "status": h.get("Статус"),
            "partner_key": h.get("Партнер_Key"),
            "project_task_key": h.get("упоПроектнаяЗадача_Key"),
        })

    return {
        "month": month_arg,
        "period_from": m_start.isoformat(),
        "period_to": m_end.isoformat(),
        "generated": datetime.now().isoformat(timespec="seconds"),
        "source": SOURCE_TAG,
        "algorithm": "A: плановая дата отгрузки строки ЗК в месяце",
        "organizations": list(TURB_ORGS.values()),
        "excluded_statuses": sorted(exclude_statuses),
        "plan_qty_total": round(total_qty, 3),
        "by_org": {
            name: round(by_org.get(guid, 0.0), 3)
            for guid, name in TURB_ORGS.items()
        },
        "status_breakdown_zk_count": status_breakdown,
        "reject_stats": reject,
        "zk_count_passed": len(passed_guids),
        "lines_count": len(kept_lines),
        "lines": kept_lines,
    }


def get_psd_vipusk_plan_snapshot(month_arg: str) -> dict:
    cache_path = _cache_path_snapshot(month_arg)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        return cached

    payload = _calculate_month_result(month_arg, all_statuses=False)
    _save_json(cache_path, payload)
    return payload


def get_psd_vipusk_plan_monthly(year: int, ref_month: int) -> dict:
    cache_path = _cache_path_monthly(year, ref_month)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        return cached

    today = date.today()
    rows_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        if year > today.year or (year == today.year and mm > today.month):
            break
        month_arg = f"{year}-{mm:02d}"
        snap = get_psd_vipusk_plan_snapshot(month_arg)
        rows_out.append({
            "year": year,
            "month": mm,
            "period_from": snap.get("period_from"),
            "period_to": snap.get("period_to"),
            "plan_qty_total": float(snap.get("plan_qty_total") or 0),
        })

    payload = {
        "year": year,
        "ref_month": ref_month,
        "source": SOURCE_TAG,
        "months": rows_out,
    }
    _save_json(cache_path, payload)
    return payload


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}

    if args:
        month_arg = args[0]
    else:
        today = date.today()
        month_arg = f"{today.year}-{today.month:02d}"

    save_json = "--json" in flags
    all_statuses = "--all-stat" in flags

    m_start, m_end = parse_month_arg(month_arg)

    print("=" * 78)
    print(f"  ПСД · Выпуск · ПЛАН отгрузки за {month_arg}")
    print(f"  Период:       {m_start} — {m_end}")
    print(f"  Организации:  {', '.join(TURB_ORGS.values())}")
    print(f"  Источник:     Document_ЗаказКлиента_Товары.ДатаОтгрузки")
    print("=" * 78)

    session = make_session()
    t0 = time.time()

    # ─── Шаг 1 ───
    print(f"\n▸ Шаг 1 · Строки ЗК с ДатаОтгрузки в {month_arg}")
    lines = load_zk_lines(session, m_start, m_end)
    uniq_guids = sorted({row["Ref_Key"] for row in lines})
    print(f"  Строк:        {len(lines)}")
    print(f"  Уник. ЗК:     {len(uniq_guids)}  · {time.time()-t0:.1f}с")

    # ─── Шаг 2 ───
    print(f"\n▸ Шаг 2 · Шапки ЗаказКлиента")
    t = time.time()
    headers = load_zk_headers(session, uniq_guids)
    print(f"  Получено:     {len(headers)} / {len(uniq_guids)}  · {time.time()-t:.1f}с")

    # ─── Шаг 3. Фильтр ───
    print(f"\n▸ Шаг 3 · Фильтрация заказов")
    exclude_statuses = set() if all_statuses else EXCLUDE_STATUS_DEFAULT

    passed_guids: set[str] = set()
    reject = {
        "not_found": 0, "org": 0, "posted": 0, "deleted": 0,
        "ne_uchit": 0, "status": 0,
    }
    status_breakdown: dict[str, int] = {}

    for g in uniq_guids:
        h = headers.get(g)
        if not h:
            reject["not_found"] += 1
            continue
        if h.get("Организация_Key") not in TURB_ORGS:
            reject["org"] += 1
            continue
        if not h.get("Posted"):
            reject["posted"] += 1
            continue
        if h.get("DeletionMark"):
            reject["deleted"] += 1
            continue
        if h.get("ТД_НеУчитыватьВПланФакте"):
            reject["ne_uchit"] += 1
            continue
        st = h.get("Статус") or "?"
        if st in exclude_statuses:
            reject["status"] += 1
            continue
        passed_guids.add(g)
        status_breakdown[st] = status_breakdown.get(st, 0) + 1

    print(f"  Прошло:       {len(passed_guids)} ЗК")
    print(f"  Отсеяно:      not_found={reject['not_found']}, org={reject['org']}, "
          f"posted={reject['posted']}, del={reject['deleted']}, "
          f"ne_uchit={reject['ne_uchit']}, status={reject['status']}")
    print(f"  По статусам (прошедшие): {status_breakdown}")

    # ─── Шаг 4. Агрегация ───
    print(f"\n▸ Шаг 4 · Σ Количество по плановым строкам")
    total_qty = 0.0
    kept_lines: list[dict] = []
    by_org: dict[str, float] = {g: 0.0 for g in TURB_ORGS}
    by_dept: dict[str, float] = {}

    for row in lines:
        g = row["Ref_Key"]
        if g not in passed_guids:
            continue
        if row.get("Отменено"):
            continue
        qty = float(row.get("Количество") or 0)
        if qty <= 0:
            continue
        h = headers[g]
        org_k = h.get("Организация_Key")
        dept_k = h.get("Подразделение_Key") or EMPTY
        total_qty += qty
        by_org[org_k] = by_org.get(org_k, 0.0) + qty
        by_dept[dept_k] = by_dept.get(dept_k, 0.0) + qty
        kept_lines.append({
            "zk_guid": g,
            "zk_number": h.get("Number"),
            "zk_date": h.get("Date"),
            "line": row.get("КодСтроки"),
            "ship_date": row.get("ДатаОтгрузки"),
            "nomenclature_key": row.get("Номенклатура_Key"),
            "warehouse_key": row.get("Склад_Key"),
            "qty": qty,
            "org_key": org_k,
            "dept_key": dept_k,
            "status": h.get("Статус"),
            "partner_key": h.get("Партнер_Key"),
            "project_task_key": h.get("упоПроектнаяЗадача_Key"),
        })

    print(f"  Строк плана:  {len(kept_lines)}")
    print(f"  Σ Количество: {total_qty:,.3f}")

    # ─── Вывод ───
    print("\n" + "=" * 78)
    print(f"  РЕЗУЛЬТАТ · {month_arg}")
    print("=" * 78)
    print(f"  План выпуска (кол-во к отгрузке): {total_qty:>14,.3f}")
    print(f"\n  По организации:")
    for guid, name in TURB_ORGS.items():
        print(f"    {name:<42s} {by_org.get(guid, 0.0):>14,.3f}")

    print(f"\n  Время: {time.time()-t0:.1f}с")

    # ─── JSON ───
    if save_json:
        result = {
            "month": month_arg,
            "period_from": m_start.isoformat(),
            "period_to": m_end.isoformat(),
            "generated": datetime.now().isoformat(timespec="seconds"),
            "algorithm": "A: плановая дата отгрузки строки ЗК в месяце",
            "organizations": list(TURB_ORGS.values()),
            "excluded_statuses": sorted(exclude_statuses),
            "plan_qty_total": round(total_qty, 3),
            "by_org": {
                name: round(by_org.get(guid, 0.0), 3)
                for guid, name in TURB_ORGS.items()
            },
            "status_breakdown_zk_count": status_breakdown,
            "reject_stats": reject,
            "zk_count_passed": len(passed_guids),
            "lines_count": len(kept_lines),
            "lines": kept_lines,
        }
        out_path = os.path.join(
            os.path.dirname(__file__),
            f"psd_vipusk_plan_{month_arg}.json",
        )
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  → {out_path}")


if __name__ == "__main__":
    main()
