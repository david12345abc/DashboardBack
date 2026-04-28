"""
calc_tekuchest_techdir.py — Текучесть персонала техдирекции.

Источник:
  - Document_ТД_ТекучестьПерсонала
    * ВидДокумента = '0' -> план
    * ВидДокумента = '1' -> факт
    * табличная часть "Текучесть": Месяц, План, Факт

Логика:
  1. Загружаются все документы по текучести.
  2. Подразделения техдирекции определяются по Catalog_СтруктураПредприятия
     через набор алиасов.
  3. Для каждой группы суммируются значения за выбранный месяц.

Важно:
  В текущих данных 1С документы текучести по техблоку заполнены не для всех
  подразделений. Скрипт выводит диагностическую информацию по тому, какие
  подразделения реально нашли соответствие в источнике.

Использование:
  python calc_tekuchest_techdir.py [ГГГГ-ММ]
  Пример:
    python calc_tekuchest_techdir.py 2026-03
"""

import functools
import json
import re
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

import requests

from .cache_manager import locked_call
from .odata_http import request_with_retry

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "techdir_q2_monthly_v1"
CACHE_VERSION = 1

GROUP_ALIASES = {
    "Заместитель тех. директора по качеству": [
        "зам. технического директора по качеству",
        "заместитель тех. директора по качеству",
        "служба качества",
        "отдел менеджмента качества",
    ],
    "Зам. технического директора по сервису": [
        "зам. технического директора по сервису",
        "заместитель технического директора по сервису",
        "сервисная служба",
        "отдел технической поддержки",
    ],
    "Сектор качества разработки": [
        "сектор качества разработки",
    ],
    "Зам. технического директора по проектированию, автоматизации и метрологии": [
        "зам. технического директора по проектированию автоматизации и метрологии",
        "сектор разработки тех. решений",
        "сектор разработки тех решений",
        "отдел метрологии и сертификации",
        "метрологическая служба",
        "отдел метрологии и испытаний",
        "проектный офис",
        "служба автоматизации",
    ],
    "Заместитель тех.директора по пром.безопасности": [
        "заместитель тех.директора по пром.безопасности",
        "заместитель тех директора по пром безопасности",
        "сектор промышленной безопасности",
    ],
    "Эксплуатационная служба": [
        "эксплуатационная служба",
    ],
}

GROUP_ORDER = list(GROUP_ALIASES.keys())

MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def normalize_name(value: str) -> str:
    value = (value or "").lower().replace("ё", "е")
    value = re.sub(r"[^0-9a-zа-я]+", " ", value)
    return " ".join(value.split())


def parse_period():
    month_arg = sys.argv[1] if len(sys.argv) > 1 else "2026-03"
    year, month = map(int, month_arg.split("-"))
    return month_arg, year, month


def fetch_all(session: requests.Session, url: str, page_size: int = 5000, timeout: int = 120):
    rows = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page_size}&$skip={skip}&$format=json"
        r = request_with_retry(session, page_url, timeout=timeout, retries=4, label="techdir_tekuchet")
        if r is None:
            print("    HTTP no-response after retries")
            return rows
        if not r.ok:
            print(f"    HTTP {r.status_code}: {r.text[:300]}")
            return rows
        batch = r.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return rows


def load_structure(session: requests.Session):
    url = (
        f"{BASE}/{quote('Catalog_СтруктураПредприятия')}"
        f"?$select={quote('Ref_Key,Description,Parent_Key,DeletionMark', safe=',_')}"
        f"&$orderby={quote('Ref_Key', safe='')}"
    )
    rows = fetch_all(session, url, page_size=5000, timeout=60)
    by_key = {}
    exact = defaultdict(list)

    for row in rows:
        key = row.get("Ref_Key")
        if not key:
            continue
        by_key[key] = row
        exact[normalize_name(row.get("Description", ""))].append(row)

    return rows, by_key, exact


def is_active_dept(row: dict) -> bool:
    if row.get("DeletionMark"):
        return False
    text = normalize_name(row.get("Description", ""))
    return "ликв" not in text


def resolve_group_department_keys(rows, exact):
    group_keys = {}
    diagnostics = {}

    for group_name, aliases in GROUP_ALIASES.items():
        matched_rows = []
        seen = set()

        for alias in aliases:
            alias_norm = normalize_name(alias)
            exact_matches = [
                row for row in exact.get(alias_norm, [])
                if is_active_dept(row)
            ]
            contains_matches = [
                row for row in rows
                if is_active_dept(row) and alias_norm in normalize_name(row.get("Description", ""))
            ]

            for row in exact_matches + contains_matches:
                key = row.get("Ref_Key")
                if key and key not in seen:
                    seen.add(key)
                    matched_rows.append(row)

        group_keys[group_name] = {row["Ref_Key"] for row in matched_rows}
        diagnostics[group_name] = sorted(
            ((row["Ref_Key"], row.get("Description", "")) for row in matched_rows),
            key=lambda item: item[1],
        )

    return group_keys, diagnostics


def load_docs(session: requests.Session):
    entity = "Document_ТД_ТекучестьПерсонала"
    flt = "DeletionMark eq false"
    url = (
        f"{BASE}/{quote(entity)}"
        f"?$filter={quote(flt, safe='')}"
        f"&$orderby={quote('Ref_Key', safe='')}"
    )
    return fetch_all(session, url, page_size=500, timeout=60)


def _cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"techdir_tekuchet_{year}_{month:02d}.json"


def _load_cache(year: int, month: int) -> dict | None:
    path = _cache_path(year, month)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("source") != SOURCE_TAG:
        return None
    if data.get("cache_version") != CACHE_VERSION:
        return None
    if data.get("cache_date") != date.today().isoformat():
        return None
    return data


def _save_cache(year: int, month: int, payload: dict) -> None:
    try:
        with _cache_path(year, month).open("w", encoding="utf-8") as f:
            json.dump(
                {
                    **payload,
                    "source": SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                    "cache_date": date.today().isoformat(),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        print(f"    ⚠ не удалось сохранить кэш TD-Q2 {year}-{month:02d}")


def aggregate_for_month(docs, group_dept_keys, target_str):
    result = defaultdict(lambda: {"plan": 0.0, "fact": 0.0, "plan_rows": 0, "fact_rows": 0, "docs": 0})
    matched_docs = defaultdict(list)

    for doc in docs:
        dept_key = doc.get("Подразделение_Key", EMPTY)
        if not dept_key or dept_key == EMPTY:
            continue

        matched_groups = [
            group_name for group_name, dept_keys in group_dept_keys.items()
            if dept_key in dept_keys
        ]
        if not matched_groups:
            continue

        vid = str(doc.get("ВидДокумента", ""))
        rows = doc.get("Текучесть", [])

        for group_name in matched_groups:
            matched_docs[group_name].append(doc)
            result[group_name]["docs"] += 1

        for row in rows:
            mes = row.get("Месяц", "")
            if not mes or mes[:7] != target_str:
                continue

            plan_val = float(row.get("План", 0) or 0)
            fact_val = float(row.get("Факт", 0) or 0)

            for group_name in matched_groups:
                if vid == "0":
                    result[group_name]["plan"] += plan_val
                    result[group_name]["plan_rows"] += 1
                elif vid == "1":
                    result[group_name]["fact"] += fact_val
                    result[group_name]["fact_rows"] += 1

    return result, matched_docs


def _last_full_quarter() -> tuple[int, int]:
    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1
    if current_quarter == 1:
        return today.year - 1, 4
    return today.year, current_quarter - 1


def _quarter_months(year: int, quarter: int) -> list[tuple[int, int]]:
    start_month = (quarter - 1) * 3 + 1
    return [(year, start_month + offset) for offset in range(3)]


def compute_td_turnover_month(year: int, month: int) -> dict:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached

    target_str = f"{year}-{month:02d}"
    try:
        session = requests.Session()
        session.auth = AUTH

        structure_rows, structure_by_key, exact = load_structure(session)
        group_dept_keys, diagnostics = resolve_group_department_keys(structure_rows, exact)
        docs = load_docs(session)
        result, matched_docs = aggregate_for_month(docs, group_dept_keys, target_str)
    except Exception as exc:
        print(f"    ⚠ TD-Q2 monthly compute failed for {year}-{month:02d}: {exc}")
        result = defaultdict(lambda: {"plan": 0.0, "fact": 0.0, "plan_rows": 0, "fact_rows": 0, "docs": 0})
        matched_docs = defaultdict(list)
        diagnostics = {group_name: [] for group_name in GROUP_ORDER}
        structure_by_key = {}
        docs = []

    total_plan = 0.0
    total_fact = 0.0
    for group_name in GROUP_ORDER:
        total_plan += result[group_name]["plan"]
        total_fact += result[group_name]["fact"]

    result = {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month],
        "groups": {
            group_name: {
                "plan": round(result[group_name]["plan"], 2),
                "fact": round(result[group_name]["fact"], 2),
                "plan_rows": result[group_name]["plan_rows"],
                "fact_rows": result[group_name]["fact_rows"],
                "docs": len(matched_docs[group_name]),
            }
            for group_name in GROUP_ORDER
        },
        "total_plan": round(total_plan, 2),
        "total_fact": round(total_fact, 2),
        "matched_group_count": sum(1 for g in GROUP_ORDER if matched_docs[g]),
        "diagnostics": {
            "matched_departments": {
                group_name: [desc for _, desc in diagnostics[group_name]]
                for group_name in GROUP_ORDER
            },
            "source_department_count": len(
                {d.get("Подразделение_Key") for d in docs if d.get("Подразделение_Key")}
            ),
        },
    }
    _save_cache(year, month, result)
    return result


def get_td_q2_ytd(year: int | None = None, quarter: int | None = None) -> dict:
    def _runner() -> dict:
        try:
            nonlocal year, quarter
            if year is None or quarter is None:
                year, quarter = _last_full_quarter()

            month_rows = []
            for row_year, row_month in _quarter_months(year, quarter):
                snapshot = compute_td_turnover_month(row_year, row_month)
                plan = snapshot["total_plan"]
                fact = snapshot["total_fact"]
                has_data = plan != 0 or fact != 0
                month_rows.append({
                    "year": row_year,
                    "month": row_month,
                    "month_name": MONTH_RU[row_month].lower(),
                    "plan": plan,
                    "fact": fact,
                    "has_data": has_data,
                })

            with_data = [row for row in month_rows if row["has_data"]]
            months_with_data = len(with_data)

            if with_data:
                avg_plan = round(sum(row["plan"] for row in with_data) / months_with_data, 2)
                avg_fact = round(sum(row["fact"] for row in with_data) / months_with_data, 2)
            else:
                avg_plan = 0.0
                avg_fact = 0.0

            quarter_row = {
                "quarter": quarter,
                "year": year,
                "label": f"Q{quarter} {year}",
                "plan_max_turnover_pct": avg_plan,
                "fact_turnover_pct": avg_fact,
                "kpi_pct": avg_fact if with_data else None,
                "data_complete": months_with_data == 3,
                "months_with_turnover_data": months_with_data,
            }

            return {
                "data_granularity": "quarterly",
                "quarterly_data": [quarter_row],
                "ytd": {
                    "total_plan": avg_plan if with_data else None,
                    "total_fact": avg_fact if with_data else None,
                    "kpi_pct": avg_fact if with_data else None,
                    "quarters_with_data": 1 if with_data else 0,
                    "quarters_total": 1,
                },
                "kpi_period": {
                    "type": "last_full_quarter",
                    "year": year,
                    "quarter": quarter,
                    "label": f"Q{quarter} {year}",
                    "data_complete": months_with_data == 3,
                },
                "debug": {
                    "status": "ok",
                    "kpi_id": "TD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала",
                    "months": month_rows,
                },
            }
        except Exception as exc:
            y, q = year, quarter
            if y is None or q is None:
                y, q = _last_full_quarter()
            print(f"    ⚠ TD-Q2 runner failed for Q{q} {y}: {exc}")
            return {
                "data_granularity": "quarterly",
                "quarterly_data": [],
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "quarters_with_data": 0,
                    "quarters_total": 1,
                },
                "kpi_period": {
                    "type": "last_full_quarter",
                    "year": y,
                    "quarter": q,
                    "label": f"Q{q} {y}",
                    "data_complete": False,
                },
                "debug": {
                    "status": "error",
                    "kpi_id": "TD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала",
                    "error": str(exc),
                },
            }

    return locked_call("techdir_td_q2", _runner)


def main():
    month_arg, year, month = parse_period()
    target_str = f"{year}-{month:02d}"

    session = requests.Session()
    session.auth = AUTH
    t0 = time.time()

    print(f"════════════════════════════════════════════════════════════════════")
    print(f"  ТЕКУЧЕСТЬ ПЕРСОНАЛА ТЕХДИРЕКЦИИ — {month_arg}")
    print(f"════════════════════════════════════════════════════════════════════\n")

    print("[1] Загрузка структуры предприятия ...")
    structure_rows, structure_by_key, exact = load_structure(session)
    print(f"    Подразделений загружено: {len(structure_rows)}")

    group_dept_keys, diagnostics = resolve_group_department_keys(structure_rows, exact)
    print("    Найденные подразделения по группам:")
    for group_name in GROUP_ORDER:
        keys = group_dept_keys[group_name]
        print(f"      {group_name}: {len(keys)}")
        for _, desc in diagnostics[group_name][:10]:
            print(f"        - {desc}")
    print(f"    ({time.time() - t0:.1f}с)")

    print("\n[2] Загрузка документов Document_ТД_ТекучестьПерсонала ...")
    docs = load_docs(session)
    print(f"    Всего документов: {len(docs)}")

    unique_doc_depts = sorted({d.get('Подразделение_Key') for d in docs if d.get('Подразделение_Key')})
    print(f"    Уникальных подразделений в источнике: {len(unique_doc_depts)}")
    for dept_key in unique_doc_depts:
        dept_name = structure_by_key.get(dept_key, {}).get("Description", dept_key)
        print(f"      {dept_name}")
    print(f"    ({time.time() - t0:.1f}с)")

    print(f"\n[3] Расчёт текучести за {MONTH_RU[month]} {year} ...")
    result, matched_docs = aggregate_for_month(docs, group_dept_keys, target_str)
    print(f"    Групп с найденными документами: {sum(1 for g in GROUP_ORDER if matched_docs[g])}")
    print(f"    ({time.time() - t0:.1f}с)")

    print(f"\n{'═' * 94}")
    print(f"  ТЕКУЧЕСТЬ ПЕРСОНАЛА ТЕХДИРЕКЦИИ — {MONTH_RU[month]} {year}")
    print(f"{'═' * 94}")
    print(f"  {'Подразделение':<70} {'План':>8} {'Факт':>8}")
    print(f"  {'-' * 70} {'-' * 8} {'-' * 8}")

    total_plan = 0.0
    total_fact = 0.0

    for group_name in GROUP_ORDER:
        plan_val = result[group_name]["plan"]
        fact_val = result[group_name]["fact"]
        total_plan += plan_val
        total_fact += fact_val
        print(f"  {group_name:<70} {plan_val:>8.2f} {fact_val:>8.2f}")

    print(f"  {'-' * 70} {'-' * 8} {'-' * 8}")
    print(f"  {'ИТОГО':<70} {total_plan:>8.2f} {total_fact:>8.2f}")

    print(f"\n  Детализация по источнику:")
    for group_name in GROUP_ORDER:
        docs_count = len(matched_docs[group_name])
        plan_rows = result[group_name]["plan_rows"]
        fact_rows = result[group_name]["fact_rows"]
        print(
            f"    {group_name}: документов={docs_count}, "
            f"строк плана={plan_rows}, строк факта={fact_rows}"
        )

    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
