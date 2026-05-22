"""
calc_tekuchest_techdir.py — Текучесть персонала техдирекции.

Источник:
  - План: Document_ТД_ТекучестьПерсонала (ВидДокумента = '0', табличная часть «Текучесть»)
  - Факт TD-Q2: штатные единицы и увольнения (techdir_tekuchet_fact)

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
from typing import Any
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
SOURCE_TAG = "techdir_q2_monthly_v2"
CACHE_VERSION = 7

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

TURNOVER_VALUES_UNIT = "%"

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


def resolve_group_department_keys(
    rows,
    exact,
    group_aliases: dict[str, list[str]] | None = None,
):
    """Сопоставление групп KPI с Ref_Key подразделений в Catalog_СтруктураПредприятия."""
    aliases_src = group_aliases if group_aliases is not None else GROUP_ALIASES
    group_keys = {}
    diagnostics = {}

    for group_name, aliases in aliases_src.items():
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


def _tile_month_pairs(year: int, ref_month: int) -> list[tuple[int, int]]:
    """Месяцы, которые нужно вернуть в monthly_data для плитки."""
    return [(year, mm) for mm in range(1, ref_month + 1)]


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


def _normalize_viddokumenta(raw) -> str | None:
    """ВидДокумента: план=0, факт=1 (в OData — строка или целое; bool не маппим)."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        if int(raw) == 0:
            return "0"
        if int(raw) == 1:
            return "1"
    s = str(raw).strip()
    if s in {"0", "1"}:
        return s
    return None


def _month_cell_matches(mes: object, target_str: str) -> bool:
    """Строка табличной части относится к целевому ГГГГ-ММ (как в target_str)."""
    if mes is None:
        return False
    s = str(mes).strip()
    return len(s) >= 7 and s[:7] == target_str


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

        vid = _normalize_viddokumenta(doc.get("ВидДокумента"))
        rows = doc.get("Текучесть", [])

        for group_name in matched_groups:
            matched_docs[group_name].append(doc)
            result[group_name]["docs"] += 1

        for row in rows:
            if not _month_cell_matches(row.get("Месяц"), target_str):
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


def _compute_td_q2_group_totals(
    docs,
    group_dept_keys,
    target_str,
    *,
    branch_aggregate: str = "max",
):
    """Собрать план/факт по группам за месяц из Текучесть + ВидДокумента.

    branch_aggregate:
      - ``max`` (TD-Q2): по каждому документу max(План)/max(Факт) по строкам месяца,
        затем по группе max по документам (подразделение).
      - ``sum_lines`` (QD-Q2 при aggregate sum_all): по документу сумма План/Факт
        по всем строкам целевого месяца, по группе — сумма вкладов документов
        (несколько документов на одно подразделение складываются).
    """
    groups: dict[str, dict[str, float | int]] = {
        group_name: {
            "plan": 0.0,
            "fact": 0.0,
            "plan_rows": 0,
            "fact_rows": 0,
            "docs": 0,
        }
        for group_name in group_dept_keys
    }
    matched_docs = defaultdict(list)

    for group_name, dept_keys in group_dept_keys.items():
        group_plan = 0.0
        group_fact = 0.0
        group_plan_rows = 0
        group_fact_rows = 0
        group_docs = 0

        for doc in docs:
            if doc.get("Подразделение_Key", EMPTY) not in dept_keys:
                continue
            vid = _normalize_viddokumenta(doc.get("ВидДокумента"))
            if vid not in {"0", "1"}:
                continue

            group_docs += 1
            matched_docs[group_name].append(doc)

            row_plan = 0.0
            row_fact = 0.0
            row_matches = 0
            for row in doc.get("Текучесть", []) or []:
                if not _month_cell_matches(row.get("Месяц"), target_str):
                    continue
                row_matches += 1
                p = float(row.get("План", 0) or 0)
                f = float(row.get("Факт", 0) or 0)
                if branch_aggregate == "sum_lines":
                    row_plan += p
                    row_fact += f
                else:
                    row_plan = max(row_plan, p)
                    row_fact = max(row_fact, f)

            if branch_aggregate == "sum_lines":
                if vid == "0":
                    group_plan += row_plan
                    group_plan_rows += row_matches
                else:
                    group_fact += row_fact
                    group_fact_rows += row_matches
            elif vid == "0":
                if row_plan > group_plan:
                    group_plan = row_plan
                    group_plan_rows = row_matches
                elif row_plan == group_plan and row_matches > group_plan_rows:
                    group_plan_rows = row_matches
            else:
                if row_fact > group_fact:
                    group_fact = row_fact
                    group_fact_rows = row_matches
                elif row_fact == group_fact and row_matches > group_fact_rows:
                    group_fact_rows = row_matches

        groups[group_name] = {
            "plan": round(group_plan, 2),
            "fact": round(group_fact, 2),
            "plan_rows": group_plan_rows,
            "fact_rows": group_fact_rows,
            "docs": group_docs,
        }

    return groups, matched_docs


def _last_full_quarter() -> tuple[int, int]:
    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1
    if current_quarter == 1:
        return today.year - 1, 4
    return today.year, current_quarter - 1


def _last_full_month() -> tuple[int, int]:
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _quarter_months(year: int, quarter: int) -> list[tuple[int, int]]:
    start_month = (quarter - 1) * 3 + 1
    return [(year, start_month + offset) for offset in range(3)]


def build_turnover_month_payload(
    year: int,
    month: int,
    *,
    group_aliases: dict[str, list[str]],
    group_order: list[str],
    aggregate: str = "top2",
    fact_from_hr: bool = False,
    hr_scope=None,
) -> dict:
    """Снимок текучести за месяц (без файлового кэша).

    TD-Q2 (aggregate top2, fact_from_hr=True): план — top2 из Document_ТД_ТекучестьПерсонала;
    факт — уволено / штат × 100 % (techdir_tekuchet_fact).

    Для qualdir / gspp / devdir — тот же принцип: top2-план из 1С, факт из HR.
    """
    target_str = f"{year}-{month:02d}"
    branch_aggregate = "sum_lines" if aggregate == "sum_all" else "max"
    fact_debug: dict[str, Any] = {}
    session: requests.Session | None = None
    try:
        session = requests.Session()
        session.auth = AUTH

        structure_rows, structure_by_key, exact = load_structure(session)
        group_dept_keys, diagnostics = resolve_group_department_keys(
            structure_rows, exact, group_aliases,
        )
        docs = load_docs(session)
        result, matched_docs = _compute_td_q2_group_totals(
            docs, group_dept_keys, target_str, branch_aggregate=branch_aggregate,
        )
    except Exception as exc:
        print(f"    ⚠ turnover monthly compute failed for {year}-{month:02d}: {exc}")
        result = {
            group_name: {"plan": 0.0, "fact": 0.0, "plan_rows": 0, "fact_rows": 0, "docs": 0}
            for group_name in group_order
        }
        matched_docs = defaultdict(list)
        diagnostics = {group_name: [] for group_name in group_order}
        docs = []

    if aggregate == "sum_all":
        total_plan = round(
            sum(result[group_name]["plan"] for group_name in group_order),
            2,
        )
        total_fact = round(
            sum(result[group_name]["fact"] for group_name in group_order),
            2,
        )
        plan_agg_label = "sum_all_groups_1c_tekuchet"
        fact_agg_label = "sum_all_groups_1c_tekuchet"
    else:
        ordered_groups = sorted(
            group_order,
            key=lambda group_name: (result.get(group_name, {}).get("plan", 0.0), group_name),
            reverse=True,
        )
        total_plan = round(
            sum((result[group_name]["plan"] for group_name in ordered_groups[:2])),
            2,
        )
        plan_agg_label = "group_max_top2_1c_tekuchet"
        if fact_from_hr:
            from . import techdir_tekuchet_fact as tekuchet_fact
            from .turnover_hr_scope import TurnoverHrScope

            scope = hr_scope
            if scope is None:
                scope = TurnoverHrScope(
                    group_aliases=group_aliases,
                    group_order=group_order,
                )
            fact_payload = tekuchet_fact.compute_turnover_fact_percent(
                year,
                month,
                session=session,
                scope=scope,
            )
            total_fact = float(fact_payload["total_fact"])
            fact_agg_label = fact_payload["fact_source"]
            fact_debug = fact_payload
            for group_name in group_order:
                result[group_name]["fact"] = 0.0
                result[group_name]["fact_rows"] = 0
        else:
            ordered_fact_groups = sorted(
                group_order,
                key=lambda group_name: (result.get(group_name, {}).get("fact", 0.0), group_name),
                reverse=True,
            )
            total_fact = round(
                sum((result[group_name]["fact"] for group_name in ordered_fact_groups[:2])),
                2,
            )
            fact_agg_label = "group_max_top2_1c_tekuchet"

    return {
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
            for group_name in group_order
        },
        "total_plan": round(total_plan, 2),
        "total_fact": round(total_fact, 2),
        "matched_group_count": sum(1 for g in group_order if matched_docs[g]),
        "diagnostics": {
            "matched_departments": {
                group_name: [desc for _, desc in diagnostics[group_name]]
                for group_name in group_order
            },
            "source_department_count": len(
                {d.get("Подразделение_Key") for d in docs if d.get("Подразделение_Key")}
            ),
            "plan_source": plan_agg_label,
            "fact_source": fact_agg_label,
            "fact_hr": fact_debug or None,
            "aggregation": aggregate,
            "branch_aggregate": branch_aggregate,
            "plan_target": None,
        },
    }


def compute_td_turnover_month(year: int, month: int) -> dict:
    cached = _load_cache(year, month)
    if cached is not None:
        return cached

    result = build_turnover_month_payload(
        year,
        month,
        group_aliases=GROUP_ALIASES,
        group_order=GROUP_ORDER,
        fact_from_hr=True,
    )
    _save_cache(year, month, result)
    return result


def get_td_q2_ytd(year: int | None = None, month: int | None = None) -> dict:
    def _runner() -> dict:
        try:
            nonlocal year, month
            if year is None or month is None:
                year, month = _last_full_month()

            month_rows = []
            for row_year, row_month in _tile_month_pairs(year, month):
                snapshot = compute_td_turnover_month(row_year, row_month)
                plan = snapshot["total_plan"]
                fact = snapshot["total_fact"]
                has_data = plan is not None and fact is not None
                month_rows.append({
                    "year": row_year,
                    "month": row_month,
                    "month_name": MONTH_RU[row_month].lower(),
                    "plan": plan,
                    "fact": fact,
                    "kpi_pct": fact,
                    "has_data": has_data,
                    "values_unit": TURNOVER_VALUES_UNIT,
                })

            with_data = [row for row in month_rows if row["has_data"]]
            months_with_data = len(with_data)
            ref_row = next((row for row in month_rows if row["month"] == month and row["year"] == year), None)
            if ref_row is None and month_rows:
                ref_row = month_rows[-1]

            return {
                "data_granularity": "monthly",
                "monthly_data": month_rows,
                "last_full_month_row": dict(ref_row) if ref_row else None,
                "ytd": {
                    "total_plan": ref_row.get("plan") if ref_row else None,
                    "total_fact": ref_row.get("fact") if ref_row else None,
                    "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
                    "months_with_data": months_with_data,
                    "months_total": len(month_rows),
                    "values_unit": TURNOVER_VALUES_UNIT,
                },
                "kpi_period": {
                    "type": "last_full_month",
                    "year": year,
                    "month": month,
                    "month_name": MONTH_RU[month],
                    "data_complete": ref_row is not None,
                },
                "debug": {
                    "status": "ok",
                    "kpi_id": "TD-Q2",
                    "source": "Document_ТД_ТекучестьПерсонала + HR staffing/dismissals",
                    "months": month_rows,
                    "plan_source": "1c_tekuchet",
                    "fact_source": "hr_staff_dismissals_turnover_pct",
                },
            }
        except Exception as exc:
            y, m = year, month
            if y is None or m is None:
                y, m = _last_full_month()
            print(f"    ⚠ TD-Q2 runner failed for {y}-{m:02d}: {exc}")
            return {
                "data_granularity": "monthly",
                "monthly_data": [],
                "last_full_month_row": None,
                "ytd": {
                    "total_plan": None,
                    "total_fact": None,
                    "kpi_pct": None,
                    "months_with_data": 0,
                    "months_total": 0,
                    "values_unit": TURNOVER_VALUES_UNIT,
                },
                "kpi_period": {
                    "type": "last_full_month",
                    "year": y,
                    "month": m,
                    "month_name": MONTH_RU[m],
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
    t0 = time.time()

    print(f"════════════════════════════════════════════════════════════════════")
    print(f"  ТЕКУЧЕСТЬ ПЕРСОНАЛА ТЕХДИРЕКЦИИ — {month_arg}")
    print(f"════════════════════════════════════════════════════════════════════\n")

    print("[1] Получение TD-Q2 snapshot ...")
    snapshot = compute_td_turnover_month(year, month)
    print(f"    Групп с найденными документами: {snapshot['matched_group_count']}")
    print(f"    ({time.time() - t0:.1f}с)")

    print(f"\n{'═' * 94}")
    print(f"  ТЕКУЧЕСТЬ ПЕРСОНАЛА ТЕХДИРЕКЦИИ — {MONTH_RU[month]} {year}")
    print(f"{'═' * 94}")
    print(f"  {'Плитка TD-M3 / месяц':<30} {'План':>8} {'Факт':>8}")
    print(f"  {'-' * 30} {'-' * 8} {'-' * 8}")

    months = snapshot.get("monthly_data") or []
    for row in months:
        print(
            f"  {row['month_name'][:30]:<30} "
            f"{(row.get('plan') or 0):>8.2f} "
            f"{(row.get('fact') or 0):>8.2f}"
        )

    print(f"  {'-' * 30} {'-' * 8} {'-' * 8}")
    ref_row = snapshot.get("last_full_month_row") or {}
    print(
        f"  {'ИТОГО':<30} "
        f"{(ref_row.get('plan') or snapshot.get('total_plan') or 0):>8.2f} "
        f"{(ref_row.get('fact') or snapshot.get('total_fact') or 0):>8.2f}"
    )

    print(f"\n  Графики:")
    print(f"    Линейный: {len(months)} точек")
    print(f"    Столбчатая: {len(months)} точек")

    print(f"\n  Готово за {time.time() - t0:.1f}с")


if __name__ == "__main__":
    main()
