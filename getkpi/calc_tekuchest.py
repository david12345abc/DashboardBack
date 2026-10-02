"""
Текучесть персонала коммерческой службы.

Факт, % = уволенные за месяц / штатные единицы на последний день месяца × 100.

Штат — прямые подразделения «Коммерческой службы» в справочнике
подразделений организаций. Увольнение — событие «Увольнение» в кадровой
истории. Сотрудник с признаком «не учитывать при текучести» не входит.
Один сотрудник в одном подразделении считается один раз.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

from hr_turnover_sql import (
    DeptSpec,
    SqlConnection,
    bin_to_guid,
    build_report,
    guid_to_1c,
    turnover_percent,
)
from hr_turnover_sql import ORG_TABLE

from . import cache_manager

logger = logging.getLogger(__name__)

# _Reference358 «Коммерческая служба» — родитель отделов, по которым
# проходят увольнения коммерческого блока.
COMMERCIAL_ORG_ROOT = "bcbf8217-32aa-11ea-82ed-ac1f6b05524d"

# Подписи и GUID как в structure.json, чтобы разворот открывал отдел.
STRUCTURE_BY_LABEL = {
    "ОДП": "7587c178-92f6-11f0-96f9-6cb31113810e",
    "Отдел ВЭД": "49480c10-e401-11e8-8283-ac1f6b05524d",
    "Отдел ОПЭОиУ": "34497ef7-810f-11e4-80d6-001e67112509",
    "Отдел продаж БМИ": "9edaa7d4-37a5-11ee-93d3-6cb31113810e",
    "Отдел по работе с ключевыми клиентами": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "Отдел по работе с ПАО «Газпром»": "bd7b5184-9f9c-11e4-80da-001e67112509",
    "Тендерный офис": "1c9f9419-d91b-11e0-8129-cd2988c3db2d",
    "Сектор рекламы и PR": "95dfd1c6-37a4-11ee-93d3-6cb31113810e",
}
NAVIGABLE_LABELS = frozenset({
    "ОДП",
    "Отдел ВЭД",
    "Отдел ОПЭОиУ",
    "Отдел продаж БМИ",
    "Отдел по работе с ключевыми клиентами",
    "Отдел по работе с ПАО «Газпром»",
})
DISPLAY_ORDER = [
    "ОДП",
    "Отдел ВЭД",
    "Отдел ОПЭОиУ",
    "Отдел по работе с ключевыми клиентами",
    "Отдел по работе с ПАО «Газпром»",
    "Отдел продаж БМИ",
    "Тендерный офис",
    "Сектор рекламы и PR",
]

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
CACHE_SOURCE_TAG = "tekuchest_hr_sql_v1"
CACHE_VERSION = 3

KD_M11_SOURCE = (
    "1С: кадровая история и штатное расписание, "
    "подразделения «Коммерческой службы»"
)
KD_M11_FORMULA = (
    "уволенные за месяц / штатные единицы на последний день месяца × 100"
)
KD_M11_DESCRIPTION = (
    "Факт:\n"
    "Текучесть, % = уволенные за месяц / штатные единицы "
    "на последний день месяца × 100.\n\n"
    "Штат — утверждённые позиции штатного расписания, которые на дату среза "
    "ещё не закрыты. Если по позиции есть история использования, берётся "
    "количество ставок из последней записи на эту дату.\n\n"
    "Увольнение — событие «Увольнение» в кадровой истории за календарный месяц. "
    "Один сотрудник в одном подразделении считается один раз. "
    "Сотрудники с признаком «не учитывать при текучести» не входят.\n\n"
    "На развороте — отделы коммерческой службы: процент и сколько уволено из штата."
)


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"tekuchest_{year}_{ref_month:02d}.json"


def _load_cache(year: int, ref_month: int) -> dict | None:
    p = _cache_path(year, ref_month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if (
            data.get("cache_source") == CACHE_SOURCE_TAG
            and data.get("cache_version") == CACHE_VERSION
            and data.get("cache_date") == date.today().isoformat()
        ):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _load_stale_cache(year: int, ref_month: int) -> dict | None:
    p = _cache_path(year, ref_month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_source") == CACHE_SOURCE_TAG and data.get("cache_version") == CACHE_VERSION:
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, ref_month: int, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, ref_month), "w", encoding="utf-8") as f:
            json.dump(
                {
                    **payload,
                    "cache_source": CACHE_SOURCE_TAG,
                    "cache_version": CACHE_VERSION,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError:
        pass


def _label_for_org(name: str) -> str | None:
    folded = name.casefold().replace("ё", "е")
    if "дилер" in folded:
        return "ОДП"
    if "внешнеэконом" in folded:
        return "Отдел ВЭД"
    if "ключев" in folded:
        return "Отдел по работе с ключевыми клиентами"
    if "газпром" in folded:
        return "Отдел по работе с ПАО «Газпром»"
    if "эталон" in folded:
        return "Отдел ОПЭОиУ"
    if "бми" in folded and "продаж" in folded:
        return "Отдел продаж БМИ"
    if "тендер" in folded:
        return "Тендерный офис"
    if "реклам" in folded:
        return "Сектор рекламы и PR"
    return None


def _commercial_org_children(sql: SqlConnection) -> list[tuple[str, str]]:
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT _IDRRef, _Description
            FROM dbo.[{ORG_TABLE}] WITH (NOLOCK)
            WHERE _Marked = 0x00 AND _ParentIDRRef = ?
            """,
            [guid_to_1c(COMMERCIAL_ORG_ROOT)],
        )
        rows = []
        for ref, desc in cur.fetchall():
            name = str(desc or "").strip()
            if not name:
                continue
            rows.append((bin_to_guid(bytes(ref)), name))
    return rows


def _rows_for_month(month_payload: dict) -> list[dict]:
    buckets: dict[str, dict] = {}
    for row in month_payload.get("rows") or []:
        org_name = str(row.get("org_name") or row.get("note") or "").strip()
        label = _label_for_org(org_name) or org_name
        if not label:
            continue
        staff = float(row.get("staff_units") or 0)
        dismissed = int(row.get("dismissed") or 0)
        bucket = buckets.get(label)
        if bucket is None:
            bucket = {"name": label, "staff": 0.0, "dismissed": 0}
            buckets[label] = bucket
        bucket["staff"] += staff
        bucket["dismissed"] += dismissed

    for label in DISPLAY_ORDER:
        buckets.setdefault(label, {"name": label, "staff": 0.0, "dismissed": 0})

    def sort_key(item: dict) -> tuple:
        name = item["name"]
        if name in DISPLAY_ORDER:
            return (0, DISPLAY_ORDER.index(name), name)
        return (1, 0, name)

    out = []
    for item in sorted(buckets.values(), key=sort_key):
        staff = round(float(item["staff"]), 2)
        dismissed = int(item["dismissed"])
        if item["name"] not in DISPLAY_ORDER and staff <= 0 and dismissed <= 0:
            continue
        out.append({
            "name": item["name"],
            "staff": staff,
            "dismissed": dismissed,
            "fact": turnover_percent(staff, dismissed),
            "navigable": item["name"] in NAVIGABLE_LABELS,
            "structure_guid": STRUCTURE_BY_LABEL.get(item["name"], ""),
        })
    return out


def _month_record(year: int, month: int, month_payload: dict) -> dict:
    rows = _rows_for_month(month_payload)
    staff = round(sum(row["staff"] for row in rows), 2)
    dismissed = sum(row["dismissed"] for row in rows)
    by_dept = {}
    for row in rows:
        guid = str(row.get("structure_guid") or "").lower()
        if not guid:
            continue
        by_dept[guid] = {
            "plan": None,
            "fact": row["fact"],
            "staff_units": row["staff"],
            "dismissed": row["dismissed"],
        }
    return {
        "year": year,
        "month": month,
        "plan": None,
        "fact": turnover_percent(staff, dismissed),
        "staff_units": staff,
        "dismissed": dismissed,
        "turnover_rows": rows,
        "by_dept": by_dept,
    }


def _calc_year(year: int, ref_month: int) -> list[dict]:
    sql = SqlConnection()
    children = _commercial_org_children(sql)
    specs = [
        DeptSpec(
            group=org_key,
            org_key=org_key,
            structure_name=_label_for_org(org_name) or org_name,
            note=org_name,
        )
        for org_key, org_name in children
    ]
    report = build_report(
        specs,
        (year, 1),
        (year, ref_month),
        sql=sql,
        hierarchy_mode="listed_only_no_auto_children",
        kpi_label="KD-M11",
    )
    by_month = {row["month"]: row for row in report.get("months") or []}
    return [
        _month_record(year, month, by_month.get(month) or {"rows": []})
        for month in range(1, ref_month + 1)
    ]


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    if not dept_guid:
        return payload
    wanted = str(dept_guid).strip().lower()
    sliced = []
    for row in payload.get("months") or []:
        match = None
        for item in row.get("turnover_rows") or []:
            if str(item.get("structure_guid") or "").lower() == wanted:
                match = item
                break
        if match is None:
            bucket = (row.get("by_dept") or {}).get(wanted) or {}
            sliced.append({
                "year": row.get("year"),
                "month": row.get("month"),
                "plan": None,
                "fact": bucket.get("fact", 0),
                "staff_units": bucket.get("staff_units", 0),
                "dismissed": bucket.get("dismissed", 0),
                "turnover_rows": [],
            })
            continue
        sliced.append({
            "year": row.get("year"),
            "month": row.get("month"),
            "plan": None,
            "fact": match.get("fact", 0),
            "staff_units": match.get("staff", 0),
            "dismissed": match.get("dismissed", 0),
            "turnover_rows": [match],
        })
    return {
        "cache_date": payload.get("cache_date"),
        "cache_refresh_status": payload.get("cache_refresh_status"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced,
    }


def get_tekuchest_monthly(year: int | None = None,
                          month: int | None = None,
                          dept_guid: str | None = None) -> dict:
    """Помесячная текучесть коммерческой службы, январь..месяц."""
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    force_compute = cache_manager.is_force_compute_context()
    cached = None if force_compute else _load_cache(ref_y, ref_m)
    if cached is not None:
        return _slice_payload(cached, dept_guid)
    if not force_compute:
        stale = _load_stale_cache(ref_y, ref_m)
        if stale is not None:
            stale = dict(stale)
            stale["cache_refresh_status"] = "running"
            return _slice_payload(stale, dept_guid)

    logger.info("calc_tekuchest: HR SQL %s-%02d", ref_y, ref_m)
    out_months = _calc_year(ref_y, ref_m)
    payload = {
        "cache_date": today.isoformat(),
        "year": ref_y,
        "ref_month": ref_m,
        "months": out_months,
    }
    _save_cache(ref_y, ref_m, payload)
    return _slice_payload(payload, dept_guid)


if __name__ == "__main__":
    import functools
    sys.stdout.reconfigure(encoding="utf-8")
    _print = functools.partial(print, flush=True)

    today = date.today()
    args = sys.argv[1:]
    if args and len(args[0]) == 7:
        y, m = int(args[0][:4]), int(args[0][5:7])
    else:
        y, m = _last_full_month(today)

    _print(f"\n{'═' * 60}")
    _print("  ТЕКУЧЕСТЬ ПЕРСОНАЛА")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_tekuchest_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'%':>8s} {'Увол.':>8s} {'Штат':>8s}")
    _print(f"  {'─' * 40}")
    for row in data.get("months", []):
        _print(
            f"  {MONTH_RU[row['month']]:<12s} "
            f"{float(row['fact']):>8.1f} "
            f"{int(row['dismissed']):>8d} "
            f"{float(row['staff_units']):>8.1f}"
        )

    _print(f"\n  По подразделениям ({MONTH_RU[m]} {y}):")
    _print(f"  {'─' * 55}")
    for row in data.get("months", []):
        if row["month"] != m:
            continue
        for item in row.get("turnover_rows") or []:
            _print(
                f"    {item['name']:<42s} "
                f"{float(item['fact']):>6.1f}%  "
                f"{int(item['dismissed'])} из {item['staff']}"
            )

    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
