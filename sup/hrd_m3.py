"""
HRD-M3 — бюджет службы управления персоналом в пределах лимита.

Эталон в DashboardBack называется HRD-M3 (может содержать ошибки):
  DashboardBack/sup/hrd_m3.py + hrd_m3_budget_fact.py + hrd_m3_budget_plan.py
  → getkpi.budget_request_fact.compute_budget_request_fact_monthly
  (оплаты из AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент)

Логика факта:
  Факт = Σ (СуммаОплаты − СуммаКВыплатеСверхЛимита)
  по активным движениям регистра ДДС за календарный месяц,
  с привязкой к заявке на расход ДС, у которой одновременно:
    • ТД_ЦФО = «Директор НПО»
    • Подразделение = «Служба управления персоналом»

  Суммы в SQL уже со знаком (сторно отрицательное).
  Здесь только СуммаОплаты − СверхЛимита (как HRD-M3 / IT-M3).

План 2026 — константы из hrd_m3_budget_plan.py (сумма 15 строк × месяц).

SQL (erp_pm):
  AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент → dbo._AccumRg51416
  Document_ЗаявкаНаРасходованиеДенежныхСредств           → dbo._Document726
  Catalog ТД_ЦФО                                         → dbo._Reference127708
  Catalog_СтруктураПредприятия                           → dbo._Reference513

Период в SQL = календарный год + 2000.

Использование:
  python sup/hrd_m3.py
  python sup/hrd_m3.py --check
  python sup/hrd_m3.py 2026
  python sup/hrd_m3.py 2026-05
  python sup/hrd_m3.py 2026-01 2026-07
"""

from __future__ import annotations

import functools
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

SCRIPT_DIR = Path(__file__).resolve().parent
YEAR_OFFSET = 2000
ROUND_TOLERANCE = 0.01

REG = "_AccumRg51416"
DOC = "_Document726"
STRUCT = "_Reference513"
CFO_CAT = "_Reference127708"

COL_ORG = "_Fld51418RRef"
COL_REQ = "_Fld140229RRef"
COL_PAY = "_Fld51433"
COL_OVER = "_Fld51443"
COL_DOC_CFO = "_Fld127709RRef"
COL_DOC_DEPT = "_Fld22796RRef"

EMPTY_BIN = b"\x00" * 16

ORG_GUIDS = (
    "fbca2148-6cfd-11e7-812d-001e67112509",  # ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО
    "fbca2143-6cfd-11e7-812d-001e67112509",  # Турбулентность-Дон ООО
)

# План 2026, руб./мес. (DashboardBack/sup/hrd_m3_budget_plan.py).
HRD_M3_PLAN_BY_MONTH_2026: dict[int, int] = {
    1: 1_582_504,
    2: 1_082_835,
    3: 1_616_114,
    4: 2_503_381,
    5: 1_786_718,
    6: 1_888_955,
    7: 1_169_276,
    8: 838_255,
    9: 1_040_987,
    10: 1_566_296,
    11: 608_639,
    12: 536_682,
}

HRD_M3_TD_CFO_LABEL = "Директор НПО"
HRD_M3_TD_CFO_ALIASES: tuple[str, ...] = ("директор нпо",)

HRD_M3_DEPARTMENT_LABEL = "Служба управления персоналом"
HRD_M3_DEPARTMENT_ALIASES: tuple[str, ...] = (
    "служба управления персоналом",
)

# Эталон факта: янв–май = кэш DashboardBack sup_hrd_m3_budget_2026_07.json;
# июнь–июль — live SQL (кэш отстаёт: было 1_590_412 / 7_250).
REFERENCE_FACT_2026: dict[int, float] = {
    1: 898_839.33,
    2: 47_300.0,
    3: 330_965.0,
    4: 298_217.0,
    5: 40_474.0,
    6: 2_423_628.0,
    7: 126_015.0,
}

MONTH_NAMES = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def guid_to_1c_binary(guid_str: str) -> bytes:
    b = bytes.fromhex(guid_str.replace("-", ""))
    return b[8:10] + b[10:16] + b[6:8] + b[4:6] + b[0:4]


ORG_BINS: tuple[bytes, ...] = tuple(guid_to_1c_binary(g) for g in ORG_GUIDS)


def normalize_name(value: str | None) -> str:
    value = (value or "").lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in value).split())


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _sql_period_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    y = year + YEAR_OFFSET
    start = datetime(y, month, 1)
    if month == 12:
        end = datetime(y + 1, 1, 1)
    else:
        end = datetime(y, month + 1, 1)
    return start, end


def plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in HRD_M3_PLAN_BY_MONTH_2026:
        return float(HRD_M3_PLAN_BY_MONTH_2026[month])
    return None


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(fact / plan * 100.0, 1)


def money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.2f}".replace(",", " ")


def parse_month(value: str) -> tuple[int, int]:
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Месяц должен быть в формате ГГГГ-ММ")
    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def parse_period_args(argv: list[str] | None = None) -> tuple[tuple[int, int], tuple[int, int], str]:
    args = [a.strip() for a in (argv if argv is not None else sys.argv[1:]) if a.strip()]
    args = [a for a in args if not a.startswith("-")]
    now = datetime.now()
    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)
    if len(args) == 1 and len(args[0]) == 4 and args[0].isdigit():
        year = int(args[0])
        return (year, 1), (year, 12), args[0]
    if len(args) == 1:
        period = parse_month(args[0])
        return period, period, args[0]
    if len(args) == 2:
        start = parse_month(args[0])
        end = parse_month(args[1])
        if start > end:
            raise ValueError("Дата начала должна быть не позже даты окончания")
        return start, end, f"{args[0]}_{args[1]}"
    raise ValueError("Используйте: ГГГГ, ГГГГ-ММ или ГГГГ-ММ ГГГГ-ММ")


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    year, month = start
    out: list[tuple[int, int]] = []
    while (year, month) <= end:
        out.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return out


def _label_matches(norm: str, needles: frozenset[str]) -> bool:
    if not norm:
        return False
    for needle in needles:
        if len(needle) < 4:
            continue
        if norm == needle or needle in norm or norm in needle:
            return True
    return False


def _cfo_norms() -> frozenset[str]:
    labels = (HRD_M3_TD_CFO_LABEL,) + HRD_M3_TD_CFO_ALIASES
    return frozenset(n for n in (normalize_name(s) for s in labels) if n)


def _dept_norms() -> frozenset[str]:
    labels = (HRD_M3_DEPARTMENT_LABEL,) + HRD_M3_DEPARTMENT_ALIASES
    return frozenset(n for n in (normalize_name(s) for s in labels) if n)


CFO_NORMS = _cfo_norms()
DEPT_NORMS = _dept_norms()


def _resolve_filter_keys(cur) -> tuple[list[bytes], list[bytes], dict[str, Any]]:
    cur.execute(f"SELECT _IDRRef, _Description FROM [{CFO_CAT}] WITH (NOLOCK)")
    cfo_keys: list[bytes] = []
    cfo_names: list[str] = []
    for rid, desc in cur.fetchall():
        if _label_matches(normalize_name(desc), CFO_NORMS):
            cfo_keys.append(bytes(rid))
            cfo_names.append(desc or "")

    cur.execute(f"SELECT _IDRRef, _Description FROM [{STRUCT}] WITH (NOLOCK)")
    dept_keys: list[bytes] = []
    dept_names: list[str] = []
    for rid, desc in cur.fetchall():
        if _label_matches(normalize_name(desc), DEPT_NORMS):
            dept_keys.append(bytes(rid))
            dept_names.append(desc or "")

    if not cfo_keys:
        raise RuntimeError(f"Не найден ТД_ЦФО «{HRD_M3_TD_CFO_LABEL}» в {CFO_CAT}")
    if not dept_keys:
        raise RuntimeError(f"Не найдено подразделение «{HRD_M3_DEPARTMENT_LABEL}» в {STRUCT}")

    return cfo_keys, dept_keys, {
        "cfo_names": cfo_names,
        "dept_names": dept_names,
        "cfo_keys_count": len(cfo_keys),
        "dept_keys_count": len(dept_keys),
    }


def compute_hrd_m3_fact_monthly(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
    *,
    cfo_keys: list[bytes] | None = None,
    dept_keys: list[bytes] | None = None,
) -> dict[str, Any]:
    """Сумма фактических оплат HRD-M3 за календарный месяц (руб.)."""
    sql = sql or SqlConnection()
    p_start, p_end = _sql_period_bounds(year, month)
    counts = {
        "register_rows_matched": 0,
        "requests_matched": 0,
        "rows_counted": 0,
    }
    meta: dict[str, Any] = {}

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        if cfo_keys is None or dept_keys is None:
            cfo_keys, dept_keys, meta = _resolve_filter_keys(cur)

        org_ph = ",".join("?" * len(ORG_BINS))
        cfo_ph = ",".join("?" * len(cfo_keys))
        dept_ph = ",".join("?" * len(dept_keys))
        cur.execute(
            f"""
            SELECT r.[{COL_PAY}], r.[{COL_OVER}], r.[{COL_REQ}]
            FROM [{REG}] r WITH (NOLOCK)
            INNER JOIN [{DOC}] d WITH (NOLOCK)
                    ON d._IDRRef = r.[{COL_REQ}]
            WHERE r._Period >= ? AND r._Period < ?
              AND r._Active = 0x01
              AND r.[{COL_ORG}] IN ({org_ph})
              AND r.[{COL_REQ}] <> ?
              AND d.[{COL_DOC_CFO}] IN ({cfo_ph})
              AND d.[{COL_DOC_DEPT}] IN ({dept_ph})
            """,
            [p_start, p_end, *ORG_BINS, EMPTY_BIN, *cfo_keys, *dept_keys],
        )
        rows = cur.fetchall()

    total = 0.0
    reqs: set[bytes] = set()
    counts["register_rows_matched"] = len(rows)
    for pay, over, req in rows:
        net = _as_float(pay) - _as_float(over)
        if net == 0:
            continue
        total += net
        counts["rows_counted"] += 1
        if req:
            reqs.add(bytes(req))
    counts["requests_matched"] = len(reqs)

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "total_fact": round(total, 2),
        "counts": counts,
        "debug": {
            "status": "ok",
            "kpi_id": "HRD-M3-FACT",
            "register": REG,
            "document": DOC,
            "period_start": p_start.isoformat(sep="T"),
            "period_end": p_end.isoformat(sep="T"),
            "required_td_cfo": HRD_M3_TD_CFO_LABEL,
            "required_department": HRD_M3_DEPARTMENT_LABEL,
            "filter_meta": meta,
            "rule": (
                "fact = sum(СуммаОплаты - СуммаКВыплатеСверхЛимита) "
                "for ТД_ЦФО=Директор НПО AND Подразделение=Служба управления персоналом"
            ),
        },
    }


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cfo_keys, dept_keys, meta = _resolve_filter_keys(cur)

    report: list[dict[str, Any]] = []
    for year, month in iter_months(start_period, end_period):
        fact_payload = compute_hrd_m3_fact_monthly(
            year,
            month,
            sql,
            cfo_keys=cfo_keys,
            dept_keys=dept_keys,
        )
        plan = plan_for_month(year, month)
        fact = fact_payload.get("total_fact")
        fact_f = float(fact) if isinstance(fact, (int, float)) else None
        report.append(
            {
                "year": year,
                "month": month,
                "month_name": MONTH_NAMES[month],
                "plan": plan,
                "fact": fact_f,
                "kpi_pct": kpi_pct(plan, fact_f),
                "has_data": plan is not None and fact_f is not None,
                "values_unit": "руб.",
                "counts": fact_payload.get("counts") or {},
                "filter_meta": meta,
            }
        )
    return report


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "HRD-M3 — бюджет (SQL)",
        f"Источник: {REG} + {DOC} + {CFO_CAT}/{STRUCT}",
        f"ТД_ЦФО: {HRD_M3_TD_CFO_LABEL}",
        f"Подразделение: {HRD_M3_DEPARTMENT_LABEL}",
        "Факт: Σ (СуммаОплаты − СверхЛимита) по оплатам в месяце",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8} {'Заявок':>8}",
        f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        plan = row["plan"]
        fact = row["fact"]
        pct = row["kpi_pct"]
        plan_s = money(plan)
        fact_s = money(fact)
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        reqs = (row.get("counts") or {}).get("requests_matched", 0)
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{plan_s:>14} {fact_s:>14} {pct_s:>8} {reqs:>8}"
        )

    plan_sum = sum(float(r["plan"] or 0) for r in rows if r.get("plan") is not None)
    fact_sum = sum(float(r["fact"] or 0) for r in rows if r.get("fact") is not None)
    lines.extend(
        [
            f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}",
            f"{'ИТОГО':<10} {money(plan_sum):>14} {money(fact_sum):>14} {'':>8} {'':>8}",
            "",
        ]
    )
    if rows:
        meta = rows[0].get("filter_meta") or {}
        lines.append(f"ЦФО: {', '.join(meta.get('cfo_names') or [])}")
        lines.append(f"Подразделения: {', '.join(meta.get('dept_names') or [])}")
        lines.append("")
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]], text: str) -> tuple[Path, Path]:
    txt_path = SCRIPT_DIR / f"hrd_m3_{period_slug}.txt"
    json_path = SCRIPT_DIR / f"hrd_m3_{period_slug}.json"
    txt_path.write_text(text, encoding="utf-8-sig")
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return txt_path, json_path


def build_hrd_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    today = date.today()
    if year is None or month is None:
        if today.month == 1:
            ref_y, ref_m = today.year - 1, 12
        else:
            ref_y, ref_m = today.year, today.month - 1
        year = year or ref_y
        month = month or ref_m

    rows = build_monthly_report((year, 1), (year, month))
    monthly_rows = [
        {
            "month": r["month"],
            "year": r["year"],
            "month_name": r["month_name"],
            "plan": r["plan"],
            "fact": r["fact"],
            "kpi_pct": r["kpi_pct"],
            "has_data": r["has_data"],
            "values_unit": "руб.",
        }
        for r in rows
    ]
    # Для HRD-M3 показываем именно выбранный месяц, включая текущий
    # незавершённый: откат на последний полный месяц здесь не применяется.
    display_row = next(
        (
            row
            for row in monthly_rows
            if row.get("year") == year and row.get("month") == month
        ),
        None,
    )
    display_m = int(display_row["month"]) if display_row and display_row.get("month") else month
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(display_row) if display_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": year,
            "month": display_m,
            "month_name": MONTH_NAMES[display_m],
        },
        "ytd": {
            "total_plan": display_row.get("plan") if display_row else None,
            "total_fact": display_row.get("fact") if display_row else None,
            "kpi_pct": display_row.get("kpi_pct") if display_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok",
            "kpi_id": "HRD-M3",
            "source": "sup.hrd_m3.sql",
            "plan_source": "HRD_M3_PLAN_BY_MONTH_2026",
            "fact_source": f"{REG} / {DOC}",
            "required_td_cfo": HRD_M3_TD_CFO_LABEL,
            "required_department": HRD_M3_DEPARTMENT_LABEL,
        },
    }


def run_check() -> int:
    print("Сверка HRD-M3 факт · 2026 (кэш DashboardBack / REFERENCE)")
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        cfo_keys, dept_keys, meta = _resolve_filter_keys(cur)
    print(f"  ЦФО: {', '.join(meta.get('cfo_names') or [])}")
    print(f"  Подразделение: {', '.join(meta.get('dept_names') or [])}")

    all_ok = True
    for month, ref in sorted(REFERENCE_FACT_2026.items()):
        snap = compute_hrd_m3_fact_monthly(
            2026, month, sql, cfo_keys=cfo_keys, dept_keys=dept_keys
        )
        fact = float(snap["total_fact"] or 0)
        ok = abs(fact - ref) <= ROUND_TOLERANCE
        if not ok:
            all_ok = False
        mark = "OK" if ok else "РАСХОЖДЕНИЕ"
        print(
            f"  {MONTH_NAMES[month]}: {money(fact)} / {money(ref)} "
            f"(Δ {money(fact - ref)}) ({mark})"
        )
    return 0 if all_ok else 2


def main() -> None:
    try:
        argv = sys.argv[1:]
        if "--check" in argv:
            sys.exit(run_check())

        start, end, slug = parse_period_args(argv)
        rows = build_monthly_report(start, end)
        text = format_report(rows)
        print(text)
        txt_path, json_path = save_report(slug, rows, text)
        print(f"Отчёт сохранён: {txt_path}")
        print(f"JSON: {json_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
# TestKPIDump: sup/hrd_m4.py (бюджет) → DashboardBack KPI HRD-M3
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

HRD_M3_YTD_CACHE_PREFIX = "sup_hrd_m3_budget"
HRD_M3_YTD_DISK_TAG = "sup_hrd_m3_budget_sql_payload_v2_selected_month"
HRD_M3_YTD_DISK_VERSION = 4
HRD_M3_MONTHLY_CACHE_PREFIX = "sup_hrd_m3_budget_fact_sql_monthly"
HRD_M3_MONTHLY_SOURCE_TAG = "sup_hrd_m3_budget_fact_sql_monthly_v1"
HRD_M3_MONTHLY_CACHE_VERSION = 1


def monthly_cache_path(year: int, month: int) -> _Path:
    return month_cache_path(HRD_M3_MONTHLY_CACHE_PREFIX, year, month)


def get_hrd_m3_fact_monthly(year: int, month: int) -> dict[str, Any]:
    """Факт бюджета за один месяц (SQL) с дисковым кэшем."""
    from devdir import ytd_json_cache

    path = monthly_cache_path(year, month)
    perpetual = ytd_json_cache.is_ref_period_fully_past(year, month)

    def _compute_and_save() -> dict[str, Any]:
        payload = compute_hrd_m3_fact_monthly(year, month)
        ytd_json_cache.save_payload(
            path,
            payload,
            source_tag=HRD_M3_MONTHLY_SOURCE_TAG,
            version=HRD_M3_MONTHLY_CACHE_VERSION,
        )
        return payload

    return ytd_json_cache.resolve_payload(
        path,
        source_tag=HRD_M3_MONTHLY_SOURCE_TAG,
        version=HRD_M3_MONTHLY_CACHE_VERSION,
        perpetual=perpetual,
        lock_key=f"sup_hrd_m3_budget_fact_sql_{year}_{month:02d}",
        compute_fn=_compute_and_save,
    )


def cache_file_path_for_period(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(HRD_M3_YTD_CACHE_PREFIX, ry, rm)


def get_hrd_m3_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=HRD_M3_YTD_CACHE_PREFIX,
        source_tag=HRD_M3_YTD_DISK_TAG,
        version=HRD_M3_YTD_DISK_VERSION,
        lock_key_prefix="sup_hrd_m3_sql",
        compute_fn=lambda y, m: build_hrd_m3_payload(y, m),
        kpi_id="HRD-M3",
    )

