"""
QD-M3 — бюджет блока в пределах лимита (факт из MSSQL-дампа 1С).

Эталон (OData, может содержать ошибки имён):
  DashboardBack/qualdir/qd_m3_fact.py + qd_m3.py

Логика факта (как docstring эталона / calc_budget_limit):
  Факт = Σ (СуммаОплаты − СуммаКВыплатеСверхЛимита)
  по активным движениям регистра ДДС за календарный месяц,
  с привязкой к заявке на расход ДС, у которой:
    • ТД_ЦФО ∈ контур качества, ИЛИ
    • Подразделение ∈ подразделения QD-M4 (семь п/п качества);
  и статья ДДС ∈ список статей контура качества.

  Суммы в SQL уже со знаком (сторно отрицательное) — доп. инверсия не нужна.
  В эталонном коде к сумме ошибочно добавлялись предоплата/постоплата
  (двойной счёт при разнесении по LineNumber) — здесь только СуммаОплаты.

План 2026 — константы из DashboardBack/qualdir/qd_m3.py.

SQL (erp_pm):
  AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент
      → dbo._AccumRg51416
        _Fld51418RRef  = Организация
        _Fld51419RRef  = Подразделение
        _Fld51423RRef  = СтатьяДвиженияДенежныхСредств
        _Fld140229RRef = ЗаявкаНаРасходованиеДенежныхСредств
        _Fld51433      = СуммаОплаты
        _Fld51443      = СуммаКВыплатеСверхЛимита
        _Fld140228     = Сторно (0x01)
  Document_ЗаявкаНаРасходованиеДенежныхСредств
      → dbo._Document726
        _Fld127709RRef = ТД_ЦФО → _Reference127708
        _Fld22796RRef  = Подразделение → _Reference513
  Catalog_СтатьиДвиженияДенежныхСредств → dbo._Reference503
  Catalog_СтруктураПредприятия          → dbo._Reference513

Период в SQL = календарный год + 2000.

Использование:
  python qualdir/qd_m3.py
  python qualdir/qd_m3.py 2026
  python qualdir/qd_m3.py 2026-03
  python qualdir/qd_m3.py 2026-01 2026-06
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

SCRIPT_DIR = Path(__file__).resolve().parent
YEAR_OFFSET = 2000

REG = "_AccumRg51416"
DOC = "_Document726"
ART = "_Reference503"
STRUCT = "_Reference513"
CFO_CAT = "_Reference127708"

COL_ORG = "_Fld51418RRef"
COL_ART = "_Fld51423RRef"
COL_REQ = "_Fld140229RRef"
COL_PAY = "_Fld51433"
COL_OVER = "_Fld51443"
COL_DOC_CFO = "_Fld127709RRef"
COL_DOC_DEPT = "_Fld22796RRef"

# Организации контура (как в calc_budget_limit.TURB_ORGS).
ORG_GUIDS = (
    "fbca2148-6cfd-11e7-812d-001e67112509",  # ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО
    "fbca2143-6cfd-11e7-812d-001e67112509",  # Турбулентность-Дон ООО
)

# План 2026, руб./мес. (DashboardBack/qualdir/qd_m3.py).
QD_M3_PLAN_BY_MONTH_2026: dict[int, int] = {
    1: 219_250,
    2: 161_100,
    3: 549_950,
    4: 148_350,
    5: 198_950,
    6: 203_950,
    7: 574_450,
    8: 387_450,
    9: 225_950,
    10: 249_950,
    11: 217_450,
    12: 221_300,
}

# ЦФО: эталон + фактическое имя в _Reference127708.
QD_M3_CFO_LABELS: tuple[str, ...] = (
    "Зам.директора по качеству",  # как в SQL-справочнике ТД_ЦФО
    "Директор по качеству",
    "Зам. технического директора по качеству",
    "ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ",
    "Заместитель директора по качеству",
    "Контур качества (зам по кач.)",
)

# Подразделения контура качества (как QD-M4 / SEVEN_DEPTS_FOT_SPEC).
QD_FOT_SPEC: list[tuple[str, tuple[str, ...]]] = [
    (
        "Зам. технического директора по качеству",
        (
            "зам. технического директора по качеству",
            "зам технического директора по качеству",
            "заместитель тех. директора по качеству",
            "заместитель технического директора по качеству",
        ),
    ),
    ("Специалист по процессному управлению", ("специалист по процессному управлению",)),
    ("ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ", ("заместитель директора по качеству",)),
    ("ОТК-1", ("отк-1", "отк 1")),
    ("ОТК-2", ("отк-2", "отк 2")),
    ("Лаборатория неразрушающего контроля", ("лаборатория неразрушающего контроля", "лнк")),
    (
        "Отдел управления несоответствиями",
        (
            "отдел управления несоответствиями",
            "отдел управления несоотвествиями",
        ),
    ),
]

# Статьи ДДС: эталонные имена + фактические из _Reference503.
QD_M3_DDS_ARTICLE_DESCRIPTIONS: tuple[str, ...] = (
    "Услуги сторонних организаций_2_ТС_СК+ПО_4.15.",
    "Услуги сторонних организаций_2_ТС_СМК_4.15.",
    "Выплаты  ГПРПС/Предложения по улучшению_2_ТС_СК+ПО_4.39.",
    "Выплаты ГПРПС/Предложения по улучшению_2_ТС_СК+ПО_4.39.",
    "Выплаты  ГПРПС/Предложения по улучшению_2_ТС_СК_4.39.",
    "ТМЦ_2_ТС_ОТК_3.11.",
    "ТМЦ_2_ТС_ОТК_3.11.с НДС",
    "Услуги сторонних организаций_2_ТС_ОТК_3.9.",
    "Услуги сторонних организаций_2_ТС_ОТК_3.9. Без НДС",
    "Инструмент и оборудование_2_ТС_ОТК_3.10.",
    "Инструмент и оборудование_2_ТС_ОТК_3.10. с НДС",
)

# Маркеры по нормализованному имени (устойчивы к НДС / СК+ПО vs СК / СМК).
QD_M3_DDS_ARTICLE_MARKERS: tuple[str, ...] = (
    "тс ск по 4 15",
    "тс смк 4 15",
    "тс ск по 4 39",
    "тс ск 4 39",
    "тс отк 3 11",
    "тс отк 3 9",
    "тс отк 3 10",
)

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

EMPTY_BIN = b"\x00" * 16


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


def _kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(fact / plan * 100.0, 1)


def _plan_for_month(year: int, month: int) -> float | None:
    if year == 2026 and month in QD_M3_PLAN_BY_MONTH_2026:
        return float(QD_M3_PLAN_BY_MONTH_2026[month])
    return None


def _quality_dept_norms() -> frozenset[str]:
    acc: set[str] = set()
    for title, aliases in QD_FOT_SPEC:
        t = normalize_name(title)
        if t:
            acc.add(t)
        for a in aliases:
            n = normalize_name(a)
            if n:
                acc.add(n)
    return frozenset(acc)


def _cfo_norms() -> frozenset[str]:
    return frozenset(n for n in (normalize_name(s) for s in QD_M3_CFO_LABELS) if n)


def _dds_allowed_norms() -> frozenset[str]:
    return frozenset(n for n in (normalize_name(s) for s in QD_M3_DDS_ARTICLE_DESCRIPTIONS) if n)


CFO_NORMS = _cfo_norms()
DEPT_NORMS = _quality_dept_norms()
DDS_NORMS = _dds_allowed_norms()


def _label_matches(norm: str, needles: frozenset[str]) -> bool:
    if not norm:
        return False
    for needle in needles:
        if len(needle) < 3:
            continue
        if norm == needle or needle in norm or norm in needle:
            return True
    return False


def _dds_matches(label: str) -> bool:
    norm = normalize_name(label)
    if not norm:
        return False
    if norm in DDS_NORMS:
        return True
    # точное совпадение без суффикса «с ндс» / «без ндс»
    for base in DDS_NORMS:
        if norm.startswith(base) or base.startswith(norm):
            return True
    return any(m in norm for m in QD_M3_DDS_ARTICLE_MARKERS)


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
    # флаги argparse обрабатываются отдельно в main; здесь только позиционные
    args = [a for a in args if not a.startswith("-")]
    now = datetime.now()
    if not args:
        return (now.year, 1), (now.year, 12), str(now.year)
    if len(args) == 1 and len(args[0]) == 4:
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


def _load_allowed_articles(cur) -> dict[bytes, str]:
    cur.execute(f"SELECT _IDRRef, _Description FROM [{ART}] WITH (NOLOCK)")
    out: dict[bytes, str] = {}
    for rid, desc in cur.fetchall():
        label = desc or ""
        if _dds_matches(label):
            out[bytes(rid)] = label
    return out


def _classify_requests(
    cur, req_keys: list[bytes]
) -> tuple[set[bytes], dict[str, int]]:
    """Из списка Ref заявок оставляет контур качества (ЦФО или подразделение)."""
    counts = {"docs_loaded": 0, "by_cfo": 0, "by_dept": 0, "rejected": 0}
    allowed: set[bytes] = set()
    if not req_keys:
        return allowed, counts

    batch = 40
    for i in range(0, len(req_keys), batch):
        chunk = req_keys[i : i + batch]
        ph = ",".join("?" * len(chunk))
        cur.execute(
            f"""
            SELECT d._IDRRef, cfo._Description, dept._Description
            FROM [{DOC}] d WITH (NOLOCK)
            LEFT JOIN [{CFO_CAT}] cfo WITH (NOLOCK)
                   ON cfo._IDRRef = d.[{COL_DOC_CFO}]
            LEFT JOIN [{STRUCT}] dept WITH (NOLOCK)
                   ON dept._IDRRef = d.[{COL_DOC_DEPT}]
            WHERE d._IDRRef IN ({ph})
            """,
            chunk,
        )
        for rid, cfo_name, dept_name in cur.fetchall():
            counts["docs_loaded"] += 1
            cfo_n = normalize_name(cfo_name)
            dept_n = normalize_name(dept_name)
            if _label_matches(cfo_n, CFO_NORMS):
                counts["by_cfo"] += 1
                allowed.add(bytes(rid))
            elif _label_matches(dept_n, DEPT_NORMS):
                counts["by_dept"] += 1
                allowed.add(bytes(rid))
            else:
                counts["rejected"] += 1
    return allowed, counts


# (req, art, pay, over, calendar_year, calendar_month)
_RegRow = tuple[bytes, bytes, float, float, int, int]


def _load_period_rows(
    cur,
    p_start: datetime,
    p_end: datetime,
    art_ids: list[bytes],
) -> list[_RegRow]:
    if not art_ids:
        return []
    org_ph = ",".join("?" * len(ORG_BINS))
    art_ph = ",".join("?" * len(art_ids))
    cur.execute(
        f"""
        SELECT r._Period, r.[{COL_REQ}], r.[{COL_ART}], r.[{COL_PAY}], r.[{COL_OVER}]
        FROM [{REG}] r WITH (NOLOCK)
        WHERE r._Period >= ? AND r._Period < ?
          AND r._Active = 0x01
          AND r.[{COL_ORG}] IN ({org_ph})
          AND r.[{COL_PAY}] <> 0
          AND r.[{COL_REQ}] <> ?
          AND r.[{COL_ART}] IN ({art_ph})
        """,
        [p_start, p_end, *ORG_BINS, EMPTY_BIN, *art_ids],
    )
    out: list[_RegRow] = []
    for period_raw, req, art, pay, over in cur.fetchall():
        if period_raw is None:
            continue
        year = int(period_raw.year) - YEAR_OFFSET
        month = int(period_raw.month)
        if year < 1 or not 1 <= month <= 12:
            continue
        out.append(
            (
                bytes(req) if req else EMPTY_BIN,
                bytes(art) if art else EMPTY_BIN,
                _as_float(pay),
                _as_float(over),
                year,
                month,
            )
        )
    return out


def _sum_fact_from_rows(
    rows: list[_RegRow],
    allowed_art: dict[bytes, str],
    allowed_req_cache: dict[bytes, bool],
) -> tuple[float, dict[str, int]]:
    """Агрегация уже загруженных строк регистра (без SQL)."""
    counts = {
        "register_rows": len(rows),
        "fact_rows_with_request": 0,
        "skipped_no_article_match": 0,
        "skipped_td_cfo": 0,
        "rows_counted": 0,
        "allowed_articles": len(allowed_art),
        "requests_classified": 0,
    }
    total = 0.0
    req_seen: set[bytes] = set()
    for req_b, art_b, pay, over, _y, _m in rows:
        if req_b == EMPTY_BIN:
            continue
        counts["fact_rows_with_request"] += 1
        req_seen.add(req_b)
        if art_b not in allowed_art:
            counts["skipped_no_article_match"] += 1
            continue
        if not allowed_req_cache.get(req_b):
            counts["skipped_td_cfo"] += 1
            continue
        net = pay - over
        if net == 0:
            continue
        total += net
        counts["rows_counted"] += 1
    counts["requests_classified"] = sum(1 for k in req_seen if allowed_req_cache.get(k))
    return round(total, 2), counts


def compute_qd_m3_fact_monthly(
    year: int,
    month: int,
    sql: SqlConnection | None = None,
    *,
    allowed_art: dict[bytes, str] | None = None,
    allowed_req_cache: dict[bytes, bool] | None = None,
) -> dict[str, Any]:
    """Сумма фактических оплат QD-M3 за календарный месяц (руб.)."""
    sql = sql or SqlConnection()
    p_start, p_end = _sql_period_bounds(year, month)

    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        if allowed_art is None:
            allowed_art = _load_allowed_articles(cur)
        art_ids = list(allowed_art.keys())
        rows = _load_period_rows(cur, p_start, p_end, art_ids)

        req_keys = sorted({r[0] for r in rows if r[0] != EMPTY_BIN})
        if allowed_req_cache is None:
            allowed_set, req_counts = _classify_requests(cur, req_keys)
            allowed_req_cache = {k: True for k in allowed_set}
            for k in req_keys:
                allowed_req_cache.setdefault(k, False)
            counts_extra = {f"req_{k}": v for k, v in req_counts.items()}
        else:
            missing = [k for k in req_keys if k not in allowed_req_cache]
            counts_extra = {}
            if missing:
                allowed_set, req_counts = _classify_requests(cur, missing)
                counts_extra = {f"req_{k}": v for k, v in req_counts.items()}
                for k in missing:
                    allowed_req_cache[k] = k in allowed_set

    total, counts = _sum_fact_from_rows(rows, allowed_art, allowed_req_cache)
    counts.update(counts_extra)

    return {
        "year": year,
        "month": month,
        "total_fact": total,
        "counts": counts,
        "debug": {
            "status": "ok",
            "kpi_id": "QD-M3-FACT",
            "register": REG,
            "document": DOC,
            "period_start": p_start.isoformat(sep="T"),
            "period_end": p_end.isoformat(sep="T"),
            "cfo_labels": list(QD_M3_CFO_LABELS),
            "dds_articles_matched": sorted(set(allowed_art.values())),
            "rule": "fact = sum(СуммаОплаты - СуммаКВыплатеСверхЛимита) for quality contour",
        },
    }


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> list[dict[str, Any]]:
    """Один SQL-скан регистра за весь период, помесячная агрегация в Python."""
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        allowed_art = _load_allowed_articles(cur)
        p0, _ = _sql_period_bounds(*start_period)
        _, p1 = _sql_period_bounds(*end_period)
        all_rows = _load_period_rows(cur, p0, p1, list(allowed_art.keys()))
        req_keys = sorted({r[0] for r in all_rows if r[0] != EMPTY_BIN})
        allowed_set, req_counts = _classify_requests(cur, req_keys)
        allowed_req_cache = {k: (k in allowed_set) for k in req_keys}

    by_month: dict[tuple[int, int], list[_RegRow]] = {}
    for row in all_rows:
        key = (row[4], row[5])
        by_month.setdefault(key, []).append(row)

    report: list[dict[str, Any]] = []
    for year, month in iter_months(start_period, end_period):
        month_rows = by_month.get((year, month), [])
        fact, counts = _sum_fact_from_rows(month_rows, allowed_art, allowed_req_cache)
        plan = _plan_for_month(year, month)
        report.append(
            {
                "year": year,
                "month": month,
                "month_name": MONTH_NAMES[month],
                "plan": plan,
                "fact": fact,
                "kpi_pct": _kpi_pct(plan, fact),
                "has_data": plan is not None and fact is not None,
                "values_unit": "руб.",
                "counts": counts,
                "req_preload": req_counts,
                "articles": sorted(set(allowed_art.values())),
            }
        )
    return report


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "QD-M3 — бюджет блока в пределах лимита (SQL)",
        f"Источник: {REG} + {DOC} + {ART} + {CFO_CAT}/{STRUCT}",
        "",
        f"{'Месяц':<10} {'План':>14} {'Факт':>14} {'KPI %':>8} {'Строк':>8}",
        f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        plan = row["plan"]
        fact = row["fact"]
        pct = row["kpi_pct"]
        plan_s = f"{plan:,.2f}" if plan is not None else "—"
        fact_s = f"{fact:,.2f}" if fact is not None else "—"
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        counted = (row.get("counts") or {}).get("rows_counted", 0)
        lines.append(
            f"{row['year']:04d}-{row['month']:02d} "
            f"{plan_s:>14} {fact_s:>14} {pct_s:>8} {counted:>8}"
        )
    lines.append(f"{'-' * 10} {'-' * 14} {'-' * 14} {'-' * 8} {'-' * 8}")
    if rows:
        arts = rows[0].get("articles") or []
        lines.extend(["", "Статьи ДДС (matched):", *[f"  • {a}" for a in arts]])
        preload = rows[0].get("req_preload") or {}
        lines.extend(
            [
                "",
                f"Заявки контура: {preload}",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def build_qd_m3_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    today = date.today()
    ref_y = year or today.year
    ref_m = month or today.month
    if year is None and month is None:
        # последний полный месяц
        if today.month == 1:
            ref_y, ref_m = today.year - 1, 12
        else:
            ref_y, ref_m = today.year, today.month - 1

    rows = build_monthly_report((ref_y, 1), (ref_y, ref_m))
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
    ref_row = monthly_rows[-1] if monthly_rows else None
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for r in monthly_rows if r.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "руб.",
        },
        "debug": {
            "status": "ok",
            "kpi_id": "QD-M3",
            "source": "qualdir.qd_m3.sql",
            "plan_source": "QD_M3_PLAN_BY_MONTH_2026",
            "fact_source": f"{REG} / {DOC}",
            "etalon_fixes": [
                "CFO label: 'Зам.директора по качеству' (catalog _Reference127708)",
                "DDS: ТС_СМК_4.15 / ТС_СК_4.39 + НДС suffixes from _Reference503",
                "fact uses СуммаОплаты-СверхЛимита only (no pre/post double-count)",
            ],
            "monthly_counts": [
                {"year": r["year"], "month": r["month"], "counts": r.get("counts")}
                for r in rows
            ],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="QD-M3: бюджет блока (факт SQL).")
    parser.add_argument("period", nargs="*", help="ГГГГ | ГГГГ-ММ | ГГГГ-ММ ГГГГ-ММ")
    parser.add_argument("--json", action="store_true", help="JSON payload")
    args = parser.parse_args()

    try:
        if args.json and not args.period:
            payload = build_qd_m3_payload()
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        start, end, slug = parse_period_args(args.period)
        if args.json and start == end:
            payload = build_qd_m3_payload(start[0], start[1])
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            out = SCRIPT_DIR / f"qd_m3_{slug}.json"
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"JSON сохранён: {out}", file=sys.stderr)
            return 0

        rows = build_monthly_report(start, end)
        report = format_report(rows)
        print(report)
        out = SCRIPT_DIR / f"qd_m3_{slug}.txt"
        out.write_text(report, encoding="utf-8-sig")
        print(f"Отчёт сохранён: {out}")
        return 0
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, normalize_period

QD_M3_YTD_CACHE_PREFIX = "qualdir_qd_m3_ytd"
QD_M3_YTD_DISK_TAG = "qualdir_qd_m3_ytd_payload_sql_v2"
QD_M3_YTD_DISK_VERSION = 11


def qd_m3_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M3_YTD_CACHE_PREFIX, ry, rm)


def get_qd_m3_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M3_YTD_CACHE_PREFIX,
        source_tag=QD_M3_YTD_DISK_TAG,
        version=QD_M3_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m3_sql",
        compute_fn=lambda y, m: build_qd_m3_payload(y, m),
        kpi_id="QD-M3",
    )
