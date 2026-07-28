"""
QD-M8 — процессные несоответствия (директор по качеству).

Эталон (OData): DashboardBack/qualdir/qd_m8.py
  → qualdir.brak_report.compute_forma0317_month
  Document_ТД_Форма0317

Логика за месяц (по Date документа, без помеченных на удаление):
  plan        — заявки, статус не из excluded
                (НеСогласовано / Отменена / НаСогласовании / Подготовлен[/о]);
  fact        — из plan со статусом «Выполнено»;
  significant — из plan с ФормаЯвляетсяЗначимой = Истина;
  departments — разбивка plan по ПодразделениеПоставщика;
  kinds       — строки ТЧ Несоответствия по ВидНесоответствия.

SQL (erp_pm):
  Document_ТД_Форма0317                → dbo._Document148563X1
    _Date_Time                         — Date (+2000 лет)
    _Marked                            — DeletionMark (0x00 = нет)
    _Fld148623RRef                     — Статус → _Enum100559
    _Fld185470                         — ФормаЯвляетсяЗначимой (0x01 = да)
    _Fld148618RRef                     — ПодразделениеПоставщика → _Reference513
  ТЧ Несоответствия                    → dbo._Document148563_VT148628X1
    _Document148563_IDRRef             — ссылка на документ
    _Fld148631RRef                     — ВидНесоответствия → _Reference100536
    _Fld148630                         — ОписаниеНесоответствия (fallback)
  Catalog_ТД_ВидыНесоответствия…       → dbo._Reference100536
  Catalog_СтруктураПредприятия         → dbo._Reference513
  Enum статусов формы                  → dbo._Enum100559

Даты в SQL хранятся со смещением YEAR_OFFSET = 2000
(2026-03-01 → 4026-03-01).

Использование:
  python qualdir/qd_m8.py
  python qualdir/qd_m8.py 2026
  python qualdir/qd_m8.py 2026-03
  python qualdir/qd_m8.py 2026-01 2026-05
"""

from __future__ import annotations

import functools
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sql_connection import SqlConnection

DOC_TABLE = "_Document148563X1"
VT_TABLE = "_Document148563_VT148628X1"
VT_DOC_COL = "_Document148563_IDRRef"
VT_KIND_COL = "_Fld148631RRef"
VT_DESC_COL = "_Fld148630"
KIND_TABLE = "_Reference100536"
DEPT_TABLE = "_Reference513"
ENUM_TABLE = "_Enum100559"

COL_DATE = "_Date_Time"
COL_MARKED = "_Marked"
COL_STATUS = "_Fld148623RRef"
COL_SIGNIFICANT = "_Fld185470"
COL_DEPT = "_Fld148618RRef"

STATUS_BY_ORDER: dict[int, str] = {
    0: "Подготовлен",
    1: "НаСогласовании",
    2: "НеСогласовано",
    3: "РазработкаКМ",
    4: "ИсполнениеКМ",
    5: "Выполнено",
    6: "Отменена",
}
EXECUTED_STATUS = "Выполнено"
PLAN_EXCLUDED_STATUSES = frozenset(
    {
        "НеСогласовано",
        "Отменена",
        "НаСогласовании",
        "Подготовлен",
        "Подготовлено",
    }
)

YEAR_OFFSET = 2000
SCRIPT_DIR = Path(__file__).resolve().parent
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


def parse_month(value: str) -> tuple[int, int]:
    if len(value) != 7 or value[4] != "-":
        raise ValueError("Месяц должен быть в формате ГГГГ-ММ")
    year = int(value[:4])
    month = int(value[5:7])
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 01 до 12")
    return year, month


def parse_period_args(argv: list[str] | None = None) -> tuple[tuple[int, int], tuple[int, int], str]:
    args = [arg.strip() for arg in (argv if argv is not None else sys.argv[1:]) if arg.strip()]
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


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def iter_months(start: tuple[int, int], end: tuple[int, int]) -> list[str]:
    year, month = start
    result: list[str] = []
    while (year, month) <= end:
        result.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return result


def to_sql_dt(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(year=value.year + YEAR_OFFSET)
    return datetime(value.year + YEAR_OFFSET, value.month, value.day)


def normalize_text(value: str | None) -> str:
    text = (value or "").strip().lower().replace("ё", "е")
    return " ".join("".join(ch if ch.isalnum() else " " for ch in text).split())


PLAN_EXCLUDED_NORM = frozenset(normalize_text(s) for s in PLAN_EXCLUDED_STATUSES)


def kpi_pct(plan: int | None, fact: int | None) -> float | None:
    if plan is None or fact is None:
        return None
    if plan <= 0:
        return 100.0 if fact <= 0 else None
    return round(fact / plan * 100.0, 1)


def load_status_bins(cur) -> dict[str, bytes]:
    cur.execute(
        f"""
        SELECT _IDRRef, _EnumOrder
        FROM [{ENUM_TABLE}] WITH (NOLOCK)
        """
    )
    result: dict[str, bytes] = {}
    for idr, order in cur.fetchall():
        name = STATUS_BY_ORDER.get(int(order))
        if name:
            result[name] = bytes(idr)
    missing = [name for name in STATUS_BY_ORDER.values() if name not in result]
    if missing:
        raise RuntimeError(f"Не найдены значения статуса в {ENUM_TABLE}: {missing}")
    return result


def is_plan_status(status_name: str | None) -> bool:
    if not status_name:
        return False
    return normalize_text(status_name) not in PLAN_EXCLUDED_NORM


def load_documents(
    cur,
    start_dt: date,
    end_dt: date,
    status_bins: dict[str, bytes],
) -> tuple[
    list[tuple[str, str | None, bool, bool]],
    list[tuple[str, str]],
]:
    """Возвращает (docs, kind_rows).

    docs: (month_key, dept_name, is_executed, is_significant)
    kind_rows: (month_key, kind_name) — по строкам ТЧ plan-документов
    """
    sql_start = to_sql_dt(start_dt)
    sql_end_exclusive = to_sql_dt(end_dt + timedelta(days=1))
    bin_to_status = {blob: name for name, blob in status_bins.items()}

    cur.execute(
        f"""
        SELECT
            doc._IDRRef,
            doc.[{COL_DATE}],
            doc.[{COL_STATUS}],
            doc.[{COL_SIGNIFICANT}],
            dept._Description
        FROM [{DOC_TABLE}] doc WITH (NOLOCK)
        LEFT JOIN [{DEPT_TABLE}] dept WITH (NOLOCK)
            ON dept._IDRRef = doc.[{COL_DEPT}]
        WHERE doc.[{COL_MARKED}] = 0x00
          AND doc.[{COL_DATE}] >= ?
          AND doc.[{COL_DATE}] < ?
        """,
        sql_start,
        sql_end_exclusive,
    )

    docs: list[tuple[str, str | None, bool, bool]] = []
    plan_ids: list[bytes] = []
    id_to_month: dict[bytes, str] = {}
    for idr, date_raw, status_bin, sig_raw, dept_name in cur.fetchall():
        if date_raw is None or status_bin is None or idr is None:
            continue
        year = date_raw.year - YEAR_OFFSET
        month = date_raw.month
        if year < 1:
            continue
        status_name = bin_to_status.get(bytes(status_bin))
        if not is_plan_status(status_name):
            continue
        month_key = f"{year:04d}-{month:02d}"
        doc_id = bytes(idr)
        plan_ids.append(doc_id)
        id_to_month[doc_id] = month_key
        dept = (dept_name or "").strip() or None
        is_executed = status_name == EXECUTED_STATUS
        is_significant = bytes(sig_raw) != b"\x00" if sig_raw is not None else False
        docs.append((month_key, dept, is_executed, is_significant))

    kind_rows: list[tuple[str, str]] = []
    if plan_ids:
        # chunk IN-list to keep parameter count reasonable
        chunk_size = 400
        for offset in range(0, len(plan_ids), chunk_size):
            chunk = plan_ids[offset : offset + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            cur.execute(
                f"""
                SELECT
                    vt.[{VT_DOC_COL}],
                    k._Description,
                    vt.[{VT_DESC_COL}]
                FROM [{VT_TABLE}] vt WITH (NOLOCK)
                LEFT JOIN [{KIND_TABLE}] k WITH (NOLOCK)
                    ON k._IDRRef = vt.[{VT_KIND_COL}]
                WHERE vt.[{VT_DOC_COL}] IN ({placeholders})
                """,
                *chunk,
            )
            for doc_id, kind_name, fallback in cur.fetchall():
                month_key = id_to_month.get(bytes(doc_id))
                if not month_key:
                    continue
                name = (kind_name or "").strip() or (fallback or "").strip() or "—"
                kind_rows.append((month_key, name))

    return docs, kind_rows


def _name_counts_payload(counts: dict[str, int]) -> list[dict[str, Any]]:
    rows = [{"name": name, "count": int(count)} for name, count in counts.items()]
    rows.sort(key=lambda row: (-row["count"], row["name"].lower()))
    return rows


def build_monthly_report(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
) -> list[dict[str, Any]]:
    sql = SqlConnection()
    with sql.connect_ctx() as conn:
        conn.timeout = 0
        cur = conn.cursor()
        status_bins = load_status_bins(cur)
        docs, kind_rows = load_documents(
            cur,
            month_start(*start_period),
            month_end(*end_period),
            status_bins,
        )

    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "plan": 0,
            "fact": 0,
            "significant": 0,
            "departments": defaultdict(int),
            "kinds": defaultdict(int),
        }
    )
    for month_key, dept, is_executed, is_significant in docs:
        bucket = stats[month_key]
        bucket["plan"] += 1
        if is_executed:
            bucket["fact"] += 1
        if is_significant:
            bucket["significant"] += 1
        bucket["departments"][(dept or "—")] += 1

    for month_key, kind_name in kind_rows:
        stats[month_key]["kinds"][kind_name] += 1

    report_rows: list[dict[str, Any]] = []
    for month_key in iter_months(start_period, end_period):
        bucket = stats[month_key]
        plan = int(bucket["plan"])
        fact = int(bucket["fact"])
        significant = int(bucket["significant"])
        report_rows.append(
            {
                "month": month_key,
                "plan": plan,
                "fact": fact,
                "significant": significant,
                "kpi_pct": kpi_pct(plan, fact),
                "departments": _name_counts_payload(dict(bucket["departments"])),
                "kinds": _name_counts_payload(dict(bucket["kinds"])),
                "has_data": True,
            }
        )
    return report_rows


def format_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "Процессные несоответствия · ТД_Форма0317 (QD-M8 / SQL)",
        f"Источник: {DOC_TABLE} + {VT_TABLE} + {KIND_TABLE} + {DEPT_TABLE}",
        "",
        f"{'Месяц':<10} {'План':>8} {'Факт':>8} {'Знач.':>8} {'KPI %':>8}",
        f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}",
    ]
    for row in rows:
        pct = row["kpi_pct"]
        pct_s = f"{pct:.1f}" if pct is not None else "—"
        lines.append(
            f"{row['month']:<10} "
            f"{row['plan']:>8} "
            f"{row['fact']:>8} "
            f"{row['significant']:>8} "
            f"{pct_s:>8}"
        )
    lines.extend(
        [
            f"{'-' * 10} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}",
            f"{'ИТОГО':<10} "
            f"{sum(row['plan'] for row in rows):>8} "
            f"{sum(row['fact'] for row in rows):>8} "
            f"{sum(row['significant'] for row in rows):>8} "
            f"{'':>8}",
            "",
        ]
    )

    ref = next((row for row in reversed(rows) if row.get("has_data")), rows[-1] if rows else None)
    if ref:
        if ref.get("departments"):
            lines.append(f"Подразделения поставщика ({ref['month']}):")
            for item in ref["departments"]:
                lines.append(f"  {item['count']:>4}  {item['name']}")
            lines.append("")
        if ref.get("kinds"):
            lines.append(f"Виды несоответствий ({ref['month']}):")
            for item in ref["kinds"]:
                lines.append(f"  {item['count']:>4}  {item['name']}")
            lines.append("")
    return "\n".join(lines)


def save_report(period_slug: str, rows: list[dict[str, Any]]) -> Path:
    output_path = SCRIPT_DIR / f"qd_m8_{period_slug}.txt"
    output_path.write_text(format_report(rows), encoding="utf-8-sig")
    return output_path


def build_qd_m8_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    now = datetime.now()
    ref_y = year or now.year
    ref_m = month or (now.month - 1 if now.month > 1 else 12)
    if month is None and now.month == 1 and year is None:
        ref_y = now.year - 1
        ref_m = 12

    rows = build_monthly_report((ref_y, 1), (ref_y, ref_m))
    monthly_rows: list[dict[str, Any]] = []
    for row in rows:
        month_num = int(str(row["month"])[5:7])
        plan = int(row["plan"])
        fact = int(row["fact"])
        significant = int(row["significant"])
        item: dict[str, Any] = {
            "month": month_num,
            "year": ref_y,
            "month_name": MONTH_NAMES[month_num].lower(),
            "plan": plan,
            "fact": fact,
            "significant": significant,
            "kpi_pct": kpi_pct(plan, fact),
            "has_data": True,
            "departments": [dict(d) for d in row.get("departments") or []],
            "kinds": [dict(d) for d in row.get("kinds") or []],
            "values_unit": "шт.",
        }
        monthly_rows.append(item)

    ref_row = next(
        (item for item in monthly_rows if item["month"] == ref_m),
        monthly_rows[-1] if monthly_rows else None,
    )
    departments_out = [dict(item) for item in (ref_row or {}).get("departments") or []]
    kinds_out = [dict(item) for item in (ref_row or {}).get("kinds") or []]
    breakdown_by_month = [
        {
            "year": item["year"],
            "month": item["month"],
            "month_name": item["month_name"],
            "total": item.get("plan"),
            "has_data": item.get("has_data"),
            "departments": [dict(d) for d in item.get("departments") or []],
            "kinds": [dict(d) for d in item.get("kinds") or []],
        }
        for item in monthly_rows
    ]

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "departments": departments_out,
        "kinds": kinds_out,
        "departments_by_month": breakdown_by_month,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
            "data_complete": bool(ref_row and ref_row.get("has_data")),
        },
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "total_significant": ref_row.get("significant") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for item in monthly_rows if item.get("has_data")),
            "months_total": len(monthly_rows),
            **({"values_unit": "шт."} if ref_row and ref_row.get("has_data") else {}),
        },
        "debug": {
            "kpi_id": "QD-M8",
            "status": "ok",
            "source": "qualdir.qd_m8.sql",
            "odata_entity": "Document_ТД_Форма0317",
            "tables": {
                "document": DOC_TABLE,
                "tabular": VT_TABLE,
                "kind_catalog": KIND_TABLE,
                "department": DEPT_TABLE,
                "status_enum": ENUM_TABLE,
                "date_col": COL_DATE,
                "status_col": COL_STATUS,
                "significant_col": COL_SIGNIFICANT,
                "department_col": COL_DEPT,
                "kind_col": VT_KIND_COL,
            },
            "rule": (
                "plan = documents in month, DeletionMark=false, status not in "
                "НеСогласовано/Отменена/НаСогласовании/Подготовлен; "
                "fact = plan with status Выполнено; "
                "significant = plan with ФормаЯвляетсяЗначимой; "
                "kinds = VT Несоответствия rows by ВидНесоответствия"
            ),
            "rows_by_month": rows,
        },
    }


def main() -> None:
    try:
        start_period, end_period, period_slug = parse_period_args()
        rows = build_monthly_report(start_period, end_period)
        report = format_report(rows)
        print(report)
        output_path = save_report(period_slug, rows)
        print(f"Отчёт сохранён: {output_path}")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# --- DashboardBack API (daily YTD cache) ---
from pathlib import Path as _Path

from qualdir.sql_tile_cache import get_ytd_via_cache, month_cache_path, normalize_period

QD_M8_YTD_CACHE_PREFIX = "qualdir_qd_m8_ytd"
QD_M8_YTD_DISK_TAG = "qualdir_qd_m8_ytd_payload_sql_v1"
QD_M8_YTD_DISK_VERSION = 20


def forma0317_month_cache_path(year: int, month: int) -> _Path:
    return month_cache_path("qualdir_forma0317_sql", year, month)


def qd_m8_ytd_cache_path(year: int | None = None, month: int | None = None) -> _Path:
    from devdir import ytd_json_cache

    ry, rm = normalize_period(year, month)
    return ytd_json_cache.cache_path(QD_M8_YTD_CACHE_PREFIX, ry, rm)


def qd_m8_tile_cache_path(year: int, month: int) -> _Path:
    return qd_m8_ytd_cache_path(year, month)


def _build_qd_m8_dashboard_payload(year: int, month: int) -> dict:
    payload = build_qd_m8_payload(year, month)
    # views ожидает breakdown_by_month
    if "breakdown_by_month" not in payload and "departments_by_month" in payload:
        payload["breakdown_by_month"] = payload["departments_by_month"]
    return payload


def get_qd_m8_ytd(year: int | None = None, month: int | None = None) -> dict:
    return get_ytd_via_cache(
        year=year,
        month=month,
        cache_prefix=QD_M8_YTD_CACHE_PREFIX,
        source_tag=QD_M8_YTD_DISK_TAG,
        version=QD_M8_YTD_DISK_VERSION,
        lock_key_prefix="qualdir_qd_m8_sql",
        compute_fn=_build_qd_m8_dashboard_payload,
        kpi_id="QD-M8",
    )
