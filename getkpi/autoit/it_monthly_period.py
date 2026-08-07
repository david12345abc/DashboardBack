"""Общее для помесячных плиток autoit (ИТ-*)."""
from __future__ import annotations

from datetime import date

MONTH_NAMES = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}


def normalize_it_tile_period(year: int | None = None, month: int | None = None) -> tuple[int, int]:
    today = date.today()
    ref_year = int(year or today.year)
    ref_month = int(month or (today.month if ref_year == today.year else 12))
    ref_month = max(1, min(12, ref_month))
    if ref_year == today.year:
        ref_month = min(ref_month, today.month)
    return ref_year, ref_month


def pick_fot_display_row(
    monthly_rows: list[dict] | None,
    ref_m: int,
    *,
    ref_year: int | None = None,
    today: date | None = None,
) -> dict | None:
    """
    Опорная строка для плиток ФОТ/бюджет «в пределах лимита».

    1) Текущий календарный (незакрытый) месяц не берём как опору — иначе
       частичный факт даёт ложный green, а фронт часто рисует прошлый месяц.
    2) Если в выбранном месяце факт 0/None — последний месяц с fact > 0.
    """
    rows = [r for r in (monthly_rows or []) if isinstance(r, dict)]
    if not rows:
        return None

    today = today or date.today()
    year = ref_year
    if year is None:
        for row in rows:
            if row.get("month") == ref_m and row.get("year") is not None:
                year = int(row["year"])
                break
        if year is None and rows[-1].get("year") is not None:
            year = int(rows[-1]["year"])

    def _nonzero_fact(row: dict) -> bool:
        if not row.get("has_data"):
            return False
        try:
            fv = float(row["fact"]) if row.get("fact") is not None else None
        except (TypeError, ValueError):
            return False
        return fv is not None and fv > 0

    # Незакрытый текущий месяц → не берём его; опора = прошлый месяц с fact > 0.
    if year == today.year and ref_m == today.month:
        closed_m = today.month - 1 if today.month > 1 else 12
        closed = next((r for r in rows if r.get("month") == closed_m), None)
        if closed is not None and _nonzero_fact(closed):
            return closed
        for row in reversed(rows):
            if row.get("month") == ref_m:
                continue
            if _nonzero_fact(row):
                return row
        # Если ненулевого факта нет — всё же прошлый календарный месяц (не текущий).
        if closed is not None:
            return closed
        for row in reversed(rows):
            if row.get("month") != ref_m:
                return row
        return None

    ref_row = next((r for r in rows if r.get("month") == ref_m), None)
    if ref_row and _nonzero_fact(ref_row):
        return ref_row
    for row in reversed(rows):
        if _nonzero_fact(row):
            return row
    return ref_row if ref_row and ref_row.get("has_data") else None


def trim_monthly_rows_to_display(
    monthly_rows: list[dict] | None,
    display_row: dict | None,
) -> list[dict]:
    """Обрезать monthly_data по опорной строке (незакрытый месяц не отдаём на плитку).

    Иначе фронт часто берёт последний ненулевой факт (текущий месяц) и красит его
    цветом от ``last_full_month_row`` — получается «126k при плане 1.2M» красным,
    хотя опора — прошлый месяц с перерасходом.
    """
    rows = [r for r in (monthly_rows or []) if isinstance(r, dict)]
    if not rows or not isinstance(display_row, dict):
        return rows
    try:
        dy = int(display_row["year"])
        dm = int(display_row["month"])
    except (KeyError, TypeError, ValueError):
        return rows
    out: list[dict] = []
    for row in rows:
        try:
            ry = int(row["year"]) if row.get("year") is not None else dy
            rm = int(row["month"])
        except (KeyError, TypeError, ValueError):
            continue
        if (ry, rm) <= (dy, dm):
            out.append(row)
    return out or rows
