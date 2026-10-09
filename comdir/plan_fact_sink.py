"""Необязательный сбор строк расшифровки план/факта.

Пока контекст не включён, note() ничего не делает и расчёт плиток не меняется.
"""
from __future__ import annotations

import contextvars

lines = contextvars.ContextVar("plan_fact_lines", default=None)
only_month = contextvars.ContextVar("plan_fact_only_month", default=None)


def note(metric: str, kind: str, amount, **fields) -> None:
    buf = lines.get()
    if buf is None:
        return
    try:
        value = float(amount or 0)
    except (TypeError, ValueError):
        return
    if abs(value) < 0.005:
        return
    want = only_month.get()
    month = fields.get("month")
    if want is not None and month is not None and int(month) != int(want):
        return
    row = {
        "metric": metric,
        "kind": kind,
        "amount": round(value, 2),
    }
    for key, val in fields.items():
        if key == "month" or val is None:
            continue
        text = str(val).strip()
        if text:
            row[key] = text
    buf.append(row)
