# -*- coding: utf-8 -*-
"""План/факт заготовительного участка: НСУ, Катапульта, Сокол, шт.

Источник — проведённые документы Document_ЭтапПроизводства2_2
подразделения «Заготовительный участок (Сборочный)».
Готовое изделие, не деталь и не комплектация:

  • НСУ — этап «финальная сборка» и сборка изделия «НСУ 2.1»;
  • Катапульта — этап «(сборка)» изделия «Катапульта 1» / «Катапульта v.2»;
  • Сокол — этап «FPV-перехватчик "Сокол"».

В плитку попадает закрытый этап: план и факт по ФактическоеОкончаниеЭтапа.
Открытый заказ без окончания не входит: это не сменный план.
Суточная плитка берёт предыдущий день относительно окна обновления в 06:00.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

from .cache_manager import CACHE_DIR
from .calc_plan import AUTH, BASE
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

SOURCE_TAG = "blanking_shop_stage_output_v1"
DEPT_KEY = "863a1138-68bc-11f1-9822-6cb31113810e"
DEPT_NAME = "Заготовительный участок (Сборочный)"
REFRESH_HOUR = 6

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

PRODUCTS = (
    ("nsu", "НСУ", "Количество НСУ"),
    ("cat", "Катапульта", "Количество катапульт"),
    ("sok", "Сокол", "Количество соколов"),
)

TILE_SPECS = (
    ("PD-Z.NSU.D", "nsu", "day"),
    ("PD-Z.NSU.M", "nsu", "month"),
    ("PD-Z.CAT.D", "cat", "day"),
    ("PD-Z.CAT.M", "cat", "month"),
    ("PD-Z.SOK.D", "sok", "day"),
    ("PD-Z.SOK.M", "sok", "month"),
)
TILE_IDS = {spec[0] for spec in TILE_SPECS}

_SELECT = ",".join((
    "Ref_Key",
    "Number",
    "Date",
    "Posted",
    "DeletionMark",
    "Статус",
    "НаименованиеЭтапа",
    "КоличествоУпаковокПлан",
    "КоличествоУпаковокФакт",
    "ФактическоеОкончаниеЭтапа",
))


def cache_path(year: int, month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"blanking_output_{int(year)}_{int(month):02d}.json"


def report_day(now: datetime | None = None) -> date:
    """День суточных плиток: вчера относительно последнего обновления в 06:00."""
    now = now or datetime.now()
    anchor = now if (now.hour, now.minute) >= (REFRESH_HOUR, 0) else now - timedelta(days=1)
    return anchor.date() - timedelta(days=1)


def _family(name: str) -> str | None:
    low = " ".join(str(name or "").lower().split())
    core = low[4:].strip() if low.startswith("(д) ") else low
    if "fpv-перехватчик" in low and "сокол" in low:
        return "sok"
    if core.startswith("катапульта") and low.endswith("(сборка)") and "зип" not in low and "ложемент" not in low:
        return "cat"
    if "финальная сборка" in low and "нсу" in low:
        return "nsu"
    if core.startswith("нсу 2.1") and low.endswith("(сборка)"):
        return "nsu"
    return None


def _qty(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_date(value) -> date | None:
    text = str(value or "")[:10]
    if len(text) < 10 or text.startswith("0001"):
        return None
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    if parsed.year < 2001:
        return None
    return parsed


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _snapshot_is_current(data: dict | None, year: int, month: int, now: datetime) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("source") != SOURCE_TAG:
        return False
    if int(data.get("year") or 0) != int(year) or int(data.get("month") or 0) != int(month):
        return False
    return str(data.get("day") or "") == report_day(now).isoformat()


def _odata_rows(session: requests.Session, filter_expr: str) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    skip = 0
    while skip < 5000:
        url = (
            f"{BASE}/Document_ЭтапПроизводства2_2"
            f"?$format=json&$select={quote(_SELECT, safe=',')}"
            f"&$filter={quote(filter_expr, safe='')}"
            f"&$top=300&$skip={skip}"
        )
        response = request_with_retry(session, url, timeout=180, label="blanking-stages")
        if response is None or response.status_code != 200:
            status = getattr(response, "status_code", None)
            raise RuntimeError(f"OData этапов производства: HTTP {status}")
        chunk = response.json().get("value") or []
        fresh = 0
        for row in chunk:
            key = str(row.get("Ref_Key") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            rows.append(row)
            fresh += 1
        if len(chunk) < 300 or fresh == 0:
            break
        skip += 300
    return rows


def _fetch_stages(start: date, end: date) -> list[dict]:
    """Этапы, у которых дата документа или фактическое окончание попадает в [start, end)."""
    start_s = f"{start.isoformat()}T00:00:00"
    end_s = f"{end.isoformat()}T00:00:00"
    dept = f"Подразделение_Key eq guid'{DEPT_KEY}' and Posted eq true and DeletionMark eq false"
    session = requests.Session()
    session.auth = AUTH
    session.headers["Accept"] = "application/json"
    by_date = _odata_rows(
        session,
        f"{dept} and Date ge datetime'{start_s}' and Date lt datetime'{end_s}'",
    )
    by_end = _odata_rows(
        session,
        f"{dept} and ФактическоеОкончаниеЭтапа ge datetime'{start_s}'"
        f" and ФактическоеОкончаниеЭтапа lt datetime'{end_s}'",
    )
    merged: dict[str, dict] = {}
    for row in by_date + by_end:
        key = str(row.get("Ref_Key") or id(row))
        merged[key] = row
    return list(merged.values())


def _empty_bucket() -> dict:
    return {"plan": 0.0, "fact": 0.0, "rows": [], "_index": {}}


def _touch(bucket: dict, row: dict) -> dict:
    key = str(row.get("Ref_Key") or id(row))
    found = bucket["_index"].get(key)
    if found is None:
        found = {
            "name": str(row.get("НаименованиеЭтапа") or "").strip(),
            "number": str(row.get("Number") or "").strip(),
            "date": str(row.get("Date") or "")[:10],
            "finished": str(row.get("ФактическоеОкончаниеЭтапа") or "")[:10],
            "status": str(row.get("Статус") or "").strip(),
            "plan": 0.0,
            "fact": 0.0,
            "plan_qty": 0.0,
            "fact_qty": 0.0,
            "values_unit": "шт.",
        }
        bucket["_index"][key] = found
        bucket["rows"].append(found)
    return found


def _finish_bucket(bucket: dict) -> dict:
    bucket.pop("_index", None)
    for row in bucket["rows"]:
        row["plan"] = round(row["plan"], 2)
        row["fact"] = round(row["fact"], 2)
        row["plan_qty"] = row["plan"]
        row["fact_qty"] = row["fact"]
    bucket["plan"] = round(sum(row["plan"] for row in bucket["rows"]), 2)
    bucket["fact"] = round(sum(row["fact"] for row in bucket["rows"]), 2)
    return bucket


def _aggregate(stages: list[dict], *, year: int, month: int, day: date) -> dict:
    products = {key: {"day": _empty_bucket(), "month": _empty_bucket()} for key, _title, _name in PRODUCTS}
    month_start = date(year, month, 1)
    month_end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    for row in stages:
        family = _family(row.get("НаименованиеЭтапа"))
        if family not in products:
            continue
        plan_qty = _qty(row.get("КоличествоУпаковокПлан"))
        fact_qty = _qty(row.get("КоличествоУпаковокФакт"))
        end_day = _as_date(row.get("ФактическоеОкончаниеЭтапа"))
        finished = str(row.get("Статус") or "").strip() == "Завершен"
        # Закрытый этап относится к дате фактического окончания.
        if not finished or end_day is None:
            continue
        if end_day == day:
            item = _touch(products[family]["day"], row)
            item["plan"] += plan_qty
            item["fact"] += fact_qty
        if month_start <= end_day < month_end:
            item = _touch(products[family]["month"], row)
            item["plan"] += plan_qty
            item["fact"] += fact_qty
    for product in products.values():
        _finish_bucket(product["day"])
        _finish_bucket(product["month"])
    return products


def _kpi_pct(plan: float, fact: float) -> float | None:
    if plan:
        return round(fact / plan * 100, 1)
    return None


def _color(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 100:
        return "green"
    if pct >= 95:
        return "yellow"
    return "red"


def compute_blanking_output(year: int, month: int, *, now: datetime | None = None) -> dict:
    now = now or datetime.now()
    day = report_day(now)
    month_start = date(year, month, 1)
    month_end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    window_start = min(month_start, day)
    window_end = max(month_end, day + timedelta(days=1))
    stages = _fetch_stages(window_start, window_end)
    products = _aggregate(stages, year=year, month=month, day=day)
    return {
        "source": SOURCE_TAG,
        "cache_date": now.date().isoformat(),
        "cache_updated_at": now.replace(microsecond=0).isoformat(),
        "department_key": DEPT_KEY,
        "department_name": DEPT_NAME,
        "document": "Document_ЭтапПроизводства2_2",
        "year": int(year),
        "month": int(month),
        "day": day.isoformat(),
        "stage_rows": len(stages),
        "products": products,
    }


def get_blanking_output(year: int, month: int) -> dict:
    now = datetime.now()
    path = cache_path(year, month)
    cached = _load_json(path)
    if _snapshot_is_current(cached, year, month, now):
        return cached
    data = compute_blanking_output(year, month, now=now)
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Не удалось сохранить кэш заготовительного участка")
    return data


def _tile_name(product_key: str, period: str, snapshot: dict) -> str:
    base = next(name for key, _title, name in PRODUCTS if key == product_key)
    if period == "day":
        day = date.fromisoformat(str(snapshot.get("day")))
        return f"{base} на {day.day} число"
    month = int(snapshot.get("month") or 1)
    return f"{base} за {MONTH_RU.get(month, str(month))}"


def _period_label(period: str, snapshot: dict) -> str:
    if period == "day":
        day = date.fromisoformat(str(snapshot.get("day")))
        return day.strftime("%d.%m.%Y")
    month = int(snapshot.get("month") or 1)
    year = int(snapshot.get("year") or 0)
    return f"{MONTH_RU.get(month, str(month)).capitalize()} {year}".strip()


def tiles_from_snapshot(snapshot: dict) -> list[dict]:
    products = snapshot.get("products") if isinstance(snapshot.get("products"), dict) else {}
    year = int(snapshot.get("year") or 0)
    month = int(snapshot.get("month") or 0)
    updated = snapshot.get("cache_updated_at")
    tiles = []
    for kpi_id, product_key, period in TILE_SPECS:
        bucket = (products.get(product_key) or {}).get(period) or _empty_bucket()
        plan = round(_qty(bucket.get("plan")), 2)
        fact = round(_qty(bucket.get("fact")), 2)
        pct = _kpi_pct(plan, fact)
        label = _period_label(period, snapshot)
        row = {
            "year": year,
            "month": month,
            "month_name": MONTH_RU.get(month, ""),
            "plan": plan,
            "fact": fact,
            "kpi_pct": pct,
            "has_data": True,
            "values_unit": "шт.",
            "label": label,
            "production_plan_rows": bucket.get("rows") or [],
        }
        name = _tile_name(product_key, period, snapshot)
        tiles.append({
            "kpi_id": kpi_id,
            "name": name,
            "goal": f"План и факт выпуска, {DEPT_NAME}",
            "kpi_pct": pct,
            "color": _color(pct),
            "period": "ежедневно" if period == "day" else "ежемесячно",
            "thresholds": {"green": "≥100%", "yellow": "95–99,9%", "red": "<95%"},
            "green_threshold": "≥100%",
            "yellow_threshold": "95–99,9%",
            "red_threshold": "<95%",
            "formula": "Факт выпуска / План выпуска × 100%",
            "unit": "шт.",
            "source": (
                "1С Document_ЭтапПроизводства2_2, подразделение "
                f"«{DEPT_NAME}». Закрытый этап: КоличествоУпаковокПлан и "
                "КоличествоУпаковокФакт на дату ФактическоеОкончаниеЭтапа"
            ),
            "description": None,
            "frequency": "ежедневно" if period == "day" else "ежемесячно",
            "plan": plan,
            "fact": fact,
            "has_data": True,
            "data_granularity": "fixed_period",
            "kpi_period": {
                "type": "previous_day" if period == "day" else "current_month",
                "year": year,
                "month": month,
                "month_name": MONTH_RU.get(month, ""),
                "label": label,
            },
            "plan_fact_period_label": label,
            "cache_updated_at": updated,
            "last_full_month_row": row,
            "monthly_data": [row],
            "production_plan_rows": row["production_plan_rows"],
        })
    return tiles
