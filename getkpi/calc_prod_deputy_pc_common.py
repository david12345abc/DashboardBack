from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

from .cache_manager import CACHE_DIR
from .calc_fot_management import MONTH_RU, _normalize_period

ShopKey = Literal["pc1", "pc2"]

PC_BUDGET_PLAN: dict[ShopKey, list[float]] = {
    "pc1": [
        64_758_916, 61_406_486, 99_353_943, 112_441_820,
        112_591_529, 130_145_238, 115_597_218, 116_376_536,
        123_139_472, 97_269_434, 91_633_804, 139_775_508,
    ],
    "pc2": [
        9_497_714, 12_208_323, 20_287_862, 23_073_103,
        24_128_390, 29_098_537, 34_492_161, 36_666_379,
        34_215_075, 31_442_195, 19_803_164, 16_736_543,
    ],
}

PC_FOT_PLAN: dict[ShopKey, list[float]] = {
    "pc1": [
        4_599_526, 4_134_637, 8_164_107, 9_370_296,
        9_618_778, 12_210_328, 11_095_698, 9_936_417,
        10_877_749, 8_724_093, 7_835_742, 12_638_527,
    ],
    "pc2": [
        3_544_225, 3_545_159, 3_673_152, 3_814_617,
        3_597_997, 3_504_695, 3_957_994, 4_240_369,
        3_728_505, 3_743_244, 3_494_346, 3_584_674,
    ],
}


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def cache_path(metric: str, shop: ShopKey, year: int, ref_month: int) -> Path:
    return CACHE_DIR / f"prod_deputy_{metric}_{shop}_{year}_{ref_month:02d}.json"


def kpi_pct(plan: float | None, fact: float | None) -> float | None:
    if plan is None or fact is None or plan <= 0:
        return None
    return round(fact / plan * 100, 1)


def month_row(year: int, month: int, plan: float, fact: float, **extra) -> dict:
    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month].lower(),
        "plan": round(float(plan), 2),
        "fact": round(float(fact), 2),
        "kpi_pct": kpi_pct(float(plan), float(fact)),
        "has_data": abs(float(plan)) > 0 or abs(float(fact)) > 0,
        "values_unit": "руб.",
        **extra,
    }


def _quarter_label(quarter: int) -> str:
    return f"Q{quarter}"


def aggregate_rows(months_out: list[dict]) -> tuple[list[dict], list[dict]]:
    by_quarter: dict[tuple[int, int], dict] = {}
    by_year: dict[int, dict] = {}

    for row in months_out:
        year = int(row.get("year"))
        month = int(row.get("month"))
        quarter = (month - 1) // 3 + 1
        plan = float(row.get("plan") or 0)
        fact = float(row.get("fact") or 0)
        has_data = bool(row.get("has_data"))

        q = by_quarter.setdefault(
            (year, quarter),
            {
                "year": year,
                "quarter": quarter,
                "label": f"{_quarter_label(quarter)} {year}",
                "plan": 0.0,
                "fact": 0.0,
                "has_data": False,
                "values_unit": "руб.",
            },
        )
        q["plan"] += plan
        q["fact"] += fact
        q["has_data"] = q["has_data"] or has_data

        y = by_year.setdefault(
            year,
            {
                "year": year,
                "plan": 0.0,
                "fact": 0.0,
                "has_data": False,
                "values_unit": "руб.",
            },
        )
        y["plan"] += plan
        y["fact"] += fact
        y["has_data"] = y["has_data"] or has_data

    quarterly = []
    for (_year, _quarter), row in sorted(by_quarter.items()):
        row["plan"] = round(row["plan"], 2)
        row["fact"] = round(row["fact"], 2)
        row["kpi_pct"] = kpi_pct(row["plan"], row["fact"])
        quarterly.append(row)

    yearly = []
    for _year, row in sorted(by_year.items()):
        row["plan"] = round(row["plan"], 2)
        row["fact"] = round(row["fact"], 2)
        row["kpi_pct"] = kpi_pct(row["plan"], row["fact"])
        yearly.append(row)

    return quarterly, yearly


def build_payload(source_tag: str, shop: ShopKey, ref_year: int, ref_month: int, months_out: list[dict]) -> dict:
    today = date.today()
    with_data = [row for row in months_out if row.get("has_data")]
    selected_month_row = next(
        (
            row for row in months_out
            if row.get("year") == ref_year and row.get("month") == ref_month
        ),
        None,
    )
    last_data_row = selected_month_row or (with_data[-1] if with_data else (months_out[-1] if months_out else None))
    total_plan = sum(float(row.get("plan") or 0) for row in months_out if row.get("plan") is not None)
    total_fact = sum(float(row.get("fact") or 0) for row in months_out)
    quarterly_data, yearly_data = aggregate_rows(months_out)

    return {
        "cache_date": today.isoformat(),
        "source": source_tag,
        "shop": shop,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "quarterly_data": quarterly_data,
        "yearly_data": yearly_data,
        "last_full_month_row": dict(last_data_row) if last_data_row else None,
        "ytd": {
            "total_plan": round(total_plan, 2) if months_out else None,
            "total_fact": round(total_fact, 2) if months_out else None,
            "kpi_pct": kpi_pct(total_plan, total_fact),
            "months_with_data": len(with_data),
            "months_total": len(months_out),
            "values_unit": "руб." if months_out else None,
        },
        "kpi_period": {
            "type": "current_month",
            "year": (last_data_row or {}).get("year", ref_year),
            "month": (last_data_row or {}).get("month", ref_month),
            "month_name": (last_data_row or {}).get("month_name", MONTH_RU[ref_month].lower()),
        },
    }


__all__ = [
    "CACHE_DIR",
    "MONTH_RU",
    "PC_BUDGET_PLAN",
    "PC_FOT_PLAN",
    "ShopKey",
    "_normalize_period",
    "build_payload",
    "cache_path",
    "load_json",
    "month_row",
    "save_json",
]
