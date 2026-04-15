"""
calc_fot.py — ФОТ коммерческой службы по регистру бухгалтерии (Хозрасчётный).

Дебетовый оборот счёта 44 (44.01, 44.02) за период в разрезе
подразделений × статей затрат.

Источник: AccountingRegister_Хозрасчетный / RecordsWithExtDimensions.
Субконто «Статьи затрат» = ExtDimensionDr1 (тип ChartOfCharacteristicTypes_СтатьиРасходов).

API:
  from getkpi.calc_fot import get_fot_monthly
  data = get_fot_monthly(2026, 3)                # агрегат всех отделов
  data = get_fot_monthly(2026, 3, dept_guid='…') # только один отдел
"""
from __future__ import annotations

import json
import logging
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ACCOUNTS_44 = {
    "fb2bde54-6250-11e7-812d-001e67112509",  # 44
    "fb2bde55-6250-11e7-812d-001e67112509",  # 44.01
    "fb2bde56-6250-11e7-812d-001e67112509",  # 44.02
}

DEPARTMENTS = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборуд. и услуг",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": "Сектор рекламы и PR",
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": "Отдел тендерных продаж",
}
DEPT_SET = frozenset(DEPARTMENTS.keys())

COST_ARTICLES = {
    "992a514f-782a-11eb-854d-ac1f6b05524d": "Оплата труда (44 сч) возмещение НПО!",
    "cc7c4aa6-3767-11ea-82f3-ac1f6b05524d": "Оплата труда (44 сч) ТД НПО!",
    "a04a98f8-782a-11eb-854d-ac1f6b05524d": "Страховые взносы (44.01) возмещение НПО!",
    "0360bf95-3768-11ea-82f3-ac1f6b05524d": "Страховые взносы (44.01) ТД НПО!",
}
ARTICLE_SET = frozenset(COST_ARTICLES.keys())

SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

# ── План расходов ФОТ на 2026 год (по месяцам 1-12) ──
FOT_PLAN: dict[str, list[float]] = {
    "bd7b5184-9f9c-11e4-80da-001e67112509": [  # Газпром
        577_789, 577_789, 580_349, 615_029, 577_789, 577_789,
        621_251, 692_784, 577_789, 613_498, 656_252, 802_249,
    ],
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": [  # ОРКК
        922_276, 927_551, 986_010, 1_055_422, 937_222, 922_276,
        1_051_636, 1_149_762, 977_822, 1_064_818, 969_256, 1_222_426,
    ],
    "49480c10-e401-11e8-8283-ac1f6b05524d": [  # ВЭД
        836_850, 986_850, 986_850, 1_095_243, 1_030_825, 986_850,
        1_050_739, 1_096_743, 1_113_826, 1_051_723, 1_025_426, 1_270_035,
    ],
    "7587c178-92f6-11f0-96f9-6cb31113810e": [  # ОДП
        1_517_715, 1_517_715, 1_541_130, 1_614_760, 1_517_715, 1_629_485,
        1_559_889, 1_637_533, 1_589_336, 1_544_183, 1_579_311, 1_690_094,
    ],
    "34497ef7-810f-11e4-80d6-001e67112509": [  # ОПЭОиУ
        897_710, 891_185, 924_207, 924_765, 888_314, 932_112,
        1_022_207, 906_062, 899_222, 998_444, 935_816, 1_016_310,
    ],
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": [  # БМИ
        662_288, 674_252, 709_603, 662_288, 670_976, 723_486,
        685_868, 662_288, 662_288, 706_841, 671_684, 662_288,
    ],
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": [  # PR
        271_527, 254_803, 255_421, 279_418, 255_421, 255_421,
        322_149, 259_523, 255_421, 258_624, 255_421, 255_421,
    ],
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": [  # Тендерный отдел
        341_460, 341_460, 522_529, 367_294, 352_844, 535_252,
        366_463, 341_460, 522_529, 376_401, 364_705, 522_529,
    ],
}

KOMDIR_OWN_PLAN: list[float] = [  # Коммерческий директор (собственные расходы)
    1_013_594, 1_042_372, 1_463_819, 989_490, 1_013_594, 1_539_322,
    1_063_397, 1_079_589, 1_504_007, 1_011_476, 1_019_662, 1_816_999,
]


def get_fot_plan(month: int, dept_guid: str | None = None) -> float:
    """План ФОТ для месяца (1-12).
    dept_guid=None → сумма всех отделов + комдир.
    dept_guid='…'  → план конкретного отдела.
    """
    idx = month - 1
    if dept_guid is not None:
        plan_list = FOT_PLAN.get(dept_guid)
        return plan_list[idx] if plan_list and 0 <= idx < len(plan_list) else 0
    total = KOMDIR_OWN_PLAN[idx] if 0 <= idx < 12 else 0
    for plan_list in FOT_PLAN.values():
        if 0 <= idx < len(plan_list):
            total += plan_list[idx]
    return total


CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"fot_{year}_{ref_month:02d}.json"


def _load_cache(year: int, ref_month: int) -> dict | None:
    p = _cache_path(year, ref_month)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_date") == date.today().isoformat():
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_cache(year: int, ref_month: int, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_cache_path(year, ref_month), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _fetch_records_for_month(session: requests.Session,
                             year: int, month: int) -> list[dict]:
    """Загружает проводки RecordsWithExtDimensions по счёту 44 за один месяц."""
    if month == 12:
        p_start = f"{year}-12-01T00:00:00"
        p_end = f"{year + 1}-01-01T00:00:00"
    else:
        p_start = f"{year}-{month:02d}-01T00:00:00"
        p_end = f"{year}-{month + 1:02d}-01T00:00:00"

    acc_parts = [f"AccountDr_Key eq guid'{ak}'" for ak in ACCOUNTS_44]
    acc_filter = " or ".join(acc_parts)

    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true"
        f" and ({acc_filter})"
    )

    sel = (
        "Period,AccountDr_Key,ПодразделениеDr_Key,Сумма,Сторно,"
        "ExtDimensionDr1,ExtDimensionTypeDr1_Key"
    )

    records: list[dict] = []
    skip = 0
    PAGE = 5000

    while True:
        url = (
            f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}"
            f"/RecordsWithExtDimensions"
            f"?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
            f"&$top={PAGE}&$skip={skip}"
        )
        try:
            r = session.get(url, timeout=120)
        except Exception as e:
            logger.error("FOT HTTP error: %s", e)
            break
        if not r.ok:
            logger.error("FOT HTTP %d: %s", r.status_code, r.text[:300])
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        records.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break

    return records


def _aggregate_records(records: list[dict]) -> dict[str, float]:
    """Агрегирует ФОТ по подразделениям (целевые статьи × целевые отделы)."""
    by_dept: dict[str, float] = defaultdict(float)
    total = 0.0

    for rec in records:
        amount = rec.get("Сумма", 0) or 0
        if rec.get("Сторно"):
            amount = -amount

        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        ext_dim1 = rec.get("ExtDimensionDr1")
        ext_type1 = rec.get("ExtDimensionTypeDr1_Key")

        article_key = None
        if ext_type1 == SUBCONTO_TYPE_COST and ext_dim1:
            article_key = ext_dim1

        if dept_key in DEPT_SET and article_key in ARTICLE_SET:
            by_dept[dept_key] += amount
            total += amount

    by_dept_rounded = {k: round(v, 2) for k, v in by_dept.items()}
    return {"total": round(total, 2), "by_dept": by_dept_rounded}


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    """Полный агрегат или срез по одному подразделению. Добавляет plan."""
    sliced = []
    for row in payload.get("months", []):
        m = row["month"]
        if dept_guid is None:
            fact = row.get("fact", 0)
        else:
            fact = row.get("by_dept", {}).get(dept_guid, 0)
        sliced.append({
            "year": row["year"],
            "month": m,
            "plan": get_fot_plan(m, dept_guid),
            "fact": fact,
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced,
    }


def get_fot_monthly(year: int | None = None,
                    month: int | None = None,
                    dept_guid: str | None = None) -> dict:
    """
    Помесячный ФОТ коммерческой службы (январь..ref_month).

    dept_guid=None  — сумма по всем подразделениям (коммерческий директор).
    dept_guid='…'   — только указанное подразделение.

    Возвращает:
      {"year": …, "ref_month": …, "months": [
          {"year": …, "month": …, "fact": …, "by_dept": {guid: amount, …}},
      ]}
    """
    today = date.today()
    ref_y, ref_m = _last_full_month(today)
    if year is not None and month is not None:
        ref_y, ref_m = year, month

    cached = _load_cache(ref_y, ref_m)
    if cached is not None:
        return _slice_payload(cached, dept_guid)

    session = requests.Session()
    session.auth = AUTH

    logger.info("calc_fot: loading records for %d months 1-%d", ref_y, ref_m)
    t0 = time.time()

    out_months = []
    for m in range(1, ref_m + 1):
        records = _fetch_records_for_month(session, ref_y, m)
        agg = _aggregate_records(records)
        out_months.append({
            "year": ref_y,
            "month": m,
            "fact": agg["total"],
            "by_dept": agg["by_dept"],
        })
        logger.info("  month %d: %d records, total=%.2f",
                     m, len(records), agg["total"])

    logger.info("calc_fot: done in %.1fs", time.time() - t0)

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
    _print(f"  ФОТ КОММЕРЧЕСКОЙ СЛУЖБЫ")
    _print(f"  Период: январь – {MONTH_RU[m]} {y}")
    _print(f"{'═' * 60}")

    t0 = time.time()
    data = get_fot_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'ФОТ':>15s}")
    _print(f"  {'─' * 28}")
    for row in data.get("months", []):
        _print(f"  {MONTH_RU[row['month']]:<12s} {row['fact']:>15,.2f}")

    _print(f"\n  По подразделениям ({MONTH_RU[m]} {y}):")
    _print(f"  {'─' * 55}")
    for row in data.get("months", []):
        if row["month"] != m:
            continue
        for dk, val in (row.get("by_dept") or {}).items():
            name = DEPARTMENTS.get(dk, dk)
            _print(f"    {name:<40s} {val:>15,.2f}")

    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'═' * 60}")
