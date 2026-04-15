"""
calc_rashody.py — Расходы по статьям бюджета (KD-M7)

Источники:
  - Счёт 44  — дебетовый оборот (12 статей затрат)
  - Счёт 51  — дебетовый оборот (2 статьи ДДС) + СДС (3 статьи ДДС по кредиту 51)
  - Счёт 71.01 — дебетовое сальдо на конец периода (кредитовое/нулевое исключается)

Алгоритм:
  1. По каждому подразделению суммируется Дт-оборот 44 по 12 статьям.
  2. Группа 44-ВРТ (4 статьи) сравнивается с СДС (3 статьи списания ДС).
     Если СДС > 44-ВРТ → в итог подставляется СДС вместо 44-ВРТ.
  3. Добавляется Дт-оборот 51 (2 статьи).
  4. Добавляется дебетовое сальдо 71.01 на конец периода
     (кредитовое/нулевое сальдо по сотруднику → 0).

API:
  from getkpi.calc_rashody import get_rashody_monthly
  data = get_rashody_monthly(2026, 3)                # агрегат всех отделов
  data = get_rashody_monthly(2026, 3, dept_guid='…') # только один отдел
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

# ── Счета ──
ACCOUNTS_44 = {
    "fb2bde54-6250-11e7-812d-001e67112509",  # 44
    "fb2bde55-6250-11e7-812d-001e67112509",  # 44.01
    "fb2bde56-6250-11e7-812d-001e67112509",  # 44.02
}
ACC_51 = "fb2bde64-6250-11e7-812d-001e67112509"
ACC_7101 = "fb2bded3-6250-11e7-812d-001e67112509"

# ── 8 подразделений ──
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
DEPT_SET = frozenset(DEPARTMENTS)

# ── 12 статей затрат для счёта 44 ──
ART_44_SET = frozenset([
    "f6b5f6a7-3418-11e8-8271-ac1f6b05524d",  # Интернет-продвижение
    "19919da8-56cb-11e0-b816-008048428575",  # Командировка
    "992a514f-782a-11eb-854d-ac1f6b05524d",  # Оплата труда возмещение НПО
    "cc7c4aa6-3767-11ea-82f3-ac1f6b05524d",  # Оплата труда ТД НПО
    "d211f6cb-ff4a-11e7-826d-ac1f6b05524d",  # Представительские расходы
    "3cba9c50-341a-11e8-8271-ac1f6b05524d",  # Расходы на выставки
    "cdc34aed-ed4a-11e6-8128-001e67112509",  # Рекламная продукция
    "a04a98f8-782a-11eb-854d-ac1f6b05524d",  # Страховые взносы возмещение НПО
    "0360bf95-3768-11ea-82f3-ac1f6b05524d",  # Страховые взносы ТД НПО
    "9900a8d0-1b83-11e8-826f-ac1f6b05524d",  # Таможенные расходы
    "eee66843-ae99-11e3-adf9-001e67112509",  # Тендер НПО информационные услуги
    "f33409f4-5d1d-11e9-828d-ac1f6b05524d",  # Транспортные расходы на выставки
])

# ── Группа 44-ВРТ (4 статьи) ──
VRT_KEYS = frozenset([
    "3cba9c50-341a-11e8-8271-ac1f6b05524d",
    "cdc34aed-ed4a-11e6-8128-001e67112509",
    "eee66843-ae99-11e3-adf9-001e67112509",
    "f33409f4-5d1d-11e9-828d-ac1f6b05524d",
])

# ── 2 статьи ДДС для счёта 51 ──
ART_51_SET = frozenset([
    "f6f0d74d-d476-11e5-810b-001e67112509",
    "33a983cc-0812-11ed-8930-ac1f6b05524d",
])

# ── 3 статьи СДС ──
ART_SDS_SET = frozenset([
    "5ec4fd64-c8a9-11e5-810b-001e67112509",
    "badd95fb-ec45-11e6-8127-001e67112509",
    "55eeaa60-533f-11eb-84f3-ac1f6b05524d",
])

SUBCONTO_TYPE_COST = "fb2bdde9-6250-11e7-812d-001e67112509"

# ── План расходов на 2026 год (по месяцам 1-12) ──
RASHODY_PLAN: dict[str, list[float]] = {
    "bd7b5184-9f9c-11e4-80da-001e67112509": [  # Газпром
        707_789, 757_789, 760_349, 795_029, 757_789, 757_789,
        801_251, 872_784, 757_789, 843_498, 886_252, 1_032_249,
    ],
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": [  # ОРКК
        1_252_276, 1_397_551, 1_456_010, 1_525_422, 1_407_222, 1_392_276,
        1_521_636, 1_619_762, 1_447_822, 1_534_818, 1_439_256, 1_692_426,
    ],
    "49480c10-e401-11e8-8283-ac1f6b05524d": [  # ВЭД
        836_850, 986_850, 986_850, 1_095_243, 1_030_825, 986_850,
        1_050_739, 1_096_743, 1_113_826, 1_051_723, 1_025_426, 1_270_035,
    ],
    "7587c178-92f6-11f0-96f9-6cb31113810e": [  # ОДП
        1_837_715, 2_117_715, 2_141_130, 2_214_760, 2_117_715, 2_229_485,
        2_159_889, 2_237_533, 2_189_336, 2_144_183, 2_179_311, 2_330_094,
    ],
    "34497ef7-810f-11e4-80d6-001e67112509": [  # ОПЭОиУ
        897_710, 1_041_185, 924_207, 1_074_765, 888_314, 1_082_112,
        1_022_207, 1_056_062, 899_222, 1_148_444, 935_816, 1_116_310,
    ],
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": [  # БМИ
        662_288, 874_252, 909_603, 862_288, 870_976, 923_486,
        885_868, 862_288, 862_288, 906_841, 871_684, 912_288,
    ],
    "95dfd1c6-37a4-11ee-93d3-6cb31113810e": [  # PR
        286_527, 269_803, 335_421, 294_418, 270_421, 405_421,
        367_149, 274_523, 365_421, 423_624, 270_421, 505_421,
    ],
    "1c9f9419-d91b-11e0-8129-cd2988c3db2d": [  # Тендерный отдел
        1_821_460, 551_460, 642_529, 587_294, 452_844, 665_252,
        536_463, 491_460, 612_529, 496_401, 574_705, 722_529,
    ],
}

KOMDIR_OWN_PLAN: list[float] = [  # Коммерческий директор (собственные расходы)
    6_477_389, 7_435_133, 19_187_640, 21_962_757, 30_154_158, 34_201_207,
    30_614_217, 21_193_420, 29_826_045, 20_082_105, 17_803_247, 38_526_888,
]


def get_rashody_plan(month: int, dept_guid: str | None = None) -> float:
    """План расходов для месяца (1-12).
    dept_guid=None → сумма всех отделов + комдир.
    dept_guid='…'  → план конкретного отдела.
    """
    idx = month - 1
    if dept_guid is not None:
        plan_list = RASHODY_PLAN.get(dept_guid)
        return plan_list[idx] if plan_list and 0 <= idx < len(plan_list) else 0
    total = KOMDIR_OWN_PLAN[idx] if 0 <= idx < 12 else 0
    for plan_list in RASHODY_PLAN.values():
        if 0 <= idx < len(plan_list):
            total += plan_list[idx]
    return total


MONTH_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"


def _last_full_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _cache_path(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"rashody_{year}_{ref_month:02d}.json"


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


def _load_paged(session: requests.Session, flt: str, sel: str,
                label: str) -> list[dict]:
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
            logger.error("RASHODY [%s] HTTP error: %s", label, e)
            break
        if not r.ok:
            logger.error("RASHODY [%s] HTTP %d: %s", label, r.status_code, r.text[:300])
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        records.extend(batch)
        skip += len(batch)
        if len(batch) < PAGE:
            break
    return records


def _period_bounds(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _fetch_month(session: requests.Session, year: int, month: int) -> dict:
    """Загружает все 4 блока за один месяц и возвращает итог по подразделениям."""
    p_start, p_end = _period_bounds(year, month)

    # ── Блок 1: Счёт 44 ──
    acc_or = " or ".join(f"AccountDr_Key eq guid'{a}'" for a in ACCOUNTS_44)
    flt44 = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}'"
        f" and Active eq true and ({acc_or})"
    )
    sel44 = (
        "Period,AccountDr_Key,ПодразделениеDr_Key,Сумма,Сторно,"
        "ExtDimensionDr1,ExtDimensionTypeDr1_Key"
    )
    recs44 = _load_paged(session, flt44, sel44, "44")

    matrix44: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in recs44:
        amount = rec.get("Сумма", 0) or 0
        if rec.get("Сторно"):
            amount = -amount
        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        ext_dim1 = rec.get("ExtDimensionDr1")
        ext_type1 = rec.get("ExtDimensionTypeDr1_Key")
        article_key = ext_dim1 if ext_type1 == SUBCONTO_TYPE_COST and ext_dim1 else None
        if dept_key in DEPT_SET and article_key in ART_44_SET:
            matrix44[dept_key][article_key] += amount

    # ── Блок 2: СДС (кредит 51, 3 статьи) ──
    flt_sds = (
        f"AccountCr_Key eq guid'{ACC_51}'"
        f" and Active eq true"
        f" and Period ge datetime'{p_start}'"
        f" and Period lt datetime'{p_end}'"
    )
    sel_sds = "Period,Сумма,Сторно,ПодразделениеDr_Key,AccountDr_Key,ExtDimensionCr2"
    recs_sds = _load_paged(session, flt_sds, sel_sds, "SDS")

    sds_by_dept: dict[str, float] = defaultdict(float)
    for rec in recs_sds:
        ext2cr = rec.get("ExtDimensionCr2", "")
        if ext2cr not in ART_SDS_SET:
            continue
        amount = rec.get("Сумма", 0) or 0
        if rec.get("Сторно"):
            amount = -amount
        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        if dept_key in DEPT_SET:
            sds_by_dept[dept_key] += amount

    # ── Блок 3: Счёт 51, дебет (2 статьи) ──
    flt51 = (
        f"AccountDr_Key eq guid'{ACC_51}'"
        f" and Active eq true"
        f" and Period ge datetime'{p_start}'"
        f" and Period lt datetime'{p_end}'"
    )
    sel51 = "Period,Сумма,Сторно,ПодразделениеDr_Key,ExtDimensionDr2"
    recs51 = _load_paged(session, flt51, sel51, "51")

    acc51_by_dept: dict[str, float] = defaultdict(float)
    for rec in recs51:
        ext2 = rec.get("ExtDimensionDr2", "")
        if ext2 not in ART_51_SET:
            continue
        amount = rec.get("Сумма", 0) or 0
        if rec.get("Сторно"):
            amount = -amount
        dept_key = rec.get("ПодразделениеDr_Key", EMPTY)
        if dept_key in DEPT_SET:
            acc51_by_dept[dept_key] += amount

    # ── Блок 4: Дебетовое сальдо 71.01 (кредитовое/нулевое исключается) ──
    flt_bal = f"Account_Key eq guid'{ACC_7101}'"
    url_bal = (
        f"{BASE}/{quote('AccountingRegister_Хозрасчетный')}"
        f"/Balance(Period=datetime'{p_end}')"
        f"?$format=json&$top=5000"
        f"&$filter={quote(flt_bal, safe='')}"
    )
    bal_7101: dict[str, float] = defaultdict(float)
    try:
        r_bal = session.get(url_bal, timeout=120)
        if r_bal.ok:
            for item in r_bal.json().get("value", []):
                bal = item.get("СуммаBalance", 0) or 0
                if bal <= 0:
                    continue
                dept_key = item.get("Подразделение_Key", EMPTY)
                if dept_key in DEPT_SET:
                    bal_7101[dept_key] += bal
        else:
            logger.error("RASHODY [71.01] HTTP %d: %s", r_bal.status_code, r_bal.text[:300])
    except Exception as e:
        logger.error("RASHODY [71.01] error: %s", e)

    # ── Расчёт итогов по подразделениям ──
    by_dept: dict[str, float] = {}
    total = 0.0

    for dk in DEPT_SET:
        s44_non_vrt = sum(
            matrix44[dk][ak] for ak in ART_44_SET if ak not in VRT_KEYS
        )
        vrt_val = sum(matrix44[dk][ak] for ak in VRT_KEYS)
        sds_val = sds_by_dept.get(dk, 0)
        effective_vrt = sds_val if sds_val > vrt_val else vrt_val

        s_51 = acc51_by_dept.get(dk, 0)
        s_71 = bal_7101.get(dk, 0)

        dept_total = s44_non_vrt + effective_vrt + s_51 + s_71
        by_dept[dk] = round(dept_total, 2)
        total += dept_total

    return {
        "total": round(total, 2),
        "by_dept": by_dept,
    }


def _slice_payload(payload: dict, dept_guid: str | None) -> dict:
    sliced = []
    for row in payload.get("months", []):
        m = row["month"]
        if dept_guid is None:
            fact = row.get("total", 0)
        else:
            fact = row.get("by_dept", {}).get(dept_guid, 0)
        sliced.append({
            "year": row["year"],
            "month": m,
            "plan": get_rashody_plan(m, dept_guid),
            "fact": fact,
        })
    return {
        "cache_date": payload.get("cache_date"),
        "year": payload.get("year"),
        "ref_month": payload.get("ref_month"),
        "months": sliced,
    }


def get_rashody_monthly(year: int | None = None,
                        month: int | None = None,
                        dept_guid: str | None = None) -> dict:
    """
    Помесячные расходы коммерческой службы (январь..ref_month).

    dept_guid=None  — сумма по всем подразделениям (коммерческий директор).
    dept_guid='…'   — только указанное подразделение.

    Возвращает:
      {"year": …, "ref_month": …, "months": [
          {"year": …, "month": …, "fact": …}, …
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

    logger.info("calc_rashody: loading data for %d months 1-%d", ref_y, ref_m)
    t0 = time.time()

    out_months = []
    for m in range(1, ref_m + 1):
        agg = _fetch_month(session, ref_y, m)
        out_months.append({
            "year": ref_y,
            "month": m,
            "total": agg["total"],
            "by_dept": agg["by_dept"],
        })
        logger.info("  month %d: total=%.2f", m, agg["total"])

    logger.info("calc_rashody: done in %.1fs", time.time() - t0)

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

    _print(f"\n{'=' * 60}")
    _print(f"  РАСХОДЫ КОММЕРЧЕСКОЙ СЛУЖБЫ")
    _print(f"  Период: январь - {MONTH_RU[m]} {y}")
    _print(f"{'=' * 60}")

    t0 = time.time()
    data = get_rashody_monthly(y, m)

    _print(f"\n  {'Месяц':<12s} {'Расходы':>16s}")
    _print(f"  {'-' * 30}")
    for row in data.get("months", []):
        _print(f"  {MONTH_RU[row['month']]:<12s} {row['fact']:>16,.2f}")

    _print(f"\n  Время: {time.time() - t0:.1f}с")
    _print(f"{'=' * 60}")
