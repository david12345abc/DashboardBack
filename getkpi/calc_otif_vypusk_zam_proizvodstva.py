# -*- coding: utf-8 -*-
"""
OTIF / выполнение плана отгрузок для заместителя директора по производству.

Источник: MSSQL erp_pm
  • План (шт): РС ТД_КонтрольныеДатыИсполненияДоговора (_InfoRg127734)
      ДатаОкончанияЮридическихОбязательствПоДоговору = _Fld127745
      ЗаказКлиента = _Fld127735_RRRef (тип Документ.ЗаказКлиента)
      КодСтроки = _Fld127738
      Количество: Document.ЗаказКлиента.Товары (_Document704_VT21248._Fld21254),
      не отменённые (_Fld21268).
  • Факт OTIF (шт): РН РаспоряженияНаОтгрузку (_AccumRg169757),
      расход, |Заказано| = ABS(_Fld169766), отгрузки к концу юр. срока
      (Σ min(отгружено_к_сроку, план_строки)).
  • Отгрузки за календарный месяц — только отладка (shipped_in_month).

Плитка: fact = OTIF к юр. сроку, kpi% = otif_by_qty_pct.

Разрез по цехам:
  pc1 — организации НПО + ООО Турбулентность-Дон, без партнёров Алмаз/СКТБ/МГС
  pc2 — организация Алмаз, без партнёров СКТБ/МГС
"""
from __future__ import annotations

import json
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

from comdir.common import connect_ctx, to_1c_dt, uuid_to_1c_bytes

from .calc_fot_management import MONTH_RU, _normalize_period

CACHE_DIR = Path(__file__).resolve().parent / "dashboard"

ShopKey = Literal["pc1", "pc2"]

SOURCE_TAG = "otif_vypusk_prod_monthly_sql_erp_pm_v2"
EMPTY16 = bytes(16)
ORDER_TREF = bytes.fromhex("000002c0")
KIND_EXPENSE = bytes.fromhex("85662942ac5e614b4aca8d30654dd705")

ORG_NPO = uuid_to_1c_bytes("fbca2148-6cfd-11e7-812d-001e67112509")
ORG_TURB = uuid_to_1c_bytes("fbca2143-6cfd-11e7-812d-001e67112509")
ORG_ALMAZ = uuid_to_1c_bytes("fbca2146-6cfd-11e7-812d-001e67112509")

PARTNER_ALMAZ = bytes.fromhex("8266ac1f6b05524d11e7a8c56ff45495")
PARTNER_SKTB = bytes.fromhex("8266ac1f6b05524d11e7a8c74babc7a7")
PARTNER_MGS = bytes.fromhex("8266ac1f6b05524d11e7a8c6d7f5ff44")

SHOP_ORGS: dict[ShopKey, tuple[bytes, ...]] = {
    "pc1": (ORG_NPO, ORG_TURB),
    "pc2": (ORG_ALMAZ,),
}
SHOP_EXCLUDE_PARTNERS: dict[ShopKey, tuple[bytes, ...]] = {
    "pc1": (PARTNER_ALMAZ, PARTNER_SKTB, PARTNER_MGS),
    "pc2": (PARTNER_SKTB, PARTNER_MGS),
}
SHOP_LABEL: dict[ShopKey, str] = {
    "pc1": "Турбулентность-Дон (НПО+ООО)",
    "pc2": "Алмаз",
}


def _normalize_shop(shop: str | None) -> ShopKey:
    return "pc2" if str(shop or "").strip().lower() == "pc2" else "pc1"


def cache_path(shop: ShopKey, year: int, ref_month: int) -> Path:
    return CACHE_DIR / f"otif_vypusk_prod_monthly_{shop}_{year}_{ref_month:02d}.json"


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    p0 = to_1c_dt(date(year, month, 1))
    last = monthrange(year, month)[1]
    p1 = to_1c_dt(date(year, month, 1) + timedelta(days=last))
    return p0, p1


def _in_clause(values: tuple[bytes, ...]) -> str:
    return ",".join("?" * len(values))


def calc_month(cur, shop: ShopKey, year: int, month: int) -> dict:
    p0, p1 = _month_bounds(year, month)
    orgs = SHOP_ORGS[shop]
    excl = SHOP_EXCLUDE_PARTNERS[shop]
    org_sql = _in_clause(orgs)
    excl_sql = _in_clause(excl) if excl else "NULL"

    params: list = [p0, p1, ORDER_TREF, *orgs, EMPTY16]
    partner_filter = ""
    if excl:
        partner_filter = f"AND o._Fld21180RRef NOT IN ({excl_sql})"
        params.extend(excl)

    cur.execute(
        f"""
        SELECT
            COUNT(*) AS plan_lines,
            SUM(q.plan_qty) AS plan_qty
        FROM (
            SELECT p.ord, p.ks,
                   SUM(CASE WHEN ISNULL(t._Fld21268, 0x00) = 0x00 THEN t._Fld21254 ELSE 0 END) AS plan_qty
            FROM (
                SELECT r._Fld127735_RRRef AS ord,
                       r._Fld127738 AS ks,
                       MAX(r._Fld127745) AS legal_dt
                FROM _InfoRg127734 r WITH (NOLOCK)
                INNER JOIN _Document704 o WITH (NOLOCK)
                  ON o._IDRRef = r._Fld127735_RRRef
                WHERE r._Fld127745 >= ? AND r._Fld127745 < ?
                  AND r._Fld127735_RTRef = ?
                  AND o._Posted = 0x01
                  AND o._Marked = 0x00
                  AND o._Fld21182RRef IN ({org_sql})
                  AND o._Fld21183RRef <> ?
                  AND ISNULL(o._Fld184301, 0x00) = 0x00
                  AND ISNULL(o._Fld185211, 0x00) = 0x00
                  {partner_filter}
                GROUP BY r._Fld127735_RRRef, r._Fld127738
            ) p
            INNER JOIN _Document704_VT21248 t WITH (NOLOCK)
              ON t._Document704_IDRRef = p.ord
             AND t._Fld21267 = p.ks
            GROUP BY p.ord, p.ks
        ) q
        """,
        *params,
    )
    plan_row = cur.fetchone()
    plan_lines = int(plan_row[0] or 0)
    plan_qty = float(plan_row[1] or 0)

    ship_params = list(params) + [p0, p1, ORDER_TREF, KIND_EXPENSE]
    cur.execute(
        f"""
        SELECT SUM(ABS(s._Fld169766))
        FROM _AccumRg169757 s WITH (NOLOCK)
        INNER JOIN (
            SELECT r._Fld127735_RRRef AS ord, r._Fld127738 AS ks
            FROM _InfoRg127734 r WITH (NOLOCK)
            INNER JOIN _Document704 o WITH (NOLOCK)
              ON o._IDRRef = r._Fld127735_RRRef
            WHERE r._Fld127745 >= ? AND r._Fld127745 < ?
              AND r._Fld127735_RTRef = ?
              AND o._Posted = 0x01
              AND o._Marked = 0x00
              AND o._Fld21182RRef IN ({org_sql})
              AND o._Fld21183RRef <> ?
              AND ISNULL(o._Fld184301, 0x00) = 0x00
              AND ISNULL(o._Fld185211, 0x00) = 0x00
              {partner_filter}
            GROUP BY r._Fld127735_RRRef, r._Fld127738
        ) p ON p.ord = s._Fld169758_RRRef AND p.ks = s._Fld169761
        WHERE s._Period >= ? AND s._Period < ?
          AND s._Active = 0x01
          AND ISNULL(s._Fld169770, 0x00) = 0x00
          AND s._Fld169758_RTRef = ?
          AND s._Fld169764RRef = ?
        """,
        *ship_params,
    )
    fact_month = float(cur.fetchone()[0] or 0)

    otif_params = list(params) + [ORDER_TREF, KIND_EXPENSE]
    cur.execute(
        f"""
        SELECT
            SUM(CASE
                  WHEN q.plan_qty > 0
                  THEN CASE WHEN ISNULL(s.shipped, 0) < q.plan_qty
                            THEN ISNULL(s.shipped, 0) ELSE q.plan_qty END
                  ELSE 0 END) AS otif_num,
            SUM(CASE WHEN q.plan_qty > 0 THEN q.plan_qty ELSE 0 END) AS otif_den,
            SUM(CASE
                  WHEN q.plan_qty > 0 AND ISNULL(s.shipped, 0) + 0.000000001 >= q.plan_qty
                  THEN 1 ELSE 0 END) AS lines_full
        FROM (
            SELECT p.ord, p.ks, p.legal_dt,
                   SUM(CASE WHEN ISNULL(t._Fld21268, 0x00) = 0x00 THEN t._Fld21254 ELSE 0 END) AS plan_qty
            FROM (
                SELECT r._Fld127735_RRRef AS ord,
                       r._Fld127738 AS ks,
                       MAX(r._Fld127745) AS legal_dt
                FROM _InfoRg127734 r WITH (NOLOCK)
                INNER JOIN _Document704 o WITH (NOLOCK)
                  ON o._IDRRef = r._Fld127735_RRRef
                WHERE r._Fld127745 >= ? AND r._Fld127745 < ?
                  AND r._Fld127735_RTRef = ?
                  AND o._Posted = 0x01
                  AND o._Marked = 0x00
                  AND o._Fld21182RRef IN ({org_sql})
                  AND o._Fld21183RRef <> ?
                  AND ISNULL(o._Fld184301, 0x00) = 0x00
                  AND ISNULL(o._Fld185211, 0x00) = 0x00
                  {partner_filter}
                GROUP BY r._Fld127735_RRRef, r._Fld127738
            ) p
            INNER JOIN _Document704_VT21248 t WITH (NOLOCK)
              ON t._Document704_IDRRef = p.ord
             AND t._Fld21267 = p.ks
            GROUP BY p.ord, p.ks, p.legal_dt
        ) q
        LEFT JOIN (
            SELECT p.ord, p.ks, SUM(ABS(s._Fld169766)) AS shipped
            FROM (
                SELECT r._Fld127735_RRRef AS ord,
                       r._Fld127738 AS ks,
                       MAX(r._Fld127745) AS legal_dt
                FROM _InfoRg127734 r WITH (NOLOCK)
                INNER JOIN _Document704 o WITH (NOLOCK)
                  ON o._IDRRef = r._Fld127735_RRRef
                WHERE r._Fld127745 >= ? AND r._Fld127745 < ?
                  AND r._Fld127735_RTRef = ?
                  AND o._Posted = 0x01
                  AND o._Marked = 0x00
                  AND o._Fld21182RRef IN ({org_sql})
                  AND o._Fld21183RRef <> ?
                  AND ISNULL(o._Fld184301, 0x00) = 0x00
                  AND ISNULL(o._Fld185211, 0x00) = 0x00
                  {partner_filter}
                GROUP BY r._Fld127735_RRRef, r._Fld127738
            ) p
            INNER JOIN _AccumRg169757 s WITH (NOLOCK)
              ON s._Fld169758_RRRef = p.ord
             AND s._Fld169761 = p.ks
            WHERE s._Active = 0x01
              AND ISNULL(s._Fld169770, 0x00) = 0x00
              AND s._Fld169758_RTRef = ?
              AND s._Fld169764RRef = ?
              AND s._Period < DATEADD(day, 1, p.legal_dt)
            GROUP BY p.ord, p.ks
        ) s ON s.ord = q.ord AND s.ks = q.ks
        """,
        *params,
        *otif_params,
    )
    otif_num, otif_den, lines_full = cur.fetchone()
    otif_num = float(otif_num or 0)
    otif_den = float(otif_den or 0)
    lines_full = int(lines_full or 0)

    pct_qty = (100.0 * otif_num / otif_den) if otif_den > 0 else None
    pct_lines = (100.0 * lines_full / plan_lines) if plan_lines > 0 else None
    exec_month_pct = (100.0 * fact_month / plan_qty) if plan_qty > 0 else None

    return {
        "year": year,
        "month": month,
        "month_name": MONTH_RU[month],
        "shop": shop,
        "scope": SHOP_LABEL[shop],
        "plan_lines": plan_lines,
        "plan_qty": round(plan_qty, 6),
        "otif_qty": round(otif_num, 6),
        "fact_shipped_qty_in_month": round(fact_month, 6),
        "execution_month_pct": round(exec_month_pct, 2) if exec_month_pct is not None else None,
        "otif_by_qty_pct": round(pct_qty, 2) if pct_qty is not None else None,
        "otif_full_lines": lines_full,
        "otif_lines_pct": round(pct_lines, 2) if pct_lines is not None else None,
    }


def get_otif_vypusk_prod_monthly(
    year: int | None = None,
    month: int | None = None,
    shop: str | None = "pc1",
) -> dict:
    today = date.today()
    shop_key = _normalize_shop(shop)
    ref_year, ref_month = _normalize_period(year, month)
    path = cache_path(shop_key, ref_year, ref_month)
    is_current_month = ref_year == today.year and ref_month == today.month

    cached = _load_json(path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        if not is_current_month or cached.get("cache_date") == today.isoformat():
            return cached

    months_out: list[dict] = []
    ref_row: dict | None = None
    with connect_ctx() as cn:
        cur = cn.cursor()
        for mm in range(1, ref_month + 1):
            row = calc_month(cur, shop_key, ref_year, mm)
            plan_qty = round(float(row.get("plan_qty") or 0), 6)
            fact_qty = round(float(row.get("otif_qty") or 0), 6)
            shipped_in_month = round(float(row.get("fact_shipped_qty_in_month") or 0), 6)
            otif_pct = row.get("otif_by_qty_pct")
            month_row = {
                "year": ref_year,
                "month": mm,
                "month_name": MONTH_RU[mm].lower(),
                "plan": plan_qty,
                "fact": fact_qty,
                "kpi_pct": otif_pct,
                "has_data": abs(plan_qty) > 0 or abs(fact_qty) > 0 or abs(shipped_in_month) > 0,
                "values_unit": "шт.",
                "execution_month_pct": otif_pct,
                "shipped_in_month": shipped_in_month,
                "rejected_qty": shipped_in_month,
                "otif_by_qty_pct": otif_pct,
                "otif_lines_pct": row.get("otif_lines_pct"),
                "plan_lines": row.get("plan_lines"),
                "otif_full_lines": row.get("otif_full_lines"),
            }
            months_out.append(month_row)
            if mm == ref_month:
                ref_row = month_row

    payload = {
        "cache_date": today.isoformat(),
        "source": SOURCE_TAG,
        "shop": shop_key,
        "year": ref_year,
        "ref_month": ref_month,
        "months": months_out,
        "last_full_month_row": dict(ref_row) if ref_row else None,
        "ytd": {
            "total_plan": ref_row.get("plan") if ref_row else None,
            "total_fact": ref_row.get("fact") if ref_row else None,
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else None,
            "months_with_data": sum(1 for row in months_out if row.get("has_data")),
            "months_total": len(months_out),
            "values_unit": "шт." if months_out else None,
        },
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_year,
            "month": ref_month,
            "month_name": MONTH_RU[ref_month].lower(),
        },
    }
    _save_json(path, payload)
    return payload


def main() -> None:
    argv = [a for a in sys.argv[1:] if not a.startswith("--")]
    shop = "pc2" if "--shop=pc2" in sys.argv or "--shop" in sys.argv and "pc2" in sys.argv else "pc1"
    for a in sys.argv[1:]:
        if a.startswith("--shop="):
            shop = _normalize_shop(a.split("=", 1)[1])
    today = date.today()
    if not argv:
        y, mo = today.year, today.month
    elif len(argv[0]) == 7 and argv[0][4] == "-":
        y, mo = int(argv[0][:4]), int(argv[0][5:7])
    else:
        print("usage: calc_otif_vypusk_zam_proizvodstva.py [YYYY-MM] [--shop=pc1|pc2]")
        sys.exit(1)

    data = get_otif_vypusk_prod_monthly(year=y, month=mo, shop=shop)
    row = data.get("last_full_month_row") or {}
    print(f"shop={data.get('shop')} {y}-{mo:02d}")
    print(f"  plan={row.get('plan')} fact={row.get('fact')} kpi={row.get('kpi_pct')}")
    print(f"  shipped_in_month={row.get('shipped_in_month')} lines={row.get('plan_lines')}")
    print(f"  otif_qty={row.get('otif_by_qty_pct')} otif_lines={row.get('otif_lines_pct')}")


if __name__ == "__main__":
    main()
