"""
calc_psd_vipusk_plan.py — ПСД · Выпуск · факт выпуска за месяц.

Факт выпуска:
  • АЛМАЗ: Document_ДвижениеПродукцииИМатериалов, операция
    ПередачаПродукцииИзПроизводства, организация АЛМАЗ ООО,
    сумма Количество из табличной части Товары.
  • НПО: те же документы по организации ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО,
    Отправитель = ОТК-1, Получатель = Склад готовой продукции №1.
    Через Распоряжение (этап производства) находим заказ на производство,
    затем ДокументОснование (заказ клиента) и сумму строки заказа клиента
    по паре Номенклатура + Характеристика.
"""
from __future__ import annotations

import functools
import json
import os
import sys
import time
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
EMPTY = "00000000-0000-0000-0000-000000000000"

ORG_NPO = "fbca2148-6cfd-11e7-812d-001e67112509"
ORG_ALMAZ = "fbca2146-6cfd-11e7-812d-001e67112509"
TURB_ORGS = {
    ORG_NPO: "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
    "fbca2143-6cfd-11e7-812d-001e67112509": "Турбулентность-Дон ООО",
    ORG_ALMAZ: "АЛМАЗ ООО",
}

OTK1_KEY = "37f2482d-62c5-11e6-8121-001e67112509"
READY_WAREHOUSE_1_KEY = "023b3c10-cab8-11e7-8267-ac1f6b05524d"

OP_TRANSFER_FROM_PRODUCTION = "ПередачаПродукцииИзПроизводства"
STAGE_TYPE = "StandardODATA.Document_ЭтапПроизводства2_2"
PRODUCTION_ORDER_TYPE = "StandardODATA.Document_ЗаказНаПроизводство2_2"
CUSTOMER_ORDER_TYPE = "StandardODATA.Document_ЗаказКлиента"

PAGE = 5000
BATCH = 20
TIMEOUT = 120
CACHE_DIR = Path(__file__).resolve().parent / "dashboard"
SOURCE_TAG = "psd_vipusk_fact_movement_products_v4"


def parse_month_arg(value: str) -> tuple[date, date]:
    y, m = value.split("-")
    y_i, m_i = int(y), int(m)
    start = date(y_i, m_i, 1)
    end = date(y_i, m_i, monthrange(y_i, m_i)[1])
    return start, end


def _next_month_start(day: date) -> date:
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def make_session() -> requests.Session:
    s = requests.Session()
    s.auth = AUTH
    return s


def _entity(name: str) -> str:
    return quote(name, safe="")


def fetch_all_paged(session: requests.Session, base_url: str, page: int = PAGE) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    sep = "&" if "?" in base_url else "?"
    while True:
        url = f"{base_url}{sep}$top={page}&$skip={skip}"
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        chunk = r.json().get("value", [])
        rows.extend(chunk)
        if len(chunk) < page:
            break
        skip += page
    return rows


def _fetch_by_refs(
    session: requests.Session,
    entity_name: str,
    refs: set[str],
    select: str,
    *,
    ref_field: str = "Ref_Key",
) -> dict[str, dict]:
    result: dict[str, dict] = {}
    ref_list = sorted(ref for ref in refs if ref and ref != EMPTY)
    for i in range(0, len(ref_list), BATCH):
        batch = ref_list[i:i + BATCH]
        flt = quote(" or ".join(f"{ref_field} eq guid'{ref}'" for ref in batch), safe="")
        url = (
            f"{BASE}/{_entity(entity_name)}"
            f"?$format=json&$filter={flt}&$select={quote(select, safe=',_')}"
        )
        for row in fetch_all_paged(session, url, page=BATCH):
            key = row.get(ref_field)
            if key:
                result[key] = row
    return result


def _fetch_table_by_refs(
    session: requests.Session,
    entity_name: str,
    refs: set[str],
    select: str,
) -> list[dict]:
    rows: list[dict] = []
    ref_list = sorted(ref for ref in refs if ref and ref != EMPTY)
    for i in range(0, len(ref_list), BATCH):
        batch = ref_list[i:i + BATCH]
        flt = quote(" or ".join(f"Ref_Key eq guid'{ref}'" for ref in batch), safe="")
        url = (
            f"{BASE}/{_entity(entity_name)}"
            f"?$format=json&$filter={flt}&$select={quote(select, safe=',_')}"
        )
        rows.extend(fetch_all_paged(session, url, page=PAGE))
    return rows


def _cache_path_snapshot(month_arg: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"psd_vipusk_plan_{month_arg}.json"


def _cache_path_monthly(year: int, ref_month: int) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"psd_vipusk_plan_monthly_{year}_{ref_month:02d}.json"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _save_json(path: Path, data: dict) -> None:
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def load_movement_headers(session: requests.Session, m_start: date, m_end: date) -> list[dict]:
    period_from = f"{m_start.isoformat()}T00:00:00"
    period_to = f"{_next_month_start(m_end).isoformat()}T00:00:00"
    flt = quote(
        f"Date ge datetime'{period_from}' and "
        f"Date lt datetime'{period_to}' and "
        f"Posted eq true and DeletionMark eq false and "
        f"ХозяйственнаяОперация eq '{OP_TRANSFER_FROM_PRODUCTION}'",
        safe="",
    )
    sel = ",".join([
        "Ref_Key", "Number", "Date", "Организация_Key", "ХозяйственнаяОперация",
        "Отправитель", "Отправитель_Type", "Получатель", "Получатель_Type",
        "Распоряжение", "Распоряжение_Type",
    ])
    url = (
        f"{BASE}/{_entity('Document_ДвижениеПродукцииИМатериалов')}"
        f"?$format=json&$filter={flt}&$select={quote(sel, safe=',_')}"
    )
    return [
        row for row in fetch_all_paged(session, url)
        if row.get("Организация_Key") in {ORG_NPO, ORG_ALMAZ}
    ]


def load_movement_goods(session: requests.Session, refs: set[str]) -> list[dict]:
    sel = ",".join([
        "Ref_Key", "LineNumber", "КодСтроки", "Количество", "Сумма",
        "Номенклатура_Key", "Характеристика_Key",
        "Распоряжение", "Распоряжение_Type",
    ])
    return _fetch_table_by_refs(
        session,
        "Document_ДвижениеПродукцииИМатериалов_Товары",
        refs,
        sel,
    )


def _stage_ref_for_line(line: dict, header: dict) -> str:
    if line.get("Распоряжение_Type") == STAGE_TYPE and line.get("Распоряжение"):
        return line["Распоряжение"]
    if header.get("Распоряжение_Type") == STAGE_TYPE and header.get("Распоряжение"):
        return header["Распоряжение"]
    return ""


def load_stages(session: requests.Session, refs: set[str]) -> dict[str, dict]:
    return _fetch_by_refs(
        session,
        "Document_ЭтапПроизводства2_2",
        refs,
        "Ref_Key,Number,Распоряжение_Key",
    )


def load_production_orders(session: requests.Session, refs: set[str]) -> dict[str, dict]:
    return _fetch_by_refs(
        session,
        "Document_ЗаказНаПроизводство2_2",
        refs,
        "Ref_Key,Number,ДокументОснование,ДокументОснование_Type",
    )


def load_customer_order_lines(session: requests.Session, refs: set[str]) -> list[dict]:
    sel = ",".join([
        "Ref_Key", "LineNumber", "КодСтроки", "Номенклатура_Key",
        "Характеристика_Key", "Количество", "Сумма", "СуммаСНДС", "Отменено",
    ])
    return _fetch_table_by_refs(session, "Document_ЗаказКлиента_Товары", refs, sel)


def _item_key(nomenclature: str | None, characteristic: str | None) -> tuple[str, str]:
    return (nomenclature or EMPTY, characteristic or EMPTY)


def _build_customer_amount_index(lines: list[dict]) -> dict[tuple[str, str, str], dict]:
    index: dict[tuple[str, str, str], dict] = {}
    for row in lines:
        if row.get("Отменено"):
            continue
        qty = float(row.get("Количество") or 0)
        amount = float(row.get("Сумма") or row.get("СуммаСНДС") or 0)
        if qty <= 0 and amount <= 0:
            continue
        key = (
            row.get("Ref_Key") or EMPTY,
            *_item_key(row.get("Номенклатура_Key"), row.get("Характеристика_Key")),
        )
        bucket = index.setdefault(key, {"qty": 0.0, "amount": 0.0, "rows": 0})
        bucket["qty"] += qty
        bucket["amount"] += amount
        bucket["rows"] += 1
    return index


def _npo_header_passes(header: dict) -> bool:
    return (
        header.get("Организация_Key") == ORG_NPO
        and header.get("Отправитель") == OTK1_KEY
        and header.get("Отправитель_Type") == "StandardODATA.Catalog_СтруктураПредприятия"
        and header.get("Получатель") == READY_WAREHOUSE_1_KEY
        and header.get("Получатель_Type") == "StandardODATA.Catalog_Склады"
    )


def _calculate_month_result(month_arg: str) -> dict:
    m_start, m_end = parse_month_arg(month_arg)
    session = make_session()

    headers = load_movement_headers(session, m_start, m_end)
    headers_by_ref = {row["Ref_Key"]: row for row in headers if row.get("Ref_Key")}
    goods = load_movement_goods(session, set(headers_by_ref))

    by_org_qty: dict[str, float] = {guid: 0.0 for guid in TURB_ORGS}
    by_org_rub: dict[str, float] = {guid: 0.0 for guid in TURB_ORGS}
    movement_lines: list[dict] = []
    npo_candidates: list[tuple[dict, dict, float]] = []
    stage_refs: set[str] = set()

    for line in goods:
        header = headers_by_ref.get(line.get("Ref_Key"))
        if not header:
            continue
        qty = float(line.get("Количество") or 0)
        if qty <= 0:
            continue
        org_key = header.get("Организация_Key")

        if org_key == ORG_ALMAZ:
            by_org_qty[ORG_ALMAZ] += qty
            by_org_rub[ORG_ALMAZ] += float(line.get("Сумма") or 0)
            movement_lines.append({
                "mode": "almaz_qty",
                "doc_ref": header.get("Ref_Key"),
                "doc_number": header.get("Number"),
                "doc_date": header.get("Date"),
                "line": line.get("КодСтроки") or line.get("LineNumber"),
                "qty": qty,
                "amount_rub": float(line.get("Сумма") or 0),
                "org_key": org_key,
                "nomenclature_key": line.get("Номенклатура_Key"),
                "characteristic_key": line.get("Характеристика_Key") or EMPTY,
            })
            continue

        if org_key == ORG_NPO and _npo_header_passes(header):
            stage_ref = _stage_ref_for_line(line, header)
            if stage_ref:
                stage_refs.add(stage_ref)
            npo_candidates.append((header, line, qty))

    stages = load_stages(session, stage_refs)
    production_order_refs = {
        stage.get("Распоряжение_Key")
        for stage in stages.values()
        if stage.get("Распоряжение_Key")
    }
    production_orders = load_production_orders(session, production_order_refs)
    customer_order_refs = {
        order.get("ДокументОснование")
        for order in production_orders.values()
        if (
            order.get("ДокументОснование")
            and order.get("ДокументОснование_Type") == CUSTOMER_ORDER_TYPE
        )
    }
    customer_lines = load_customer_order_lines(session, customer_order_refs)
    amount_index = _build_customer_amount_index(customer_lines)

    npo_reject = {
        "no_stage": 0,
        "stage_not_found": 0,
        "no_production_order": 0,
        "production_order_not_found": 0,
        "no_customer_order": 0,
        "customer_line_not_found": 0,
    }

    for header, line, qty in npo_candidates:
        stage_ref = _stage_ref_for_line(line, header)
        if not stage_ref:
            npo_reject["no_stage"] += 1
            continue
        stage = stages.get(stage_ref)
        if not stage:
            npo_reject["stage_not_found"] += 1
            continue
        production_order_ref = stage.get("Распоряжение_Key")
        if not production_order_ref:
            npo_reject["no_production_order"] += 1
            continue
        production_order = production_orders.get(production_order_ref)
        if not production_order:
            npo_reject["production_order_not_found"] += 1
            continue
        customer_order_ref = production_order.get("ДокументОснование")
        if (
            not customer_order_ref
            or production_order.get("ДокументОснование_Type") != CUSTOMER_ORDER_TYPE
        ):
            npo_reject["no_customer_order"] += 1
            continue

        amount_key = (
            customer_order_ref,
            *_item_key(line.get("Номенклатура_Key"), line.get("Характеристика_Key")),
        )
        amount_info = amount_index.get(amount_key)
        if not amount_info:
            npo_reject["customer_line_not_found"] += 1
            continue

        order_qty = float(amount_info.get("qty") or 0)
        order_amount = float(amount_info.get("amount") or 0)
        amount = round(order_amount / order_qty * qty, 2) if order_qty > 0 else round(order_amount, 2)

        by_org_qty[ORG_NPO] += qty
        by_org_rub[ORG_NPO] += amount
        movement_lines.append({
            "mode": "npo_customer_order_amount",
            "doc_ref": header.get("Ref_Key"),
            "doc_number": header.get("Number"),
            "doc_date": header.get("Date"),
            "line": line.get("КодСтроки") or line.get("LineNumber"),
            "qty": qty,
            "amount_rub": amount,
            "org_key": ORG_NPO,
            "nomenclature_key": line.get("Номенклатура_Key"),
            "characteristic_key": line.get("Характеристика_Key") or EMPTY,
            "stage_ref": stage_ref,
            "production_order_ref": production_order_ref,
            "customer_order_ref": customer_order_ref,
            "customer_order_qty": round(order_qty, 3),
            "customer_order_amount": round(order_amount, 2),
        })

    total_qty = sum(by_org_qty.values())
    total_rub = sum(by_org_rub.values())

    return {
        "month": month_arg,
        "period_from": m_start.isoformat(),
        "period_to": m_end.isoformat(),
        "generated": datetime.now().isoformat(timespec="seconds"),
        "source": SOURCE_TAG,
        "algorithm": (
            "Document_ДвижениеПродукцииИМатериалов: АЛМАЗ qty from Товары; "
            "НПО amount via ЭтапПроизводства2_2 -> ЗаказНаПроизводство2_2 -> ЗаказКлиента"
        ),
        "organizations": list(TURB_ORGS.values()),
        "fact_rub_total": round(total_rub, 2),
        "fact_qty_total": round(total_qty, 3),
        "by_org": {
            name: round(by_org_qty.get(guid, 0.0), 3)
            for guid, name in TURB_ORGS.items()
        },
        "by_org_rub": {
            name: round(by_org_rub.get(guid, 0.0), 2)
            for guid, name in TURB_ORGS.items()
        },
        "movement_doc_count": len(headers),
        "movement_line_count": len(goods),
        "lines_count": len(movement_lines),
        "npo_candidate_lines": len(npo_candidates),
        "npo_reject_stats": npo_reject,
        "lines": movement_lines,
    }


def get_psd_vipusk_plan_snapshot(month_arg: str) -> dict:
    cache_path = _cache_path_snapshot(month_arg)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        return cached

    payload = _calculate_month_result(month_arg)
    _save_json(cache_path, payload)
    return payload


def get_psd_vipusk_plan_monthly(year: int, ref_month: int) -> dict:
    cache_path = _cache_path_monthly(year, ref_month)
    cached = _load_json(cache_path)
    if cached is not None and cached.get("source") == SOURCE_TAG:
        return cached

    today = date.today()
    rows_out: list[dict] = []
    for mm in range(1, ref_month + 1):
        if year > today.year or (year == today.year and mm > today.month):
            break
        month_arg = f"{year}-{mm:02d}"
        snap = get_psd_vipusk_plan_snapshot(month_arg)
        rows_out.append({
            "year": year,
            "month": mm,
            "period_from": snap.get("period_from"),
            "period_to": snap.get("period_to"),
            "fact_rub_total": float(snap.get("fact_rub_total") or 0),
            "fact_qty_total": float(snap.get("fact_qty_total") or 0),
            "by_org": snap.get("by_org") or {},
            "by_org_rub": snap.get("by_org_rub") or {},
            "npo_reject_stats": snap.get("npo_reject_stats") or {},
        })

    payload = {
        "year": year,
        "ref_month": ref_month,
        "source": SOURCE_TAG,
        "months": rows_out,
    }
    _save_json(cache_path, payload)
    return payload


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}

    if args:
        month_arg = args[0]
    else:
        today = date.today()
        month_arg = f"{today.year}-{today.month:02d}"

    save_json = "--json" in flags
    m_start, m_end = parse_month_arg(month_arg)

    print("=" * 78)
    print(f"  ПСД · Выпуск · факт выпуска за {month_arg}")
    print(f"  Период:       {m_start} — {m_end}")
    print("  Источник:     Document_ДвижениеПродукцииИМатериалов")
    print("=" * 78)

    t0 = time.time()
    result = _calculate_month_result(month_arg)

    print(f"  Документов движения: {result.get('movement_doc_count')}")
    print(f"  Строк движения:      {result.get('movement_line_count')}")
    print(f"  Строк факта:         {result.get('lines_count')}")
    print(f"  Факт выпуска, руб.:  {result.get('fact_rub_total'):>14,.2f}")
    print(f"  Количество, шт.:     {result.get('fact_qty_total'):>14,.3f}")
    print("\n  По организациям:")
    by_org = result.get("by_org") or {}
    by_org_rub = result.get("by_org_rub") or {}
    for _guid, name in TURB_ORGS.items():
        print(f"    {name:<42s} qty={by_org.get(name, 0):>12,.3f} rub={by_org_rub.get(name, 0):>14,.2f}")
    print(f"\n  NPO reject: {result.get('npo_reject_stats')}")
    print(f"  Время: {time.time()-t0:.1f}с")

    if save_json:
        out_path = os.path.join(os.path.dirname(__file__), f"psd_vipusk_plan_{month_arg}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  → {out_path}")


if __name__ == "__main__":
    main()
