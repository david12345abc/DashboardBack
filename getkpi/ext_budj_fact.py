"""
Факт оплат по заявкам на расходование ДС по доп. реквизиту «Проект».

Источник оплат (верифицировано по метаданным ИБ):
  AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType
  — Period: дата фактического движения ДС (критерий периода по умолчанию)
  — Recorder_Type = StandardODATA.Document_ЗаявкаНаРасходованиеДенежныхСредств
  — Active eq true, Сторно eq false
  — сумма (руб.): СуммаОплатыРегл → СуммаОплаты →
    СуммаКВыплатеВРамкахЛимита + СуммаКВыплатеСверхЛимита

Доп. реквизит «Проект» (способ A):
  Document_ЗаявкаНаРасходованиеДенежныхСредств_ДополнительныеРеквизиты
  — Свойство: ChartOfCharacteristicTypes_ДополнительныеРеквизитыИСведения
    Description = «Проект», ИдентификаторДляФормул = «Проект»
    ValueType = CatalogRef.Проекты
  — Значение_Type = StandardODATA.Catalog_Проекты

Использование:
  python techdir/ext_budj_fact.py --month 2025-03 "ТД_ПолноеНаименование проекта"
  python techdir/ext_budj_fact.py 2025-03-01 2025-03-31 "Проект"
  python techdir/ext_budj_fact.py --csv report.csv --month 2025-03 "Проект"
  python techdir/ext_budj_fact.py --by-doc-date --month 2025-03 "Проект"
  python techdir/ext_budj_fact.py --protocol --month 2025-03 "Проект"

Переменные окружения: ONEC_BASE_URL, ODATA_USER, ODATA_PASSWORD
"""

from __future__ import annotations

import argparse
import csv
import functools
import os
import re
import sys
from calendar import monthrange
from collections import defaultdict
from datetime import date, timedelta
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

sys.stdout.reconfigure(encoding="utf-8")
print = functools.partial(print, flush=True)
log = functools.partial(print, flush=True, file=sys.stderr)

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
PROJECT_ENTITY = "Catalog_Проекты"
DOC_ZAYAVKA = "Document_ЗаявкаНаРасходованиеДенежныхСредств"
EXTRA_ENTITY = "Document_ЗаявкаНаРасходованиеДенежныхСредств_ДополнительныеРеквизиты"
PROP_ENTITY = "ChartOfCharacteristicTypes_ДополнительныеРеквизитыИСведения"
REG_DDS = "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент_RecordType"
RECORDER_TYPE_ZAYAVKA = "StandardODATA.Document_ЗаявкаНаРасходованиеДенежныхСредств"
PROJECT_VALUE_TYPE = "StandardODATA.Catalog_Проекты"

# Верифицировано в ИБ (2026-05): единственное свойство с Description/ИдентификаторДляФормул = «Проект»
PROJECT_PROPERTY_KEY = "c4321807-f64f-11e7-826c-ac1f6b05524d"

EXCLUDED_STATUSES = frozenset(
    {
        "Отклонена",
        "Аннулирована",
        "НеСогласована",
        "Отозвана",
    }
)
ROUND_TOLERANCE = 0.01


def normalize_odata_base(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    if base.endswith("/odata/standard.odata"):
        return base
    return f"{base}/odata/standard.odata"


BASE = normalize_odata_base(os.getenv("ONEC_BASE_URL", DEFAULT_BASE_URL))
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)


def normalize_name(value: str | None) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip()).lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return " ".join(text.split())


def odata_escape(value: str) -> str:
    return (value or "").replace("'", "''")


def fetch_all(
    session: requests.Session,
    url: str,
    page: int = 500,
    timeout: int = 120,
) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        response = session.get(page_url, timeout=timeout)
        if response.status_code == 401:
            raise PermissionError("Доступ запрещён (HTTP 401). Проверьте права OData-пользователя.")
        if not response.ok:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
        batch = response.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows


def money(value: float) -> str:
    return f"{value:,.2f}".replace(",", " ")


def parse_month_arg(value: str) -> tuple[str, str, str]:
    """YYYY-MM → (p_start, p_end, label)."""
    text = (value or "").strip().replace(".", " ").replace("/", " ")
    if "-" in text and " " not in text:
        parts = text.split("-", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            year, month = int(parts[0]), int(parts[1])
            if not 1 <= month <= 12:
                raise ValueError("месяц должен быть от 1 до 12")
            d0 = date(year, month, 1)
            if month == 12:
                p_start = f"{year}-12-01T00:00:00"
                p_end = f"{year + 1}-01-01T00:00:00"
            else:
                p_start = f"{year}-{month:02d}-01T00:00:00"
                p_end = f"{year}-{month + 1:02d}-01T00:00:00"
            d1 = date(year, month, monthrange(year, month)[1])
            return p_start, p_end, f"{d0.isoformat()} — {d1.isoformat()} (месяц {text})"

    parts = text.split()
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        first, second = int(parts[0]), int(parts[1])
        if 1 <= first <= 12 and second > 31:
            month, year = first, second
        elif 1 <= second <= 12:
            year, month = first, second
        else:
            raise ValueError(f"не удалось разобрать месяц: {value}")
        return parse_month_arg(f"{year}-{month:02d}")

    raise ValueError("формат месяца: YYYY-MM или «YYYY M», например 2025-03 или «2025 3»")


def parse_period_args(args: argparse.Namespace) -> tuple[str, str, str, str]:
    """(p_start, p_end, label, criterion)."""
    if args.month:
        p_start, p_end, label = parse_month_arg(args.month)
        criterion = "doc" if args.by_doc_date else "planned" if args.by_planned_date else "payment"
        return p_start, p_end, label, criterion

    if args.date_from and args.date_to:
        d0 = date.fromisoformat(args.date_from)
        d1 = date.fromisoformat(args.date_to)
        if d1 < d0:
            raise ValueError("дата окончания раньше начала")
        p_start = f"{d0.isoformat()}T00:00:00"
        p_end = f"{(d1 + timedelta(days=1)).isoformat()}T00:00:00"
        label = f"{d0.isoformat()} — {d1.isoformat()} (вкл.)"
        criterion = "doc" if args.by_doc_date else "planned" if args.by_planned_date else "payment"
        return p_start, p_end, label, criterion

    raise ValueError("укажите --month YYYY-MM или пару дат YYYY-MM-DD YYYY-MM-DD")


def load_project_by_full_name(session: requests.Session, full_name: str) -> dict:
    escaped = odata_escape(full_name.strip())
    flt = quote(f"DeletionMark eq false and ТД_ПолноеНаименование eq '{escaped}'", safe="")
    select = quote("Ref_Key,Description,Code,ТД_ПолноеНаименование", safe=",_")
    url = f"{BASE}/{quote(PROJECT_ENTITY)}?$format=json&$filter={flt}&$select={select}"
    rows = fetch_all(session, url, page=100, timeout=120)
    if not rows:
        norm = normalize_name(full_name)
        url_all = (
            f"{BASE}/{quote(PROJECT_ENTITY)}"
            f"?$format=json&$select={select}"
            f"&$filter={quote('DeletionMark eq false and IsFolder eq false', safe='')}"
        )
        candidates = [
            row
            for row in fetch_all(session, url_all, page=1000, timeout=120)
            if normalize_name(row.get("ТД_ПолноеНаименование")) == norm
            or normalize_name(row.get("Description")) == norm
        ]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            names = "; ".join(
                (row.get("ТД_ПолноеНаименование") or row.get("Description") or "")[:60]
                for row in candidates[:5]
            )
            raise LookupError(f"неоднозначное совпадение для «{full_name}»: {names}")
        raise LookupError(f"проект не найден: {full_name}")
    if len(rows) > 1:
        raise LookupError(f"несколько проектов с ТД_ПолноеНаименование = «{full_name}»")
    return rows[0]


def load_project_property(session: requests.Session, show_protocol: bool) -> dict:
    """Свойство «Проект» из плана видов характеристик."""
    flt = quote(
        f"DeletionMark eq false and Description eq '{odata_escape('Проект')}'",
        safe="",
    )
    url = (
        f"{BASE}/{quote(PROP_ENTITY)}"
        f"?$format=json&$filter={flt}"
        f"&$select={quote('Ref_Key,Description,ИдентификаторДляФормул,Имя,ValueType', safe=',_')}"
    )
    rows = fetch_all(session, url, page=50, timeout=120)
    exact = [
        r
        for r in rows
        if (r.get("ИдентификаторДляФормул") or "").strip() == "Проект"
        or (r.get("Description") or "").strip() == "Проект"
    ]
    if len(exact) == 1:
        prop = exact[0]
    elif rows:
        prop = rows[0]
    else:
        prop = {
            "Ref_Key": PROJECT_PROPERTY_KEY,
            "Description": "Проект",
            "ИдентификаторДляФормул": "Проект",
        }
        if show_protocol:
            log("  Предупреждение: свойство «Проект» не найдено через OData, используется эталонный Ref_Key")

    if show_protocol:
        vt = (prop.get("ValueType") or {}).get("Types") or []
        log("  Протокол — доп. реквизит «Проект»:")
        log(f"    План видов характеристик: {PROP_ENTITY}")
        log(f"    Ref_Key: {prop.get('Ref_Key')}")
        log(f"    Description: {prop.get('Description')}")
        log(f"    ИдентификаторДляФормул: {prop.get('ИдентификаторДляФормул')}")
        log(f"    ValueType: {vt or ['CatalogRef.Проекты']}")
        log(f"    ТЧ документа: {EXTRA_ENTITY}")
        log("    OData-фильтр по Значение (guid) не поддерживается — отбор по проекту на клиенте")
    return prop


def load_zayavki_by_project(
    session: requests.Session,
    project_key: str,
    property_key: str,
) -> set[str]:
    """Ref_Key заявок с доп. реквизитом Проект = project_key (уровень шапки)."""
    flt = quote(f"Свойство_Key eq guid'{property_key}'", safe="")
    url = (
        f"{BASE}/{quote(EXTRA_ENTITY)}"
        f"?$format=json&$filter={flt}"
        f"&$select={quote('Ref_Key,Значение,Значение_Type', safe=',_')}"
    )
    rows = fetch_all(session, url, page=1000, timeout=180)
    project_lower = project_key.lower()
    refs: set[str] = set()
    for row in rows:
        if PROJECT_VALUE_TYPE not in (row.get("Значение_Type") or ""):
            continue
        if str(row.get("Значение") or "").lower() != project_lower:
            continue
        ref = row.get("Ref_Key")
        if ref:
            refs.add(str(ref).lower())
    return refs


def reg_amount_rub(row: dict) -> float:
    regl = float(row.get("СуммаОплатыРегл") or 0)
    if regl:
        return regl
    so = float(row.get("СуммаОплаты") or 0)
    if so:
        return so
    lim = float(row.get("СуммаКВыплатеВРамкахЛимита") or 0)
    over = float(row.get("СуммаКВыплатеСверхЛимита") or 0)
    if lim or over:
        return lim + over
    return 0.0


def load_movements(
    session: requests.Session,
    p_start: str,
    p_end: str,
    recorder_keys: set[str] | None = None,
) -> list[dict]:
    flt = (
        f"Period ge datetime'{p_start}' and Period lt datetime'{p_end}' "
        f"and Active eq true and Сторно eq false "
        f"and Recorder_Type eq '{RECORDER_TYPE_ZAYAVKA}'"
    )
    sel = quote(
        "Period,Recorder,СуммаОплаты,СуммаОплатыРегл,"
        "СуммаКВыплатеВРамкахЛимита,СуммаКВыплатеСверхЛимита,ВалютаПлатежа_Key",
        safe=",_",
    )
    url = f"{BASE}/{quote(REG_DDS)}?$format=json&$filter={quote(flt, safe='')}&$select={sel}"
    rows = fetch_all(session, url, timeout=180)
    if recorder_keys is None:
        return rows
    return [r for r in rows if str(r.get("Recorder") or "").lower() in recorder_keys]


def load_all_movements_for_zayavki(
    session: requests.Session,
    ref_keys: list[str],
) -> dict[str, float]:
    """Сумма всех оплат по заявке (для статуса полная/частичная)."""
    totals: dict[str, float] = defaultdict(float)
    chunk = 20
    for i in range(0, len(ref_keys), chunk):
        part = ref_keys[i : i + chunk]
        cond = " or ".join(
            f"ЗаявкаНаРасходованиеДенежныхСредств_Key eq guid'{k}'" for k in part
        )
        flt = quote(
            f"Active eq true and Сторно eq false and ({cond})",
            safe="()",
        )
        sel = quote(
            "ЗаявкаНаРасходованиеДенежныхСредств_Key,"
            "СуммаОплаты,СуммаОплатыРегл,СуммаКВыплатеВРамкахЛимита,СуммаКВыплатеСверхЛимита",
            safe=",_",
        )
        url = f"{BASE}/{quote(REG_DDS)}?$format=json&$filter={flt}&$select={sel}"
        for row in fetch_all(session, url, timeout=180):
            rec = str(row.get("ЗаявкаНаРасходованиеДенежныхСредств_Key") or "").lower()
            totals[rec] += reg_amount_rub(row)
    return dict(totals)


def load_zayavka_headers(session: requests.Session, ref_keys: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    chunk = 40
    select = quote(
        "Ref_Key,Number,Date,Posted,DeletionMark,Статус,СуммаДокумента,"
        "Организация_Key,Контрагент_Key,Валюта_Key,ДатаПлатежа,ЖелательнаяДатаПлатежа",
        safe=",_",
    )
    for i in range(0, len(ref_keys), chunk):
        part = ref_keys[i : i + chunk]
        flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in part)
        url = (
            f"{BASE}/{quote(DOC_ZAYAVKA)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={select}"
        )
        for row in fetch_all(session, url, timeout=120):
            key = row.get("Ref_Key")
            if key:
                out[str(key).lower()] = row
    return out


def load_catalog_map(
    session: requests.Session,
    entity: str,
    keys: set[str],
    name_field: str = "Description",
) -> dict[str, str]:
    out: dict[str, str] = {}
    keys = {k for k in keys if k and k != "00000000-0000-0000-0000-000000000000"}
    chunk = 40
    key_list = list(keys)
    for i in range(0, len(key_list), chunk):
        part = key_list[i : i + chunk]
        flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in part)
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(f'Ref_Key,{name_field}', safe=',_')}"
        )
        for row in fetch_all(session, url, timeout=60):
            ref = row.get("Ref_Key")
            if ref:
                out[str(ref).lower()] = (row.get(name_field) or "").strip()
    return out


def payment_status_label(total_paid: float, doc_sum: float) -> str:
    if doc_sum <= 0:
        return "частичная" if total_paid > ROUND_TOLERANCE else "не оплачена"
    if total_paid + ROUND_TOLERANCE >= doc_sum:
        return "полная"
    if total_paid > ROUND_TOLERANCE:
        return "частичная"
    return "не оплачена"


def doc_in_period(row: dict, p_start: str, p_end: str, criterion: str) -> bool:
    if criterion == "doc":
        field = row.get("Date") or ""
    elif criterion == "planned":
        field = row.get("ДатаПлатежа") or row.get("ЖелательнаяДатаПлатежа") or ""
    else:
        return True
    if not field:
        return False
    return p_start <= str(field) < p_end


def month_period_bounds(year: int, month: int) -> tuple[str, str, str]:
    """Границы месяца для OData (p_start, p_end, label)."""
    return parse_month_arg(f"{year}-{month:02d}")


def report_paid_period_total(rows: list[dict]) -> float:
    """Сумма колонки «Оплачено за период» по строкам отчёта."""
    return round(sum(float(row.get("paid_period") or 0) for row in rows), 2)


def compute_fact_total_rub(
    session: requests.Session,
    project_full_name: str,
    year: int,
    month: int,
    *,
    criterion: str = "payment",
    property_row: dict | None = None,
    show_protocol: bool = False,
) -> float:
    """
    Факт оплат по проекту за календарный месяц (руб.), по ТД_ПолноеНаименование / Description.
    Если проект не найден в 1С или нет оплат — 0.0.
    """
    name = (project_full_name or "").strip()
    if not name:
        return 0.0
    try:
        p_start, p_end, _label = month_period_bounds(year, month)
        project = load_project_by_full_name(session, name)
        prop = property_row or load_project_property(session, show_protocol)
        property_key = str(prop["Ref_Key"])
        project_refs = load_zayavki_by_project(session, str(project["Ref_Key"]), property_key)
        if not project_refs:
            return 0.0
        rows = build_report(
            session,
            project,
            project_refs,
            p_start,
            p_end,
            criterion,
            show_protocol,
        )
        return report_paid_period_total(rows)
    except LookupError:
        return 0.0


def compute_fact_totals_for_projects(
    session: requests.Session,
    project_full_names: list[str],
    year: int,
    month: int,
    *,
    criterion: str = "payment",
) -> tuple[float, list[dict[str, Any]]]:
    """
    Сумма факта по списку проектов за месяц.
    Возвращает (итого руб., детализация по проектам).
    """
    prop = load_project_property(session, False)
    total = 0.0
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_name in project_full_names:
        name = (raw_name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        amount = compute_fact_total_rub(
            session,
            name,
            year,
            month,
            criterion=criterion,
            property_row=prop,
        )
        details.append({"project_name": name, "fact": amount})
        total += amount
    return round(total, 2), details


def build_report(
    session: requests.Session,
    project: dict,
    project_refs: set[str],
    p_start: str,
    p_end: str,
    criterion: str,
    show_protocol: bool,
) -> list[dict]:
    if show_protocol:
        log("  Протокол — определение оплаты:")
        log(f"    Регистр: {REG_DDS}")
        log(f"    Recorder_Type: {RECORDER_TYPE_ZAYAVKA}")
        log(f"    Критерий периода: {criterion}")
        log("    Сумма: СуммаОплатыРегл → СуммаОплаты → СуммаКВыплате*")
        log("    Исключаются: не проведённые, помеченные на удаление, статусы " + ", ".join(sorted(EXCLUDED_STATUSES)))

    if criterion == "payment":
        movements = load_movements(session, p_start, p_end, project_refs)
    else:
        movements = load_movements(session, p_start, p_end, None)
        movements = [m for m in movements if str(m.get("Recorder") or "").lower() in project_refs]

    period_by_zayavka: dict[str, float] = defaultdict(float)
    last_pay_date: dict[str, str] = {}
    currency_by_zayavka: dict[str, str] = {}

    for row in movements:
        rec = str(row.get("Recorder") or "").lower()
        if rec not in project_refs:
            continue
        amt = reg_amount_rub(row)
        if amt <= 0:
            continue
        period_by_zayavka[rec] += amt
        per = str(row.get("Period") or "")[:10]
        if per and per > last_pay_date.get(rec, ""):
            last_pay_date[rec] = per
        cur = row.get("ВалютаПлатежа_Key")
        if cur:
            currency_by_zayavka[rec] = str(cur).lower()

    if criterion in ("doc", "planned"):
        candidate_refs = sorted(project_refs)
    else:
        candidate_refs = sorted(period_by_zayavka.keys())

    if not candidate_refs:
        return []

    headers = load_zayavka_headers(session, candidate_refs)

    if criterion in ("doc", "planned"):
        filtered_refs = []
        for ref in candidate_refs:
            hdr = headers.get(ref)
            if hdr and doc_in_period(hdr, p_start, p_end, criterion):
                filtered_refs.append(ref)
        candidate_refs = filtered_refs
        if criterion == "doc":
            movements_all = load_all_movements_for_zayavki(session, candidate_refs)
            period_by_zayavka = {k: movements_all.get(k, 0.0) for k in candidate_refs}
        else:
            period_by_zayavka = {
                k: v for k, v in period_by_zayavka.items() if k in set(candidate_refs)
            }

    paid_refs = [ref for ref in candidate_refs if period_by_zayavka.get(ref, 0) > ROUND_TOLERANCE]
    if not paid_refs:
        return []

    total_paid_all = load_all_movements_for_zayavki(session, paid_refs)

    org_keys = {headers[r].get("Организация_Key", "").lower() for r in paid_refs if headers.get(r)}
    ctr_keys = {headers[r].get("Контрагент_Key", "").lower() for r in paid_refs if headers.get(r)}
    cur_keys = set(currency_by_zayavka.values()) | {
        str(headers[r].get("Валюта_Key") or "").lower() for r in paid_refs if headers.get(r)
    }

    org_names = load_catalog_map(session, "Catalog_Организации", org_keys)
    ctr_names = load_catalog_map(session, "Catalog_Контрагенты", ctr_keys)
    cur_names = load_catalog_map(session, "Catalog_Валюты", cur_keys, "Description")

    report_rows: list[dict] = []
    for ref in paid_refs:
        hdr = headers.get(ref)
        if not hdr:
            continue
        if hdr.get("DeletionMark"):
            continue
        if not hdr.get("Posted"):
            continue
        status = (hdr.get("Статус") or "").strip()
        if status in EXCLUDED_STATUSES:
            continue

        doc_sum = float(hdr.get("СуммаДокумента") or 0)
        paid_period = period_by_zayavka.get(ref, 0.0)
        paid_total = total_paid_all.get(ref, paid_period)
        pay_status = payment_status_label(paid_total, doc_sum)

        org_key = str(hdr.get("Организация_Key") or "").lower()
        ctr_key = str(hdr.get("Контрагент_Key") or "").lower()
        cur_key = currency_by_zayavka.get(ref) or str(hdr.get("Валюта_Key") or "").lower()

        report_rows.append(
            {
                "number": hdr.get("Number") or "",
                "date": str(hdr.get("Date") or "")[:10],
                "ref_key": ref,
                "organization": org_names.get(org_key, org_key or "—"),
                "project": project.get("ТД_ПолноеНаименование") or project.get("Description") or "",
                "pay_status": pay_status,
                "doc_status": status or "—",
                "doc_sum": doc_sum,
                "paid_period": paid_period,
                "currency": cur_names.get(cur_key, cur_key or "—"),
                "pay_date": last_pay_date.get(ref, ""),
                "counterparty": ctr_names.get(ctr_key, ctr_key or "—"),
            }
        )

    report_rows.sort(key=lambda r: (r["pay_date"], r["date"], r["number"]))
    return report_rows


def print_report(
    project: dict,
    period_label: str,
    criterion: str,
    rows: list[dict],
    show_protocol: bool,
) -> None:
    title = project.get("ТД_ПолноеНаименование") or project.get("Description") or ""
    code = project.get("Code") or ""
    criterion_label = {
        "payment": "дата фактической оплаты (Period регистра ДДС)",
        "doc": "дата заявки",
        "planned": "дата планируемой оплаты",
    }.get(criterion, criterion)

    print()
    print(f"Проект: {code}  {title}")
    print(f"Период: {period_label}")
    print(f"Критерий даты: {criterion_label}")
    print(f"Заявок с оплатой: {len(rows)}")

    if not rows:
        print("  Нет оплаченных заявок по проекту за указанный период.")
        return

    print()
    print(
        f"  {'Заявка':<18} {'Дата':<12} {'Организация':<22} {'Статус':<10} "
        f"{'Сумма заявки':>14} {'Оплачено за период':>18} {'Валюта':<6} {'Дата оплаты':<12} {'Контрагент'}"
    )
    print(f"  {'-' * 18} {'-' * 12} {'-' * 22} {'-' * 10} {'-' * 14} {'-' * 18} {'-' * 6} {'-' * 12} {'-' * 20}")

    totals_by_currency: dict[str, float] = defaultdict(float)
    for row in rows:
        print(
            f"  {row['number']:<18} {row['date']:<12} {row['organization'][:22]:<22} "
            f"{row['pay_status']:<10} {money(row['doc_sum']):>14} {money(row['paid_period']):>18} "
            f"{row['currency'][:6]:<6} {row['pay_date']:<12} {row['counterparty'][:40]}"
        )
        totals_by_currency[row["currency"]] += row["paid_period"]

    print()
    print("  Итого оплачено за период:")
    for cur, total in sorted(totals_by_currency.items()):
        print(f"    {cur}: {money(total)}")

    if show_protocol:
        log("")
        log("  Протокол — замечания:")
        log("    Значение доп. реквизита «Проект» читается текущим (история изменений не ведётся через OData).")
        log("    Уровень отбора: шапка заявки (ТЧ ДополнительныеРеквизиты документа).")


def write_csv(path: str, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(
            [
                "Заявка",
                "Дата",
                "Организация",
                "Проект",
                "Статус оплаты",
                "Статус заявки",
                "Сумма заявки",
                "Оплачено за период",
                "Валюта",
                "Дата оплаты",
                "Контрагент",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["number"],
                    row["date"],
                    row["organization"],
                    row["project"],
                    row["pay_status"],
                    row["doc_status"],
                    row["doc_sum"],
                    row["paid_period"],
                    row["currency"],
                    row["pay_date"],
                    row["counterparty"],
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Факт оплат по заявкам на расходование ДС по доп. реквизиту «Проект».",
    )
    parser.add_argument(
        "project",
        help="ТД_ПолноеНаименование или наименование проекта (Catalog_Проекты)",
    )
    parser.add_argument("--month", help="Месяц: YYYY-MM или «YYYY M»")
    parser.add_argument("date_from", nargs="?", help="Дата начала YYYY-MM-DD (альтернатива --month)")
    parser.add_argument("date_to", nargs="?", help="Дата окончания YYYY-MM-DD")
    parser.add_argument("--csv", dest="csv_path", help="Путь к CSV-файлу")
    parser.add_argument(
        "--by-doc-date",
        action="store_true",
        help="Критерий периода: дата заявки (вариант 2 ТЗ)",
    )
    parser.add_argument(
        "--by-planned-date",
        action="store_true",
        help="Критерий периода: дата планируемой оплаты (вариант 3 ТЗ)",
    )
    parser.add_argument(
        "--protocol",
        action="store_true",
        help="Вывести протокол верификации метаданных в stderr",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.by_doc_date and args.by_planned_date:
        log("Ошибка: нельзя одновременно --by-doc-date и --by-planned-date")
        sys.exit(2)

    try:
        p_start, p_end, period_label, criterion = parse_period_args(args)
    except ValueError as exc:
        log(f"Ошибка: {exc}")
        sys.exit(2)

    session = requests.Session()
    session.auth = AUTH

    log(f"Проект: {args.project}")
    log(f"Период: {period_label}")

    try:
        project = load_project_by_full_name(session, args.project)
    except (LookupError, RuntimeError, PermissionError) as exc:
        log(f"Ошибка: {exc}")
        sys.exit(1)

    project_key = str(project["Ref_Key"])
    log(f"  Ref_Key: {project_key}")

    prop = load_project_property(session, args.protocol)
    property_key = str(prop["Ref_Key"])

    log("Загрузка заявок с доп. реквизитом «Проект» …")
    project_refs = load_zayavki_by_project(session, project_key, property_key)
    log(f"  Заявок с проектом (всего): {len(project_refs)}")

    if not project_refs:
        print()
        print(f"Проект: {project.get('Code', '')}  {project.get('ТД_ПолноеНаименование') or project.get('Description')}")
        print(f"Период: {period_label}")
        print("  Нет заявок с заполненным доп. реквизитом «Проект» для этого проекта.")
        sys.exit(0)

    rows = build_report(
        session,
        project,
        project_refs,
        p_start,
        p_end,
        criterion,
        args.protocol,
    )

    if args.csv_path:
        write_csv(args.csv_path, rows)
        log(f"CSV сохранён: {args.csv_path}")

    if not args.csv_path:
        print_report(project, period_label, criterion, rows, args.protocol)

    sys.exit(0)


if __name__ == "__main__":
    main()
