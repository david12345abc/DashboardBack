# -*- coding: utf-8 -*-
"""ТКП в SLA по правилам отчёта 1С «Отработка ОЛ за период».

Период ОЛ — дата версии 1 (дата начала жизни в ERP), не дата документа.
План — все ОЛ текущего периода: отработанные и ещё открытые.
Факт — отработанные в срок: обычный ОЛ до 3 рабочих дней,
спецзаказ до 5 рабочих дней включительно.
Регистр версий в выгрузке SQL пуст, даты версий читаются из 1С по OData.
"""
from __future__ import annotations

import os
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from comdir.common import connect, uuid_to_1c_bytes

YEAR_OFFSET = 2000
DAYS_IN_CALENDAR_YEAR = 247
CALENDAR = bytes.fromhex("812d001e6711250911e76313d658bace")
EMPTY = "00000000-0000-0000-0000-000000000000"
EMPTY_DATE = "0001-01-01T00:00:00"
TKP = "314a18fa-de55-11e8-8283-ac1f6b05524d"
ANN = "284f6e13-01de-11e9-8286-ac1f6b05524d"
DOC_TYPES = (
    "Document_ТД_КартаЗаказаUFG",
    "Document_ТД_КартаЗаказаCFM",
    "Document_ТД_КартаЗаказаUFGH",
    "Document_ТД_КартаЗаказаTFG",
    "Document_ТД_КартаЗаказаUFL",
    "Document_ТД_КартаЗаказаПлотномер",
)
BASE = os.getenv("ONEC_BASE_URL", "http://192.168.2.229:81/erp_pm").rstrip("/")
if not BASE.endswith("/odata/standard.odata"):
    BASE = f"{BASE}/odata/standard.odata"
AUTH = HTTPBasicAuth(
    os.getenv("ODATA_USER", "odata.user"),
    os.getenv("ODATA_PASSWORD", "npo852456"),
)


def _month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def _fetch(session: requests.Session, url: str, timeout: int = 120) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        response = session.get(f"{url}{sep}$top=1000&$skip={skip}&$format=json", timeout=timeout)
        response.raise_for_status()
        batch = response.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 1000:
            return rows
        skip += len(batch)


def _load_documents(session: requests.Session, start: date, end: date) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    flt = (
        f"Date ge datetime'{start.isoformat()}T00:00:00'"
        f" and Date le datetime'{end.isoformat()}T23:59:59'"
    )
    select = "Ref_Key,Date,Статус,Спецзаказ,Ответственный_Key"
    for doc_type in DOC_TYPES:
        url = (
            f"{BASE}/{quote(doc_type)}"
            f"?$filter={quote(flt, safe='')}"
            f"&$select={quote(select, safe=',_')}"
        )
        for row in _fetch(session, url):
            row["_type"] = doc_type
            docs[row["Ref_Key"]] = row
    return docs


def _load_facts(session: requests.Session, point: str, start: date) -> tuple[dict[str, set[str]], dict[str, str]]:
    flt = (
        f"ТочкаЭтапа_Key eq guid'{point}'"
        f" and ДатаЗавершенияФакт ge datetime'{start.isoformat()}T00:00:00'"
        f" and ДатаЗавершенияФакт ne datetime'{EMPTY_DATE}'"
        f" and Active eq true"
    )
    url = (
        f"{BASE}/AccumulationRegister_ТД_МониторингЭтаповОпросныхЛистов_RecordType"
        f"?$filter={quote(flt, safe='')}"
        f"&$select={quote('Recorder,Recorder_Type,ДатаЗавершенияФакт', safe=',')}"
    )
    facts: dict[str, set[str]] = {}
    types: dict[str, str] = {}
    for row in _fetch(session, url):
        ref = row.get("Recorder")
        if not ref:
            continue
        facts.setdefault(ref, set()).add(str(row.get("ДатаЗавершенияФакт") or "")[:10])
        doc_type = str(row.get("Recorder_Type") or "").split(".")[-1]
        if doc_type in DOC_TYPES:
            types[ref] = doc_type
    return facts, types


def _load_docs_by_ref(session: requests.Session, pairs: list[tuple[str, str]]) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    by_type: dict[str, list[str]] = {}
    for ref, doc_type in pairs:
        if doc_type in DOC_TYPES:
            by_type.setdefault(doc_type, []).append(ref)
    select = "Ref_Key,Date,Статус,Спецзаказ,Ответственный_Key"
    for doc_type, refs in by_type.items():
        for offset in range(0, len(refs), 20):
            chunk = refs[offset:offset + 20]
            flt = " or ".join(f"Ref_Key eq guid'{ref}'" for ref in chunk)
            url = (
                f"{BASE}/{quote(doc_type)}"
                f"?$filter={quote(flt, safe='')}"
                f"&$select={quote(select, safe=',_')}"
            )
            for row in _fetch(session, url, timeout=60):
                row["_type"] = doc_type
                docs[row["Ref_Key"]] = row
    return docs


def _version_date(item: tuple[str, str]) -> tuple[str, str | None]:
    ref, doc_type = item
    url = (
        f"{BASE}/InformationRegister_ВерсииОбъектов("
        f"Объект=guid'{ref}',Объект_Type='StandardODATA.{doc_type}',НомерВерсии=1)"
        f"?$format=json&$select=ДатаВерсии"
    )
    last_error: Exception | None = None
    for _ in range(2):
        try:
            response = requests.get(url, auth=AUTH, timeout=30)
        except requests.RequestException as exc:
            last_error = exc
            continue
        if response.status_code == 404:
            return ref, None
        if not response.ok:
            last_error = RuntimeError(response.text[:200])
            continue
        value = response.json().get("ДатаВерсии")
        return ref, (str(value)[:10] if value else None)
    if last_error is not None:
        return ref, None
    return ref, None


def _load_versions(pairs: list[tuple[str, str]]) -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    if not pairs:
        return versions
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_version_date, item) for item in pairs]
        for future in as_completed(futures):
            ref, version = future.result()
            versions[ref] = version
    return versions


def _calendar(year: int) -> dict[date, int]:
    cn = connect()
    try:
        cur = cn.cursor()
        cur.execute(
            """
            SELECT _Fld45243, _Fld45245
            FROM _InfoRg45240 WITH (NOLOCK)
            WHERE _Fld45241RRef = ?
              AND _Fld45243 >= ? AND _Fld45243 < ?
            """,
            CALENDAR,
            datetime(year + YEAR_OFFSET, 1, 1),
            datetime(year + YEAR_OFFSET + 2, 1, 1),
        )
        out: dict[date, int] = {}
        for moment, days in cur.fetchall():
            out[date(moment.year - YEAR_OFFSET, moment.month, moment.day)] = int(days)
        return out
    finally:
        cn.close()


def _business_days(calendar: dict[date, int], start: date, end: date) -> int:
    if start >= end:
        return 0

    def value(day: date) -> int:
        if day in calendar:
            return calendar[day]
        earlier = [key for key in calendar if key <= day]
        return calendar[max(earlier)] if earlier else 0

    return value(end) - value(start) + DAYS_IN_CALENDAR_YEAR * (end.year - start.year)


def _odata_uuid(raw: bytes) -> str:
    hx = raw.hex()
    body = hx[24:32] + hx[20:24] + hx[16:20] + hx[0:4] + hx[4:16]
    return f"{body[0:8]}-{body[8:12]}-{body[12:16]}-{body[16:20]}-{body[20:32]}"


def _user_departments(user_ids: set[str]) -> dict[str, str]:
    ids = [item for item in user_ids if item and item != EMPTY]
    if not ids:
        return {}
    cn = connect()
    out: dict[str, str] = {}
    try:
        cur = cn.cursor()
        for offset in range(0, len(ids), 40):
            chunk = ids[offset:offset + 40]
            marks = ",".join("?" for _ in chunk)
            cur.execute(
                f"""
                SELECT _IDRRef, _Fld10996RRef
                FROM _Reference366 WITH (NOLOCK)
                WHERE _IDRRef IN ({marks})
                """,
                *[uuid_to_1c_bytes(item) for item in chunk],
            )
            for user_ref, dept_ref in cur.fetchall():
                if not dept_ref or dept_ref == bytes(16):
                    continue
                out[_odata_uuid(user_ref)] = _odata_uuid(dept_ref)
        return out
    finally:
        cn.close()


def compute_tkp_sla_months(year: int, through_month: int) -> list[dict]:
    """Январь..through_month: план/факт ТКП в SLA и разрез по отделам."""
    start = date(year, 1, 1)
    end = _month_end(year, through_month)
    session = requests.Session()
    session.auth = AUTH
    documents = _load_documents(session, start, end)
    tkp_facts, tkp_types = _load_facts(session, TKP, start)
    ann_facts, ann_types = _load_facts(session, ANN, start)
    missing = [
        (ref, doc_type)
        for ref, doc_type in {**ann_types, **tkp_types}.items()
        if ref not in documents
    ]
    documents.update(_load_docs_by_ref(session, missing))
    versions = _load_versions([(ref, doc["_type"]) for ref, doc in documents.items()])
    calendar = _calendar(year)
    departments = _user_departments(
        {str(doc.get("Ответственный_Key") or "") for doc in documents.values()}
    )
    months: list[dict] = []
    for month in range(1, through_month + 1):
        month_start = date(year, month, 1).isoformat()
        month_end = _month_end(year, month).isoformat()
        plan_map: dict[str, float] = {}
        fact_map: dict[str, float] = {}
        for ref, doc in documents.items():
            version = versions.get(ref)
            if not version or not (month_start <= version <= month_end):
                continue
            status = doc.get("Статус") or ""
            facts = ann_facts.get(ref, set()) if status == "Аннулирован" else tkp_facts.get(ref, set())
            dept = departments.get(str(doc.get("Ответственный_Key") or ""), "")
            plan_map[dept] = plan_map.get(dept, 0) + 1
            if not facts:
                continue
            started = date.fromisoformat(version)
            life = min(_business_days(calendar, started, date.fromisoformat(fact)) for fact in facts)
            special = bool(doc.get("Спецзаказ"))
            if life <= 3 or (special and life <= 5):
                fact_map[dept] = fact_map.get(dept, 0) + 1
        guids = {key for key in plan_map if key}
        by_dept = {
            guid: {"plan": plan_map.get(guid, 0), "fact": fact_map.get(guid, 0)}
            for guid in guids
        }
        plan_total = sum(plan_map.values())
        fact_total = sum(fact_map.values())
        months.append(
            {
                "year": year,
                "month": month,
                "fact": fact_total,
                "plan": plan_total,
                "pct": round(fact_total / plan_total * 100, 1) if plan_total else None,
                "by_dept": by_dept,
            }
        )
    return months
