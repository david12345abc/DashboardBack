"""Построчная расшифровка денег, отгрузок и договоров за месяц.

Строки собираются теми же фильтрами, что и цифры плиток KD-M1/KD-M2/KD-M3.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from comdir.plan_fact_sink import lines as lines_var
from comdir.plan_fact_sink import only_month as month_var

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent.parent / "getkpi" / "dashboard"
METRICS = (
    ("money", "KD-T-MONEY", "Деньги"),
    ("ship", "KD-T-SHIP", "Отгрузки"),
    ("dog", "KD-T-DOG", "Договоры"),
)


def cache_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"comdir_plan_fact_lines_{year}_{month:02d}.json"


def _odata_names(entity: str, keys: set[str], field: str = "Description") -> dict[str, str]:
    from urllib.parse import quote

    import requests
    from requests.auth import HTTPBasicAuth

    base = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
    session = requests.Session()
    session.auth = HTTPBasicAuth("odata.user", "npo852456")
    out: dict[str, str] = {}
    clean = sorted({k for k in keys if k and k != "00000000-0000-0000-0000-000000000000"})
    select = quote(f"Ref_Key,{field}", safe=",_")
    for i in range(0, len(clean), 15):
        part = clean[i:i + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in part), safe="")
        url = (
            f"{base}/{entity}?$format=json&$filter={flt}"
            f"&$select={select}&$top={len(part)}"
        )
        try:
            response = session.get(url, timeout=60)
            response.raise_for_status()
            for item in response.json().get("value") or []:
                ref = item.get("Ref_Key") or ""
                text = (item.get(field) or "").strip()
                if ref and text:
                    out[ref] = text
        except Exception:
            logger.exception("Не удалось прочитать %s для расшифровки", entity)
    return out


def _dept_name(value: str) -> str:
    from getkpi.calc_plan import DEPARTMENTS
    from comdir.common import DEPT_NAME_TO_ODATA

    text = (value or "").strip()
    if text in DEPARTMENTS:
        return DEPARTMENTS[text]
    for name, guid in DEPT_NAME_TO_ODATA.items():
        if guid == text:
            return name
    return text


def _display_date(value: str) -> str:
    text = (value or "")[:10]
    if len(text) == 10 and text[4] == "-" and text[:4] not in ("0001", "2001"):
        return f"{text[8:10]}.{text[5:7]}.{text[:4]}"
    return ""


def _resolve(rows: list[dict]) -> list[dict]:
    partners = _odata_names("Catalog_Партнеры", {row.get("partner_key") or "" for row in rows})
    orders = _odata_names(
        "Document_ЗаказКлиента",
        {row.get("order_key") or "" for row in rows},
        field="Number",
    )
    ready = []
    for row in rows:
        department = _dept_name(row.get("department") or "")
        partner = (row.get("partner") or "").strip() or partners.get(row.get("partner_key") or "", "")
        document = (row.get("document") or "").strip()
        number = orders.get(row.get("order_key") or "", "")
        if number and document in ("Платёж", "Взаимозачёт", "Комиссия"):
            document = f"{document} · заказ {number}"
        ready.append({
            "date": _display_date(row.get("date") or ""),
            "department": department,
            "partner": partner,
            "document": document or "—",
            "kind": row.get("kind") or "",
            "amount": round(float(row.get("amount") or 0), 2),
            "metric": row.get("metric") or "",
            "name": document or "—",
        })
    ready.sort(key=lambda item: (
        item["kind"] != "Факт",
        item["date"][6:10] + item["date"][3:5] + item["date"][:2] if len(item["date"]) == 10 else "9999",
        item["department"],
        item["partner"],
        item["document"],
    ))
    return ready


def collect(year: int, month: int) -> list[dict]:
    buf: list[dict] = []
    token_lines = lines_var.set(buf)
    token_month = month_var.set(month)
    try:
        from getkpi.calc_dengi_fact import collect_fact_lines
        from getkpi.calc_plan import get_dengi_expected_by_month, get_otgruzki_expected_by_month
        import comdir.calc_plan_fact_dogovory as dog
        import comdir.calc_plan_fact_otgruzki as otg

        for label, fn in (
            ("деньги факт", lambda: collect_fact_lines(year, month)),
            ("деньги ожидаемо", lambda: get_dengi_expected_by_month(year, month)),
            ("отгрузки ожидаемо", lambda: get_otgruzki_expected_by_month(year, month)),
        ):
            try:
                fn()
            except Exception:
                logger.exception("Расшифровка: %s", label)
        start = date(year, month, 1)
        nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        p0 = otg.to_1c_dt(start)
        p_next = otg.to_1c_dt(nxt)
        connection = otg.connect()
        try:
            cursor = connection.cursor()
            for label, fn in (
                ("отгрузки факт", lambda: otg.calc_fact(cursor, p0, p_next)),
                ("договоры факт", lambda: dog.calc_fact(cursor, p0, p_next)),
                ("договоры ожидаемо", lambda: dog.calc_expected(cursor, p0, p_next, p_asof=p_next)),
            ):
                try:
                    fn()
                except Exception:
                    logger.exception("Расшифровка: %s", label)
        finally:
            connection.close()
    finally:
        lines_var.reset(token_lines)
        month_var.reset(token_month)
    return _resolve(buf)


def save(year: int, month: int, rows: list[dict]) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = cache_path(year, month)
    path.write_text(
        json.dumps({"year": year, "month": month, "rows": rows}, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def load(year: int, month: int) -> list[dict]:
    path = cache_path(year, month)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = data.get("rows") if isinstance(data, dict) else None
    return rows if isinstance(rows, list) else []


def tables_for_month(year: int, month: int, dept_name: str | None = None) -> dict:
    month_name = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
    }.get(month, "")
    rows = load(year, month)
    if dept_name:
        wanted = dept_name.strip().lower()
        rows = [row for row in rows if str(row.get("department") or "").strip().lower() == wanted]
    tables = {}
    for metric, key, title in METRICS:
        picked = [row for row in rows if row.get("metric") == metric]
        fact = round(sum(float(row.get("amount") or 0) for row in picked if row.get("kind") == "Факт"), 2)
        expected = round(sum(float(row.get("amount") or 0) for row in picked if row.get("kind") == "Ожидаемо"), 2)
        tables[key] = {
            "name": f"{title} за {month_name} {year}",
            "periodicity": "ежемесячно",
            "description": (
                f"Расшифровка плитки: факт {fact}, ожидаемо {expected}, "
                f"прогноз {round(fact + expected, 2)}"
            ),
            "columns": ["Дата", "Отдел", "Партнер", "Документ", "Вид", "Сумма, руб."],
            "rows": picked,
            "fact_total": fact,
            "expected_total": expected,
        }
    return tables
