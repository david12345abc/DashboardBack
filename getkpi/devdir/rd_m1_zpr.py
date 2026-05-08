"""KPI RD-M1 (ЗПР) — счётчик документов «ТД_ЗаявкаОПотребностиРынка» за месяц.

Кэш файла: ``getkpi/dashboard/devdir_rd_m1_zpr_<год>_<месяц>.json`` — прошлые
опорные месяцы без срока годности; текущий месяц — действителен до смены календарной даты.

Общее: период по реквизиту ``ДатаПриемаВРазработку``, ``DeletionMark eq false``
(общая OData-выгрузка месяца).

План: все документы из этой выгрузки за месяц (только календарный отбор по дате приёма в разработку).

Факт: из той же выгрузки — только статусы «Закрыта» / «Приостановлена»; исключаем автора
«Робот регламентных заданий» и статусы «Аннулирована» / «Не согласована в работу».

``kpi_pct`` = факт / план × 100 для месяца; в ``ytd`` — накопленные план/факт и суммарный %.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from ..cache_manager import locked_call
from ..fot_techdir_fact import AUTH, BASE, EMPTY
from ..odata_http import request_with_retry
from .rd_monthly_period import MONTH_NAMES, normalize_rd_tile_period
from . import ytd_json_cache

logger = logging.getLogger(__name__)

CACHE_FILE_PREFIX = "devdir_rd_m1_zpr"
CACHE_SOURCE_TAG = "devdir_rd_m1_zpr_ytd"
CACHE_VERSION = 1

DOC_ENTITY_DEFAULT = "Document_ТД_ЗаявкаОПотребностиРынка"

DATE_FIELD = "ДатаПриемаВРазработку"

STATUSES_ALLOWED = frozenset({"закрыта", "приостановлена"})
STATUSES_EXCLUDED = frozenset({
    "аннулирована",
    "не согласована в работу",
})

ROBOT_AUTHOR_PHRASE = "робот регламентных заданий"

_DISCOVERED_ENTITY: str | None = None


def cache_file_path_for_period(year: int | None, month: int | None) -> Path:
    """Путь к файлу кэша (для прогрева `cache_manager.warm_all_caches`)."""
    return ytd_json_cache.public_cache_path(CACHE_FILE_PREFIX, year, month)


def _discover_zpr_entity(session: requests.Session) -> str:
    global _DISCOVERED_ENTITY
    if _DISCOVERED_ENTITY:
        return _DISCOVERED_ENTITY

    try:
        url = f"{BASE}/$metadata"
        r = session.get(url, timeout=30)
        if r.ok and r.text:
            text = r.text
            if f'Name="{DOC_ENTITY_DEFAULT}"' in text:
                _DISCOVERED_ENTITY = DOC_ENTITY_DEFAULT
                return _DISCOVERED_ENTITY
            patterns = [
                r'EntitySet\s+Name="(?P<name>Document_[^"]*Потребност[^"]*Рынк[^"]*)"',
                r'EntitySet\s+Name="(?P<name>Document_[^"]*Заявк[^"]*Потребност[^"]*)"',
            ]
            for pat in patterns:
                m = re.search(pat, text)
                if m and m.group("name"):
                    _DISCOVERED_ENTITY = m.group("name")
                    return _DISCOVERED_ENTITY
    except Exception:
        logger.debug("RD-M1: metadata discovery failed", exc_info=True)

    candidates = (
        DOC_ENTITY_DEFAULT,
        "Document_ЗаявкаОПотребностиРынкаТД",
        "Document_ТД_ЗаявкаОПотребностиРынкаТД",
    )
    for name in candidates:
        probe = f"{BASE}/{quote(name)}?$top=1&$format=json"
        pr = request_with_retry(session, probe, timeout=20, retries=2, label="RD-M1/probe")
        if pr is not None and pr.ok:
            _DISCOVERED_ENTITY = name
            return _DISCOVERED_ENTITY

    _DISCOVERED_ENTITY = DOC_ENTITY_DEFAULT
    return _DISCOVERED_ENTITY


def _month_bounds_odata(year: int, month: int) -> tuple[str, str]:
    if month == 12:
        return f"{year}-12-01T00:00:00", f"{year + 1}-01-01T00:00:00"
    return f"{year}-{month:02d}-01T00:00:00", f"{year}-{month + 1:02d}-01T00:00:00"


def _load_users_description_map(session: requests.Session) -> dict[str, str]:
    """Ref_Key пользователя → Description (для фильтра по автору)."""
    raw: dict[str, dict] = {}
    skip = 0
    page = 5000
    while True:
        url = (
            f"{BASE}/{quote('Catalog_Пользователи')}?$format=json"
            f"&$top={page}&$skip={skip}"
            f"&$select={quote('Ref_Key,Description', safe=',_')}"
            f"&$orderby=Ref_Key"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label="RD-M1/users")
        if r is None or not r.ok:
            if r is not None:
                logger.error("RD-M1 Catalog_Пользователи HTTP %d: %s", r.status_code, r.text[:200])
            break
        rows = r.json().get("value", [])
        if not rows:
            break
        for item in rows:
            raw[item["Ref_Key"]] = item
        if len(rows) < page:
            break
        skip += len(rows)
    return {k: (v.get("Description") or "").strip() for k, v in raw.items()}


def _norm_text(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, dict):
        inner = (
            val.get("Description")
            or val.get("Presentation")
            or val.get("presentation")
            or ""
        )
        return str(inner).strip().lower()
    return str(val).strip().lower()


def _author_key_from_row(row: dict) -> str:
    ak = row.get("Автор_Key")
    if isinstance(ak, str) and ak.strip() and ak.strip() != EMPTY:
        return ak.strip()
    auth = row.get("Автор")
    if isinstance(auth, dict):
        k = auth.get("Ref_Key")
        if isinstance(k, str) and k.strip():
            return k.strip()
    return ""


def _author_description_for_filter(row: dict, user_desc_by_key: dict[str, str]) -> str:
    """Текст автора для сравнения с «Робот регламентных заданий»."""
    ak = _author_key_from_row(row)
    if ak:
        return (user_desc_by_key.get(ak) or "").strip().lower()
    auth = row.get("Автор")
    if isinstance(auth, str):
        return auth.strip().lower()
    if isinstance(auth, dict):
        return _norm_text(auth)
    return ""


def _doc_matches_zpr_fact(
    row: dict,
    user_desc_by_key: dict[str, str],
) -> bool:
    st = _norm_text(row.get("Статус"))
    if st in STATUSES_EXCLUDED:
        return False
    if st not in STATUSES_ALLOWED:
        return False

    desc = _author_description_for_filter(row, user_desc_by_key)
    if desc == ROBOT_AUTHOR_PHRASE:
        return False
    return True


def _fetch_zpr_docs_for_month(
    session: requests.Session,
    entity: str,
    year: int,
    month: int,
) -> list[dict]:
    p_start, p_end = _month_bounds_odata(year, month)
    flt = (
        f"DeletionMark eq false "
        f"and {DATE_FIELD} ge datetime'{p_start}' "
        f"and {DATE_FIELD} lt datetime'{p_end}'"
    )
    skip = 0
    page = 5000
    docs: list[dict] = []
    while True:
        url = (
            f"{BASE}/{quote(entity)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$top={page}&$skip={skip}"
        )
        r = request_with_retry(session, url, timeout=120, retries=4, label=f"RD-M1/{year}-{month}")
        if r is None or not r.ok:
            if r is not None:
                logger.error(
                    "RD-M1 %s HTTP %d: %s", entity, r.status_code, r.text[:400],
                )
            break
        batch = r.json().get("value", [])
        if not batch:
            break
        docs.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return docs


def zpr_plan_fact_counts_for_month(
    session: requests.Session,
    entity: str,
    year: int,
    month: int,
    user_map: dict[str, str],
) -> tuple[int, int, dict[str, Any]]:
    """
    Возвращает (план, факт, meta): одна загрузка документов OData за месяц.
    """
    raw = _fetch_zpr_docs_for_month(session, entity, year, month)
    plan_n = len(raw)
    fact_n = sum(1 for row in raw if _doc_matches_zpr_fact(row, user_map))
    return plan_n, fact_n, {
        "entity": entity,
        "raw_docs_loaded": len(raw),
        "month_filter": DATE_FIELD,
        "period": _month_bounds_odata(year, month),
    }


def _build_rd_m1_zpr_monthly_payload(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    pairs = [(ref_y, mm) for mm in range(1, ref_m + 1)]

    session = requests.Session()
    session.auth = AUTH
    entity = _discover_zpr_entity(session)
    user_map = _load_users_description_map(session)

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None
    last_meta: dict[str, Any] = {}

    for y, m in pairs:
        plan_n, fact_n, meta = zpr_plan_fact_counts_for_month(
            session, entity, y, m, user_map,
        )
        last_meta = meta
        pct = (
            round(fact_n / plan_n * 100, 1)
            if plan_n
            else None
        )
        row = {
            "month": m,
            "year": y,
            "month_name": MONTH_NAMES[m],
            "plan": float(plan_n),
            "fact": float(fact_n),
            "kpi_pct": pct,
            "has_data": True,
            "values_unit": "шт.",
        }
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

    total_plan = sum(float(r.get("plan") or 0) for r in monthly_rows)
    total_fact = sum(float(r.get("fact") or 0) for r in monthly_rows)
    ytd_pct = (
        round(total_fact / total_plan * 100, 1)
        if total_plan > 0
        else None
    )

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row) if ref_row and ref_row.get("has_data") else None,
        "kpi_period": {
            "type": "last_full_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": {
            "total_plan": float(total_plan),
            "total_fact": float(total_fact),
            "kpi_pct": ref_row.get("kpi_pct") if ref_row else ytd_pct,
            "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
            "months_total": len(monthly_rows),
            "values_unit": "шт.",
        },
        "debug": {
            "kpi_route": "devdir_rd_m1_zpr",
            "odata_entity": last_meta.get("entity"),
            "date_field": DATE_FIELD,
            "plan_rules": (
                "все документы за месяц по ДатаПриемаВРазработку, "
                "DeletionMark eq false (без фильтра по статусу/автору)"
            ),
            "allowed_statuses": sorted(STATUSES_ALLOWED),
            "excluded_statuses": sorted(STATUSES_EXCLUDED),
            "excluded_author_phrase": ROBOT_AUTHOR_PHRASE,
            "last_month_meta": last_meta,
            "ytd_kpi_pct_cumulative": ytd_pct,
        },
    }


def get_rd_m1_zpr_ytd(year: int | None = None, month: int | None = None) -> dict | None:
    ref_y, ref_m = normalize_rd_tile_period(year, month)
    c_path = ytd_json_cache.cache_path(CACHE_FILE_PREFIX, ref_y, ref_m)
    perpetual = ytd_json_cache.is_ref_period_fully_past(ref_y, ref_m)

    def _runner() -> dict | None:
        cached = ytd_json_cache.load_payload(
            c_path,
            source_tag=CACHE_SOURCE_TAG,
            version=CACHE_VERSION,
            perpetual=perpetual,
        )
        if cached is not None:
            return cached
        try:
            payload = _build_rd_m1_zpr_monthly_payload(year=year, month=month)
        except Exception:
            logger.exception("Ошибка при расчёте RD-M1 (ЗПР)")
            return None
        if payload is not None:
            ytd_json_cache.save_payload(
                c_path,
                payload,
                source_tag=CACHE_SOURCE_TAG,
                version=CACHE_VERSION,
            )
        return payload

    return locked_call(f"devdir_rd_m1_zpr_{ref_y}_{ref_m}", _runner)
