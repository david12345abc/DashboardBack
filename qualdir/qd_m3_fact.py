"""
Факт бюджета QD-M3 (директор по качеству / qualdir).

Источник: регистр ``AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент``
(как у плитки лимита бюджета): движения с типом регистратора «фактическая оплата»,
``Period`` внутри календарного месяца — сумма фактической оплаты за период по заявкам.

Отбор:
  • связь с документом ``ЗаявкаНаРасходованиеДенежныхСредств`` (поле
    ``ЗаявкаНаРасходованиеДенежныхСредств_Key``);
  • у заявки в реквизите ``ТД_ЦФО`` / ``ТД_ЦФО_Key`` — ЦФО **контура директора по качеству**
    (или ``Подразделение`` / ``Подразделение_Key`` совпадает с одним из подразделений
    QD-M4 из ``QD_FOT_SPEC``, когда ЦФО в шапке не заполнен);
  • статья ДДС (``СтатьяДвиженияДенежныхСредств_Key``) — одна из перечисленных
    в ``QD_M3_DDS_ARTICLE_DESCRIPTIONS`` (сравнение по ``normalize_name``).

Сумма по строке: ``СуммаОплаты - СуммаКВыплатеСверхЛимита`` с учётом ``Сторно``,
как в ``getkpi.calc_budget_limit.calc_month`` для факта.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any
from urllib.parse import quote

import requests

from getkpi.calc_budget_limit import (
    DdsCache,
    FACT_RECORDER_TYPES,
    PLAN_RECORDER_TYPE,
    load_records,
    period_bounds,
)
from getkpi.calc_budget_techdir_m3 import REQUEST_DOC_ENTITY_CANDIDATES
from getkpi.fot_techdir_fact import AUTH, BASE, EMPTY, load_structure, normalize_name
from qualdir.qd_m4_fact import QD_FOT_SPEC

logger = logging.getLogger(__name__)

# ЦФО в заявке — контур качества (не «Технический директор» целиком).
QD_M3_CFO_LABELS: tuple[str, ...] = (
    "Директор по качеству",
    "Зам. технического директора по качеству",
    "ЗАМЕСТИТЕЛЬ ДИРЕКТОРА ПО КАЧЕСТВУ",
    "Заместитель директора по качеству",
    "Контур качества (зам по кач.)",
)
QD_M3_CFO_LABEL_NORMS: frozenset[str] = frozenset(
    normalize_name(s) for s in QD_M3_CFO_LABELS
)

# Старое имя константы (удалённые ссылки / устаревший .pyc → NameError).
QD_M3_TD_CFO_LABEL: str = "; ".join(QD_M3_CFO_LABELS)

# Подразделения контура качества — как в QD-M4 (если в заявке пустой ТД_ЦФО, но указано подразделение ОТК и т.д.).
def _qd_quality_department_norms() -> frozenset[str]:
    acc: set[str] = set()
    for title, aliases in QD_FOT_SPEC:
        acc.add(normalize_name(title))
        for a in aliases:
            t = normalize_name(a)
            if t:
                acc.add(t)
    return frozenset(acc)


QD_M3_QUALITY_DEPT_NORMS: frozenset[str] = _qd_quality_department_norms()

# Статьи ДДС (наименования из 1С); дубликаты в ТЗ сведены к уникальному набору.
QD_M3_DDS_ARTICLE_DESCRIPTIONS: tuple[str, ...] = (
    "Услуги сторонних организаций_2_ТС_СК+ПО_4.15.",
    "Выплаты  ГПРПС/Предложения по улучшению_2_ТС_СК+ПО_4.39.",
    "Выплаты ГПРПС/Предложения по улучшению_2_ТС_СК+ПО_4.39.",
    "ТМЦ_2_ТС_ОТК_3.11.",
    "Услуги сторонних организаций_2_ТС_ОТК_3.9.",
    "Инструмент и оборудование_2_ТС_ОТК_3.10.",
)

QD_M3_ALLOWED_DDS_NORMS: frozenset[str] = frozenset(
    normalize_name(s) for s in QD_M3_DDS_ARTICLE_DESCRIPTIONS
)

# Доп. типы регистраторов (варианты метаданных заявки на расход ДС).
QD_M3_EXTRA_FACT_RECORDER_TYPES: frozenset[str] = frozenset(
    {
        "StandardODATA.Document_ЗаявкаНаРасходованиеДС",
        "StandardODATA.Document_ЗаявкаНаРасходованиеДенежныхСредствТД",
        "StandardODATA.Document_ТД_ЗаявкаНаРасходованиеДенежныхСредств",
    }
)

QD_M3_ALL_FACT_TYPES: frozenset[str] = frozenset(FACT_RECORDER_TYPES) | QD_M3_EXTRA_FACT_RECORDER_TYPES

# Фрагменты нормализованных наименований статей ДДС (если в 1С отличается пунктуация).
QD_M3_DDS_ARTICLE_MARKERS: frozenset[str] = frozenset(
    {
        "тс ск по 4 15",
        "тс ск по 4 39",
        "тс отк 3 11",
        "тс отк 3 9",
        "тс отк 3 10",
    }
)


def _resolve_znrds_entity(session: requests.Session) -> str:
    """Имя сущности OData с непустой выборкой (первый подходящий вариант)."""
    for ent in REQUEST_DOC_ENTITY_CANDIDATES:
        url = f"{BASE}/{quote(ent)}?$format=json&$top=1"
        try:
            r = session.get(url, timeout=20)
        except requests.RequestException:
            continue
        if not r.ok:
            continue
        val = r.json().get("value")
        if isinstance(val, list) and len(val) > 0:
            return ent
    return REQUEST_DOC_ENTITY_CANDIDATES[0]


def _fetch_znrds_headers_merged(
    session: requests.Session, ref_keys: list[str]
) -> tuple[dict[str, dict[str, Any]], str]:
    """Читает шапки заявок по всем кандидатам сущности, пока не исчерпаны ключи."""
    out: dict[str, dict[str, Any]] = {}
    used: list[str] = []
    pending = sorted({k.lower() for k in ref_keys if k})
    for ent in REQUEST_DOC_ENTITY_CANDIDATES:
        if not pending:
            break
        chunk = _fetch_znrds_by_refs(session, ent, pending)
        if chunk:
            used.append(ent)
        for k, doc in chunk.items():
            out[k] = doc
        pending = [k for k in pending if k not in out]
    label = ",".join(used) if used else _resolve_znrds_entity(session)
    return out, label


def _structure_row_by_key(structure_by_key: dict[str, Any], k: str) -> dict[str, Any] | None:
    if not k:
        return None
    ks = str(k).strip()
    if ks in structure_by_key:
        return structure_by_key[ks]
    kl = ks.lower()
    for rk, row in structure_by_key.items():
        if str(rk).strip().lower() == kl:
            return row
    return None


def _normalize_presentation(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, dict):
        for key in ("Description", "Name", "Presentation", "Number"):
            v = raw.get(key)
            if v not in (None, ""):
                return normalize_name(str(v))
        return ""
    return normalize_name(str(raw))


def _row_request_ref(r: dict[str, Any]) -> str:
    """Ссылка на заявку: измерение Заявка… или, для движений самой заявки, Recorder."""
    zk = (r.get("ЗаявкаНаРасходованиеДенежныхСредств_Key") or "").strip()
    if zk and str(zk).lower() != EMPTY.lower():
        return str(zk).lower()
    rt = r.get("Recorder_Type") or ""
    rec = (r.get("Recorder") or "").strip()
    if not rec or str(rec).lower() == EMPTY.lower():
        return ""
    if rt == PLAN_RECORDER_TYPE:
        return str(rec).lower()
    if "ЗаявкаНаРасходование" in rt and "Денеж" in rt and "Списание" not in rt and "Платеж" not in rt:
        return str(rec).lower()
    return ""


def _signed_payment_gross_and_over(r: dict[str, Any]) -> tuple[float, float]:
    sign = -1 if r.get("Сторно") else 1
    opl = float(r.get("СуммаОплаты") or 0) * sign
    pre = float(r.get("СуммаПредоплаты") or 0) * sign
    post = float(r.get("СуммаПостоплаты") or 0) * sign
    over = float(r.get("СуммаКВыплатеСверхЛимита") or 0) * sign
    return opl + pre + post, over


def _row_has_payment_in_period(r: dict[str, Any]) -> bool:
    gross, _ = _signed_payment_gross_and_over(r)
    return gross != 0


def _dds_article_matches(dds: DdsCache, art_key: str) -> bool:
    label = normalize_name(dds.label(str(art_key)))
    if not label or label.startswith("<"):
        return False
    if label in QD_M3_ALLOWED_DDS_NORMS:
        return True
    return any(m in label for m in QD_M3_DDS_ARTICLE_MARKERS)


def _fetch_znrds_by_refs(
    session: requests.Session, doc_entity: str, ref_keys: list[str]
) -> dict[str, dict[str, Any]]:
    """Ref_Key (нижний регистр) -> шапка заявки."""
    out: dict[str, dict[str, Any]] = {}
    if not ref_keys:
        return out
    batch_size = 12
    for i in range(0, len(ref_keys), batch_size):
        batch = ref_keys[i : i + batch_size]
        flt = " or ".join(f"Ref_Key eq guid'{k}'" for k in batch)
        sel = "Ref_Key,ТД_ЦФО,ТД_ЦФО_Key,Подразделение,Подразделение_Key"
        url = (
            f"{BASE}/{quote(doc_entity)}?$format=json"
            f"&$filter={quote(flt, safe='')}"
            f"&$select={quote(sel, safe=',_')}"
        )
        try:
            r = session.get(url, timeout=120)
        except requests.RequestException as exc:
            logger.warning("QD-M3 fact: batch заявок %s: %s", doc_entity, exc)
            continue
        if not r.ok:
            logger.warning(
                "QD-M3 fact: HTTP %s при чтении %s: %s",
                r.status_code,
                doc_entity,
                r.text[:300],
            )
            continue
        for row in r.json().get("value") or []:
            rk = (row.get("Ref_Key") or "").strip().lower()
            if rk:
                out[rk] = row
    return out


def _label_matches_qd_m3_cfo(norm: str) -> bool:
    if not norm:
        return False
    for needle in QD_M3_CFO_LABEL_NORMS:
        if norm == needle or needle in norm:
            return True
    return False


def _doc_podrazdelenie_norm(
    doc: dict[str, Any], structure_by_key: dict[str, Any]
) -> str:
    raw = doc.get("Подразделение")
    if raw is not None and str(raw).strip():
        if isinstance(raw, dict):
            return _normalize_presentation(raw)
        return normalize_name(str(raw))
    pk = doc.get("Подразделение_Key")
    if not pk or str(pk).lower() == EMPTY.lower():
        return ""
    row = _structure_row_by_key(structure_by_key, str(pk))
    if not row:
        return ""
    return normalize_name(str(row.get("Description", "") or ""))


def _department_in_qd_quality_contour(desc_norm: str) -> bool:
    if not desc_norm:
        return False
    for needle in QD_M3_QUALITY_DEPT_NORMS:
        if len(needle) < 4:
            continue
        if desc_norm == needle or needle in desc_norm or desc_norm in needle:
            return True
    return False


def _qd_m3_cfo_matches(doc: dict[str, Any], structure_by_key: dict[str, Any]) -> bool:
    raw = doc.get("ТД_ЦФО")
    if raw is not None and str(raw).strip():
        pres = _normalize_presentation(raw)
        if _label_matches_qd_m3_cfo(pres):
            return True
    k = doc.get("ТД_ЦФО_Key")
    if k and str(k).lower() != EMPTY.lower():
        row = _structure_row_by_key(structure_by_key, str(k))
        if row:
            desc_n = normalize_name(row.get("Description", ""))
            if _label_matches_qd_m3_cfo(desc_n):
                return True
    dept = _doc_podrazdelenie_norm(doc, structure_by_key)
    if dept and _department_in_qd_quality_contour(dept):
        return True
    return False


def compute_qd_m3_fact_monthly(
    year: int,
    month: int,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Сумма фактических оплат по правилам QD-M3 за календарный месяц (руб.)."""
    owns_session = session is None
    if owns_session:
        session = requests.Session()
        session.auth = AUTH
    counts = {
        "register_rows": 0,
        "fact_like_rows": 0,
        "fact_rows_with_request": 0,
        "skipped_no_article_match": 0,
        "skipped_no_request_header": 0,
        "skipped_td_cfo": 0,
        "rows_counted": 0,
        "requests_loaded": 0,
        "doc_entity": "",
        "allowed_requests_td": 0,
    }
    try:
        p_start, p_end = period_bounds(year, month)
        rows = load_records(session, p_start, p_end)
        counts["register_rows"] = len(rows)

        dds = DdsCache(session)
        _structure_rows, structure_by_key, _by_parent, _exact_index = load_structure(session)

        art_keys: set[str] = set()
        fact_rows: list[dict[str, Any]] = []
        for r in rows:
            rt = r.get("Recorder_Type") or ""
            if rt in QD_M3_ALL_FACT_TYPES:
                req = _row_request_ref(r)
                if req:
                    fact_rows.append(r)
                continue
            if rt == PLAN_RECORDER_TYPE and _row_has_payment_in_period(r):
                req = _row_request_ref(r)
                if req:
                    fact_rows.append(r)
        counts["fact_like_rows"] = len(fact_rows)

        for r in fact_rows:
            art = r.get("СтатьяДвиженияДенежныхСредств_Key") or EMPTY
            art_keys.add(str(art))
        dds.ensure(art_keys)

        keys_for_docs: set[str] = set()
        for r in fact_rows:
            art = r.get("СтатьяДвиженияДенежныхСредств_Key") or EMPTY
            if not _dds_article_matches(dds, str(art)):
                counts["skipped_no_article_match"] += 1
                continue
            req = _row_request_ref(r)
            if req:
                keys_for_docs.add(req)

        headers, doc_label = _fetch_znrds_headers_merged(session, sorted(keys_for_docs))
        counts["doc_entity"] = doc_label
        counts["requests_loaded"] = len(headers)

        allowed_req: set[str] = set()
        for rk, doc in headers.items():
            if _qd_m3_cfo_matches(doc, structure_by_key):
                allowed_req.add(rk)
        counts["allowed_requests_td"] = len(allowed_req)

        # Примеры строк, отфильтрованных по ДДС / ЦФО (для отладки «факт везде 0»).
        dds_no_match_samples: list[dict[str, str]] = []
        seen_dds: set[str] = set()
        for r in fact_rows:
            art = r.get("СтатьяДвиженияДенежныхСредств_Key") or EMPTY
            aks = str(art)
            if _dds_article_matches(dds, aks):
                continue
            label = normalize_name(dds.label(aks))
            if not label or label in seen_dds or len(dds_no_match_samples) >= 10:
                continue
            seen_dds.add(label)
            dds_no_match_samples.append({"art_key": aks[:40], "dds_label_norm": label[:240]})

        rejected_cfo_samples: list[str] = []
        seen_cfo: set[str] = set()
        for rk, doc in headers.items():
            if rk in allowed_req:
                continue
            raw = doc.get("ТД_ЦФО")
            s = ""
            if raw is not None and str(raw).strip():
                s = _normalize_presentation(raw)
            else:
                ck = doc.get("ТД_ЦФО_Key")
                row = _structure_row_by_key(structure_by_key, str(ck)) if ck else None
                s = normalize_name(row.get("Description", "")) if row else ""
            dept_s = _doc_podrazdelenie_norm(doc, structure_by_key)
            combo = f"td_cfo={s or '-'} | podr={dept_s or '-'}"
            if combo in seen_cfo or len(rejected_cfo_samples) >= 10:
                continue
            if not s and not dept_s:
                continue
            seen_cfo.add(combo)
            rejected_cfo_samples.append(combo[:240])

        rec_type_hist = Counter(
            (r.get("Recorder_Type") or "") for r in fact_rows
        )
        recorder_types_in_fact_rows = dict(rec_type_hist.most_common(20))

        total = 0.0
        for r in fact_rows:
            art = r.get("СтатьяДвиженияДенежныхСредств_Key") or EMPTY
            if not _dds_article_matches(dds, str(art)):
                continue
            zk = _row_request_ref(r)
            if not zk:
                continue
            counts["fact_rows_with_request"] += 1
            if zk not in headers:
                counts["skipped_no_request_header"] += 1
                continue
            if zk not in allowed_req:
                counts["skipped_td_cfo"] += 1
                continue

            gross, over = _signed_payment_gross_and_over(r)
            net = gross - over
            if net == 0:
                continue
            total += net
            counts["rows_counted"] += 1

        return {
            "year": year,
            "month": month,
            "total_fact": round(total, 2),
            "counts": counts,
            "debug": {
                "status": "ok",
                "kpi_id": "QD-M3-FACT",
                "register": "AccumulationRegister_ДвиженияДенежныеСредстваКонтрагент",
                "period_start": p_start[:19],
                "period_end": p_end[:19],
                "cfo_labels": [*QD_M3_CFO_LABELS],
                "td_cfo": QD_M3_TD_CFO_LABEL,
                "dds_articles": list(QD_M3_DDS_ARTICLE_DESCRIPTIONS),
                "recorder_types_in_fact_rows": recorder_types_in_fact_rows,
                "dds_no_match_samples": dds_no_match_samples,
                "rejected_cfo_samples_norm": rejected_cfo_samples,
                "_counts_hints": (
                    "register_rows: строк регистра за месяц; "
                    "fact_like_rows: с привязкой к заявке; "
                    "skipped_no_article_match: статья ДДС не из списка QD-M3; "
                    "skipped_no_request_header: нет шапки заявки в OData; "
                    "skipped_td_cfo: заявка не подошла под ЦФО контура качества; "
                    "rows_counted: учтено в сумме."
                ),
            },
        }
    except Exception as exc:
        logger.exception("QD-M3 fact: ошибка за %d-%02d", year, month)
        return {
            "year": year,
            "month": month,
            "total_fact": None,
            "counts": counts,
            "debug": {
                "status": "error",
                "kpi_id": "QD-M3-FACT",
                "error": str(exc),
            },
        }
