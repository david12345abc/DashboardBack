"""
TD-M6: план/факт ФОТ по проектам внешних заказов техдира.

План/факт по текущему срезу внешних заказов зашит в модуль. Для новых проектов, которых ещё нет
во встроенной таблице, сохраняем прежний резервный алгоритм: находим проект в 1С и берём помесячные
суммы из **самого старого** БДДС (`Document_ТД_БДДС`, ТЧ «Ресурсы», строки с показателем «затраты»).

OData:
  <base_url>/odata/standard.odata/Document_ТД_БДДС
Переменные окружения: ONEC_BASE_URL, ODATA_USER, ODATA_PASSWORD.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from collections import defaultdict
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from . import cache_manager
from . import techdir_projects as tp

logger = logging.getLogger(__name__)

MONTH_NAMES = tp.MONTH_NAMES

# --- OData / БДДС (ранее calc_bdds_project_costs.py) ---------------------------------

DEFAULT_BASE_URL = "http://192.168.2.229:81/erp_pm"
DOC_ENTITY = "Document_ТД_БДДС"
PROJECT_ENTITY = "Catalog_Проекты"
FOT_REFERENCE_MONTHLY: dict[str, dict[str, float]] = {
    "2026-01": {"plan": 12089147.30, "fact": 15111854.70},
    "2026-02": {"plan": 1510775.70, "fact": 6045567.83},
    "2026-03": {"plan": 4135495.49, "fact": 128525495.48},
    "2026-04": {"plan": 64339272.32, "fact": 19738752.20},
    "2026-05": {"plan": 8735300.91, "fact": 671810.27},
    "2026-06": {"plan": 54173710.59, "fact": 0.0},
    "2026-07": {"plan": 1354518.44, "fact": 0.0},
    "2026-08": {"plan": 1062479.41, "fact": 0.0},
    "2026-09": {"plan": 9567093.39, "fact": 0.0},
    "2026-10": {"plan": 19519167.77, "fact": 0.0},
}
FOT_REFERENCE_PROJECT_LABELS = [
    "Выполнение пусконаладочных работ газоизмерительной системы СПУ ПГ-018 (ЦФО БМИ) 30.12.2025",
    "Выполнение условий договора №МПГ00007318 от 15.01.2024 на поставку ПУРГС-500-01 (ЦФО БМИ) 29.05.2026",
    "Выполнение условий договора №МПГ00007820 от 18.03.2024 (ЦФО БМИ) 31.10.2026",
    "Выполнение условий договора №МПГ00009991 от 16.01.2025 (Западная прорва) (ЦФО БМИ) 31.07.2026",
    "Выполнение условий договора №МПГ00010328 от 20.02.2025 ДНС-1 Северо-Хохряковского мр (ЦФО БМИ) 30.06.2026",
    "Выполнение условий договора №МПГ00010434 от 06.03.2025 для м/р Западный Тузколь (ЦФО БМИ) 30.06.2026",
    "Выполнение условий договора №МПГ00012110 от 25.11.2025г. (ФХП Вуктыл) (ЦФО БМИ) 01.10.2026",
    "Выполнение условий договора МПГ00008851 от 01.08.2024 на поставку СИКГ (ЦФО БМИ) 31.12.2026",
    "Выполнение условий договора поставки МПГ00008562 от 24.06.2024 на поставку УИРГ для Тас-Юряхского месторождения",
    "Выполнение условия договора на поставку блоков резервирования импульсного газа для нужд ООО «Газпром трансгаз Санкт-Петербург» (ЦФО БМИ) 30.08.2026",
    "Изготовление и поставка ПУРГ согласно условий договора для АО Уралэлектромедь (ЦФО БМИ) 01.07.2026",
    "Разработка типовых тех проектов на оборудование \"ГИС однониточная и коллекторная для применения на объектах ПАО \"Газпром\" (ЦФО БМИ) 31.07.2026",
]


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


class ODataRequestError(RuntimeError):
    """Ошибка ответа OData 1С. Не завершать процесс через ``sys.exit`` при вызове из Django/внутри API."""

    def __init__(self, message: str, *, status_code: int | None = None, url: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.url = url


def normalize_name(value: str | None) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip()).lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return " ".join(text.split())


def compact_name(value: str | None) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", (value or "").strip().lower().replace("ё", "е"))


def contract_tokens(value: str | None) -> set[str]:
    compact = compact_name(value)
    tokens: set[str] = set()
    for prefix, number in re.findall(r"(мпг)0*(\d{5,})", compact):
        tokens.add(f"{prefix}{int(number)}")
    return tokens


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
        if not response.ok:
            snippet = (response.text or "")[:800]
            msg = f"HTTP {response.status_code} при запросе {page_url}: {snippet}"
            raise ODataRequestError(msg, status_code=response.status_code, url=page_url)

        try:
            batch = response.json().get("value", [])
        except ValueError as exc:
            raise ODataRequestError(
                f"Некорректный JSON OData: {page_url}: {exc!s}",
                status_code=response.status_code,
                url=page_url,
            ) from exc
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)

    return rows


def load_projects(session: requests.Session) -> list[dict]:
    select = quote("Ref_Key,Code,Description,DeletionMark,IsFolder", safe=",_")
    flt = quote("DeletionMark eq false and IsFolder eq false", safe="")
    url = (
        f"{BASE}/{quote(PROJECT_ENTITY)}"
        f"?$format=json&$select={select}&$filter={flt}&$orderby=Description"
    )
    return fetch_all(session, url, page=1000, timeout=120)


def _project_candidates(projects: list[dict], query: str) -> tuple[list[dict], list[dict]]:
    query_norm = normalize_name(query)
    if not query_norm:
        return [], []

    exact = [
        row
        for row in projects
        if query_norm
        in {
            normalize_name(row.get("Description")),
            normalize_name(row.get("Code")),
        }
    ]
    fuzzy = [
        row
        for row in projects
        if query_norm in normalize_name(row.get("Description"))
        or query_norm in normalize_name(row.get("Code"))
    ]
    return exact, fuzzy


def best_fuzzy_project_match(projects: list[dict], query: str) -> dict | None:
    query_norm = normalize_name(query)
    if not query_norm:
        return None
    exact, fuzzy = _project_candidates(projects, query)
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        for row in exact:
            if normalize_name(row.get("Description")) == query_norm:
                return row
        return exact[0]

    if not fuzzy:
        return None
    if len(fuzzy) == 1:
        return fuzzy[0]

    code_hits = [r for r in fuzzy if normalize_name(r.get("Code")) == query_norm]
    if len(code_hits) == 1:
        return code_hits[0]

    fuzzy.sort(key=lambda r: len(normalize_name(r.get("Description") or "")))
    return fuzzy[0]


def cell_display_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return str(
            value.get("Description")
            or value.get("description")
            or value.get("Presentation")
            or value.get("__value")
            or value.get("Value")
            or "",
        ).strip()
    return str(value).strip()


def month_key_iso(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        inner = value.get("Date") or value.get("date") or value.get("__value") or value.get("Value")
        if inner is not None:
            return month_key_iso(inner)
        return ""
    s = str(value).strip()
    if len(s) >= 7 and s[4:5] == "-":
        return s[:7]
    return ""


def resource_row_amount(row: dict) -> float:
    v = row.get("Сумма")
    if isinstance(v, dict):
        v = v.get("__value") or v.get("value") or v.get("Value")
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def find_project_for_query(projects: list[dict], query: str) -> dict | None:
    query_norm = normalize_name(query)
    if not query_norm:
        return None

    exact, fuzzy = _project_candidates(projects, query)
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        for row in exact:
            if normalize_name(row.get("Description")) == query_norm:
                return row
        return exact[0]

    if not fuzzy:
        return None
    if len(fuzzy) == 1:
        return fuzzy[0]
    return None


def _guid_for_odata_url(ref_key: str) -> str:
    g = str(ref_key or "").strip().strip("{}")
    return g.lower()


def fetch_bdds_document_by_ref(session: requests.Session, ref_key: str, timeout: int = 120) -> dict | None:
    g = _guid_for_odata_url(ref_key)
    if not g:
        return None
    url = f"{BASE}/{quote(DOC_ENTITY)}(guid'{g}')?$format=json"
    response = session.get(url, timeout=timeout)
    if not response.ok:
        return None
    try:
        body = response.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


def _resources_row_count(doc: dict) -> int:
    rb = doc.get("Ресурсы")
    if rb is None:
        return 0
    if isinstance(rb, dict) and "value" in rb:
        rb = rb["value"]
    if isinstance(rb, list):
        return len(rb)
    return 0


def _load_bdds_docs_for_project_guid(session: requests.Session, project_guid: str) -> list[dict]:
    flt = quote(
        f"DeletionMark eq false and Проект_Key eq guid'{project_guid}'",
        safe="",
    )
    url = (
        f"{BASE}/{quote(DOC_ENTITY)}"
        f"?$format=json&$filter={flt}"
        f"&$orderby={quote('Date asc,Number asc', safe=' ,')}"
    )
    return fetch_all(session, url, page=100, timeout=120)


def load_bdds_docs(session: requests.Session, project_key: str) -> list[dict]:
    raw = str(project_key or "").strip().strip("{}")
    variants: list[str] = []
    for v in (raw, raw.lower(), raw.upper()):
        if v and v not in variants:
            variants.append(v)

    docs: list[dict] = []
    for g in variants:
        docs = _load_bdds_docs_for_project_guid(session, g)
        if docs:
            break

    if not docs:
        return docs
    head = docs[0]
    ref = head.get("Ref_Key")
    if not ref:
        return docs
    if _resources_row_count(head) > 0:
        return docs
    full = fetch_bdds_document_by_ref(session, str(ref))
    if full and _resources_row_count(full) > 0:
        return [full, *docs[1:]]
    return docs


def _is_zatraty_resource_label(data_label: str, tokens: list[str]) -> bool:
    return data_label == "затраты" or (bool(tokens) and tokens[-1] == "затраты")


def _resource_rows_from_doc(doc: dict) -> list[dict]:
    resource_block = doc.get("Ресурсы")
    if resource_block is None:
        resource_block = []
    if isinstance(resource_block, dict) and "value" in resource_block:
        resource_block = resource_block["value"]
    rows = resource_block if isinstance(resource_block, list) else []
    return [r for r in rows if isinstance(r, dict)]


def aggregate_resource_costs(docs: list[dict]) -> dict[str, float]:
    if not docs:
        return {}
    month_totals: dict[str, float] = defaultdict(float)

    for row in _resource_rows_from_doc(docs[0]):
        data_label = normalize_name(cell_display_text(row.get("Данные")))
        tokens = data_label.split()
        if not _is_zatraty_resource_label(data_label, tokens):
            continue
        month = month_key_iso(row.get("Месяц"))
        if len(month) != 7:
            continue

        month_totals[month] += resource_row_amount(row)

    return dict(month_totals)


def zatraty_latest_bdds_detail(session: requests.Session, project_ref_key: str) -> dict:
    g = str(project_ref_key).strip().strip("{}")
    docs = load_bdds_docs(session, g)
    out: dict = {
        "project_catalog_ref_key": g,
        "monthly_zatraty_rub": {},
        "total_rub": 0.0,
        "bdds_documents_count": len(docs),
        "latest_doc_ref_key": None,
        "latest_doc_number": None,
        "latest_doc_date": None,
        "resources_rows_in_latest": 0,
        "distinct_data_labels_normalized": [],
        "sample_resource_rows": [],
        "empty_reason": None,
    }
    if not docs:
        out["empty_reason"] = "no_bdds_documents"
        return out

    oldest = docs[0]
    out["latest_doc_ref_key"] = oldest.get("Ref_Key")
    out["latest_doc_number"] = oldest.get("Number")
    out["latest_doc_date"] = oldest.get("Date")

    rows = _resource_rows_from_doc(oldest)
    out["resources_rows_in_latest"] = len(rows)

    labels: set[str] = set()
    for row in rows:
        lbl = normalize_name(cell_display_text(row.get("Данные")))
        if lbl:
            labels.add(lbl)
    out["distinct_data_labels_normalized"] = sorted(labels)[:40]

    for row in rows[:5]:
        out["sample_resource_rows"].append({
            "dannye_normalized": normalize_name(cell_display_text(row.get("Данные"))),
            "mesyac_raw": str(row.get("Месяц") or "")[:32],
            "summa": row.get("Сумма"),
        })

    monthly = aggregate_resource_costs(docs)
    out["monthly_zatraty_rub"] = {k: round(v, 2) for k, v in sorted(monthly.items())}
    out["total_rub"] = round(sum(monthly.values()), 2)

    if not rows:
        out["empty_reason"] = "no_resources_rows"
    elif not monthly:
        out["empty_reason"] = "no_zatraty_rows"
    else:
        out["empty_reason"] = None
    return out


def monthly_zatraty_totals_latest_bdds(
    session: requests.Session,
    project_ref_key: str,
) -> dict[str, float]:
    d = zatraty_latest_bdds_detail(session, project_ref_key)
    return {k: float(v) for k, v in (d.get("monthly_zatraty_rub") or {}).items()}


# --- Встроенный ручной план/факт ФОТ по внешним заказам -------------------------------


def _load_fot_reference_table() -> dict[str, Any]:
    """Вернуть срез из бывшего `fot_vneshnie_zakazy.xlsx`, зашитый в модуль."""
    return {
        "monthly": {
            month_key: dict(values)
            for month_key, values in FOT_REFERENCE_MONTHLY.items()
        },
        "project_labels": list(FOT_REFERENCE_PROJECT_LABELS),
    }


def _excel_label_matches_project(label: str, project: dict[str, Any]) -> bool:
    label_norm = normalize_name(label)
    label_compact = compact_name(label)
    label_contracts = contract_tokens(label)
    for field in ("project_name", "project_code"):
        value = str(project.get(field) or "")
        if label_contracts and label_contracts.intersection(contract_tokens(value)):
            return True
        value_norm = normalize_name(value)
        value_compact = compact_name(value)
        if value_norm and (value_norm in label_norm or label_norm in value_norm):
            return True
        if value_compact and (value_compact in label_compact or label_compact in value_compact):
            return True
    return False


def _excel_covered_project_ids(
    turbo_projects: list[dict[str, Any]],
    project_labels: list[str],
) -> set[str]:
    covered: set[str] = set()
    for project in turbo_projects:
        fid = project.get("file_id")
        if fid in (None, ""):
            continue
        if any(_excel_label_matches_project(label, project) for label in project_labels):
            covered.add(str(fid))
    return covered


# --- Плитка TD-M6 ---------------------------------------------------------------------


def _month_row_td_m6(
    y: int,
    m: int,
    *,
    plan_sum: float,
    fact_sum: float,
    alive_n: int,
    credited_n: int,
) -> dict[str, Any]:
    plan_val = round(float(plan_sum), 2)
    fact_val = round(float(fact_sum), 2)
    kpi_pct_val = 0.0 if plan_val == 0 else round(fact_val / plan_val * 100, 2)
    has_alive = alive_n > 0
    return {
        "month": m,
        "year": y,
        "month_name": MONTH_NAMES[m],
        "plan": plan_val,
        "fact": fact_val,
        "kpi_pct": kpi_pct_val,
        "has_data": has_alive,
        "values_unit": "руб.",
        "alive_projects_count": alive_n,
        "bdds_projects_with_month_row": credited_n if has_alive else 0,
    }


def _zero_payload_for_period(ref_y: int, ref_m: int) -> dict[str, Any]:
    """Ответ плитки TD-M6 при ошибке OData / импорта: нули, та же форма, что у обычного payload."""
    pairs = tp._month_pairs_until(ref_y, ref_m)
    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None
    for y, m in pairs:
        row = _month_row_td_m6(y, m, plan_sum=0.0, fact_sum=0.0, alive_n=0, credited_n=0)
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row
    assert ref_row is not None
    ytd_block: dict[str, Any] = {
        "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
        "months_total": len(monthly_rows),
        "values_unit": "руб.",
        "total_plan": ref_row["plan"],
        "total_fact": ref_row["fact"],
        "kpi_pct": ref_row["kpi_pct"],
    }
    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "current_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": ytd_block,
    }


def _bdds_totals_cache(
    turbo_projects: list[dict[str, Any]],
    session: requests.Session,
    catalog_projects: list[dict],
) -> dict[str, dict[str, float]]:
    """file_id TurboProject → помесячные суммы «затраты» из самого старого БДДС по проекту."""
    out: dict[str, dict[str, float]] = {}
    for row in turbo_projects:
        fid = row.get("file_id")
        if fid is None or fid == "":
            continue
        fk = str(fid)
        if fk in out:
            continue
        onec = None
        name = str(row.get("project_name") or "").strip()
        code = str(row.get("project_code") or "").strip()
        if name:
            onec = find_project_for_query(catalog_projects, name)
        if onec is None and code:
            onec = find_project_for_query(catalog_projects, code)
        if onec is None:
            onec = best_fuzzy_project_match(catalog_projects, name or code or "")
            if onec is not None:
                logger.info(
                    "TD-M6: нестрогое сопоставление «%s» → 1С «%s»",
                    (name or code or "").strip(),
                    onec.get("Description") or onec.get("Code"),
                )
        if onec is None:
            logger.warning(
                "TD-M6: проект 1С не найден для внешнего заказа «%s» (код %s)",
                name or "(без имени)",
                code or "—",
            )
            out[fk] = {}
            continue
        ref_key = onec.get("Ref_Key")
        if not ref_key:
            out[fk] = {}
            continue
        try:
            out[fk] = monthly_zatraty_totals_latest_bdds(session, str(ref_key))
        except Exception:
            logger.exception("TD-M6: ошибка OData БДДС для проекта %s", name or code or fid)
            out[fk] = {}
    return out


def _build_payload(year: int | None, month: int | None) -> dict[str, Any]:
    ref_y, ref_m = tp._normalize_ref_period(year, month)
    pairs = tp._month_pairs_until(ref_y, ref_m)

    turbo_list = tp._projects_for_filter(tp.TARGET_PROJECT_TYPE_TD_M1)

    monthly_rows: list[dict[str, Any]] = []
    ref_row: dict[str, Any] | None = None

    session = requests.Session()
    session.auth = AUTH
    fot_reference = _load_fot_reference_table()
    fot_monthly = fot_reference.get("monthly", {}) if fot_reference else {}
    fot_project_labels = fot_reference.get("project_labels", []) if fot_reference else []
    excel_covered_ids = _excel_covered_project_ids(turbo_list, fot_project_labels)
    fallback_projects = [
        project
        for project in turbo_list
        if str(project.get("file_id") or "") not in excel_covered_ids
    ]
    bdds_map: dict[str, dict[str, float]] = {}
    if fallback_projects:
        try:
            catalog_projects = load_projects(session)
            bdds_map = _bdds_totals_cache(fallback_projects, session, catalog_projects)
        except Exception:
            logger.exception("TD-M6: ошибка резервного расчёта БДДС для проектов вне Excel")

    for y, m in pairs:
        month_key = f"{y:04d}-{m:02d}"
        excel_values = fot_monthly.get(month_key, {}) if isinstance(fot_monthly, dict) else {}
        plan_sum = float(excel_values.get("plan", 0.0) or 0.0)
        fact_sum = float(excel_values.get("fact", 0.0) or 0.0)
        alive_n = 0
        credited_n = 0
        for project in turbo_list:
            if not tp._project_is_alive_in_month(project, y, m):
                continue
            alive_n += 1
            pf = project.get("file_id")
            fid = str(pf) if pf not in (None, "") else ""
            if fid in excel_covered_ids:
                continue
            totals = bdds_map.get(fid, {}) if fid else {}
            amount = float(totals.get(month_key, 0.0))
            if amount:
                credited_n += 1
            plan_sum += amount

        plan_sum = round(plan_sum, 2)
        fact_sum = round(fact_sum, 2)
        row = _month_row_td_m6(
            y,
            m,
            plan_sum=plan_sum,
            fact_sum=fact_sum,
            alive_n=alive_n,
            credited_n=credited_n,
        )
        monthly_rows.append(row)
        if (y, m) == (ref_y, ref_m):
            ref_row = row

    assert ref_row is not None
    ytd_block: dict[str, Any] = {
        "months_with_data": sum(1 for row in monthly_rows if row.get("has_data")),
        "months_total": len(monthly_rows),
        "values_unit": "руб.",
        "total_plan": ref_row["plan"],
        "total_fact": ref_row["fact"],
        "kpi_pct": ref_row["kpi_pct"],
    }

    return {
        "data_granularity": "monthly",
        "monthly_data": monthly_rows,
        "last_full_month_row": dict(ref_row),
        "kpi_period": {
            "type": "current_month",
            "year": ref_y,
            "month": ref_m,
            "month_name": MONTH_NAMES[ref_m],
        },
        "ytd": ytd_block,
    }


def get_td_m6_ytd(year: int | None = None, month: int | None = None) -> dict[str, Any]:
    """Плитка TD-M6: план/факт ФОТ по внешним заказам. При сбое — нули, не null."""

    def _runner() -> dict[str, Any]:
        ry, rm = tp._normalize_ref_period(year, month)
        try:
            return _build_payload(year, month)
        except Exception:
            logger.exception("Ошибка при расчёте TD-M6 (БДДС по внешним заказам)")
            return _zero_payload_for_period(ry, rm)

    ry, rm = tp._normalize_ref_period(year, month)
    return cache_manager.locked_call(f"techdir_td_m6_bdds_{ry}_{rm:02d}_v11", _runner)


# --- Опциональный CLI: один проект (как прежний calc_bdds_project_costs.main) -----------


def _money(value: float) -> str:
    return f"{value:,.2f}".replace(",", " ")


_MONTH_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
    7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _month_label(month_value: str) -> str:
    if not month_value or len(month_value) < 7:
        return month_value or "(месяц не указан)"
    year = int(month_value[:4])
    month = int(month_value[5:7])
    return f"{_MONTH_RU.get(month, month_value[5:7])} {year}"


def _pick_project_cli(projects: list[dict], query: str) -> dict:
    query_norm = normalize_name(query)
    if not query_norm:
        print("  Название проекта не указано")
        sys.exit(1)

    exact, fuzzy = _project_candidates(projects, query)
    candidates = exact or fuzzy

    if not candidates:
        print(f"  Проект не найден: {query}")
        sys.exit(1)

    if len(candidates) == 1:
        return candidates[0]

    print(f"\nНайдено несколько проектов по запросу «{query}»:")
    for idx, row in enumerate(candidates[:20], start=1):
        code = row.get("Code") or ""
        name = row.get("Description") or ""
        print(f"  {idx:>2}. {code}  {name}")

    if len(candidates) > 20:
        print(f"  ... ещё {len(candidates) - 20}")

    while True:
        raw = input("Введите номер нужного проекта: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= min(len(candidates), 20):
            return candidates[int(raw) - 1]
        print("  Неверный номер, попробуйте ещё раз.")


def _print_bdds_result(project: dict, month_totals: dict[str, float]) -> None:
    project_code = project.get("Code") or ""
    project_name = project.get("Description") or ""

    print(f"\nПроект: {project_code}  {project_name}")
    print(f"  {'Месяц':<20} {'Сумма':>18}")
    print(f"  {'-' * 20} {'-' * 18}")

    grand_total = 0.0
    for month in sorted(month_totals):
        total = month_totals[month]
        grand_total += total
        print(f"  {_month_label(month):<20} {_money(total):>18}")

    print(f"  {'-' * 20} {'-' * 18}")
    print(f"  {'ИТОГО':<20} {_money(grand_total):>18}")


def _cli_main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    query = " ".join(sys.argv[1:]).strip()
    if not query:
        query = input("Введите название проекта: ").strip()

    try:
        session = requests.Session()
        session.auth = AUTH

        projects = load_projects(session)

        project = _pick_project_cli(projects, query)

        docs = load_bdds_docs(session, project["Ref_Key"])
        if not docs:
            print("  Для выбранного проекта документы БДДС не найдены.")
            return
        month_totals = aggregate_resource_costs(docs)

        _print_bdds_result(project, month_totals)
    except ODataRequestError as exc:
        print(f"  {exc}")
        sys.exit(1)


if __name__ == "__main__":
    # Запуск из корня репозитория: python -m getkpi.techdir_m6_bdds [запрос проекта]
    _cli_main()
