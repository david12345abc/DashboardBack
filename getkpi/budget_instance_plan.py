"""План ФОТ и «Бюджет план» из проведённого Документ.ЭкземплярБюджета.

На подразделение берётся один документ, чей интервал шапки покрывает
запрошенный период: с максимальной датой, при равенстве — с большим номером.
ФОТ — пять статей оплаты труда. Бюджет план — остальные статьи того же
документа, кроме наименований на «ЦФО». Суммы только из
ОборотыПоСтатьямБюджетов, поле ``Сумма`` (плановая сумма в валюте бюджета).
``СуммаВВалюте`` не складывается.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from urllib.parse import quote

import requests

from .fot_techdir_fact import AUTH, BASE
from .odata_http import request_with_retry

logger = logging.getLogger(__name__)

AMOUNT_FIELD = "Сумма"
# В базе несколько сценариев. Контурные KPI плана берут этот, его и передают явно.
PLAN_SCENARIO_NAME = "Плановые данные - ЦФО"
NO_DOCUMENT_REASON = "нет проведённого экземпляра бюджета на этот период"
EMPTY_DEPARTMENTS_REASON = "пустой список подразделений"
ABSENT_ARTICLE_NOTE = "статья в документе отсутствует"
EXCLUDED_FOT = "статья ФОТ"
EXCLUDED_CFO = "наименование начинается с ЦФО"
CFO_PREFIX = "цфо"

FOT_ARTICLE_NAMES: tuple[str, ...] = (
    "Затраты на оплату труда (амортизация ТС)",
    "Затраты на оплату труда (переменная)",
    "Затраты на оплату труда (постоянная)",
    "Затраты на оплату труда (соц. выплаты)",
    "Налог на заработную плату",
)
FOT_ARTICLE_KEYS = frozenset(name.casefold() for name in FOT_ARTICLE_NAMES)

DOC_ENTITY = "Document_ЭкземплярБюджета"
ANALYTICS_ENTITY = "Document_ЭкземплярБюджета_АналитикаСтатейБюджетов"
TURNOVER_ENTITY = "Document_ЭкземплярБюджета_ОборотыПоСтатьямБюджетов"

_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_CACHE_TTL_SEC = 15 * 60
_lock = threading.Lock()
_catalogs: dict[str, Any] | None = None
_catalogs_at = 0.0
_headers_cache: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}
_lines_cache: dict[str, tuple[float, list[dict], list[dict]]] = {}


class BudgetPlanIntegrityError(RuntimeError):
    """Сумма расходится с правилами отбора документа и статей."""


def article_key(name: str | None) -> str:
    return (name or "").strip().casefold()


def classify_article(name: str | None) -> str:
    """``fot`` | ``cfo`` | ``budget``."""
    key = article_key(name)
    if key in FOT_ARTICLE_KEYS:
        return "fot"
    if key.startswith(CFO_PREFIX):
        return "cfo"
    return "budget"


def month_bounds(year: int, month: int) -> tuple[date, date]:
    if not 1 <= month <= 12:
        raise ValueError("Месяц должен быть от 1 до 12")
    start = date(year, month, 1)
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    end = next_month.fromordinal(next_month.toordinal() - 1)
    return start, end


def document_covers(doc_start: date, doc_end: date, period_start: date, period_end: date) -> bool:
    return doc_start <= period_start and doc_end >= period_end


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _as_date(value: Any) -> date | None:
    parsed = _parse_datetime(value)
    return parsed.date() if parsed else None


def _money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _dim_label(doc: dict[str, Any], dim: str) -> str:
    name = (doc.get(f"{dim}_name") or "").strip()
    key = (doc.get(f"{dim}_key") or "").strip()
    return name or key or "<пусто>"


def _matches_dim(doc: dict[str, Any], dim: str, expected: str) -> bool:
    token = (expected or "").strip()
    if not token:
        return False
    folded = token.casefold()
    name = (doc.get(f"{dim}_name") or "").strip().casefold()
    key = (doc.get(f"{dim}_key") or "").strip().casefold()
    return folded == name or folded == key


def _number_rank(number: str) -> tuple[int, int | str]:
    text = (number or "").strip()
    if text.isdigit():
        return (1, int(text))
    return (0, text)


def choose_document(
    candidates: list[dict[str, Any]],
    *,
    organization: str | None = None,
    scenario: str | None = None,
    budgeting_model: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Один документ подразделения либо причина, почему сумму брать нельзя."""
    docs = list(candidates)
    filters = (
        ("organization", organization),
        ("scenario", scenario),
        ("model", budgeting_model),
    )
    for dim, expected in filters:
        if expected is not None and str(expected).strip():
            docs = [doc for doc in docs if _matches_dim(doc, dim, str(expected))]
    if not docs:
        return None, NO_DOCUMENT_REASON

    problems: list[str] = []
    labels = {
        "organization": "организация",
        "scenario": "сценарий",
        "model": "модель бюджетирования",
    }
    for dim, expected in filters:
        if expected is not None and str(expected).strip():
            continue
        values = sorted({_dim_label(doc, dim) for doc in docs})
        if len(values) > 1:
            problems.append(f"{labels[dim]}: " + "; ".join(values))
    if problems:
        return None, "нельзя выбрать документ, в данных несколько значений: " + " | ".join(problems)

    chosen = max(
        docs,
        key=lambda doc: (
            doc.get("date") or datetime.min,
            _number_rank(str(doc.get("number") or "")),
            str(doc.get("ref") or ""),
        ),
    )
    return chosen, None


def aggregate_document(
    analytics: list[dict[str, Any]],
    turnovers: list[dict[str, Any]],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Сложить обороты одного уже выбранного документа за период."""
    article_by_line: dict[str, str] = {}
    line_order: list[str] = []
    for row in analytics:
        line_id = str(row.get("line_id") or "")
        if not line_id or line_id in article_by_line:
            continue
        article_by_line[line_id] = (row.get("article_name") or "").strip()
        line_order.append(line_id)

    sums: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    lines_with_turnover: set[str] = set()
    orphan_turnovers: list[dict[str, Any]] = []
    for row in turnovers:
        period = row.get("period")
        if not isinstance(period, date):
            period = _as_date(period)
        if period is None or period < period_start or period > period_end:
            continue
        line_id = str(row.get("line_id") or "")
        amount = _dec(row.get("amount"))
        if line_id not in article_by_line:
            orphan_turnovers.append(
                {
                    "line_id": line_id,
                    "period": period.isoformat(),
                    "amount": _money(amount),
                }
            )
            continue
        sums[line_id] += amount
        lines_with_turnover.add(line_id)

    present_fot: dict[str, Decimal] = {}
    budget_by_name: dict[str, Decimal] = {}
    budget_display: dict[str, str] = {}
    excluded: dict[tuple[str, str], Decimal] = {}
    excluded_display: dict[tuple[str, str], str] = {}

    for line_id in line_order:
        name = article_by_line[line_id]
        kind = classify_article(name)
        amount = sums.get(line_id, Decimal("0"))
        if kind == "fot":
            present_fot[article_key(name)] = present_fot.get(article_key(name), Decimal("0")) + amount
            bucket = (article_key(name), EXCLUDED_FOT)
            excluded[bucket] = excluded.get(bucket, Decimal("0")) + amount
            excluded_display.setdefault(bucket, name)
        elif kind == "cfo":
            bucket = (article_key(name), EXCLUDED_CFO)
            excluded[bucket] = excluded.get(bucket, Decimal("0")) + amount
            excluded_display.setdefault(bucket, name)
        else:
            key = article_key(name)
            budget_by_name[key] = budget_by_name.get(key, Decimal("0")) + amount
            budget_display.setdefault(key, name)

    fot_articles = []
    fot_total = Decimal("0")
    for canonical in FOT_ARTICLE_NAMES:
        key = canonical.casefold()
        missing = key not in present_fot
        amount = Decimal("0") if missing else present_fot[key]
        fot_total += amount
        fot_articles.append(
            {
                "name": canonical,
                "amount": _money(amount),
                "missing": missing,
                "note": ABSENT_ARTICLE_NOTE if missing else None,
            }
        )

    budget_articles = [
        {"name": budget_display[key], "amount": _money(budget_by_name[key])}
        for key in sorted(budget_by_name, key=lambda item: budget_display[item].casefold())
    ]
    excluded_articles = [
        {
            "name": excluded_display[bucket],
            "amount": _money(amount),
            "reason": bucket[1],
        }
        for bucket, amount in sorted(excluded.items(), key=lambda item: (item[0][1], excluded_display[item[0]].casefold()))
    ]
    lines_without_turnover = [
        {"line_id": line_id, "article": article_by_line[line_id]}
        for line_id in line_order
        if line_id not in lines_with_turnover
    ]

    for article in budget_articles:
        kind = classify_article(article["name"])
        if kind != "budget":
            raise BudgetPlanIntegrityError(
                f"статья {article['name']!r} попала в бюджет план"
            )
    fot_names = {article_key(name) for name in FOT_ARTICLE_NAMES}
    if fot_names & {article_key(article["name"]) for article in budget_articles}:
        raise BudgetPlanIntegrityError("статья ФОТ попала в бюджет план")

    return {
        "fot_total": _money(fot_total),
        "fot_articles": fot_articles,
        "budget_plan_total": _money(sum(budget_by_name.values(), Decimal("0"))),
        "budget_plan_articles": budget_articles,
        "excluded_articles": excluded_articles,
        "lines_without_turnover": lines_without_turnover,
        "turnovers_without_article": orphan_turnovers,
    }


def _empty_department(name: str, period_start: date, period_end: date, reason: str) -> dict[str, Any]:
    return {
        "department": name,
        "department_key": None,
        "document": None,
        "requested_period": {"start": period_start.isoformat(), "end": period_end.isoformat()},
        "fot_total": None,
        "fot_articles": [],
        "budget_plan_total": None,
        "budget_plan_articles": [],
        "excluded_articles": [],
        "lines_without_turnover": [],
        "turnovers_without_article": [],
        "reason": reason,
    }


def _document_payload(doc: dict[str, Any]) -> dict[str, Any]:
    moment = doc.get("date")
    return {
        "ref": doc.get("ref"),
        "number": doc.get("number"),
        "date": moment.isoformat(sep=" ") if isinstance(moment, datetime) else None,
        "organization": doc.get("organization_name") or None,
        "organization_key": doc.get("organization_key"),
        "scenario": doc.get("scenario_name") or None,
        "scenario_key": doc.get("scenario_key"),
        "model": doc.get("model_name") or None,
        "model_key": doc.get("model_key"),
        "period_start": doc["period_start"].isoformat() if doc.get("period_start") else None,
        "period_end": doc["period_end"].isoformat() if doc.get("period_end") else None,
        "status": doc.get("status"),
    }


def _finalize(rows: list[dict[str, Any]], period_start: date, period_end: date) -> dict[str, Any]:
    # Одно подразделение входит в итог один раз: строки уже дедуплицированы вызывающим кодом.
    seen: set[str] = set()
    for row in rows:
        key = row.get("department_key") or f"name:{(row.get('department') or '').casefold()}"
        if key in seen:
            raise BudgetPlanIntegrityError(f"подразделение повторилось в итоге: {row.get('department')}")
        seen.add(key)
        if row.get("reason") and row.get("fot_total") not in (None,) and row.get("document") is None:
            raise BudgetPlanIntegrityError("подразделение без документа показано числом")

    complete = bool(rows) and all(row.get("document") and not row.get("reason") for row in rows)
    reason = None
    fot_total = None
    budget_total = None
    if not rows:
        reason = EMPTY_DEPARTMENTS_REASON
    elif complete:
        fot_total = _money(sum((_dec(row["fot_total"]) for row in rows), Decimal("0")))
        budget_total = _money(sum((_dec(row["budget_plan_total"]) for row in rows), Decimal("0")))
    else:
        parts = [
            f"{row.get('department')}: {row.get('reason')}"
            for row in rows
            if row.get("reason")
        ]
        reason = "; ".join(parts) if parts else "итог не собран"
    return {
        "amount_field": AMOUNT_FIELD,
        "period": {"start": period_start.isoformat(), "end": period_end.isoformat()},
        "departments": rows,
        "fot_total": fot_total,
        "budget_plan_total": budget_total,
        "total_complete": complete,
        "reason": reason,
    }


def calculate(
    departments: list[str],
    period_start: date,
    period_end: date,
    *,
    organization: str | None = None,
    scenario: str | None = None,
    budgeting_model: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Расчёт по списку подразделений. Пустой список — считать нечего."""
    if period_end < period_start:
        raise ValueError("Конец периода раньше начала")
    if not departments:
        return _finalize([], period_start, period_end)

    own_session = session is None
    if own_session:
        session = requests.Session()
        session.auth = AUTH
    assert session is not None

    catalogs = _load_catalogs(session)
    resolved: list[tuple[str, dict[str, Any] | None, str | None]] = []
    seen_keys: set[str] = set()
    seen_unresolved: set[str] = set()
    for raw in departments:
        token = (raw or "").strip()
        if not token:
            if "" in seen_unresolved:
                continue
            seen_unresolved.add("")
            resolved.append(("", None, "подразделение не найдено"))
            continue
        found, error = _resolve_department(token, catalogs["departments"])
        if error or found is None:
            unresolved_key = token.casefold()
            if unresolved_key in seen_unresolved:
                continue
            seen_unresolved.add(unresolved_key)
            resolved.append((token, None, error or "подразделение не найдено"))
            continue
        if found["ref"] in seen_keys:
            continue
        seen_keys.add(found["ref"])
        resolved.append((found["name"], found, None))

    need_docs = any(item[1] is not None for item in resolved)
    grouped: dict[str, list[dict[str, Any]]] = {}
    if need_docs:
        headers = _load_headers(session, period_start, period_end, catalogs)
        for header in headers:
            grouped.setdefault(header["department_key"], []).append(header)

    rows: list[dict[str, Any]] = []
    for display_name, found, error in resolved:
        if error or found is None:
            rows.append(_empty_department(display_name, period_start, period_end, error or "подразделение не найдено"))
            continue
        chosen, choose_error = choose_document(
            grouped.get(found["ref"], []),
            organization=organization,
            scenario=scenario,
            budgeting_model=budgeting_model,
        )
        if choose_error or chosen is None:
            row = _empty_department(found["name"], period_start, period_end, choose_error or NO_DOCUMENT_REASON)
            row["department_key"] = found["ref"]
            rows.append(row)
            continue
        analytics, turnovers = _load_lines(session, str(chosen["ref"]))
        summed = aggregate_document(analytics, turnovers, period_start, period_end)
        rows.append(
            {
                "department": found["name"],
                "department_key": found["ref"],
                "document": _document_payload(chosen),
                "requested_period": {
                    "start": period_start.isoformat(),
                    "end": period_end.isoformat(),
                },
                **summed,
                "reason": None,
            }
        )
    return _finalize(rows, period_start, period_end)


def indicator_from_result(result: dict[str, Any], metric: str) -> float | None:
    """Сумма показателя по подразделениям, у которых есть документ.

    Подразделение без документа в сумму не входит и нулём не заменяется.
    Если подразделение не найдено или по нему нельзя выбрать документ,
    число не возвращается: иначе из итога пропал бы заказанный контур.
    """
    if metric not in {"fot", "budget_plan"}:
        raise ValueError("metric должен быть fot или budget_plan")
    field = "fot_total" if metric == "fot" else "budget_plan_total"
    if result.get("total_complete"):
        return result[field]

    blocking: list[str] = []
    available: list[Decimal] = []
    skipped: list[str] = []
    for row in result.get("departments") or []:
        reason = row.get("reason") or ""
        label = f"{row.get('department')}: {reason}" if reason else str(row.get("department"))
        if reason.startswith("нельзя выбрать") or "не найдено" in reason or reason.startswith("несколько подразделений"):
            blocking.append(label)
            continue
        if row.get("document") and row.get(field) is not None:
            available.append(_dec(row[field]))
            continue
        skipped.append(label)
    if blocking or not available:
        logger.warning(
            "план %s не собран: %s",
            metric,
            "; ".join(blocking or skipped) or result.get("reason"),
        )
        return None
    if skipped:
        logger.warning(
            "план %s без подразделений, у которых нет документа: %s",
            metric,
            "; ".join(skipped),
        )
    return _money(sum(available, Decimal("0")))


def indicator_for_month(
    departments: list[str],
    year: int,
    month: int,
    metric: str,
    *,
    organization: str | None = None,
    scenario: str | None = None,
    budgeting_model: str | None = None,
) -> float | None:
    """``metric`` — ``fot`` или ``budget_plan``. Нет ни одного документа — ``None``, не ноль."""
    if metric not in {"fot", "budget_plan"}:
        raise ValueError("metric должен быть fot или budget_plan")
    start, end = month_bounds(year, month)
    result = calculate(
        departments,
        start,
        end,
        organization=organization,
        scenario=scenario,
        budgeting_model=budgeting_model,
    )
    return indicator_from_result(result, metric)


def clear_cache() -> None:
    global _catalogs, _catalogs_at
    with _lock:
        _catalogs = None
        _catalogs_at = 0.0
        _headers_cache.clear()
        _lines_cache.clear()


def _resolve_department(
    token: str, departments: list[dict[str, Any]]
) -> tuple[dict[str, Any] | None, str | None]:
    if _GUID_RE.match(token):
        matches = [row for row in departments if row["ref"].casefold() == token.casefold()]
    else:
        key = token.casefold()
        matches = [row for row in departments if row["name"].strip().casefold() == key]
    if not matches:
        return None, "подразделение не найдено"
    active = [row for row in matches if not row.get("deletion_mark")]
    pool = active or matches
    if len(pool) > 1:
        listed = "; ".join(f"{row['name']} ({row['ref']})" for row in pool)
        return None, f"несколько подразделений с таким наименованием: {listed}"
    return pool[0], None


def _load_catalogs(session: requests.Session) -> dict[str, Any]:
    global _catalogs, _catalogs_at
    now = time.time()
    with _lock:
        if _catalogs is not None and now - _catalogs_at < _CACHE_TTL_SEC:
            return _catalogs
    departments = _fetch_named(session, "Catalog_СтруктураПредприятия", with_deletion=True)
    articles = {
        row["ref"]: row["name"]
        for row in _fetch_named(session, "Catalog_СтатьиБюджетов", with_deletion=False)
    }
    data = {
        "departments": departments,
        "articles": articles,
        "organizations": {row["ref"]: row["name"] for row in _fetch_named(session, "Catalog_Организации", with_deletion=False)},
        "scenarios": {row["ref"]: row["name"] for row in _fetch_named(session, "Catalog_Сценарии", with_deletion=False)},
        "models": {
            row["ref"]: row["name"]
            for row in _fetch_named(session, "Catalog_МоделиБюджетирования", with_deletion=False)
        },
    }
    with _lock:
        _catalogs = data
        _catalogs_at = time.time()
    return data


def _fetch_named(session: requests.Session, entity: str, *, with_deletion: bool) -> list[dict[str, Any]]:
    select = "Ref_Key,Description,DeletionMark" if with_deletion else "Ref_Key,Description"
    rows = _fetch_all(session, _odata_url(entity, select=select))
    out: list[dict[str, Any]] = []
    for row in rows:
        ref = row.get("Ref_Key")
        if not ref:
            continue
        item = {"ref": ref, "name": (row.get("Description") or "").strip()}
        if with_deletion:
            item["deletion_mark"] = bool(row.get("DeletionMark"))
        out.append(item)
    return out


def _load_headers(
    session: requests.Session,
    period_start: date,
    period_end: date,
    catalogs: dict[str, Any],
) -> list[dict[str, Any]]:
    cache_key = (period_start.isoformat(), period_end.isoformat())
    now = time.time()
    with _lock:
        cached = _headers_cache.get(cache_key)
        if cached and now - cached[0] < _CACHE_TTL_SEC:
            return cached[1]
    # Сравнение по дате: окончание документа 31.01 00:00 покрывает период, который кончается 31.01.
    start_dt = datetime(period_start.year, period_start.month, period_start.day).isoformat(
        sep="T", timespec="seconds"
    )
    end_dt = datetime(period_end.year, period_end.month, period_end.day).isoformat(
        sep="T", timespec="seconds"
    )
    flt = (
        "Posted eq true and DeletionMark eq false and "
        f"НачалоПериода le datetime'{start_dt}' and "
        f"ОкончаниеПериода ge datetime'{end_dt}'"
    )
    select = (
        "Ref_Key,Number,Date,Posted,МодельБюджетирования_Key,Организация_Key,"
        "Подразделение_Key,Сценарий_Key,НачалоПериода,ОкончаниеПериода,Статус"
    )
    raw_rows = _fetch_all(session, _odata_url(DOC_ENTITY, select=select, flt=flt))
    parsed: list[dict[str, Any]] = []
    for row in raw_rows:
        if row.get("Posted") is False:
            continue
        doc_start = _as_date(row.get("НачалоПериода"))
        doc_end = _as_date(row.get("ОкончаниеПериода"))
        moment = _parse_datetime(row.get("Date"))
        dept_key = row.get("Подразделение_Key") or ""
        if not doc_start or not doc_end or not moment or not dept_key:
            continue
        if not document_covers(doc_start, doc_end, period_start, period_end):
            continue
        org_key = row.get("Организация_Key") or ""
        scenario_key = row.get("Сценарий_Key") or ""
        model_key = row.get("МодельБюджетирования_Key") or ""
        parsed.append(
            {
                "ref": row.get("Ref_Key"),
                "number": row.get("Number") or "",
                "date": moment,
                "department_key": dept_key,
                "organization_key": org_key,
                "organization_name": catalogs["organizations"].get(org_key, ""),
                "scenario_key": scenario_key,
                "scenario_name": catalogs["scenarios"].get(scenario_key, ""),
                "model_key": model_key,
                "model_name": catalogs["models"].get(model_key, ""),
                "period_start": doc_start,
                "period_end": doc_end,
                "status": row.get("Статус") or "",
            }
        )
    with _lock:
        _headers_cache[cache_key] = (time.time(), parsed)
    return parsed


def _load_lines(session: requests.Session, ref: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = time.time()
    with _lock:
        cached = _lines_cache.get(ref)
        if cached and now - cached[0] < _CACHE_TTL_SEC:
            return cached[1], cached[2]
    catalogs = _load_catalogs(session)
    flt = "Ref_Key eq guid'" + ref + "'"
    analytics_raw = _fetch_all(
        session,
        _odata_url(
            ANALYTICS_ENTITY,
            select="ИдентификаторСтроки,СтатьяБюджетов",
            flt=flt,
        ),
    )
    turnover_raw = _fetch_all(
        session,
        _odata_url(
            TURNOVER_ENTITY,
            select="ИдентификаторСтроки,ПериодПланирования,Сумма",
            flt=flt,
        ),
    )
    analytics = []
    for row in analytics_raw:
        article_ref = row.get("СтатьяБюджетов") or ""
        analytics.append(
            {
                "line_id": row.get("ИдентификаторСтроки"),
                "article_name": catalogs["articles"].get(article_ref, ""),
            }
        )
    turnovers = [
        {
            "line_id": row.get("ИдентификаторСтроки"),
            "period": _as_date(row.get("ПериодПланирования")),
            "amount": row.get(AMOUNT_FIELD),
        }
        for row in turnover_raw
    ]
    with _lock:
        _lines_cache[ref] = (time.time(), analytics, turnovers)
    return analytics, turnovers


def _odata_url(entity: str, *, select: str, flt: str | None = None) -> str:
    parts = [f"$format=json", f"$select={quote(select, safe=',_')}"]
    if flt:
        parts.append(f"$filter={quote(flt, safe='')}")
    return f"{BASE}/{quote(entity)}?{'&'.join(parts)}"


def _fetch_all(session: requests.Session, url: str, page: int = 1000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        sep = "&" if "?" in url else "?"
        page_url = f"{url}{sep}$top={page}&$skip={skip}"
        response = request_with_retry(session, page_url, timeout=120, label="budget-instance")
        if response is None:
            raise RuntimeError(f"OData недоступен: {page_url[:160]}")
        if not response.ok:
            raise RuntimeError(
                f"OData HTTP {response.status_code}: {response.text[:400]}"
            )
        batch = response.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        skip += len(batch)
    return rows
