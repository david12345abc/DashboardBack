"""
calc_tenders_bmi.py — % выигранных тендеров для учредителя (MRK-09).

Алгоритм:
  Берутся документы ТД_СлужебнаяЗаписка с начала года с отборами:
    - ТемаСлужебнойЗаписки = "Запрос документов по тендеру (регл.)"
    - УТО_ПодразделениеТендер входит в список коммерческих тендерных отделов

  План  = количество таких документов (все тендеры)
  Факт  = план с фильтром (УТО_РезультатТендера = 1)  (выигранные)
  %     = Факт / План * 100

Использование (CLI):
  python calc_tenders_bmi.py [ГГГГ]

Использование (как модуль):
  from .calc_tenders_bmi import get_tenders_departments
  data = get_tenders_departments(2026)   # {'plan': int, 'fact': int, 'pct': float|None, ...}
"""
import functools
import logging
import sys
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from comdir.common import uuid_to_1c_bytes

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")
logger = logging.getLogger(__name__)

BMI_KEY   = "9edaa7d4-37a5-11ee-93d3-6cb31113810e"  # Отдел продаж БМИ
TEMA_KEY  = "f88a0ca1-82eb-11e8-827b-ac1f6b05524d"  # "Запрос документов по тендеру (регл.)"
TEMA_NAME = "Запрос документов по тендеру (регл.)"

TENDER_DEPARTMENTS: dict[str, str] = {
    BMI_KEY: "Отдел продаж БМИ",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел внешнеэкономической деятельности",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборудования и услуг",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
}

REZ_NAME = {
    0: "не указан / в работе",
    1: "выиграли",
    2: "проиграли",
    3: "отказались участвовать",
    4: "не состоялся",
    5: "отменён",
    6: "иное",
}

# Канонические статусы, которые видит пользователь в 1С.
# Порядок совпадает с тем, который назвал пользователь.
STATUS_LABELS = [
    "на подведении итогов",
    "выиграли",
    "проиграли",
    "отменен",
    "отклонили",
    "без итогов",
    "не участвуем",
]

# Синонимы/подстроки для fuzzy-мэтча лейблов с тем, что реально отдаёт 1С.
STATUS_MATCHERS = {
    "на подведении итогов": ("подвед", "итог"),
    "выиграли": ("выигр",),
    "проиграли": ("проигр",),
    "отменен": ("отмен",),
    "отклонили": ("отклон",),
    "без итогов": ("без итог",),
    "не участвуем": ("не участв", "отказ"),
}

SQL_DOC_TABLE = "_Document76733"
SQL_TOPIC_COL = "_Fld76743_RRRef"
SQL_TENDER_DEPT_COL = "_Fld184255RRef"
SQL_RESULT_COL = "_Fld178500"
SQL_TENDER_NUMBER_COL = "_Fld112271"
SQL_TENDER_NAME_COL = "_Fld114339"
SQL_CUSTOMER_COL = "_Fld112272"
SQL_NMC_COL = "_Fld112273"
SQL_TKP_COL = "_Fld178516"
SQL_RESULT_COMMENT_COL = "_Fld184345"
SQL_YEAR_OFFSET = 2000
SQL_EMPTY_REF = b"\x00" * 16


def _normalize_result_code(value) -> int:
    """OData может вернуть код результата строкой; приводим к int для стабильного расчёта."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sql_ref_to_guid(ref: bytes | bytearray | memoryview | None) -> str:
    if not ref:
        return ""
    hx = bytes(ref).hex()
    if len(hx) != 32 or hx == "0" * 32:
        return ""
    return f"{hx[24:32]}-{hx[20:24]}-{hx[16:20]}-{hx[0:4]}-{hx[4:16]}".lower()


def _sql_1c_date_to_iso(value) -> str:
    if not isinstance(value, datetime):
        return ""
    year = value.year - SQL_YEAR_OFFSET if value.year >= SQL_YEAR_OFFSET else value.year
    if year <= 1:
        return ""
    return value.replace(year=year).isoformat()


def _to_sql_dt(value: date) -> datetime:
    return datetime(value.year + SQL_YEAR_OFFSET, value.month, value.day)


def _sql_code_to_canonical() -> dict[int, str]:
    result: dict[int, str] = {}
    for code, raw_label in REZ_NAME.items():
        lc = str(raw_label).lower()
        for canonical, needles in STATUS_MATCHERS.items():
            if any(n in lc for n in needles):
                result[int(code)] = canonical
                break
    return result


# Кэш маппинга code -> canonical_label (канонический ярлык из STATUS_LABELS).
# Вычисляется один раз на процесс, при первом вызове get_tenders_bmi.
_CODE_TO_CANON: dict[int, str] | None = None


def _fetch_enum_labels(session) -> dict[int, str]:
    """Получить labels enum'а УТО_РезультатТендера из 1C OData.

    Возвращает {order_index: human_label}. Если не удалось — пустой словарь.
    Пробуем несколько имён entity-set'а, потому что в разных конфигурациях
    префикс/имя может отличаться.
    """
    candidates = [
        "Enum_УТО_РезультатТендера",
        "Enumeration_УТО_РезультатТендера",
    ]
    for name in candidates:
        url = f"{BASE}/{quote(name)}?$format=json"
        try:
            r = session.get(url, timeout=30)
        except Exception:
            continue
        if not r.ok:
            continue
        try:
            items = r.json().get("value", []) or []
        except Exception:
            continue
        out: dict[int, str] = {}
        for it in items:
            order = it.get("Order")
            if order is None:
                order = it.get("Index")
            try:
                idx = int(order) if order is not None else None
            except (TypeError, ValueError):
                idx = None
            label = (
                it.get("Synonym")
                or it.get("Description")
                or it.get("Presentation")
                or it.get("Name")
                or ""
            )
            label = str(label or "").strip()
            if idx is not None and label:
                out[idx] = label
        if out:
            return out
    return {}


def _build_code_to_canonical(session) -> dict[int, str]:
    """Построить маппинг {code -> canonical_label} по данным enum 1С.

    Если enum не удалось получить — используем REZ_NAME как fallback.
    """
    raw_labels = _fetch_enum_labels(session)
    if not raw_labels:
        raw_labels = dict(REZ_NAME)

    result: dict[int, str] = {}
    for code, raw_label in raw_labels.items():
        lc = str(raw_label).lower()
        canon = ""
        for canonical, needles in STATUS_MATCHERS.items():
            if any(n in lc for n in needles):
                canon = canonical
                break
        if canon:
            result[int(code)] = canon
    return result


def _get_code_to_canonical(session) -> dict[int, str]:
    global _CODE_TO_CANON
    if _CODE_TO_CANON is None:
        _CODE_TO_CANON = _build_code_to_canonical(session)
    return _CODE_TO_CANON


def _empty_status_counts() -> dict[str, int]:
    return {lbl: 0 for lbl in STATUS_LABELS}


def _fetch_all(session, base_url, page_size=1000, timeout=120):
    out, skip = [], 0
    while True:
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}$top={page_size}&$skip={skip}&$format=json"
        r = session.get(url, timeout=timeout)
        if not r.ok:
            break
        items = r.json().get("value", [])
        out.extend(items)
        if len(items) < page_size:
            break
        skip += page_size
    return out


def _month_end(year: int, month: int) -> date:
    last_day = 31 if month in {1, 3, 5, 7, 8, 10, 12} else (30 if month != 2 else (29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28))
    return date(year, month, last_day)


def _build_tenders_result(
    rows: list[dict],
    *,
    year: int,
    month: int,
    period_start: str,
    period_end: str,
    cumulative: bool,
    code_to_canon: dict[int, str],
    dept_names: dict[str, str],
) -> dict:
    plan = len(rows)
    distribution: dict[int, int] = {}
    status_counts = _empty_status_counts()
    by_dept: dict[str, dict] = {
        key: {
            "department_key": key,
            "department": name,
            "plan": 0,
            "fact": 0,
            "won": 0,
            "found": 0,
            "not_participating": 0,
            "pct": None,
            "status_counts": _empty_status_counts(),
        }
        for key, name in dept_names.items()
    }

    for r in rows:
        k = _normalize_result_code(r.get("УТО_РезультатТендера", 0))
        distribution[k] = distribution.get(k, 0) + 1
        canon = code_to_canon.get(k)
        if canon and canon in status_counts:
            status_counts[canon] += 1

        dept_key = str(r.get("УТО_ПодразделениеТендер_Key") or "")
        dept_row = by_dept.get(dept_key)
        if dept_row is not None:
            dept_row["plan"] += 1
            dept_row["found"] += 1
            if canon and canon in dept_row["status_counts"]:
                dept_row["status_counts"][canon] += 1
            if k == 1:
                dept_row["fact"] += 1
                dept_row["won"] += 1
            if canon == "не участвуем":
                dept_row["not_participating"] += 1

    for dept_row in by_dept.values():
        dept_row["pct"] = round(dept_row["fact"] / dept_row["plan"] * 100, 1) if dept_row["plan"] else None

    fact = distribution.get(1, 0)
    not_participating = status_counts.get("не участвуем", 0)
    pct = round(fact / plan * 100, 1) if plan else None

    samples = []
    for r in sorted(rows, key=lambda x: x.get("Date", ""), reverse=True)[:15]:
        code = _normalize_result_code(r.get("УТО_РезультатТендера", 0))
        samples.append({
            "number": r.get("Number"),
            "date": (r.get("Date") or "")[:10],
            "department": dept_names.get(str(r.get("УТО_ПодразделениеТендер_Key") or ""), ""),
            "result": code,
            "status": code_to_canon.get(code) or REZ_NAME.get(code, ""),
            "name": (r.get("УТО_НаименованиеТендера") or "").strip(),
            "customer": (r.get("УТО_Заказчик") or "").strip(),
        })

    departments = sorted(by_dept.values(), key=lambda row: row["department"])
    return {
        "year": year,
        "month": month,
        "period_start": period_start,
        "period_end": period_end,
        "plan": plan,
        "fact": fact,
        "pct": pct,
        "found": plan,
        "won": fact,
        "not_participating": not_participating,
        "status_counts": status_counts,
        "distribution": distribution,
        "departments": departments,
        "samples": samples,
        "cumulative": bool(cumulative),
    }


def _fetch_from_sql(
    *,
    year: int,
    month: int,
    departments: dict[str, str],
    cumulative: bool,
) -> dict:
    from sql_connection import SqlConnection

    end_dt = _month_end(year, month)
    today = date.today()
    if year == today.year and end_dt >= today:
        end_dt = today
    start_dt = date(year, 1, 1) if cumulative else date(year, month, 1)
    start_sql = _to_sql_dt(start_dt)
    end_sql = _to_sql_dt(end_dt + timedelta(days=1))
    topic_ref = uuid_to_1c_bytes(TEMA_KEY)

    with SqlConnection().connect_ctx() as conn:
        cur = conn.cursor()
        cur.execute("SET NOCOUNT ON")
        cur.execute("IF OBJECT_ID('tempdb..#tender_depts') IS NOT NULL DROP TABLE #tender_depts")
        cur.execute("CREATE TABLE #tender_depts (id binary(16) PRIMARY KEY, guid char(36))")
        for guid in departments:
            cur.execute(
                "INSERT INTO #tender_depts (id, guid) VALUES (?, ?)",
                uuid_to_1c_bytes(guid),
                guid,
            )
        cur.execute(
            f"""
            SELECT
              d.guid AS dept_guid,
              s._IDRRef AS ref_key,
              s._Number AS number,
              s._Date_Time AS date_time,
              s._Marked AS deletion_mark,
              s._Posted AS posted,
              s.[{SQL_RESULT_COL}] AS result_code,
              s.[{SQL_TENDER_NUMBER_COL}] AS tender_number,
              s.[{SQL_TENDER_NAME_COL}] AS tender_name,
              s.[{SQL_CUSTOMER_COL}] AS customer,
              s.[{SQL_NMC_COL}] AS nmc_sum,
              s.[{SQL_TKP_COL}] AS tkp_sum,
              s.[{SQL_RESULT_COMMENT_COL}] AS result_comment
            FROM dbo.[{SQL_DOC_TABLE}] s WITH (NOLOCK)
            INNER JOIN #tender_depts d ON d.id = s.[{SQL_TENDER_DEPT_COL}]
            WHERE s._Date_Time >= ?
              AND s._Date_Time < ?
              AND s.[{SQL_TOPIC_COL}] = ?
            """,
            start_sql,
            end_sql,
            topic_ref,
        )
        raw_rows = cur.fetchall()

    rows = []
    for row in raw_rows:
        if bytes(row.deletion_mark or b"") == b"\x01":
            continue
        dept_guid = str(row.dept_guid or "").strip().lower()
        rows.append({
            "Ref_Key": _sql_ref_to_guid(row.ref_key),
            "Number": (row.number or "").strip(),
            "Date": _sql_1c_date_to_iso(row.date_time),
            "Posted": bytes(row.posted or b"") == b"\x01",
            "DeletionMark": False,
            "ТемаСлужебнойЗаписки": TEMA_KEY,
            "УТО_ПодразделениеТендер_Key": dept_guid,
            "УТО_РезультатТендера": int(row.result_code or 0),
            "УТО_НомерТендера": (row.tender_number or "").strip(),
            "УТО_НаименованиеТендера": (row.tender_name or "").strip(),
            "УТО_Заказчик": (row.customer or "").strip(),
            "УТО_СуммаНМЦ": float(row.nmc_sum or 0),
            "УТО_СуммаТКПТендера": float(row.tkp_sum or 0),
            "УТО_КомментарийПоРезультатуТендера": (row.result_comment or "").strip(),
            "source": "sql_erp_pm",
        })

    return _build_tenders_result(
        rows,
        year=year,
        month=month,
        period_start=start_dt.isoformat(),
        period_end=end_dt.isoformat(),
        cumulative=cumulative,
        code_to_canon=_sql_code_to_canonical(),
        dept_names=departments,
    )


def get_tenders_departments(
    year: int | None = None,
    *,
    month: int | None = None,
    departments: dict[str, str] | None = None,
    cumulative: bool = True,
) -> dict:
    today = date.today()
    y = int(year) if year else today.year
    m = max(1, min(12, int(month))) if month else 12
    end_dt = _month_end(y, m)
    if y == today.year and end_dt >= today:
        end_dt = today

    start_dt = date(y, 1, 1) if cumulative else date(y, m, 1)
    start = f"{start_dt.isoformat()}T00:00:00"
    end = f"{end_dt.isoformat()}T23:59:59"

    dept_names = departments or TENDER_DEPARTMENTS
    try:
        result = _fetch_from_sql(year=y, month=m, departments=dept_names, cumulative=cumulative)
        result["source"] = "sql_erp_pm"
        return result
    except Exception as exc:
        logger.warning("tenders: SQL fetch failed, fallback to OData: %s", exc)

    dept_filter = " or ".join(
        f"УТО_ПодразделениеТендер_Key eq guid'{key}'"
        for key in dept_names
    )

    s = requests.Session()
    s.auth = AUTH
    flt = (
        f"({dept_filter})"
        f" and Date ge datetime'{start}'"
        f" and Date le datetime'{end}'"
    )
    url = (
        f"{BASE}/{quote('Document_ТД_СлужебнаяЗаписка')}"
        f"?$filter={quote(flt, safe='')}"
        f"&$select=Ref_Key,Number,Date,Posted,DeletionMark,"
        f"ТемаСлужебнойЗаписки,ТемаСлужебнойЗаписки_Type,"
        f"УТО_ПодразделениеТендер_Key,"
        f"УТО_РезультатТендера,УТО_НомерТендера,"
        f"УТО_НаименованиеТендера,УТО_Заказчик,УТО_СуммаНМЦ,"
        f"УТО_СуммаТКПТендера,УТО_КомментарийПоРезультатуТендера"
    )

    rows_all = _fetch_all(s, url)
    rows = [r for r in rows_all
            if r.get("ТемаСлужебнойЗаписки") == TEMA_KEY
            or r.get("ТемаСлужебнойЗаписки") == TEMA_NAME]
    alive = [r for r in rows if not r.get("DeletionMark")]
    code_to_canon = _get_code_to_canonical(s)
    return _build_tenders_result(
        alive,
        year=y,
        month=m,
        period_start=start[:10],
        period_end=end_dt.isoformat(),
        cumulative=cumulative,
        code_to_canon=code_to_canon,
        dept_names=dept_names,
    )


def get_tenders_bmi(year: int | None = None,
                    *,
                    month: int | None = None,
                    dept_guid: str | None = None,
                    cumulative: bool = True) -> dict:
    """
    % выигранных тендеров БМИ.

    cumulative=True  (по умолчанию) — период с 01.01 `year` по конец `month`
                      (накопительно с начала года). Используется для плитки «итого».
    cumulative=False — только в пределах указанного `month` (c 1 по последний день).
                      Используется для помесячных точек monthly_data.

    Параметр dept_guid игнорируется (плитка всегда по БМИ).

    Возвращает:
        {
          'year': int, 'month': int,
          'period_start', 'period_end',
          'plan': int, 'fact': int, 'pct': float | None,
          'distribution': {int: int}, 'samples': [dict],
          'cumulative': bool,
        }
    """
    return get_tenders_departments(
        year,
        month=month,
        departments={BMI_KEY: TENDER_DEPARTMENTS[BMI_KEY]},
        cumulative=cumulative,
    )


def _main_cli() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    out = functools.partial(print, flush=True)

    year = int(sys.argv[1]) if len(sys.argv) > 1 else date.today().year
    t0 = time.time()

    out("═" * 60)
    out(f"  % ВЫИГРАННЫХ ТЕНДЕРОВ БМИ (для учредителя)")
    out(f"  Период: {year}-01-01 – {year}-12-31")
    out(f"  Подразделение: Отдел продаж БМИ")
    out("═" * 60)

    data = get_tenders_bmi(year)

    out("\n" + "═" * 60)
    out(f"  РЕЗУЛЬТАТ")
    out("═" * 60)
    out(f"  Найдено всего:         {data['found']}")
    out(f"  Не участвуем:          {data['not_participating']}")
    out(f"  Выиграно:              {data['won']}")
    if data["pct"] is not None:
        out(f"  Процент выигранных:    {data['pct']:.1f}%")

    out("\n  Распределение по статусам (канонический лейбл):")
    for lbl, cnt in data.get("status_counts", {}).items():
        out(f"    {lbl:25s}: {cnt}")

    out("\n  Сырое распределение по УТО_РезультатТендера:")
    for k in sorted(data["distribution"]):
        out(f"    {k} — {REZ_NAME.get(k, '?'):25s}: {data['distribution'][k]}")

    out("\n  Последние 15 записей:")
    for s in data["samples"]:
        mark = "✓ ВЫИГР" if s["result"] == 1 else f"  rez={s['result']}"
        nm = (s.get("name") or "")[:55]
        zak = (s.get("customer") or "")[:30]
        out(f"    {mark:8s}  {s['date']} №{str(s['number']):12s}  {nm} / {zak}")

    out(f"\n  ({time.time()-t0:.1f}с)")


if __name__ == "__main__":
    _main_cli()
