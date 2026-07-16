"""
Отчётная таблица анкет удовлетворённости клиентов (1С HTTP-сервис).

Источник:
  GET https://web.turbo-don.ru/web1C/hs/Options1/DataUK

Использование:
  python http/get_customer_satisfaction.py 26.03.2025 15.04.2026
  python http/get_customer_satisfaction.py 26.03.2025 15.04.2026 report.xlsx
  python http/get_customer_satisfaction.py --input cache.json 26.03.2025 15.04.2026

Переменные окружения:
  TURBO_HS_USER, TURBO_HS_PASSWORD   — доступ к HTTP-сервису
  ODATA_USER, ODATA_PASSWORD         — внутренний OData (поиск компании)
  TURBO_USE_WEB_ODATA=1              — искать компанию в web OData (медленно)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

if TYPE_CHECKING:
    import pandas as pd

# --- настройки ---

DEFAULT_USER = "Искусственный интеллект 2"
DEFAULT_PASSWORD = "npo852456"
BASE_URL = "https://web.turbo-don.ru/web1C/hs/Options1/DataUK"
INTERNAL_ODATA = os.getenv(
    "ONEC_ODATA_URL",
    "http://192.168.2.229:81/erp_pm/odata/standard.odata",
).rstrip("/")
WEB_ODATA = os.getenv(
    "TURBO_ODATA_URL",
    "https://web.turbo-don.ru/web1C/odata/standard.odata",
).rstrip("/")
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_TIMEOUT = int(os.getenv("TURBO_HS_TIMEOUT", "90"))
DEFAULT_RETRIES = int(os.getenv("TURBO_HS_RETRIES", "2"))
WEB_ODATA_TIMEOUT = int(os.getenv("TURBO_ODATA_TIMEOUT", "8"))
USE_WEB_ODATA = os.getenv("TURBO_USE_WEB_ODATA", "").lower() in ("1", "true", "yes")
EMPTY_DATE = "0001-01-01T00:00:00"

QUALITY_LABELS = {3: "Отлично", 2: "Хорошо", 1: "Плохо"}

SERVICE_SATISFACTION_FIELDS = (
    "УдовлетворенностьГрамотностьКонсультаций",
    "УдовлетворенностьОперативностьРаботы",
    "УдовлетворенностьСвоевременностьСообщений",
    "УдовлетворенностьСоблюдениеСроков",
    "УдовлетворенностьДокументация",
)

EQUIPMENT_SATISFACTION_FIELDS = (
    "УдовлетворенностьВнешнийВидОборудования",
    "УдовлетворенностьКачествоУпаковки",
    "УдовлетворенностьКомплектностьОборудования",
    "УдовлетворенностьУдобствоПриМонтаже",
    "УдовлетворенностьЦенаКачество",
)

COOPERATION_OPTIONS: tuple[tuple[str, str], ...] = (
    (
        "СУдовольствиемПродолжимРаботать",
        "С удовольствием продолжим и рекомендуем сотрудничество с вашим предприятием.",
    ),
    (
        "ГотовыПродолжитьРаботать",
        "Готовы продолжать и рекомендовать сотрудничество с вашим предприятием.",
    ),
    (
        "ПродолжениеСотрудничестваПриемлемо",
        "Продолжение сотрудничества с вашим предприятием приемлемо.",
    ),
    (
        "ПродолжениеСотрудничестваПодЖесткимКонтролем",
        "Продолжение сотрудничества с вашим предприятием возможно только в рамках жёсткого контроля. Деятельность предприятия требует улучшения.",
    ),
    (
        "ПродолжениеСотрудничестваНеПриемлемо",
        "Продолжение сотрудничества с вашим предприятием не приемлемо. Будем обращаться к другим поставщикам.",
    ),
)

REPORT_COLUMNS = (
    "Период",
    "Пользователь или Заказчик",
    "Компания",
    "Оценка качества оказываемых услуг",
    "Оценка качества оборудования",
    "Удовлетворение потребностей",
    "Комментарий к удовлетворению потребностей",
    "Готовность к дальнейшему сотрудничеству",
)


# --- утилиты ---


def basic_auth_header(user: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def parse_date(value: str) -> date:
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"не удалось разобрать дату: {value}")


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _odata_escape(value: str) -> str:
    return (value or "").replace("'", "''")


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower().replace("ё", "е")


def _partner_name(row: dict) -> str:
    return (row.get("НаименованиеПолное") or row.get("Description") or "").strip()


def quality_label(value: Any) -> str:
    number = _as_int(value)
    if number is None or number <= 0:
        return ""
    return QUALITY_LABELS.get(number, str(value))


def _average_positive_scores(row: dict[str, Any], fields: tuple[str, ...]) -> float | None:
    scores = [_as_int(row.get(field)) for field in fields]
    positive = [score for score in scores if score is not None and score > 0]
    if not positive:
        return None
    return sum(positive) / len(positive)


def _label_from_five_point_average(avg: float) -> str:
    if avg >= 4.0:
        return QUALITY_LABELS[3]
    if avg >= 3.0:
        return QUALITY_LABELS[2]
    return QUALITY_LABELS[1]


def quality_label_for_aggregate(
    row: dict[str, Any],
    aggregate_field: str,
    detail_fields: tuple[str, ...],
) -> str:
    """Оценка 1–3 из агрегата; если 1С вернула 0 — среднее по детальным полям (шкала 1–5)."""
    direct = quality_label(row.get(aggregate_field))
    if direct:
        return direct
    avg = _average_positive_scores(row, detail_fields)
    if avg is None:
        return ""
    return _label_from_five_point_average(avg)


def cooperation_label(row: dict[str, Any]) -> str:
    for field, label in COOPERATION_OPTIONS:
        if row.get(field) is True:
            return label
    return ""


def needs_answer(row: dict[str, Any]) -> tuple[str, str]:
    answer = str(row.get("ВажностьУдовлетворениеПотребностей") or "").strip()
    comment = str(row.get("ДругоеУдовлетворениеПотребностей") or "").strip()
    return answer, comment


# --- загрузка данных ---


def fetch_surveys(
    date_from: date | None = None,
    date_to: date | None = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> list[dict]:
    session = requests.Session()
    session.headers.update(
        basic_auth_header(
            os.getenv("TURBO_HS_USER", DEFAULT_USER),
            os.getenv("TURBO_HS_PASSWORD", DEFAULT_PASSWORD),
        )
    )

    params: dict[str, str] = {}
    if date_from:
        params["ДатаНачала"] = date_from.isoformat()
    if date_to:
        params["ДатаОкончания"] = date_to.isoformat()

    response = None
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(BASE_URL, params=params or None, timeout=timeout)
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries:
                raise RuntimeError(
                    f"не удалось получить данные с {BASE_URL} после {retries} попыток: {exc}"
                ) from exc

    if response is None:
        raise RuntimeError(f"не удалось получить данные: {last_error}")
    if response.status_code == 401:
        raise PermissionError("401: проверьте TURBO_HS_USER / TURBO_HS_PASSWORD.")
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")

    rows = response.json().get("data") or []
    return filter_by_period(rows, date_from, date_to)


def filter_by_period(
    rows: list[dict],
    date_from: date | None,
    date_to: date | None,
) -> list[dict]:
    if not date_from and not date_to:
        return rows

    start = date_from or date.min
    end = date_to or date.max
    filtered = []
    for row in rows:
        period_raw = str(row.get("Период") or "")[:10]
        try:
            period = parse_date(period_raw)
        except ValueError:
            continue
        if start <= period <= end:
            filtered.append(row)
    return filtered


def load_surveys_from_file(
    path: Path,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("data") or payload
        if not isinstance(rows, list):
            raise ValueError(f"в {path} ожидался список записей")
    elif suffix in {".xlsx", ".xls"}:
        import pandas as pd

        frame = pd.read_excel(path)
        rows = frame.to_dict(orient="records")
        for row in rows:
            for key, value in list(row.items()):
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, str) and value in ("да", "нет"):
                    row[key] = value == "да"
    else:
        raise ValueError(f"неподдерживаемый формат: {path}")

    return filter_by_period(rows, date_from, date_to)


# --- поиск компании ---


class CompanyResolver:
    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._session = requests.Session()
        self._internal_auth = HTTPBasicAuth(
            os.getenv("ODATA_USER", "odata.user"),
            os.getenv("ODATA_PASSWORD", "npo852456"),
        )

    def resolve(self, external_user: str) -> str:
        name = (external_user or "").strip()
        key = _normalize_name(name)
        if not key:
            return ""
        if key in self._cache:
            return self._cache[key]

        company = self._lookup_contact_company(name)
        if not company:
            company = self._lookup_external_user_company(name, INTERNAL_ODATA, self._internal_auth)
        if not company and USE_WEB_ODATA:
            company = self._lookup_external_user_company(
                name,
                WEB_ODATA,
                basic_auth_header(
                    os.getenv("TURBO_HS_USER", DEFAULT_USER),
                    os.getenv("TURBO_HS_PASSWORD", DEFAULT_PASSWORD),
                ),
            )

        self._cache[key] = company
        return company

    def _lookup_external_user_company(self, name: str, base_url: str, auth) -> str:
        entity = quote("Catalog_ВнешниеПользователи")
        escaped = _odata_escape(name)
        flt = quote(f"DeletionMark eq false and Description eq '{escaped}'", safe="")
        select = quote("Description,НаименованиеЮрЛица", safe=",_")
        url = f"{base_url}/{entity}?$format=json&$filter={flt}&$top=5&$select={select}"
        timeout = WEB_ODATA_TIMEOUT if base_url == WEB_ODATA else 30
        kwargs: dict = {"timeout": timeout}
        if isinstance(auth, dict):
            kwargs["headers"] = auth
        else:
            kwargs["auth"] = auth
        response = self._session.get(url, **kwargs)
        if response.status_code in (401, 404) or not response.ok:
            return ""
        for row in response.json().get("value", []):
            company = (row.get("НаименованиеЮрЛица") or "").strip()
            if company:
                return company
        return ""

    def _lookup_contact_company(self, name: str) -> str:
        entity = quote("Catalog_КонтактныеЛицаПартнеров")
        escaped = _odata_escape(name)
        flt = quote(f"DeletionMark eq false and Description eq '{escaped}'", safe="")
        select = quote(
            "Description,Owner_Key,ДатаПрекращенияСвязи,ДатаРегистрацииСвязи",
            safe=",_",
        )
        url = f"{INTERNAL_ODATA}/{entity}?$format=json&$filter={flt}&$top=10&$select={select}"
        response = self._session.get(url, auth=self._internal_auth, timeout=30)
        if not response.ok:
            return ""

        contacts = [
            row
            for row in response.json().get("value", [])
            if (row.get("ДатаПрекращенияСвязи") or "") in ("", EMPTY_DATE)
            and (row.get("Owner_Key") or "")
        ]
        contacts.sort(key=lambda row: row.get("ДатаРегистрацииСвязи") or "", reverse=True)
        if not contacts:
            return ""

        owner_key = contacts[0].get("Owner_Key") or ""
        partner_url = (
            f"{INTERNAL_ODATA}/{quote('Catalog_Партнеры')}"
            f"(guid'{owner_key}')?$format=json"
            f"&$select={quote('Ref_Key,Description,НаименованиеПолное', safe=',_')}"
        )
        partner_response = self._session.get(partner_url, auth=self._internal_auth, timeout=30)
        if not partner_response.ok:
            return ""
        return _partner_name(partner_response.json())


# --- отчётная таблица ---


def build_report_row(row: dict[str, Any], company: str) -> dict[str, str]:
    needs, needs_comment = needs_answer(row)
    return {
        "Период": str(row.get("Период") or "").strip(),
        "Пользователь или Заказчик": str(row.get("ВнешнийПользователь") or "").strip(),
        "Компания": company,
        "Оценка качества оказываемых услуг": quality_label_for_aggregate(
            row,
            "УдовлетворенностьКачествомОказываемыхУслуг",
            SERVICE_SATISFACTION_FIELDS,
        ),
        "Оценка качества оборудования": quality_label_for_aggregate(
            row,
            "УдовлетворенностьКачествомОборудования",
            EQUIPMENT_SATISFACTION_FIELDS,
        ),
        "Удовлетворение потребностей": needs,
        "Комментарий к удовлетворению потребностей": needs_comment,
        "Готовность к дальнейшему сотрудничеству": cooperation_label(row),
    }


def build_report_table(
    rows: list[dict],
    *,
    resolver: CompanyResolver | None = None,
) -> list[dict[str, str]]:
    company_resolver = resolver or CompanyResolver()
    users = sorted(
        {
            str(row.get("ВнешнийПользователь") or "").strip()
            for row in rows
            if str(row.get("ВнешнийПользователь") or "").strip()
        }
    )
    for user in users:
        company_resolver.resolve(user)
    return [build_report_row(row, company_resolver.resolve(
        str(row.get("ВнешнийПользователь") or "").strip()
    )) for row in rows]


def get_report_table(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    *,
    input_file: str | Path | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[dict[str, str]]:
    """Основная функция: возвращает отчётную таблицу как список словарей."""
    if isinstance(date_from, str):
        date_from = parse_date(date_from)
    if isinstance(date_to, str):
        date_to = parse_date(date_to)
    if date_to is None:
        date_to = date_from

    if input_file:
        rows = load_surveys_from_file(Path(input_file), date_from, date_to)
    else:
        rows = fetch_surveys(date_from, date_to, timeout=timeout)

    return build_report_table(rows)


def report_to_dataframe(table: list[dict[str, str]]) -> "pd.DataFrame":
    import pandas as pd

    return pd.DataFrame(table, columns=list(REPORT_COLUMNS))


def save_report_excel(
    table: list[dict[str, str]],
    output_path: Path,
) -> Path:
    if not table:
        raise ValueError("нет данных для отчёта")
    frame = report_to_dataframe(table)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_excel(output_path, index=False, sheet_name="Отчёт")
    return output_path


def default_output_path(date_from: date | None, date_to: date | None) -> Path:
    from_part = (date_from or date.min).strftime("%Y%m%d")
    to_part = (date_to or date_from or date.max).strftime("%Y%m%d")
    return SCRIPT_DIR / f"customer_satisfaction_report_{from_part}_{to_part}.xlsx"


# --- CLI ---


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Отчётная таблица анкет удовлетворённости клиентов."
    )
    parser.add_argument("date_from", nargs="?", help="Дата начала (2025-03-26 или 26.03.2025)")
    parser.add_argument("date_to", nargs="?", help="Дата окончания")
    parser.add_argument("output", nargs="?", help="Путь к Excel (по умолчанию — auto)")
    parser.add_argument("--input", metavar="FILE", help="Взять сырые данные из JSON/Excel")
    parser.add_argument("--json", action="store_true", help="Вывести таблицу в JSON в stdout")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Таймаут HTTP-запроса, сек (по умолчанию {DEFAULT_TIMEOUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    date_from = parse_date(args.date_from) if args.date_from else None
    date_to = parse_date(args.date_to) if args.date_to else date_from

    table = get_report_table(
        date_from,
        date_to,
        input_file=args.input,
        timeout=args.timeout,
    )

    if args.json:
        json.dump(table, sys.stdout, ensure_ascii=False, indent=2)
        print()
        return

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = default_output_path(date_from, date_to)

    saved = save_report_excel(table, output_path)
    print(f"Отчёт: {saved}")
    print(f"Записей: {len(table)}")
    for index, row in enumerate(table, start=1):
        print(
            f"{index:>2}. {row['Период']} · {row['Пользователь или Заказчик']} · "
            f"{row['Компания'] or '—'}"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)
