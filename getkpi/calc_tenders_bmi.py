"""
calc_tenders_bmi.py — % выигранных тендеров БМИ для учредителя (MRK-09).

Алгоритм:
  Берутся документы ТД_СлужебнаяЗаписка с начала года с отборами:
    - ТемаСлужебнойЗаписки = "Запрос документов по тендеру (регл.)"
    - УТО_ПодразделениеТендер = "Отдел продаж БМИ"

  План  = количество таких документов (все тендеры БМИ)
  Факт  = план с фильтром (УТО_РезультатТендера = 1)  (выигранные)
  %     = Факт / План * 100

Использование (CLI):
  python calc_tenders_bmi.py [ГГГГ]

Использование (как модуль):
  from .calc_tenders_bmi import get_tenders_bmi
  data = get_tenders_bmi(2026)           # {'plan': int, 'fact': int, 'pct': float|None, ...}
"""
import functools
import sys
import time
from datetime import date
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

BMI_KEY   = "9edaa7d4-37a5-11ee-93d3-6cb31113810e"  # Отдел продаж БМИ
TEMA_KEY  = "f88a0ca1-82eb-11e8-827b-ac1f6b05524d"  # "Запрос документов по тендеру (регл.)"
TEMA_NAME = "Запрос документов по тендеру (регл.)"

REZ_NAME = {
    0: "не указан / в работе",
    1: "выиграли",
    2: "проиграли",
    3: "отказались участвовать",
    4: "не состоялся",
    5: "отменён",
    6: "иное",
}


def _normalize_result_code(value) -> int:
    """OData может вернуть код результата строкой; приводим к int для стабильного расчёта."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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


def get_tenders_bmi(year: int | None = None,
                    *,
                    month: int | None = None,
                    dept_guid: str | None = None) -> dict:
    """
    % выигранных тендеров БМИ с начала `year` по 31.12 (или по `month`, если задан).

    Сигнатура повторяет стиль других calc-модулей (year/month/dept_guid) для совместимости
    с cache_manager.locked_call; параметр dept_guid здесь игнорируется (плитка всегда по БМИ).

    Возвращает:
        {
          'year': int,
          'month': int,          # верхняя граница окна (12 по умолчанию)
          'plan': int,           # всего тендеров БМИ за период
          'fact': int,           # выигранных тендеров БМИ
          'pct': float | None,   # fact / plan * 100 (округлено до 0.1)
          'distribution': {int: int},  # УТО_РезультатТендера → count
          'samples': [dict],     # последние 15 записей для отладки
        }
    """
    today = date.today()
    y = int(year) if year else today.year
    m = max(1, min(12, int(month))) if month else 12

    last_day = 31 if m in {1, 3, 5, 7, 8, 10, 12} else (30 if m != 2 else (29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28))
    end_dt = date(y, m, last_day)
    if y == today.year and end_dt >= today:
        end_dt = today

    start = f"{y}-01-01T00:00:00"
    end   = f"{end_dt.isoformat()}T23:59:59"

    s = requests.Session()
    s.auth = AUTH

    flt = (
        f"УТО_ПодразделениеТендер_Key eq guid'{BMI_KEY}'"
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

    plan = len(alive)
    fact = sum(1 for r in alive if _normalize_result_code(r.get("УТО_РезультатТендера")) == 1)
    distribution: dict[int, int] = {}
    for r in alive:
        k = _normalize_result_code(r.get("УТО_РезультатТендера", 0))
        distribution[k] = distribution.get(k, 0) + 1

    pct = round(fact / plan * 100, 1) if plan else None

    samples = []
    for r in sorted(alive, key=lambda x: x.get("Date", ""), reverse=True)[:15]:
        samples.append({
            "number": r.get("Number"),
            "date": (r.get("Date") or "")[:10],
            "result": _normalize_result_code(r.get("УТО_РезультатТендера", 0)),
            "name": (r.get("УТО_НаименованиеТендера") or "").strip(),
            "customer": (r.get("УТО_Заказчик") or "").strip(),
        })

    return {
        "year": y,
        "month": m,
        "period_start": start[:10],
        "period_end": end_dt.isoformat(),
        "plan": plan,
        "fact": fact,
        "pct": pct,
        "distribution": distribution,
        "samples": samples,
    }


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
    out(f"  План (всего тендеров): {data['plan']}")
    out(f"  Факт (выигранных):     {data['fact']}")
    if data["pct"] is not None:
        out(f"  Процент выигранных:    {data['pct']:.1f}%")

    out("\n  Распределение по УТО_РезультатТендера:")
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
