# -*- coding: utf-8 -*-
"""
ДЗ / просроченная ДЗ коммерческого блока — SQL.

Регистр: AccumulationRegister_РасчетыСКлиентамиПоСрокам → _AccumRg107662
  ОбъектРасчетов = _Fld140445RRef → Catalog_ОбъектыРасчетов (_Reference134945)
  ДатаПлановогоПогашения = _Fld107667
  Долг* = _Fld107672/_Fld107673/_Fld107674 (валюта/регл/упр; в RUB совпадают)
  Предоплата* = _Fld107669/_Fld107670/_Fld107671
  RecordKind: 0 = приход (+), 1 = расход (−)

Подразделение — из ОбъектыРасчетов._Fld138169RRef.
В итог комдира входят 6 коммерческих отделов + ликвидированные дилерские/холдинги
(холдинги для среза отдела алиасятся в «ключевые клиенты»).

Сверка 30.06.2026 с отчётом 1С «Задолженность клиентов по срокам»:
  ДЗ ≈ 323 930 180.89, просрочка ≈ 164 779 574 (все сроки < НаДату).
"""
from __future__ import annotations

import sys
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime
from typing import Any

_ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from comdir.common import (  # noqa: E402
    connect_ctx,
    to_1c_dt,
    uuid_to_1c_bytes,
)

# --- 1C SQL mapping -----------------------------------------------------------
ACCUM = "_AccumRg107662"
OBJ_COL = "_Fld140445RRef"
PLAN_COL = "_Fld107667"
DOLG_COL = "_Fld107672"       # Долг (валюта отчёта / упр совпадают)
PRED_COL = "_Fld107669"       # Предоплата
OBJ_TABLE = "_Reference134945"
OBJ_DEPT = "_Fld138169RRef"
DEPT_TABLE = "_Reference513"

COMMERCIAL_DEPTS: dict[str, str] = {
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел ВЭД",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел эталонного оборудования",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел БМИ",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел ключевых клиентов",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел ПАО Газпром",
}

# Ликвидированные: в итоге комдира есть, в срез ОДП дилерские НЕ алиасятся.
LIQUIDATED_DEPTS: dict[str, str] = {
    "4edcf3a0-9f99-11e4-80da-001e67112509": "(ликв.) Отдел дилерских продаж бытового оборудования",
    "ff740269-d71e-11e6-8127-001e67112509": "(ликв.) Отдел дилерских продаж промышленного оборудования",
    "c6810cc3-cf32-11ef-95e8-6cb31113810e": "(ликв.) Отдел по работе с холдингами 1",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e": "(ликв.) Отдел по работе с холдингами 2",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e": "(ликв.) Отдел по работе с холдингами 3",
}

# Для среза отдела: холдинги → ключевые клиенты (как в остальных KPI).
# Дилерские ликвидированные в ОДП НЕ мапятся (как DEBITORKA_DEPT_ALIASES).
HOLDINGS_TO_KEY = {
    "c6810cc3-cf32-11ef-95e8-6cb31113810e": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "ebd2d511-cf38-11ef-95e8-6cb31113810e": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e": "639ec87b-67b6-11eb-8523-ac1f6b05524d",
}

ALL_DEPT_GUIDS = list(COMMERCIAL_DEPTS) + list(LIQUIDATED_DEPTS)
TOLERANCE = 0.01
BUCKETS = ["1-29 дн.", "30-59 дн.", "60-179 дн.", "180+ дн."]


def _aging_bucket(days: int) -> str:
    if days <= 29:
        return "1-29 дн."
    if days <= 59:
        return "30-59 дн."
    if days <= 179:
        return "60-179 дн."
    return "180+ дн."


def _dept_display_name(guid: str) -> str:
    return COMMERCIAL_DEPTS.get(guid) or LIQUIDATED_DEPTS.get(guid) or guid[:8]


def _normalize_slice_guid(guid: str) -> str:
    """GUID для среза отдела (холдинги → ключевые)."""
    return HOLDINGS_TO_KEY.get(guid, guid)


def snapshot_on_date(na_datu: date) -> dict[str, Any]:
    """Снимок ДЗ/КЗ/просрочки на дату (конец дня)."""
    from datetime import timedelta

    p_end = to_1c_dt(na_datu + timedelta(days=1))  # Period < начало след. дня
    na_1c = to_1c_dt(na_datu)

    with connect_ctx() as cn:
        cur = cn.cursor()
        cur.execute("SET NOCOUNT ON")
        cur.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
        cur.execute("CREATE TABLE #d (id binary(16) PRIMARY KEY, guid char(36))")
        for g in ALL_DEPT_GUIDS:
            cur.execute(
                "INSERT INTO #d (id, guid) VALUES (?, ?)",
                uuid_to_1c_bytes(g),
                g,
            )

        # Нетто по (объект, срок, подразделение)
        cur.execute(
            f"""
            SELECT
              CONVERT(varchar(36), d.guid) AS dept_guid,
              s.[{OBJ_COL}] AS obj,
              s.[{PLAN_COL}] AS plan_dt,
              SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{DOLG_COL}] ELSE s.[{DOLG_COL}] END) AS dolg,
              SUM(CASE WHEN s._RecordKind = 1 THEN -s.[{PRED_COL}] ELSE s.[{PRED_COL}] END) AS pred
            FROM [{ACCUM}] s WITH (NOLOCK)
            INNER JOIN [{OBJ_TABLE}] o WITH (NOLOCK) ON o._IDRRef = s.[{OBJ_COL}]
            INNER JOIN #d d ON d.id = o.[{OBJ_DEPT}]
            WHERE s._Period < ? AND s._Active = 0x01
            GROUP BY d.guid, s.[{OBJ_COL}], s.[{PLAN_COL}]
            """,
            p_end,
        )
        rows = cur.fetchall()

    per_order: dict[tuple[str, bytes], dict] = defaultdict(
        lambda: {
            "dept_guid": "",
            "dz_net": 0.0,
            "kz_net": 0.0,
            "overdue_net": 0.0,
            "aging": defaultdict(float),
        }
    )

    for dept_guid, obj, plan_dt, dolg, pred in rows:
        dept_guid = (dept_guid or "").strip().lower()
        dolg = float(dolg or 0)
        pred = float(pred or 0)
        key = (dept_guid, bytes(obj) if obj else b"")
        e = per_order[key]
        e["dept_guid"] = dept_guid
        e["dz_net"] += dolg
        e["kz_net"] += pred
        if plan_dt is not None and isinstance(plan_dt, datetime):
            if plan_dt.year > 2000 and plan_dt < na_1c:
                e["overdue_net"] += dolg
                days = (na_1c - plan_dt).days
                if dolg > TOLERANCE:
                    e["aging"][_aging_bucket(days)] += dolg

    dz_by_dept: dict[str, float] = defaultdict(float)
    kz_by_dept: dict[str, float] = defaultdict(float)
    overdue_by_dept: dict[str, float] = defaultdict(float)
    aging_by_dept: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    # Также агрегаты по «срезовым» GUID (холдинги → KEY) для подчиненных
    dz_by_slice: dict[str, float] = defaultdict(float)
    overdue_by_slice: dict[str, float] = defaultdict(float)

    for e in per_order.values():
        guid = e["dept_guid"]
        name = _dept_display_name(guid)
        slice_guid = _normalize_slice_guid(guid)
        # Без фильтра «только +заказы»: иначе +~21k vs отчёт 1С 323 930 180.89.
        dz_by_dept[name] += e["dz_net"]
        dz_by_slice[slice_guid] += e["dz_net"]
        kz_by_dept[name] += e["kz_net"]
        overdue_by_dept[name] += e["overdue_net"]
        overdue_by_slice[slice_guid] += e["overdue_net"]
        for b, amt in e["aging"].items():
            aging_by_dept[name][b] += amt

    depts_all = sorted(
        d
        for d in set(list(dz_by_dept) + list(kz_by_dept) + list(overdue_by_dept))
        if abs(dz_by_dept.get(d, 0)) >= TOLERANCE
        or abs(kz_by_dept.get(d, 0)) >= TOLERANCE
        or abs(overdue_by_dept.get(d, 0)) >= TOLERANCE
    )
    return {
        "na_datu": na_datu.isoformat(),
        "dept_alias_source": "debitorka_sql_v1",
        "source": "comdir.sql._AccumRg107662",
        "total_dz": round(sum(dz_by_dept.values()), 2),
        "total_kz": round(sum(max(0.0, v) for v in kz_by_dept.values()), 2),
        "total_overdue": round(sum(overdue_by_dept.values()), 2),
        "kz_source": "predoplata",
        "by_dept": {
            d: {
                "dz": round(dz_by_dept.get(d, 0), 2),
                "kz": round(max(0.0, kz_by_dept.get(d, 0)), 2),
                "overdue": round(overdue_by_dept.get(d, 0), 2),
                "aging": {b: round(aging_by_dept[d].get(b, 0), 2) for b in BUCKETS},
            }
            for d in depts_all
        },
        "by_dept_guid": {
            g: {
                "dz": round(dz_by_slice.get(g, 0), 2),
                "overdue": round(overdue_by_slice.get(g, 0), 2),
                "name": COMMERCIAL_DEPTS.get(g, g[:8]),
            }
            for g in COMMERCIAL_DEPTS
        },
    }


def month_end(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])
