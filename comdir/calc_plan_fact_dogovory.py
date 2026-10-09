# -*- coding: utf-8 -*-
"""
План / факт / ожидаемые по договорам для коммерческой службы.

Маркетинговый план (МП) — без изменений:
  _AccumRg96963, вид «Договоры», 6 коммерческих отделов.

Факт договоров = регистр + счёт-оферта + дозаказ (как в отчёте «План-факт»):

  A) ТД_ДоговорыПодписанные (live OData; SQL — fallback):
  • ДатаПодписания в периоде, Спецификация.Статус = Действует
  • перепродажа без МГС / ТД_НеУчитыватьВПланФакте
  • сопровождение на заказе режем, кроме МГС (в отчёте 1С МГС остаётся)
  • курсы валют заказа

  B) Счёт-оферта (из расшифровки Excel): заказы с ТД_СчетОферта,
     без строки в ТД_ДоговорыПодписанные, проведённые, с оплатой в периоде;
     сумма = СуммаОплатыРегл за период (ОПБО/ОДП — перепродажа без МГС).

  C) Дозаказ по уже подписанному соглашению:
  • ТД_СчетОферта = нет, соглашение действует
  • типовое: оплата за период; если заказ уже в регистре за месяц —
    берём оплату только когда она не дублирует сумму регистра
    (в отчёте 1С это две строки под одним заказом)
  • типовое без строки в регистре: только дилер/дистрибьютор
  • спецификация 12×: ТО при пустом заказе / декабре, либо подписание с марта
    текущего года по договору прошлого/текущего года (Юг Руси; не Индушкин)
  • ОДП: без Владикавказа в дозаказе/оферте (в регистре подписанных — остаётся)
  • проведён, не «не учитывать»; сопровождение режем, кроме МГС
  • перепродажа без МГС (МГС в факте 1С остаётся во всех отделах)
  • сумма = оплата РасчетыСКлиентами за период
  • спецификация без суммы соглашения — не берём (не отличить 1×/12×)

Ожидаемые = UNION как в отчёте 1С «План-факт»:

  A) ТД_ДоговорыПотенциальные (_InfoRg112240):
  • ДатаПодписанияПлан в выбранном периоде (для текущего месяца — по сегодня)
  • КП заполнен; ЗаказКлиента пуст
  • статус КП не НеСогласовано / Аннулировано (+ Черновик, Отменено)
  • 6 коммерческих отделов, кроме БМИ (в Excel колонка ожидаемых по БМИ пустая)
  • перепродажа (ОДП — без МГС)
  • сумма: ТД_СуммаТКПБМИ при ТД_ОсновноеТКПДляБМИ, иначе СуммаДоговора × курс КП

  B) Счёт-оферта (РасчетыСКлиентами.Остатки на конец месяца):
  • соглашение с «оферт» в наименовании (флаг SQL _Fld13700 врёт:
    «ТИПОВОЕ … по договору» тоже помечен как оферта)
  • не в ТД_ДоговорыПодписанные
  • этап оплаты ДатаПлатежа < конец месяца; долг ≈ СуммаДокумента
  • ТД_ПредполагаемаяДатаАванса < конец среза; в текущем месяце ещё
    захватываем авансы прошлого месяца (как 1–15 в «План-факт»)
  • не ТД_НеУчитыватьВПланФакте / не ТД_СопровождениеПродажи; не перепродажа
  • СуммаДоговораПлан = СуммаДокумента
"""
from __future__ import annotations

import json
import logging
import re
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import pyodbc

logger = logging.getLogger(__name__)

YEAR_OFFSET = 2000
PLAN_DEALS = bytes.fromhex("9370c9cb1d3024c84863b32957436199")
EMPTY16 = bytes(16)

# СоглашенияСКлиентами.Статус = Действует
AG_STATUS_ACTIVE = bytes.fromhex("9bc837718a593711499faab4626a96cf")

# СтатусыКоммерческихПредложенийКлиентам (_Enum1651) — имена из Config:
#   0 Черновик, 1 Согласовано, 2 НеСогласовано, 3 Действует,
#   4 Аннулировано, 5 Отменено, 6 Исполнено
# Чёрный список: не «ожидаемые». Белый: Согласовано / Действует / Исполнено.
KP_STATUS_BLACKLIST = [
    bytes.fromhex("9997b28a99e76971468fd27d9692c43e"),  # 0 Черновик
    bytes.fromhex("96ad0bb5f0c5c55d49f80062c82e990f"),  # 2 НеСогласовано
    bytes.fromhex("b6d16d5e86f9a4ab4dd5e9acb562957f"),  # 4 Аннулировано
    bytes.fromhex("8d9131a3b7366b594a93f1ade4584df3"),  # 5 Отменено
]
KP_TREF = bytes.fromhex("00000302")

# Валюты (_Reference53)
CUR_USD = bytes.fromhex("963e001cc4d0438811dfe1b60a7c6f22")
CUR_EUR = bytes.fromhex("81cd001583b3d75c11e07405d328a18d")
CUR_BYN = bytes.fromhex("8756ac1f6b05524d11ec45dc095e2c36")
CUR_KZT = bytes.fromhex("95fc6cb31113810e11efde2ee2bc7bc0")
CUR_RUB = bytes.fromhex("812d001e6711250911e762523fdf75b4")

# Ликвидированные холдинги → ключевые клиенты
HOLDINGS_DEPTS: list[tuple[str, str]] = [
    ("(ликв.) Отдел по работе с холдингами 1", "95e86cb31113810e11efcf32c6810cc3"),
    ("(ликв.) Отдел по работе с холдингами 2", "95e86cb31113810e11efcf38ebd2d511"),
    ("(ликв.) Отдел по работе с холдингами 3", "95e86cb31113810e11efcf39ad83f8bd"),
]

# СоглашенияСКлиентами.ТД_СчетОферта (_Reference473)
AG_OFFER_FLAG = "_Fld13700"

ORDER_TREF = bytes.fromhex("000002c0")  # Документ.ЗаказКлиента
# ХозяйственнаяОперация.ВозвратОплатыКлиенту — для знака оплаты
RET_OP = bytes.fromhex("b4af52c1b39555e54eeac8d5724dc975")
# Документ.ЗаказКлиента.Статус
ORDER_STATUS_FIELD = "_Fld21195RRef"
ORDER_STATUS_NOT_AGREED = bytes.fromhex("a1675473ecec326649b4b85516d451ca")  # НеСогласован

# КП: ТД_ОсновноеТКПДляБМИ / ТД_СуммаТКПБМИ
KP_BMI_FLAG = "_Fld184256"
KP_BMI_SUM = "_Fld86876"
KP_CURRENCY = "_Fld25034RRef"

# ЗаказКлиента.ТД_ПредполагаемаяДатаАванса — дата ветки счёт-оферта в отчёте
ORDER_ADVANCE_DT = "_Fld21197"

# Курсы Константы.ТД_ВалютаПланФакта_УЕ_* — подгружаются из OData при расчёте.
FX_RATES: dict[str, float] = {"USD": 1.0, "EUR": 1.0, "BYN": 1.0, "KZT": 1.0}

COMMERCIAL_DEPTS: list[tuple[str, str]] = [
    ("Отдел по работе с ПАО Газпром", "80da001e6711250911e49f9cbd7b5184"),
    ("Отдел дилерских продаж", "96f96cb31113810e11f092f67587c178"),
    ("Отдел по работе с ключевыми клиентами", "8523ac1f6b05524d11eb67b6639ec87b"),
    ("Отдел продаж эталонного оборудования и услуг", "80d6001e6711250911e4810f34497ef7"),
    ("Отдел внешнеэкономической деятельности", "8283ac1f6b05524d11e8e40149480c10"),
    ("Отдел продаж БМИ", "93d36cb31113810e11ee37a59edaa7d4"),
]

LIQUIDATED_DEPTS: list[tuple[str, str]] = [
    ("(ликв.) Отдел дилерских продаж бытового оборудования", "80da001e6711250911e49f994edcf3a0"),
    ("(ликв.) Отдел дилерских продаж промышленного оборудования", "8127001e6711250911e6d71eff740269"),
]

FACT_ORDER: list[str] = [
    "(ликв.) Отдел дилерских продаж бытового оборудования",
    "(ликв.) Отдел дилерских продаж промышленного оборудования",
    "Отдел внешнеэкономической деятельности",
    "Отдел дилерских продаж",
    "Отдел по работе с ключевыми клиентами",
    "Отдел по работе с ПАО Газпром",
    "Отдел продаж БМИ",
    "Отдел продаж эталонного оборудования и услуг",
]

OUT_DIR = Path(__file__).resolve().parent


def connect() -> pyodbc.Connection:
    from comdir.common import connect as _connect

    return _connect()


def to_1c_dt(d: date) -> datetime:
    return datetime(d.year + YEAR_OFFSET, d.month, d.day)


def fmt(x) -> str:
    return f"{float(x or 0):,.2f}".replace(",", " ").replace(".", ",")


def pct(fact: float, plan: float) -> str:
    if not plan:
        return "—"
    return f"{fact / plan * 100:.1f}%"


def load_depts(cur, rows: list[tuple[str, str]], table: str = "#depts") -> None:
    cur.execute(f"IF OBJECT_ID('tempdb..{table}') IS NOT NULL DROP TABLE {table}")
    cur.execute(f"CREATE TABLE {table} (id binary(16) PRIMARY KEY, name nvarchar(255))")
    for name, hx in rows:
        cur.execute(f"INSERT INTO {table}(id, name) VALUES (?, ?)", bytes.fromhex(hx), name)


def load_resale(cur) -> None:
    from comdir.resale import load_resale_temp

    load_resale_temp(cur)


def fx_sql(amount_expr: str, currency_expr: str) -> str:
    """SQL expression: amount * rate by currency (else as-is)."""
    return f"""
    CASE
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      WHEN {currency_expr} = ? THEN ({amount_expr}) * ?
      ELSE ({amount_expr})
    END
    """


def refresh_fx_rates() -> dict[str, float]:
    """Загрузить курсы из констант 1С; при ошибке оставить текущие FX_RATES."""
    global FX_RATES
    try:
        from comdir.resale import fetch_fx_rates

        FX_RATES = fetch_fx_rates()
    except Exception:
        pass
    return FX_RATES


def fx_params() -> list:
    rates = refresh_fx_rates()
    return [
        CUR_USD, rates["USD"],
        CUR_EUR, rates["EUR"],
        CUR_BYN, rates["BYN"],
        CUR_KZT, rates["KZT"],
    ]


def calc_mp_plan(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """МП по договорам: вид «Договоры», только объекты с пустой датой архива."""
    load_depts(cur, COMMERCIAL_DEPTS, "#plan_depts")
    cur.execute(
        """
        SELECT d.name,
               SUM(CASE WHEN p._Active = 0x01 THEN p._Fld96971 ELSE 0 END) AS PlanSum
        FROM _AccumRg96963 p WITH (NOLOCK)
        INNER JOIN #plan_depts d ON d.id = p._Fld96965RRef
        LEFT JOIN _Reference112236 plan_obj WITH (NOLOCK)
          ON plan_obj._IDRRef = p._Fld96964_RRRef
        WHERE p._Fld122525RRef = ?
          AND p._Period >= ? AND p._Period < ?
          AND (
                plan_obj._IDRRef IS NULL
                OR plan_obj._Fld122423 <= ?
              )
        GROUP BY d.name
        """,
        PLAN_DEALS,
        p0,
        p_next,
        datetime(2001, 1, 1),
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def _dept_name_by_bin() -> dict[bytes, str]:
    return {
        bytes.fromhex(hx): name
        for name, hx in (COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS)
    }


def calc_fact_odata(p0: datetime, p_next: datetime) -> dict[str, float]:
    """Факт договоров из live OData — тот же запрос 1С, актуальные строки регистра.

    SQL-копия `_InfoRg112278` в erp_pm часто отстаёт (нет строк за последние дни),
    поэтому для факта берём живой регистр через OData.
    """
    from comdir.resale import _base, _session, fetch_fx_rates, guid_to_1c_bytes

    session = _session()
    base = _base()
    d0 = _odata_period_str(p0)
    d1 = _odata_period_str(p_next)
    rows = [
        r for r in load_signed_register_odata()
        if d0 <= (r.get("ДатаПодписания") or "")[:10] < d1
    ]
    if not rows:
        return {}

    resale, resale_nomgs, opbo = _odata_resale_guid_sets(session, base)
    rates = fetch_fx_rates()
    cur_map = {
        "0a7c6f22-e1b6-11df-963e-001cc4d04388": rates["USD"],
        "d328a18d-7405-11e0-81cd-001583b3d75c": rates["EUR"],
        "095e2c36-45dc-11ec-8756-ac1f6b05524d": rates["BYN"],
        "e2bc7bc0-de2e-11ef-95fc-6cb31113810e": rates["KZT"],
    }
    specs = _odata_batch_by_ref(
        session,
        base,
        "Catalog_СоглашенияСКлиентами",
        {r.get("Спецификация_Key") or "" for r in rows},
        "Ref_Key,Статус,Description,ТД_СчетОферта",
        label="Dog/SpecStatus",
    )
    orders = _odata_batch_by_ref(
        session,
        base,
        "Document_ЗаказКлиента",
        {
            r.get("ЗаказКлиента_Key") or ""
            for r in rows
            if r.get("ЗаказКлиента_Key") not in ("", None, _EMPTY_GUID)
        },
        "Ref_Key,Number,Партнер_Key,Валюта_Key,Соглашение_Key,ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи",
        label="Dog/SignedOrders",
    )
    order_agreements = _odata_batch_by_ref(
        session,
        base,
        "Catalog_СоглашенияСКлиентами",
        {o.get("Соглашение_Key") or "" for o in orders.values()},
        "Ref_Key,Description,ТД_СчетОферта",
        label="Dog/OrderAgr",
    )

    name_by_bin = _dept_name_by_bin()
    out: dict[str, float] = {}
    for r in rows:
        if (specs.get(r.get("Спецификация_Key") or "") or {}).get("Статус") != "Действует":
            continue
        dept_key = r.get("Подразделение_Key") or _EMPTY_GUID
        if dept_key == _EMPTY_GUID:
            continue
        try:
            dept_name = name_by_bin.get(guid_to_1c_bytes(dept_key))
        except Exception:
            dept_name = None
        if not dept_name:
            continue

        partner = r.get("Партнер_Key") or _EMPTY_GUID
        sopr_reg = bool(r.get("ТД_СопровождениеПродажи"))
        if opbo and dept_key == opbo:
            if partner in resale_nomgs:
                continue
        else:
            if partner in resale_nomgs and not sopr_reg:
                continue

        ok = r.get("ЗаказКлиента_Key") or _EMPTY_GUID
        rate = 1.0
        od = {}
        if ok != _EMPTY_GUID:
            od = orders.get(ok) or {}
            if od.get("ТД_НеУчитыватьВПланФакте"):
                continue
            op = od.get("Партнер_Key") or _EMPTY_GUID
            # Сопровождение в отчёте 1С остаётся только у МГС.
            if od.get("ТД_СопровождениеПродажи") and op not in (resale - resale_nomgs):
                continue
            # Перепродажа без МГС — во всех отделах. МГС в факте 1С остаётся.
            if op in resale_nomgs:
                continue
            rate = float(cur_map.get(od.get("Валюта_Key") or "", 1.0) or 1.0)

        amt = float(r.get("СуммаДоговора") or 0) * rate
        out[dept_name] = out.get(dept_name, 0.0) + amt
        # Отчёт показывает сумму дважды, когда в регистре ещё счёт-оферта,
        # а заказ уже переведён на типовой договор.
        order_agr = od.get("Соглашение_Key") or ""
        spec = specs.get(r.get("Спецификация_Key") or "") or {}
        current = order_agreements.get(order_agr) or {}
        spec_is_offer = bool(spec.get("ТД_СчетОферта")) or "оферт" in (spec.get("Description") or "").lower()
        current_is_typical = (current.get("Description") or "").lower().startswith("типовое")
        doubled = bool(
            order_agr and spec_is_offer and current_is_typical
            and order_agr != (r.get("Спецификация_Key") or "")
        )
        if doubled:
            out[dept_name] = out.get(dept_name, 0.0) + amt
        from comdir.plan_fact_sink import note

        number = (od.get("Number") or "").strip()
        spec_name = (spec.get("Description") or "").strip()
        document = spec_name or (("Заказ клиента " + number) if number else "Договор")
        note(
            "dog",
            "Факт",
            amt * (2 if doubled else 1),
            date=(r.get("ДатаПодписания") or "")[:10],
            department=dept_name,
            partner_key=partner,
            document=document,
        )

    return {k: round(v, 2) for k, v in out.items()}


def calc_fact_sql(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Факт договоров из SQL erp_pm (fallback, если OData недоступна)."""
    from comdir.resale import ORDER_SOPR_FIELD

    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#fact_depts")
    load_resale(cur)

    amt = fx_sql("s._Fld112283", "ord._Fld21185RRef")
    cur.execute(
        f"""
        SELECT d.name, SUM({amt}) AS FactSum
        FROM _InfoRg112278 s WITH (NOLOCK)
        INNER JOIN #fact_depts d ON d.id = s._Fld112280RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = s._Fld112279RRef
        LEFT JOIN _Document704 ord WITH (NOLOCK)
          ON ord._IDRRef = s._Fld112481RRef
        WHERE s._Fld112284 >= ? AND s._Fld112284 < ?
          AND s._Fld112280RRef <> ?
          AND a._Fld13714RRef = ?
          AND (
                s._Fld112481RRef = ?
                OR (
                     ISNULL(ord._Fld184301, 0x00) = 0x00
                     AND (
                          ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                          OR (
                            EXISTS (
                              SELECT 1 FROM #resale r
                              WHERE r.id = ord._Fld21180RRef
                            )
                            AND NOT EXISTS (
                              SELECT 1 FROM #resale_nomgs r
                              WHERE r.id = ord._Fld21180RRef
                            )
                          )
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM #resale_nomgs r WHERE r.id = ord._Fld21180RRef
                     )
                   )
              )
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = s._Fld112280RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = s._Fld112282RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = s._Fld112282RRef
                    ) AND ISNULL(s._Fld123477, 0x00) = 0x00
                    THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        *fx_params(),
        p0,
        p_next,
        EMPTY16,
        AG_STATUS_ACTIVE,
        EMPTY16,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def _odata_period_str(p: datetime) -> str:
    return f"{p.year - YEAR_OFFSET:04d}-{p.month:02d}-{p.day:02d}"


_EMPTY_GUID = "00000000-0000-0000-0000-000000000000"
_SIGNED_REG_CACHE: list[dict] | None = None

# ОДП: «Владикавказ» в ветке дозаказа/оферты отчёт 1С не берёт
# (в регистре подписанных — берёт; поэтому сюда, не в calc_fact_odata).
VLADIKAVKAZ_PARTNER = "237a2c5f-3b94-11e7-812b-001e67112509"


_SPEC_CONTRACT_DT = re.compile(r"от\s+\d{2}\.\d{2}\.(\d{2,4})")


def _spec_contract_year(desc: str) -> int | None:
    m = _SPEC_CONTRACT_DT.search(desc or "")
    if not m:
        return None
    y = int(m.group(1))
    return y + 2000 if y < 100 else y


def _opbo_skip_partner(
    partner: str,
    *,
    dept_key: str,
    opbo: str | None,
    resale: set[str],
    resale_nomgs: set[str],
    extra_opbo_exclude: frozenset[str] | set[str] | None = None,
) -> bool:
    """True — строку не брать. extra_opbo_exclude — доп. партнёры только для ОДП."""
    extra = extra_opbo_exclude or ()
    if opbo and dept_key == opbo:
        return partner in resale_nomgs or partner in extra
    return partner in resale_nomgs


def _odata_json(session, url: str, timeout: int = 120, label: str = "dogovory") -> list[dict]:
    from getkpi.odata_http import request_with_retry

    r = request_with_retry(session, url, timeout=timeout, retries=3, label=label)
    if r is None or not r.ok:
        return []
    try:
        return r.json().get("value") or []
    except Exception:
        return []


def _odata_batch_by_ref(
    session,
    base: str,
    entity: str,
    keys: set[str],
    select: str,
    label: str = "dogovory",
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    keys_l = sorted({k for k in keys if k and k != _EMPTY_GUID})
    for i in range(0, len(keys_l), 15):
        batch = keys_l[i : i + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{base}/{quote(entity)}?$format=json&$filter={flt}"
            f"&$select={quote(select, safe=',_')}&$top={len(batch)}"
        )
        for it in _odata_json(session, url, timeout=45, label=label):
            if it.get("Ref_Key"):
                out[it["Ref_Key"]] = it
    return out


def load_signed_register_odata() -> list[dict]:
    """Все строки ТД_ДоговорыПодписанные.

    Регистр независимый (поля Period нет): ключ — измерения, ДатаПодписания — ресурс.
    Отбор ``ДатаПодписания ge/lt datetime'...'`` на этой базе проходит, но для
    дозаказа нужны все строки (заказ в регистре вообще, соглашение подписано
    до начала месяца), поэтому грузим целиком и режем период в Python.
    """
    global _SIGNED_REG_CACHE
    if _SIGNED_REG_CACHE is not None:
        return _SIGNED_REG_CACHE
    from comdir.resale import _base, _session

    session = _session()
    base = _base()
    entity = quote("InformationRegister_ТД_ДоговорыПодписанные")
    select = quote(
        "Спецификация_Key,Подразделение_Key,Партнер_Key,ЗаказКлиента_Key,"
        "СуммаДоговора,ДатаПодписания,ТД_СопровождениеПродажи",
        safe=",",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = f"{base}/{entity}?$format=json&$top=5000&$skip={skip}&$select={select}"
        batch = _odata_json(session, url, timeout=120, label="Dog/SignedReg")
        rows.extend(batch)
        if len(batch) < 5000:
            break
        skip += 5000
    _SIGNED_REG_CACHE = rows
    return rows


def _odata_resale_guid_sets(session, base: str) -> tuple[set[str], set[str], str | None]:
    """(перепродажа, перепродажа без МГС, guid ОПБО)."""
    from comdir.resale import (
        PREDEFINED_MGS_REF,
        PREDEFINED_OPBO_REF,
        PREDEFINED_RESALE_REF,
    )

    resale: set[str] = set()
    flt = quote(f"Ref_Key eq guid'{PREDEFINED_RESALE_REF}'", safe="")
    url = (
        f"{base}/Catalog_ТД_ПредопределенныеЗначения_ДополнительныеЗначения"
        f"?$format=json&$filter={flt}&$select=Значение,Значение_Type&$top=5000"
    )
    for row in _odata_json(session, url, timeout=45, label="Dog/Resale"):
        val = row.get("Значение")
        if val and "Catalog_Партнеры" in str(row.get("Значение_Type") or ""):
            resale.add(val)
    flt_m = quote(f"Ref_Key eq guid'{PREDEFINED_MGS_REF}'", safe="")
    mgs = (
        (_odata_json(
            session,
            f"{base}/Catalog_ТД_ПредопределенныеЗначения?$format=json"
            f"&$filter={flt_m}&$select=Значение,Значение_Type&$top=1",
            timeout=30,
            label="Dog/MGS",
        ) or [{}])[0].get("Значение")
    )
    if mgs:
        resale.add(mgs)
    resale_nomgs = set(resale) - ({mgs} if mgs else set())
    flt_o = quote(f"Ref_Key eq guid'{PREDEFINED_OPBO_REF}'", safe="")
    opbo = (
        (_odata_json(
            session,
            f"{base}/Catalog_ТД_ПредопределенныеЗначения?$format=json"
            f"&$filter={flt_o}&$select=Значение,Значение_Type&$top=1",
            timeout=30,
            label="Dog/OPBO",
        ) or [{}])[0].get("Значение")
    )
    return resale, resale_nomgs, opbo or None


_PAYMENT_RECORDER_MARKERS = (
    "Document_ПоступлениеБезналичныхДенежныхСредств",
    "Document_ПоступлениеНаличныхДенежныхСредств",
    "Document_ОперацияПоПлатежнойКарте",
    "Document_ПриходныйКассовыйОрдер",
)


_PAY_ODATA_CACHE: dict[tuple[str, str], dict[str, float]] = {}


def _fetch_settlement_payments_odata(p0: datetime, p_next: datetime) -> dict[str, float]:
    """ОбъектРасчетов_Key → сумма оплат (СуммаРегл) из live OData за период.

    SQL-копия ``_AccumRg51416`` часто отстаёт на последние дни; для ветки
    счёт-оферта берём поступления из живого регистра РасчетыСКлиентами.
    """
    from comdir.resale import _base, _session

    d0 = _odata_period_str(p0)
    d1 = _odata_period_str(p_next)
    cache_key = (d0, d1)
    if cache_key in _PAY_ODATA_CACHE:
        return _PAY_ODATA_CACHE[cache_key]

    session = _session()
    base = _base()
    entity = quote("AccumulationRegister_РасчетыСКлиентами_RecordType")
    # startswith(Recorder_Type, ...) в этой 1С OData даёт 500 — фильтруем в Python.
    flt = quote(
        f"Period ge datetime'{d0}T00:00:00' and Period lt datetime'{d1}T00:00:00' and "
        f"СуммаРегл gt 0"
    )
    select = quote("ОбъектРасчетов_Key,СуммаРегл,Recorder_Type", safe=",")
    by_obj: dict[str, float] = {}
    skip = 0
    while True:
        url = (
            f"{base}/{entity}?$format=json&$top=1000&$skip={skip}"
            f"&$select={select}&$filter={flt}"
        )
        raw = session.get(url, timeout=120)
        try:
            payload = raw.json()
        except Exception:
            payload = json.loads((raw.content or b"").decode("utf-8-sig") or "{}")
        batch = (payload or {}).get("value") or []
        for row in batch:
            rtype = str(row.get("Recorder_Type") or "")
            if not any(m in rtype for m in _PAYMENT_RECORDER_MARKERS):
                continue
            obj = row.get("ОбъектРасчетов_Key") or ""
            if not obj or obj.startswith("00000000"):
                continue
            by_obj[obj] = by_obj.get(obj, 0.0) + float(row.get("СуммаРегл") or 0)
        if len(batch) < 1000:
            break
        skip += 1000
    _PAY_ODATA_CACHE[cache_key] = by_obj
    return by_obj


def calc_fact_offer_sql(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Счёт-оферта факт из SQL erp_pm (может отставать по оплатам)."""
    from comdir.resale import ORDER_SOPR_FIELD

    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#offer_fact_depts")
    load_resale(cur)
    cur.execute(
        f"""
        SELECT d.name, SUM(pay.pay_amt) AS FactSum
        FROM _Document704 ord WITH (NOLOCK)
        INNER JOIN #offer_fact_depts d ON d.id = ord._Fld21220RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = ord._Fld21183RRef
        INNER JOIN (
          SELECT obj._Fld138162_RRRef AS ord_id,
                 SUM(
                   CASE WHEN c._Fld51417RRef = ? THEN -c._Fld51434 ELSE c._Fld51434 END
                 ) AS pay_amt
          FROM _AccumRg51416 c WITH (NOLOCK)
          INNER JOIN _Reference134945 obj WITH (NOLOCK)
            ON obj._IDRRef = c._Fld140225_RRRef
           AND obj._Fld138162_RTRef = ?
          WHERE c._Period >= ? AND c._Period < ?
            AND c._Active = 0x01
            AND ISNULL(c._Fld140228, 0x00) = 0x00
          GROUP BY obj._Fld138162_RRRef
          HAVING SUM(
            CASE WHEN c._Fld51417RRef = ? THEN -c._Fld51434 ELSE c._Fld51434 END
          ) > 0
        ) pay ON pay.ord_id = ord._IDRRef
        WHERE a.[{AG_OFFER_FLAG}] = 0x01
          AND a._Fld13714RRef = ?
          AND ord._Posted = 0x01
          AND ISNULL(ord._Fld184301, 0x00) = 0x00
          AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
          AND ord.[{ORDER_STATUS_FIELD}] <> ?
          AND NOT EXISTS (
            SELECT 1 FROM _InfoRg112278 s WITH (NOLOCK)
            WHERE s._Fld112481RRef = ord._IDRRef
          )
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = ord._Fld21220RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        RET_OP,
        ORDER_TREF,
        p0,
        p_next,
        RET_OP,
        AG_STATUS_ACTIVE,
        ORDER_STATUS_NOT_AGREED,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_fact_offer_odata(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Счёт-оферта факт по live-оплатам OData, отфильтрованным SQL-условиями заказа."""
    from comdir.resale import ORDER_SOPR_FIELD, guid_to_1c_bytes

    pay_by_obj = _fetch_settlement_payments_odata(p0, p_next)
    if not pay_by_obj:
        return {}

    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#offer_fact_depts")
    load_resale(cur)

    cur.execute("IF OBJECT_ID('tempdb..#pay_obj') IS NOT NULL DROP TABLE #pay_obj")
    cur.execute("CREATE TABLE #pay_obj (id binary(16) PRIMARY KEY, amt float)")
    for obj_guid, amt in pay_by_obj.items():
        try:
            cur.execute(
                "INSERT INTO #pay_obj(id, amt) VALUES (?, ?)",
                guid_to_1c_bytes(obj_guid),
                float(amt),
            )
        except Exception:
            continue

    cur.execute(
        f"""
        SELECT d.name, SUM(p.amt) AS FactSum
        FROM #pay_obj p
        INNER JOIN _Reference134945 obj WITH (NOLOCK)
          ON obj._IDRRef = p.id
         AND obj._Fld138162_RTRef = ?
        INNER JOIN _Document704 ord WITH (NOLOCK)
          ON ord._IDRRef = obj._Fld138162_RRRef
        INNER JOIN #offer_fact_depts d ON d.id = ord._Fld21220RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = ord._Fld21183RRef
        WHERE a.[{AG_OFFER_FLAG}] = 0x01
          AND a._Fld13714RRef = ?
          AND ord._Posted = 0x01
          AND ISNULL(ord._Fld184301, 0x00) = 0x00
          AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
          AND ord.[{ORDER_STATUS_FIELD}] <> ?
          AND NOT EXISTS (
            SELECT 1 FROM _InfoRg112278 s WITH (NOLOCK)
            WHERE s._Fld112481RRef = ord._IDRRef
          )
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = ord._Fld21220RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        ORDER_TREF,
        AG_STATUS_ACTIVE,
        ORDER_STATUS_NOT_AGREED,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_fact_offer_live_odata(p0: datetime, p_next: datetime) -> dict[str, float]:
    """Счёт-оферта по live-флагу соглашения. SQL-копия _Fld13700 часто врёт."""
    from comdir.resale import _base, _session, guid_to_1c_bytes

    pay_by_obj = _fetch_settlement_payments_odata(p0, p_next)
    if not pay_by_obj:
        return {}
    session = _session()
    base = _base()
    signed_any = {
        r.get("ЗаказКлиента_Key") or ""
        for r in load_signed_register_odata()
        if (r.get("ЗаказКлиента_Key") or "") not in ("", _EMPTY_GUID)
    }
    objs = _odata_batch_by_ref(
        session, base, "Catalog_ОбъектыРасчетов", set(pay_by_obj),
        "Ref_Key,Объект,Объект_Type", label="Dog/OfferObj",
    )
    order_keys = {
        o.get("Объект") or ""
        for o in objs.values()
        if o.get("Объект") and "Document_ЗаказКлиента" in str(o.get("Объект_Type") or "")
    }
    orders = _odata_batch_by_ref(
        session, base, "Document_ЗаказКлиента", order_keys,
        "Ref_Key,Posted,Статус,Подразделение_Key,Партнер_Key,Соглашение_Key,"
        "ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи",
        label="Dog/OfferLiveOrders",
    )
    agreements = _odata_batch_by_ref(
        session, base, "Catalog_СоглашенияСКлиентами",
        {o.get("Соглашение_Key") or "" for o in orders.values()},
        "Ref_Key,ТД_СчетОферта,Статус",
        label="Dog/OfferLiveAgr",
    )
    resale, resale_nomgs, opbo = _odata_resale_guid_sets(session, base)
    name_by_bin = _dept_name_by_bin()
    out: dict[str, float] = {}
    for obj_key, amt in pay_by_obj.items():
        obj = objs.get(obj_key) or {}
        if "Document_ЗаказКлиента" not in str(obj.get("Объект_Type") or ""):
            continue
        order_key = obj.get("Объект") or ""
        if not order_key or order_key in signed_any:
            continue
        order = orders.get(order_key)
        if not order or not order.get("Posted"):
            continue
        if (order.get("Статус") or "") == "НеСогласован":
            continue
        if order.get("ТД_НеУчитыватьВПланФакте") or order.get("ТД_СопровождениеПродажи"):
            continue
        agr = agreements.get(order.get("Соглашение_Key") or "") or {}
        if not agr.get("ТД_СчетОферта"):
            continue
        if (agr.get("Статус") or "") != "Действует":
            continue
        dept_key = order.get("Подразделение_Key") or _EMPTY_GUID
        try:
            dept_name = name_by_bin.get(guid_to_1c_bytes(dept_key))
        except Exception:
            dept_name = None
        if not dept_name:
            continue
        partner = order.get("Партнер_Key") or _EMPTY_GUID
        if _opbo_skip_partner(
            partner,
            dept_key=dept_key,
            opbo=opbo,
            resale=resale,
            resale_nomgs=resale_nomgs,
            extra_opbo_exclude={VLADIKAVKAZ_PARTNER},
        ):
            continue
        out[dept_name] = out.get(dept_name, 0.0) + float(amt or 0)
        from comdir.plan_fact_sink import note

        note(
            "dog",
            "Факт",
            amt,
            department=dept_name,
            partner_key=partner,
            document="Счёт-оферта",
        )
    return {k: round(v, 2) for k, v in out.items()}


def calc_fact_offer(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Ветка счёт-оферта факта: live OData-флаг, иначе SQL."""
    try:
        return calc_fact_offer_live_odata(p0, p_next)
    except Exception:
        logger.exception("OData счёт-оферта недоступна — SQL fallback")
        return calc_fact_offer_sql(cur, p0, p_next)


def calc_fact_reorder_odata(p0: datetime, p_next: datetime) -> dict[str, float]:
    """Дозаказ: оплата по уже подписанному соглашению, как в отчёте «План-факт».

    Типовое: оплата за период. Если заказ уже в регистре за месяц — не
    дублируем ту же сумму (разные суммы в отчёте 1С складываются).
    Без строки в регистре берём только рамку с СуммаДокумента > 0.

    Спецификация: заказа нет в регистре. 1× уже подписанная не повторяется;
    новая 1× — оплаты за период (в т.ч. частичные). 12× — дата заказа в
    периоде и полная оплата. Сезон / некратное — только деньги и отгрузки.
    """
    from comdir.resale import _base, _session, guid_to_1c_bytes

    pay_by_obj = _fetch_settlement_payments_odata(p0, p_next)
    if not pay_by_obj:
        return {}

    session = _session()
    base = _base()
    d0 = _odata_period_str(p0)
    d1 = _odata_period_str(p_next)
    orders_in_period: set[str] = set()
    order_period_amt: dict[str, float] = {}
    signed_any: set[str] = set()
    agr_prior: set[str] = set()
    # Годовой ТО: в регистре либо пустой заказ, либо подписание в декабре.
    # Ежемесячный повтор (БТК / Котельщик) подписан обычным заказом — факт 0.
    spec_allow_12x: set[str] = set()
    spec_12x_cy_order: set[str] = set()
    fact_year = p0.year - YEAR_OFFSET
    y = f"{fact_year:04d}"
    for r in load_signed_register_odata():
        dt = (r.get("ДатаПодписания") or "")[:10]
        ok = r.get("ЗаказКлиента_Key") or ""
        sk = r.get("Спецификация_Key") or ""
        if ok and ok != _EMPTY_GUID:
            signed_any.add(ok)
            if d0 <= dt < d1:
                orders_in_period.add(ok)
                try:
                    order_period_amt[ok] = order_period_amt.get(ok, 0.0) + float(
                        r.get("СуммаДоговора") or 0
                    )
                except (TypeError, ValueError):
                    pass
        if dt and dt < d0 and sk and sk != _EMPTY_GUID:
            agr_prior.add(sk)
        if sk and sk != _EMPTY_GUID:
            empty_ord = (not ok) or ok == _EMPTY_GUID
            if empty_ord or (len(dt) >= 7 and dt[5:7] == "12"):
                spec_allow_12x.add(sk)
            elif dt[:4] == y and len(dt) >= 7 and dt[5:7] >= "03" and not empty_ord:
                spec_12x_cy_order.add(sk)

    objs = _odata_batch_by_ref(
        session,
        base,
        "Catalog_ОбъектыРасчетов",
        set(pay_by_obj),
        "Ref_Key,Объект,Объект_Type",
        label="Dog/ReorderObj",
    )
    order_keys = {
        o.get("Объект") or ""
        for o in objs.values()
        if o.get("Объект") and "Document_ЗаказКлиента" in str(o.get("Объект_Type") or "")
    }
    orders = _odata_batch_by_ref(
        session,
        base,
        "Document_ЗаказКлиента",
        order_keys,
        "Ref_Key,Number,Date,Posted,Статус,СуммаДокумента,Подразделение_Key,Партнер_Key,"
        "Соглашение_Key,ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи",
        label="Dog/ReorderOrders",
    )
    agreements = _odata_batch_by_ref(
        session,
        base,
        "Catalog_СоглашенияСКлиентами",
        {o.get("Соглашение_Key") or "" for o in orders.values()},
        "Ref_Key,Description,ТД_СчетОферта,Статус,СуммаДокумента",
        label="Dog/ReorderAgr",
    )
    resale, resale_nomgs, opbo = _odata_resale_guid_sets(session, base)
    name_by_bin = _dept_name_by_bin()
    out: dict[str, float] = {}
    for obj_key, amt in pay_by_obj.items():
        obj = objs.get(obj_key) or {}
        if "Document_ЗаказКлиента" not in str(obj.get("Объект_Type") or ""):
            continue
        order_key = obj.get("Объект") or ""
        if not order_key:
            continue
        order = orders.get(order_key)
        if not order or not order.get("Posted"):
            continue
        if (order.get("Статус") or "") == "НеСогласован":
            continue
        if order.get("ТД_НеУчитыватьВПланФакте") or order.get("ТД_СопровождениеПродажи"):
            continue
        agr_key = order.get("Соглашение_Key") or ""
        agr = agreements.get(agr_key) or {}
        if agr.get("ТД_СчетОферта"):
            continue
        if (agr.get("Статус") or "") != "Действует":
            continue
        desc = (agr.get("Description") or "").strip()
        is_typical = desc.lower().startswith("типовое")
        if is_typical:
            if order_key in orders_in_period:
                signed_amt = float(order_period_amt.get(order_key) or 0)
                if abs(float(amt or 0) - signed_amt) <= 1:
                    continue
            elif agr_key not in agr_prior:
                low = desc.lower()
                if "дилер" not in low and "дистрибьютор" not in low:
                    continue
        else:
            if order_key in signed_any:
                continue
            doc_amt = float(order.get("СуммаДокумента") or 0)
            agr_amt = float(agr.get("СуммаДокумента") or 0)
            if not doc_amt or not agr_amt:
                continue
            ratio = agr_amt / doc_amt
            order_dt = (order.get("Date") or "")[:10]
            if abs(ratio - 12) <= 0.02:
                allowed_12x = agr_key in spec_allow_12x
                if not allowed_12x and agr_key in spec_12x_cy_order:
                    cy = _spec_contract_year(desc)
                    allowed_12x = cy is not None and cy >= fact_year - 1
                if agr_key not in agr_prior or not allowed_12x:
                    continue
                if not (d0 <= order_dt < d1):
                    continue
                if abs(float(amt or 0) - doc_amt) > 1:
                    continue
            elif abs(ratio - 1) <= 0.02 and agr_amt <= 20000:
                # Новая мелкая 1× спецификация (ЦСМ Тверской, Бумфа): оплаты,
                # в т.ч. частичные. Крупные 1× без регистра в отчёте 1С — 0.
                if p0.month == 1:
                    lookback = p0.replace(year=p0.year - 1, month=12, day=1)
                else:
                    lookback = p0.replace(month=p0.month - 1, day=1)
                if not (_odata_period_str(lookback) <= order_dt < d1):
                    continue
            elif (
                agr_key not in agr_prior
                and abs(ratio - 6) <= 0.02
                and abs(float(amt or 0) - doc_amt) <= 1
                and d0 <= order_dt < d1
            ):
                # Первая 6× спецификация без регистра (ДТС) — в отчёте в факт.
                pass
            else:
                continue
        dept_key = order.get("Подразделение_Key") or _EMPTY_GUID
        try:
            dept_name = name_by_bin.get(guid_to_1c_bytes(dept_key))
        except Exception:
            dept_name = None
        if not dept_name:
            continue
        partner = order.get("Партнер_Key") or _EMPTY_GUID
        if _opbo_skip_partner(
            partner,
            dept_key=dept_key,
            opbo=opbo,
            resale=resale,
            resale_nomgs=resale_nomgs,
            extra_opbo_exclude={VLADIKAVKAZ_PARTNER},
        ):
            continue
        out[dept_name] = out.get(dept_name, 0.0) + float(amt or 0)
        from comdir.plan_fact_sink import note

        number = (order.get("Number") or "").strip()
        note(
            "dog",
            "Факт",
            amt,
            date=(order.get("Date") or "")[:10],
            department=dept_name,
            partner_key=partner,
            document=("Заказ клиента " + number) if number else "Дозаказ",
        )
    return {k: round(v, 2) for k, v in out.items()}


def _merge_fact(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
    out = dict(a)
    for name, val in b.items():
        out[name] = round(out.get(name, 0.0) + float(val or 0), 2)
    return out


def calc_fact(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Договоры заключённые (факт) = регистр + счёт-оферта + дозаказ.

    Все месяцы: live OData и ветка дозаказа (как отчёт «План-факт»).
    SQL — только fallback, если OData недоступен.
    """
    try:
        reg = calc_fact_odata(p0, p_next)
    except Exception:
        logger.exception("OData факт договоров недоступен — SQL fallback")
        try:
            reg = calc_fact_sql(cur, p0, p_next)
        except Exception:
            logger.exception("SQL факт договоров тоже недоступен")
            reg = {}
    try:
        offer = calc_fact_offer_live_odata(p0, p_next)
    except Exception:
        logger.exception("Ветка счёт-оферта факта договоров недоступна")
        offer = {}
    try:
        reorder = calc_fact_reorder_odata(p0, p_next)
    except Exception:
        logger.exception("Ветка дозаказа факта договоров недоступна")
        reorder = {}
    return _merge_fact(_merge_fact(reg, offer), reorder)


_DEPT_GUID_TO_NAME = {
    "bd7b5184-9f9c-11e4-80da-001e67112509": "Отдел по работе с ПАО Газпром",
    "7587c178-92f6-11f0-96f9-6cb31113810e": "Отдел дилерских продаж",
    "639ec87b-67b6-11eb-8523-ac1f6b05524d": "Отдел по работе с ключевыми клиентами",
    "34497ef7-810f-11e4-80d6-001e67112509": "Отдел продаж эталонного оборудования и услуг",
    "49480c10-e401-11e8-8283-ac1f6b05524d": "Отдел внешнеэкономической деятельности",
    "9edaa7d4-37a5-11ee-93d3-6cb31113810e": "Отдел продаж БМИ",
}
_KP_STATUS_BAD = {"Черновик", "НеСогласовано", "Аннулировано", "Отменено", "Не согласовано"}
_EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


def _odata_stamp(dt: datetime) -> str:
    return f"{dt.year - YEAR_OFFSET:04d}-{dt.month:02d}-{dt.day:02d}T00:00:00"


def _odata_get_pages(session, url: str) -> list[dict]:
    rows: list[dict] = []
    skip = 0
    while True:
        response = session.get(f"{url}&$top=500&$skip={skip}", timeout=120)
        response.raise_for_status()
        batch = response.json().get("value") or []
        rows.extend(batch)
        if len(batch) < 500:
            break
        skip += len(batch)
    return rows


def _odata_batch(session, base: str, entity: str, keys: set[str], select: str) -> dict[str, dict]:
    from urllib.parse import quote

    out: dict[str, dict] = {}
    clean = [k for k in keys if k and k != _EMPTY_GUID]
    for i in range(0, len(clean), 15):
        part = clean[i:i + 15]
        flt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in part), safe="")
        url = (
            f"{base}/{quote(entity)}?$format=json&$filter={flt}"
            f"&$select={quote(select, safe=',_')}&$top={len(part)}"
        )
        response = session.get(url, timeout=60)
        response.raise_for_status()
        for row in response.json().get("value") or []:
            if row.get("Ref_Key"):
                out[row["Ref_Key"]] = row
    return out


def _resale_bins():
    from comdir.resale import _fetch_from_odata

    resale, mgs, opbo = _fetch_from_odata()
    nomgs = [b for b in resale if b != mgs]
    return resale, nomgs, opbo


def _keep_partner(dept_guid: str, partner_guid: str, sopr: bool, resale, nomgs, opbo) -> bool:
    from comdir.common import uuid_to_1c_bytes

    try:
        partner = uuid_to_1c_bytes(partner_guid) if partner_guid else b""
        dept_bin = uuid_to_1c_bytes(dept_guid) if dept_guid else b""
    except Exception:
        return False
    if opbo and dept_bin == opbo:
        return partner not in nomgs
    return not (partner in resale and not sopr)


def calc_expected_potential_odata(p0: datetime, p_asof: datetime) -> dict[str, float]:
    """Потенциальные договоры из живого регистра. SQL-копия отстаёт на новые КП."""
    from urllib.parse import quote

    import requests
    from requests.auth import HTTPBasicAuth

    base = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
    session = requests.Session()
    session.auth = HTTPBasicAuth("odata.user", "npo852456")
    resale, nomgs, opbo = _resale_bins()
    flt = quote(
        f"ДатаПодписанияПлан ge datetime'{_odata_stamp(p0)}' and "
        f"ДатаПодписанияПлан lt datetime'{_odata_stamp(p_asof)}'",
        safe="",
    )
    sel = quote(
        "КоммерческоеПредложение,Подразделение_Key,Партнер_Key,ЗаказКлиента_Key,СуммаДоговора",
        safe=",_",
    )
    rows = _odata_get_pages(
        session,
        f"{base}/{quote('InformationRegister_ТД_ДоговорыПотенциальные')}"
        f"?$format=json&$filter={flt}&$select={sel}",
    )
    props = _odata_batch(
        session,
        base,
        "Document_КоммерческоеПредложениеКлиенту",
        {row.get("КоммерческоеПредложение") or "" for row in rows},
        "Ref_Key,Number,Статус,ТД_ОсновноеТКПДляБМИ,ТД_СуммаТКПБМИ",
    )
    out: dict[str, float] = {}
    for row in rows:
        dept = _DEPT_GUID_TO_NAME.get(row.get("Подразделение_Key") or "")
        if not dept or dept == "Отдел продаж БМИ":
            continue
        if (row.get("ЗаказКлиента_Key") or _EMPTY_GUID) != _EMPTY_GUID:
            continue
        prop = props.get(row.get("КоммерческоеПредложение") or "")
        if not prop or prop.get("Статус") in _KP_STATUS_BAD:
            continue
        if not _keep_partner(
            row.get("Подразделение_Key") or "",
            row.get("Партнер_Key") or "",
            False,
            resale,
            nomgs,
            opbo,
        ):
            continue
        amount = float(row.get("СуммаДоговора") or 0)
        if prop.get("ТД_ОсновноеТКПДляБМИ"):
            amount = float(prop.get("ТД_СуммаТКПБМИ") or 0)
        out[dept] = out.get(dept, 0.0) + amount
        from comdir.plan_fact_sink import note

        kp_number = (prop.get("Number") or "").strip()
        note(
            "dog",
            "Ожидаемо",
            amount,
            date=(row.get("ДатаПодписанияПлан") or "")[:10],
            department=dept,
            partner_key=row.get("Партнер_Key") or "",
            document=("Коммерческое предложение " + kp_number) if kp_number else "Потенциальный договор",
        )
    return {k: round(v, 2) for k, v in out.items()}


def calc_expected_offer_live(p0: datetime, p_next: datetime) -> dict[str, float]:
    """Счёт-оферты, которых ещё нет в SQL-копии (заказ создан в последние дни)."""
    from urllib.parse import quote

    import requests
    from requests.auth import HTTPBasicAuth

    base = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
    session = requests.Session()
    session.auth = HTTPBasicAuth("odata.user", "npo852456")
    resale, nomgs, opbo = _resale_bins()
    dept_filter = " or ".join(
        f"Подразделение_Key eq guid'{g}'" for g in _DEPT_GUID_TO_NAME if g != "9edaa7d4-37a5-11ee-93d3-6cb31113810e"
    )
    flt = quote(
        f"Date ge datetime'{_odata_stamp(p0)}' and Date lt datetime'{_odata_stamp(p_next)}' "
        f"and Posted eq true and ({dept_filter})",
        safe="",
    )
    sel = quote(
        "Ref_Key,Number,СуммаДокумента,Подразделение_Key,Партнер_Key,Соглашение_Key,Статус,"
        "ТД_ПредполагаемаяДатаАванса,ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи",
        safe=",_",
    )
    orders = _odata_get_pages(
        session,
        f"{base}/Document_ЗаказКлиента?$format=json&$filter={flt}&$select={sel}",
    )
    agreements = _odata_batch(
        session,
        base,
        "Catalog_СоглашенияСКлиентами",
        {row.get("Соглашение_Key") or "" for row in orders},
        "Ref_Key,Description,ТД_СчетОферта",
    )
    numbers = sorted({
        (row.get("Number") or "").strip()
        for row in orders
        if (row.get("Number") or "").strip()
    })
    in_sql: set[str] = set()
    if numbers:
        cn = connect()
        try:
            cur = cn.cursor()
            for i in range(0, len(numbers), 80):
                part = numbers[i:i + 80]
                marks = ",".join("?" * len(part))
                cur.execute(
                    f"""
                    SELECT _Number FROM _Document704 WITH (NOLOCK)
                    WHERE _Date_Time >= ? AND _Date_Time < ?
                      AND _Number IN ({marks})
                    """,
                    p0,
                    p_next,
                    *part,
                )
                in_sql.update((r[0] or "").strip() for r in cur.fetchall())
        finally:
            cn.close()

    out: dict[str, float] = {}
    for row in orders:
        number = (row.get("Number") or "").strip()
        if not number or number in in_sql:
            continue
        if (row.get("Статус") or "") in {"НеСогласован", "Черновик", "Отменен", "Отменён"}:
            continue
        advance = (row.get("ТД_ПредполагаемаяДатаАванса") or "")[:10]
        if advance and advance > "0001-01-01" and advance < _odata_stamp(p0)[:10]:
            continue
        if row.get("ТД_НеУчитыватьВПланФакте") or row.get("ТД_СопровождениеПродажи"):
            continue
        dept = _DEPT_GUID_TO_NAME.get(row.get("Подразделение_Key") or "")
        if not dept or dept == "Отдел продаж БМИ":
            continue
        agr = agreements.get(row.get("Соглашение_Key") or "") or {}
        desc = (agr.get("Description") or "").lower()
        if not agr.get("ТД_СчетОферта") or "оферт" not in desc:
            continue
        if not _keep_partner(
            row.get("Подразделение_Key") or "",
            row.get("Партнер_Key") or "",
            False,
            resale,
            nomgs,
            opbo,
        ):
            continue
        amount = float(row.get("СуммаДокумента") or 0)
        out[dept] = out.get(dept, 0.0) + amount
        from comdir.plan_fact_sink import note

        note(
            "dog",
            "Ожидаемо",
            amount,
            date=(row.get("ТД_ПредполагаемаяДатаАванса") or row.get("Date") or "")[:10],
            department=dept,
            partner_key=row.get("Партнер_Key") or "",
            document=("Заказ клиента " + number) if number else "Счёт-оферта",
        )
    return {k: round(v, 2) for k, v in out.items()}


def calc_expected_potential(
    cur,
    p0: datetime,
    p_next: datetime,
    p_asof: datetime | None = None,
) -> dict[str, float]:
    """Ветка потенциальных КП (ТД_ДоговорыПотенциальные)."""
    p_asof = p_asof or p_next
    try:
        return calc_expected_potential_odata(p0, p_asof)
    except Exception:
        logger.exception("OData потенциальные договоры недоступны — SQL")
    load_depts(cur, COMMERCIAL_DEPTS, "#exp_depts")
    load_resale(cur)

    amt_raw = f"""
      CASE WHEN ISNULL(kp.[{KP_BMI_FLAG}], 0x00) = 0x01
           THEN kp.[{KP_BMI_SUM}]
           ELSE p._Fld112244
      END
    """
    amt = fx_sql(amt_raw, f"kp.[{KP_CURRENCY}]")
    bl = ",".join("?" * len(KP_STATUS_BLACKLIST))

    cur.execute(
        f"""
        SELECT d.name, SUM({amt}) AS ExpSum
        FROM _InfoRg112240 p WITH (NOLOCK)
        INNER JOIN #exp_depts d ON d.id = p._Fld112241RRef
        INNER JOIN _Document770 kp WITH (NOLOCK)
          ON kp._IDRRef = p._Fld112292_RRRef
         AND p._Fld112292_RTRef = ?
        WHERE p._Fld112245 >= ? AND p._Fld112245 < ?
          AND p._Fld112241RRef <> ?
          AND p._Fld114063RRef = ?
          AND p._Fld112292_RRRef <> ?
          AND d.name <> N'Отдел продаж БМИ'
          AND kp._Fld25044RRef NOT IN ({bl})
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = p._Fld112241RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = p._Fld112243RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = p._Fld112243RRef
                    ) THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        *fx_params(),
        KP_TREF,
        p0,
        p_asof,
        EMPTY16,
        EMPTY16,
        EMPTY16,
        *KP_STATUS_BLACKLIST,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def _expected_advance_from(p0: datetime) -> datetime:
    """В текущем месяце 1С ещё держит неоплаченные оферты прошлого месяца."""
    period = date(p0.year - YEAR_OFFSET, p0.month, 1)
    today = date.today().replace(day=1)
    if period != today:
        return p0
    if p0.month == 1:
        return p0.replace(year=p0.year - 1, month=12, day=1)
    return p0.replace(month=p0.month - 1, day=1)


def _odata_order_status(numbers: list[str]) -> dict[str, str]:
    """Живой статус заказа. SQL-копия _Fld21195RRef отстаёт на дни."""
    from urllib.parse import quote

    import requests
    from requests.auth import HTTPBasicAuth

    base = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
    session = requests.Session()
    session.auth = HTTPBasicAuth("odata.user", "npo852456")
    out: dict[str, str] = {}
    clean = sorted({n.strip() for n in numbers if n and n.strip()})
    for i in range(0, len(clean), 10):
        part = clean[i:i + 10]
        flt = quote(
            "(" + " or ".join(f"Number eq '{n}'" for n in part) + ")"
            " and Date ge datetime'2026-01-01T00:00:00'",
            safe="",
        )
        sel = quote("Number,Date,Статус", safe=",_")
        response = session.get(
            f"{base}/Document_ЗаказКлиента?$format=json&$filter={flt}&$select={sel}&$top=50",
            timeout=60,
        )
        response.raise_for_status()
        for row in response.json().get("value") or []:
            number = (row.get("Number") or "").strip()
            prev = out.get(number)
            if prev is None or (row.get("Date") or "") >= prev[0]:
                out[number] = (row.get("Date") or "", row.get("Статус") or "")
    return {number: status for number, (_dt, status) in out.items()}


def calc_expected_offer(
    cur,
    p0: datetime,
    p_next: datetime,
    p_asof: datetime | None = None,
) -> dict[str, float]:
    """Счёт-оферта: полностью неоплаченный заказ по соглашению «оферта»."""
    from comdir.resale import ORDER_SOPR_FIELD

    p_asof = p_asof or p_next
    adv_from = _expected_advance_from(p0)
    load_depts(cur, COMMERCIAL_DEPTS, "#offer_depts")
    load_resale(cur)

    amt = fx_sql("ord._Fld21186", "ord._Fld21185RRef")
    cur.execute(
        f"""
        SELECT d.name, SUM({amt}) AS ExpSum
        FROM _Document704 ord WITH (NOLOCK)
        INNER JOIN #offer_depts d ON d.id = ord._Fld21220RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = ord._Fld21183RRef
        WHERE a.[{AG_OFFER_FLAG}] = 0x01
          AND a._Description LIKE N'%оферт%'
          AND ord._Posted = 0x01
          AND ISNULL(ord._Fld184301, 0x00) = 0x00
          AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
          AND ord._Fld138973RRef <> ?
          AND ord.[{ORDER_ADVANCE_DT}] >= ?
          AND ord.[{ORDER_ADVANCE_DT}] < ?
          AND EXISTS (
            SELECT 1 FROM _Document704_VT21278 st WITH (NOLOCK)
            WHERE st._Document704_IDRRef = ord._IDRRef
              AND st._Fld21281 < ?
          )
          AND NOT EXISTS (
            SELECT 1 FROM _InfoRg112278 s WITH (NOLOCK)
            WHERE s._Fld112481RRef = ord._IDRRef
          )
          AND (
            EXISTS (
              SELECT 1
              FROM _AccumRg53885 s WITH (NOLOCK)
              WHERE s._Fld140429RRef = ord._Fld138973RRef
                AND s._Period < ?
                AND s._Active = 0x01
                AND ISNULL(s._Fld140434, 0x00) = 0x00
                AND s._Fld53890 <> 0
              GROUP BY s._Fld140429RRef
              HAVING ABS(
                SUM(CASE WHEN s._RecordKind = 1 THEN -s._Fld53890 ELSE s._Fld53890 END)
                - ord._Fld21186
              ) < 1
            )
            OR (
              ord._Date_Time >= ? AND ord._Date_Time < ?
              AND NOT EXISTS (
                SELECT 1
                FROM _AccumRg53885 s WITH (NOLOCK)
                WHERE s._Fld140429RRef = ord._Fld138973RRef
                  AND s._Period < ?
                  AND s._Active = 0x01
                  AND ISNULL(s._Fld140434, 0x00) = 0x00
                  AND s._Fld53890 <> 0
              )
            )
          )
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = ord._Fld21220RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                END
              ) = 1
        GROUP BY d.name
        """,
        *fx_params(),
        EMPTY16,
        adv_from,
        p_asof,
        p_next,
        p_next,
        p0,
        p_next,
        p_next,
    )
    out = {r[0]: float(r[1] or 0) for r in cur.fetchall()}
    from comdir.plan_fact_sink import lines as _pf_lines
    from comdir.plan_fact_sink import note as _pf_note

    if _pf_lines.get() is not None:
        try:
            cur.execute(
                f"""
                SELECT d.name, ord._Number,
                       CONVERT(varchar(10), DATEADD(year, -2000, ord.[{ORDER_ADVANCE_DT}]), 23),
                       p._Description, {amt}
                FROM _Document704 ord WITH (NOLOCK)
                INNER JOIN #offer_depts d ON d.id = ord._Fld21220RRef
                INNER JOIN _Reference473 a WITH (NOLOCK)
                  ON a._IDRRef = ord._Fld21183RRef
                LEFT JOIN _Reference328 p WITH (NOLOCK)
                  ON p._IDRRef = ord._Fld21180RRef
                WHERE a.[{AG_OFFER_FLAG}] = 0x01
                  AND a._Description LIKE N'%оферт%'
                  AND ord._Posted = 0x01
                  AND ISNULL(ord._Fld184301, 0x00) = 0x00
                  AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                  AND ord._Fld138973RRef <> ?
                  AND ord.[{ORDER_ADVANCE_DT}] >= ?
                  AND ord.[{ORDER_ADVANCE_DT}] < ?
                  AND EXISTS (
                    SELECT 1 FROM _Document704_VT21278 st WITH (NOLOCK)
                    WHERE st._Document704_IDRRef = ord._IDRRef
                      AND st._Fld21281 < ?
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM _InfoRg112278 s WITH (NOLOCK)
                    WHERE s._Fld112481RRef = ord._IDRRef
                  )
                  AND (
                    EXISTS (
                      SELECT 1
                      FROM _AccumRg53885 s WITH (NOLOCK)
                      WHERE s._Fld140429RRef = ord._Fld138973RRef
                        AND s._Period < ?
                        AND s._Active = 0x01
                        AND ISNULL(s._Fld140434, 0x00) = 0x00
                        AND s._Fld53890 <> 0
                      GROUP BY s._Fld140429RRef
                      HAVING ABS(
                        SUM(CASE WHEN s._RecordKind = 1 THEN -s._Fld53890 ELSE s._Fld53890 END)
                        - ord._Fld21186
                      ) < 1
                    )
                    OR (
                      ord._Date_Time >= ? AND ord._Date_Time < ?
                      AND NOT EXISTS (
                        SELECT 1
                        FROM _AccumRg53885 s WITH (NOLOCK)
                        WHERE s._Fld140429RRef = ord._Fld138973RRef
                          AND s._Period < ?
                          AND s._Active = 0x01
                          AND ISNULL(s._Fld140434, 0x00) = 0x00
                          AND s._Fld53890 <> 0
                      )
                    )
                  )
                  AND (
                        CASE
                          WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = ord._Fld21220RRef) THEN
                            CASE WHEN EXISTS (
                              SELECT 1 FROM #resale_nomgs r WHERE r.id = ord._Fld21180RRef
                            ) THEN 0 ELSE 1 END
                          ELSE
                            CASE WHEN EXISTS (
                              SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef
                            ) THEN 0 ELSE 1 END
                        END
                      ) = 1
                """,
                *fx_params(),
                EMPTY16,
                adv_from,
                p_asof,
                p_next,
                p_next,
                p0,
                p_next,
                p_next,
            )
            for name, number, pay_date, partner_name, amount in cur.fetchall():
                num = (number or "").strip()
                _pf_note(
                    "dog",
                    "Ожидаемо",
                    amount,
                    date=pay_date or "",
                    department=name,
                    partner=partner_name or "",
                    document=("Заказ клиента " + num) if num else "Счёт-оферта",
                )
        except Exception:
            logger.exception("Строки счёт-оферты ожидаемых договоров не собрались")
    # В SQL-копии статус заказа отстаёт от 1С. Снимаем только те,
    # что и в живом OData ещё «НеСогласован».
    cur.execute(
        f"""
        SELECT d.name, ord._Number, {amt} AS ExpSum
        FROM _Document704 ord WITH (NOLOCK)
        INNER JOIN #offer_depts d ON d.id = ord._Fld21220RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = ord._Fld21183RRef
        WHERE a.[{AG_OFFER_FLAG}] = 0x01
          AND a._Description LIKE N'%оферт%'
          AND ord._Posted = 0x01
          AND ISNULL(ord._Fld184301, 0x00) = 0x00
          AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
          AND ord.[{ORDER_STATUS_FIELD}] = ?
          AND ord._Fld138973RRef <> ?
          AND ord.[{ORDER_ADVANCE_DT}] >= ?
          AND ord.[{ORDER_ADVANCE_DT}] < ?
          AND EXISTS (
            SELECT 1 FROM _Document704_VT21278 st WITH (NOLOCK)
            WHERE st._Document704_IDRRef = ord._IDRRef
              AND st._Fld21281 < ?
          )
          AND NOT EXISTS (
            SELECT 1 FROM _InfoRg112278 s WITH (NOLOCK)
            WHERE s._Fld112481RRef = ord._IDRRef
          )
          AND (
            EXISTS (
              SELECT 1
              FROM _AccumRg53885 s WITH (NOLOCK)
              WHERE s._Fld140429RRef = ord._Fld138973RRef
                AND s._Period < ?
                AND s._Active = 0x01
                AND ISNULL(s._Fld140434, 0x00) = 0x00
                AND s._Fld53890 <> 0
              GROUP BY s._Fld140429RRef
              HAVING ABS(
                SUM(CASE WHEN s._RecordKind = 1 THEN -s._Fld53890 ELSE s._Fld53890 END)
                - ord._Fld21186
              ) < 1
            )
            OR (
              ord._Date_Time >= ? AND ord._Date_Time < ?
              AND NOT EXISTS (
                SELECT 1
                FROM _AccumRg53885 s WITH (NOLOCK)
                WHERE s._Fld140429RRef = ord._Fld138973RRef
                  AND s._Period < ?
                  AND s._Active = 0x01
                  AND ISNULL(s._Fld140434, 0x00) = 0x00
                  AND s._Fld53890 <> 0
              )
            )
          )
          AND (
                CASE
                  WHEN EXISTS (SELECT 1 FROM #dept_nomgs x WHERE x.id = ord._Fld21220RRef) THEN
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale_nomgs r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                  ELSE
                    CASE WHEN EXISTS (
                      SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef
                    ) THEN 0 ELSE 1 END
                END
              ) = 1
        """,
        *fx_params(),
        ORDER_STATUS_NOT_AGREED,
        EMPTY16,
        adv_from,
        p_asof,
        p_next,
        p_next,
        p0,
        p_next,
        p_next,
    )
    stale = [(r[0], (r[1] or "").strip(), float(r[2] or 0)) for r in cur.fetchall()]
    if stale:
        live_status = _odata_order_status([num for _, num, _ in stale if num])
        for name, number, amount in stale:
            if live_status.get(number) == "НеСогласован":
                out[name] = out.get(name, 0.0) - amount
                buf = _pf_lines.get()
                doc = "Заказ клиента " + (number or "").strip()
                if buf is not None and doc.strip():
                    buf[:] = [
                        row for row in buf
                        if not (
                            row.get("metric") == "dog"
                            and row.get("kind") == "Ожидаемо"
                            and row.get("document") == doc
                        )
                    ]
    return {k: round(v, 2) for k, v in out.items() if abs(v) >= 0.005}


def calc_expected(
    cur,
    p0: datetime,
    p_next: datetime,
    p_asof: datetime | None = None,
) -> dict[str, float]:
    """Договоры, ожидаемые к заключению (потенциал КП + счёт-оферта)."""
    p_asof = p_asof or p_next
    pot = calc_expected_potential(cur, p0, p_next, p_asof=p_asof)
    offer = calc_expected_offer(cur, p0, p_next, p_asof=p_asof)
    try:
        live = calc_expected_offer_live(p0, p_asof)
    except Exception:
        logger.exception("OData счёт-оферты ожидаемых договоров недоступны")
        live = {}
    out: dict[str, float] = dict(pot)
    for name, val in offer.items():
        out[name] = out.get(name, 0.0) + float(val or 0)
    for name, val in live.items():
        out[name] = out.get(name, 0.0) + float(val or 0)
    return out


def main(as_of: date | None = None) -> None:
    as_of = as_of or date.today()
    y, m = as_of.year, as_of.month
    days_in_month = monthrange(y, m)[1]
    elapsed = min(as_of.day, days_in_month)
    p0 = to_1c_dt(date(y, m, 1))
    if m == 12:
        p_next = to_1c_dt(date(y + 1, 1, 1))
    else:
        p_next = to_1c_dt(date(y, m + 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    print(f"Период: {y}-{m:02d}, на дату {as_of.isoformat()} ({elapsed}/{days_in_month} дн.)")
    print(f"БД: erp_pm @ localhost")
    print("Метрика: договоры (МП / факт / ожидаемые)")
    print(f"FX rates: {FX_RATES}")
    print("Партнёры перепродажи: из ТД_ПредопределенныеЗначения (OData/SQL)\n")

    mp = calc_mp_plan(cur, p0, p_next)
    fact = calc_fact(cur, p0, p_next)
    expected = calc_expected(cur, p0, p_next)

    commercial = dict(COMMERCIAL_DEPTS)

    header = (
        f"{'Отдел':<55} {'МП план':>14} {'МП проп.':>14} "
        f"{'Факт':>14} {'% проп.':>8} {'Ожидаемые':>14}"
    )
    print("=" * 130)
    print(header)
    print("-" * 130)

    report_lines = [
        f"Период: {y}-{m:02d}, на дату {as_of.isoformat()} ({elapsed}/{days_in_month})",
        "Колонки: Маркетинговый план по договорам | Договоры заключенные (факт) | "
        "Договоры, ожидаемые к заключению (план)",
        f"FX: {FX_RATES}",
        "",
        header,
        "-" * 130,
    ]

    mp_full_total = 0.0
    mp_pr_total = 0.0
    fact_total = 0.0
    exp_total = 0.0

    for name in FACT_ORDER:
        mp_full = mp.get(name, 0.0)
        mp_pr = mp_full * elapsed / days_in_month if name in commercial else 0.0
        fact_v = fact.get(name, 0.0)
        exp_v = expected.get(name, 0.0)

        if name in commercial:
            mp_full_total += mp_full
            mp_pr_total += mp_pr
        fact_total += fact_v
        exp_total += exp_v

        mp_cell = fmt(mp_full) if name in commercial else "—"
        mp_pr_cell = fmt(mp_pr) if name in commercial else "—"
        line = (
            f"{name:<55} {mp_cell:>14} {mp_pr_cell:>14} "
            f"{fmt(fact_v):>14} {pct(fact_v, mp_pr):>8} {fmt(exp_v):>14}"
        )
        print(line)
        report_lines.append(line)

    tot = (
        f"{'ИТОГО коммерческий директор':<55} "
        f"{fmt(mp_full_total):>14} {fmt(mp_pr_total):>14} "
        f"{fmt(fact_total):>14} {pct(fact_total, mp_pr_total):>8} {fmt(exp_total):>14}"
    )
    print("-" * 130)
    print(tot)
    print()
    print(f"МП план (полный месяц):     {fmt(mp_full_total)}")
    print(f"МП план (пропорц. {elapsed}/{days_in_month}): {fmt(mp_pr_total)}")
    print(f"Факт заключённые:           {fmt(fact_total)}")
    print(f"Ожидаемые к заключению:     {fmt(exp_total)}")
    print(f"% факта к пропорц. МП:      {pct(fact_total, mp_pr_total)}")
    print(f"% факта к полному МП:       {pct(fact_total, mp_full_total)}")

    report_lines += [
        "-" * 130,
        tot,
        "",
        f"МП полный месяц: {mp_full_total}",
        f"МП пропорционально: {mp_pr_total}",
        f"Факт итого: {fact_total}",
        f"Ожидаемые итого: {exp_total}",
        "",
        "Примечания:",
        "- МП: _AccumRg96963, вид Договоры",
        "- Факт: _InfoRg112278, статус Действует, перепродажа + ТД_СопровождениеПродажи",
        "- Ожидаемые: чёрный список статусов КП "
        "(Черновик, НеСогласовано, Аннулировано, Отменено);",
        "  КП заполнен, заказ пуст, коммерч. отделы, перепродажа (ОДП — без МГС), БМИ-сумма",
        "- Ветка счёт-оферта: _Reference473._Fld13700 + остаток КОплате + этапы оплаты",
        f"- FX rates: {FX_RATES} (в июле почти все суммы в RUB)",
    ]
    out = OUT_DIR / f"plan_fact_dogovory_{y}_{m:02d}.txt"
    out.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nОтчёт сохранён: {out}")
    cn.close()


if __name__ == "__main__":
    main(as_of=date(2026, 7, 27))
