# -*- coding: utf-8 -*-
"""
План / факт / ожидаемые по договорам для коммерческой службы.

Маркетинговый план (МП) — без изменений:
  _AccumRg96963, вид «Договоры», 6 коммерческих отделов.

Факт договоров = регистр + ветка счёт-оферта (как в отчёте «План-факт»):

  A) ТД_ДоговорыПодписанные (live OData; SQL — fallback):
  • ДатаПодписания в периоде, Спецификация.Статус = Действует
  • перепродажа / ТД_СопровождениеПродажи / ТД_НеУчитыватьВПланФакте
  • доп.: партнёр заказа ∈ перепродажи или сопровождение на заказе
  • курсы валют заказа

  B) Счёт-оферта (из расшифровки Excel): заказы с ТД_СчетОферта,
     без строки в ТД_ДоговорыПодписанные, проведённые, с оплатой в периоде;
     сумма = СуммаОплатыРегл за период (ОПБО/ОДП — перепродажа без МГС).

Ожидаемые (ТД_ДоговорыПотенциальные / _InfoRg112240):
  • ДатаПодписанияПлан в периоде
  • КП по ссылке заполнен
  • подразделение из коммерческого списка (не пустое)
  • ЗаказКлиента пуст (иначе это уже не «ожидаемый»)
  • статус КП — чёрный список (Enum1651):
      НеСогласовано, Аннулировано (+ Черновик, Отменено);
      проходят: Согласовано, Действует, Исполнено
  • партнёр не из перепродажи; для ОДП (и ликв. ОПБО) — список без Метрогазсервис
  • если ТД_ОсновноеТКПДляБМИ — берём ТД_СуммаТКПБМИ
  • курсы валют КП
  • плюс ветка счёт-оферта: заказы по соглашению ТД_СчетОферта,
    без записи в ТД_ДоговорыПодписанные, с этапом оплаты и остатком КОплате > 0
"""
from __future__ import annotations

import logging
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
    load_depts(cur, COMMERCIAL_DEPTS, "#plan_depts")
    cur.execute(
        """
        SELECT d.name,
               SUM(CASE WHEN p._Active = 0x01 THEN p._Fld96971 ELSE 0 END) AS PlanSum
        FROM _AccumRg96963 p WITH (NOLOCK)
        INNER JOIN #plan_depts d ON d.id = p._Fld96965RRef
        WHERE p._Fld122525RRef = ?
          AND p._Period >= ? AND p._Period < ?
        GROUP BY d.name
        """,
        PLAN_DEALS,
        p0,
        p_next,
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
    from comdir.resale import (
        PREDEFINED_MGS_REF,
        PREDEFINED_OPBO_REF,
        PREDEFINED_RESALE_REF,
        _base,
        _session,
        fetch_fx_rates,
        guid_to_1c_bytes,
    )

    session = _session()
    base = _base()
    empty = "00000000-0000-0000-0000-000000000000"
    d0 = f"{p0.year - YEAR_OFFSET:04d}-{p0.month:02d}-{p0.day:02d}"
    d1 = f"{p_next.year - YEAR_OFFSET:04d}-{p_next.month:02d}-{p_next.day:02d}"

    # партнёры перепродажи / МГС / ОПБО
    flt = quote(f"Ref_Key eq guid'{PREDEFINED_RESALE_REF}'", safe="")
    url = (
        f"{base}/Catalog_ТД_ПредопределенныеЗначения_ДополнительныеЗначения"
        f"?$format=json&$filter={flt}&$select=Значение,Значение_Type&$top=5000"
    )
    resale: set[str] = set()
    for row in session.get(url, timeout=45).json().get("value") or []:
        val = row.get("Значение")
        if val and "Catalog_Партнеры" in str(row.get("Значение_Type") or ""):
            resale.add(val)
    flt_m = quote(f"Ref_Key eq guid'{PREDEFINED_MGS_REF}'", safe="")
    mgs = (
        (session.get(
            f"{base}/Catalog_ТД_ПредопределенныеЗначения?$format=json"
            f"&$filter={flt_m}&$select=Значение,Значение_Type&$top=1",
            timeout=30,
        ).json().get("value") or [{}])[0].get("Значение")
    )
    if mgs:
        resale.add(mgs)
    resale_nomgs = set(resale) - ({mgs} if mgs else set())

    flt_o = quote(f"Ref_Key eq guid'{PREDEFINED_OPBO_REF}'", safe="")
    opbo = (
        (session.get(
            f"{base}/Catalog_ТД_ПредопределенныеЗначения?$format=json"
            f"&$filter={flt_o}&$select=Значение,Значение_Type&$top=1",
            timeout=30,
        ).json().get("value") or [{}])[0].get("Значение")
    )

    rates = fetch_fx_rates()
    cur_map = {
        "0a7c6f22-e1b6-11df-963e-001cc4d04388": rates["USD"],
        "d328a18d-7405-11e0-81cd-001583b3d75c": rates["EUR"],
        "095e2c36-45dc-11ec-8756-ac1f6b05524d": rates["BYN"],
        "e2bc7bc0-de2e-11ef-95fc-6cb31113810e": rates["KZT"],
    }

    entity = quote("InformationRegister_ТД_ДоговорыПодписанные")
    flt = quote(
        f"ДатаПодписания ge datetime'{d0}T00:00:00' and "
        f"ДатаПодписания lt datetime'{d1}T00:00:00'"
    )
    select = quote(
        "Спецификация_Key,Подразделение_Key,Партнер_Key,ЗаказКлиента_Key,"
        "СуммаДоговора,ТД_СопровождениеПродажи",
        safe=",",
    )
    rows: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{base}/{entity}?$format=json&$top=1000&$skip={skip}"
            f"&$select={select}&$filter={flt}"
        )
        batch = session.get(url, timeout=120).json().get("value") or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        skip += 1000

    status: dict[str, str] = {}
    for sk in {r.get("Спецификация_Key") for r in rows if r.get("Спецификация_Key")}:
        rr = session.get(
            f"{base}/{quote('Catalog_СоглашенияСКлиентами')}(guid'{sk}')"
            f"?$format=json&$select=Статус",
            timeout=30,
        )
        if rr.ok:
            status[sk] = rr.json().get("Статус") or ""

    order_keys = sorted({
        r.get("ЗаказКлиента_Key")
        for r in rows
        if r.get("ЗаказКлиента_Key") and r.get("ЗаказКлиента_Key") != empty
    })
    orders: dict[str, dict] = {}
    for i in range(0, len(order_keys), 15):
        batch = order_keys[i : i + 15]
        oflt = quote(" or ".join(f"Ref_Key eq guid'{k}'" for k in batch), safe="")
        url = (
            f"{base}/{quote('Document_ЗаказКлиента')}?$format=json&$filter={oflt}"
            f"&$select={quote('Ref_Key,Партнер_Key,Валюта_Key,ТД_НеУчитыватьВПланФакте,ТД_СопровождениеПродажи', safe=',')}"
            f"&$top=50"
        )
        rr = session.get(url, timeout=60)
        if not rr.ok:
            continue
        for it in rr.json().get("value") or []:
            orders[it["Ref_Key"]] = it

    name_by_bin = _dept_name_by_bin()
    out: dict[str, float] = {}
    for r in rows:
        if status.get(r.get("Спецификация_Key") or "") != "Действует":
            continue
        dept_key = r.get("Подразделение_Key") or empty
        if dept_key == empty:
            continue
        try:
            dept_name = name_by_bin.get(guid_to_1c_bytes(dept_key))
        except Exception:
            dept_name = None
        if not dept_name:
            continue

        partner = r.get("Партнер_Key") or empty
        sopr_reg = bool(r.get("ТД_СопровождениеПродажи"))
        if opbo and dept_key == opbo:
            if partner in resale_nomgs:
                continue
        else:
            if partner in resale and not sopr_reg:
                continue

        ok = r.get("ЗаказКлиента_Key") or empty
        rate = 1.0
        if ok != empty:
            od = orders.get(ok) or {}
            if od.get("ТД_НеУчитыватьВПланФакте"):
                continue
            if od.get("ТД_СопровождениеПродажи"):
                continue
            if (od.get("Партнер_Key") or empty) in resale:
                continue
            rate = float(cur_map.get(od.get("Валюта_Key") or "", 1.0) or 1.0)

        amt = float(r.get("СуммаДоговора") or 0) * rate
        out[dept_name] = out.get(dept_name, 0.0) + amt

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
                     AND ISNULL(ord.[{ORDER_SOPR_FIELD}], 0x00) = 0x00
                     AND NOT EXISTS (
                       SELECT 1 FROM #resale r WHERE r.id = ord._Fld21180RRef
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
                      SELECT 1 FROM #resale r WHERE r.id = s._Fld112282RRef
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


def calc_fact_offer(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Ветка счёт-оферта факта договоров (расшифровка отчёта 1С / Excel).

    Заказы с соглашением ТД_СчетОферта, которых нет в ТД_ДоговорыПодписанные:
    проведённые, не «НеСогласован», с оплатой в периоде → сумма оплат регл.
    """
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


def _merge_fact(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
    out = dict(a)
    for name, val in b.items():
        out[name] = round(out.get(name, 0.0) + float(val or 0), 2)
    return out


def calc_fact(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Договоры заключённые (факт) = регистр + счёт-оферта (как в отчёте 1С).

    Источник регистра — MSSQL erp_pm; OData только как аварийный fallback.
    """
    try:
        reg = calc_fact_sql(cur, p0, p_next)
    except Exception:
        logger.exception("SQL факт договоров недоступен — fallback OData")
        try:
            reg = calc_fact_odata(p0, p_next)
        except Exception:
            logger.exception("OData факт договоров тоже недоступен")
            reg = {}
    try:
        offer = calc_fact_offer(cur, p0, p_next)
    except Exception:
        logger.exception("Ветка счёт-оферта факта договоров недоступна")
        offer = {}
    return _merge_fact(reg, offer)


def calc_expected_potential(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Договоры, ожидаемые к заключению — ветка потенциальных КП."""
    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#exp_depts")
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
        p_next,
        EMPTY16,
        EMPTY16,
        EMPTY16,
        *KP_STATUS_BLACKLIST,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_expected_offer(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Ветка счёт-оферта: СуммаДокумента заказа при остатке КОплате > 0."""
    del p0  # оферта завязана на остаток/этапы к концу месяца, не на дату КП
    all_depts = COMMERCIAL_DEPTS + LIQUIDATED_DEPTS + HOLDINGS_DEPTS
    load_depts(cur, all_depts, "#offer_depts")
    load_resale(cur)

    cur.execute(
        """
        SELECT d.name, SUM(ord._Fld21186) AS ExpSum
        FROM _Document704 ord WITH (NOLOCK)
        INNER JOIN #offer_depts d ON d.id = ord._Fld21220RRef
        INNER JOIN _Reference473 a WITH (NOLOCK)
          ON a._IDRRef = ord._Fld21183RRef
        WHERE a.[{flag}] = 0x01
          AND ISNULL(ord._Fld184301, 0x00) = 0x00
          AND ord._Fld138973RRef <> ?
          AND EXISTS (
            SELECT 1 FROM _Document704_VT21278 st WITH (NOLOCK)
            WHERE st._Document704_IDRRef = ord._IDRRef
              AND st._Fld21281 < ?
          )
          AND NOT EXISTS (
            SELECT 1 FROM _InfoRg112278 s WITH (NOLOCK)
            WHERE s._Fld112481RRef = ord._IDRRef
          )
          AND EXISTS (
            SELECT 1
            FROM _AccumRg53885 s WITH (NOLOCK)
            WHERE s._Fld140429RRef = ord._Fld138973RRef
              AND s._Period < ?
              AND s._Active = 0x01
              AND ISNULL(s._Fld140434, 0x00) = 0x00
              AND s._Fld53890 <> 0
            GROUP BY s._Fld140429RRef
            HAVING SUM(
              CASE WHEN s._RecordKind = 1 THEN -s._Fld53890 ELSE s._Fld53890 END
            ) > 0
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
        """.replace("{flag}", AG_OFFER_FLAG),
        EMPTY16,
        p_next,
        p_next,
    )
    return {r[0]: float(r[1] or 0) for r in cur.fetchall()}


def calc_expected(cur, p0: datetime, p_next: datetime) -> dict[str, float]:
    """Договоры, ожидаемые к заключению (потенциал КП + счёт-оферта)."""
    pot = calc_expected_potential(cur, p0, p_next)
    offer = calc_expected_offer(cur, p0, p_next)
    out: dict[str, float] = dict(pot)
    for name, val in offer.items():
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
