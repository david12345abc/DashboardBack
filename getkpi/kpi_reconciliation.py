"""
Сверка KPI дашборда с выгрузкой 1С: откуда берутся цифры и что спросить при расхождении.

Сводка источников (текущая реализация дашборда ОДП / коммерции):
  Договоры — InformationRegister_ТД_ДоговорыПодписанные, ДатаПодписания в месяце,
    Спецификация.Статус = «Действует», подразделение = Подразделение_Key строки регистра
    (+ алиасы ликвидированных → ОДП), фильтр перепродажи, заказ не «Не учитывать в план-факте»,
    сумма × курс по валюте заказа.
  Деньги / отгрузки — см. заголовки calc_dengi_fact.py, calc_otgruzki_fact.py (движения + заказ).
"""
from __future__ import annotations

from typing import Any

import logging

logger = logging.getLogger(__name__)

ODP_RECONCILIATION_QUESTIONS_RU: list[str] = [
    (
        "Из какого именно объекта метаданных 1С строится колонка «Договоры заключенные (факт)» "
        "в вашем отчёте (регистр, запрос к документам, срез)? Совпадает ли он с "
        "InformationRegister.ТД_ДоговорыПодписанные?"
    ),
    (
        "По какой дате отбираются договоры за месяц: ДатаПодписания регистра, дата документа, "
        "другая (например проведения)?"
    ),
    (
        "Какое условие по спецификации/соглашению в отчёте (мы берём только Статус = «Действует» "
        "из Catalog_СоглашенияСКлиентами по Спецификация_Key)? Есть ли включение строк без статуса "
        "или с другими статусами?"
    ),
    (
        "По какому подразделению режется ОДП в отчёте: Подразделение в строке регистра договоров, "
        "подразделение заказа, подразделение договора, произвольное ВЫБОР?"
    ),
    (
        "Учитываются ли в отчёте договоры с заказами, у которых ТД_НеУчитыватьВПланФакте = Истина? "
        "(на дашборде такие строки в сумму не попадают.)"
    ),
    (
        "Те же правила отбора партнёров «перепродажи» и ветка ОПБО без МГС, что в запросе ВТ_ДоговорыФакт?"
    ),
    (
        "Сумма в отчёте всегда в рублях из поля регистра или пересчитывается по валюте заказа "
        "теми же курсами, что в типовом отчёте?"
    ),
    (
        "Колонка «Отгрузки произведенные (факт)»: тот же набор регистров и исключение "
        "ТД_СопровождениеПродажи на заказе, что заложено в AccumulationRegister.РаспоряженияНаОтгрузку?"
    ),
]


def attach_dashboard_vs_reference(
    reference_block: dict[str, Any] | None,
    dashboard_dogovory: float | None,
    dashboard_dengi: float | None,
    dashboard_otgruzki: float | None,
) -> dict[str, Any] | None:
    """
    Дополнить блок ODP-T-REFERENCE сравнением с фактом KPI и вопросами при расхождении.
    """
    if reference_block is None:
        return None
    totals = reference_block.get("totals_row") or {}
    ex_d = totals.get("dogovory")
    ex_m = totals.get("dengi")
    ex_s = totals.get("otgruzki")

    def _d(a: float | None, b: float | None) -> float | None:
        if a is None or b is None:
            return None
        return round(float(a) - float(b), 2)

    dash = {
        "dogovory": dashboard_dogovory,
        "dengi": dashboard_dengi,
        "otgruzki": dashboard_otgruzki,
    }
    delta = {
        "dogovory": _d(ex_d, dashboard_dogovory),
        "dengi": _d(ex_m, dashboard_dengi),
        "otgruzki": _d(ex_s, dashboard_otgruzki),
    }
    tol = 0.05
    mismatch = []
    if ex_d is not None and dashboard_dogovory is not None and abs(ex_d - dashboard_dogovory) > tol:
        mismatch.append("dogovory")
    if ex_m is not None and dashboard_dengi is not None and abs(ex_m - dashboard_dengi) > tol:
        mismatch.append("dengi")
    if ex_s is not None and dashboard_otgruzki is not None and abs(ex_s - dashboard_otgruzki) > tol:
        mismatch.append("otgruzki")

    out = dict(reference_block)
    out["dashboard_fact_same_period"] = dash
    out["delta_excel_minus_dashboard"] = delta
    out["metrics_match_within_tol"] = len(mismatch) == 0
    if mismatch:
        out["mismatched_metrics"] = mismatch
        out["questions_for_1c_if_not_match"] = ODP_RECONCILIATION_QUESTIONS_RU
        logger.warning(
            "Сверка ОДП с файлом выгрузки: не совпали %s; дельта (Excel − дашборд) %s",
            mismatch,
            delta,
        )
    return out
