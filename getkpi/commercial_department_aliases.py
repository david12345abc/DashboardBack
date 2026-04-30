"""Алиасы коммерческих подразделений для агрегации KPI.

Некоторые ликвидированные отделы продолжают встречаться в документах 1С.
Для отчетности их суммы должны попадать в текущие коммерческие подразделения.
"""
from __future__ import annotations

DEALER_SALES_DEPT = "7587c178-92f6-11f0-96f9-6cb31113810e"
KEY_CLIENTS_DEPT = "639ec87b-67b6-11eb-8523-ac1f6b05524d"

COMMERCIAL_DEPT_ALIASES: dict[str, str] = {
    # (ликв.) Отдел дилерских продаж бытового оборудования
    "4edcf3a0-9f99-11e4-80da-001e67112509": DEALER_SALES_DEPT,
    # (ликв.) Отдел дилерских продаж промышленного оборудования
    "ff740269-d71e-11e6-8127-001e67112509": DEALER_SALES_DEPT,
    # (ликв.) Отдел по работе с холдингами 1
    "c6810cc3-cf32-11ef-95e8-6cb31113810e": KEY_CLIENTS_DEPT,
    # (ликв.) Отдел по работе с холдингами 2
    "ebd2d511-cf38-11ef-95e8-6cb31113810e": KEY_CLIENTS_DEPT,
    # (ликв.) Отдел по работе с холдингами 3
    "ad83f8bd-cf39-11ef-95e8-6cb31113810e": KEY_CLIENTS_DEPT,
}


def normalize_commercial_dept_guid(dept_guid: str | None) -> str:
    """Вернуть действующий GUID подразделения для коммерческих KPI."""
    if not dept_guid:
        return ""
    return COMMERCIAL_DEPT_ALIASES.get(dept_guid, dept_guid)
