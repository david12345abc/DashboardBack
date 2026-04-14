"""
Коммерческий блок: 10 ежемесячных KPI для коммерческого директора и дочерних отделов.
Все подразделения коммерческого блока используют одинаковый набор KPI (KD-M1..KD-M10).
"""
from __future__ import annotations


def norm_dept(s: str) -> str:
    return ' '.join(s.strip().lower().split())


DEPT_TO_KPI_KEY: dict[str, str | None] = {
    norm_dept('Заместитель коммерческого директора по развитию продаж'): (
        'Заместитель коммерческого директора по развитию продаж'
    ),
    norm_dept('Заместитель директора по перспективным проектам'): None,
    norm_dept('Отдел рекламы и PR'): None,
    norm_dept('Отдел ВЭД'): 'ОВЭД',
    norm_dept('Отдел ПМУ и ЭО'): 'ОПЭОиУ',
    norm_dept('Тендерный офис'): 'Тендерный отдел',
    norm_dept('Тендерный отдел'): 'Тендерный отдел',
    norm_dept('Отдел продаж БМИ'): 'Отдел продаж БМИ',
    norm_dept('Отдел по работе с ключевыми клиентами'): 'Отдел по работе с ключевыми клиентами',
    norm_dept('Отдел по работе с ПАО «Газпром»'): 'Отдел по работе с ПАО «Газпром»',
    norm_dept('Отдел дилерских продаж промышленного оборудования'): 'Отдел дилерских продаж',
    norm_dept('Отдел дилерских продаж'): 'Отдел дилерских продаж',
    norm_dept('Коммерческая служба'): 'Коммерческая служба',
    norm_dept('ОПЭОиУ'): 'ОПЭОиУ',
    norm_dept('ОВЭД'): 'ОВЭД',
}

KOMDIR_TILE_IDS = (
    'KD-M1', 'KD-M2', 'KD-M3', 'KD-M4', 'KD-M5',
    'KD-M6', 'KD-M7', 'KD-M8', 'KD-M9', 'KD-M10',
)

TILE_ORDER_BY_KPI_KEY: dict[str, tuple[str, ...]] = {
    'Заместитель коммерческого директора по развитию продаж': KOMDIR_TILE_IDS,
    'ОВЭД': KOMDIR_TILE_IDS,
    'ОПЭОиУ': KOMDIR_TILE_IDS,
    'Тендерный отдел': KOMDIR_TILE_IDS,
    'Отдел продаж БМИ': KOMDIR_TILE_IDS,
    'Отдел по работе с ключевыми клиентами': KOMDIR_TILE_IDS,
    'Отдел по работе с ПАО «Газпром»': KOMDIR_TILE_IDS,
    'Отдел дилерских продаж': KOMDIR_TILE_IDS,
    'Коммерческая служба': KOMDIR_TILE_IDS,
}


def commercial_kpi_key(department: str) -> str | None | bool:
    """
    Если подразделение из коммерческой матрицы плиток:
    возвращает str (ключ KPI_DATA) или None (явно нет каталога).
    Если не из матрицы — возвращает False (обычная логика API).
    """
    if department in TILE_ORDER_BY_KPI_KEY:
        return department
    n = norm_dept(department)
    if n not in DEPT_TO_KPI_KEY:
        return False
    return DEPT_TO_KPI_KEY[n]


def tile_order_for_kpi_key(kpi_key: str) -> tuple[str, ...] | None:
    return TILE_ORDER_BY_KPI_KEY.get(kpi_key)


KPI_KEY_TO_DEPT_GUID: dict[str, str] = {
    'ОВЭД': '49480c10-e401-11e8-8283-ac1f6b05524d',
    'ОПЭОиУ': '34497ef7-810f-11e4-80d6-001e67112509',
    'Отдел продаж БМИ': '9edaa7d4-37a5-11ee-93d3-6cb31113810e',
    'Отдел по работе с ключевыми клиентами': '639ec87b-67b6-11eb-8523-ac1f6b05524d',
    'Отдел дилерских продаж': '7587c178-92f6-11f0-96f9-6cb31113810e',
    'Отдел по работе с ПАО «Газпром»': 'bd7b5184-9f9c-11e4-80da-001e67112509',
    'Тендерный отдел': None,
    'Коммерческая служба': None,
    'Заместитель коммерческого директора по развитию продаж': None,
}

DEPT_GUID_TO_DZ_NAME: dict[str, str] = {
    '49480c10-e401-11e8-8283-ac1f6b05524d': 'Отдел ВЭД',
    '34497ef7-810f-11e4-80d6-001e67112509': 'Отдел эталонного оборудования',
    '9edaa7d4-37a5-11ee-93d3-6cb31113810e': 'Отдел БМИ',
    '639ec87b-67b6-11eb-8523-ac1f6b05524d': 'Отдел ключевых клиентов',
    '7587c178-92f6-11f0-96f9-6cb31113810e': 'Отдел дилерских продаж',
    'bd7b5184-9f9c-11e4-80da-001e67112509': 'Отдел ПАО Газпром',
}


def dept_guid_for_kpi_key(kpi_key: str) -> str | None:
    """Резолвит KPI-ключ отдела в GUID подразделения OData (или None)."""
    return KPI_KEY_TO_DEPT_GUID.get(kpi_key)


def is_komdir_child(department: str) -> bool:
    """Проверяет, является ли подразделение дочерним коммерческого директора."""
    ck = commercial_kpi_key(department)
    return isinstance(ck, str)
