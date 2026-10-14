"""Компании плиток «Юрисконсультант» и их организации в 1С."""
from __future__ import annotations

DEPARTMENT = "Юрисконсультант"

# Название на плитке — как в задаче. odata_name — Description в Catalog_Организации.
COMPANIES: tuple[dict, ...] = (
    {
        "code": "td",
        "title": "ООО НПО «Турбулентность-ДОН»",
        "odata_name": "ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО",
        "org_guid": "fbca2148-6cfd-11e7-812d-001e67112509",
        "count_kpi_id": "YR-TD-Q",
        "sum_kpi_id": "YR-TD-S",
    },
    {
        "code": "al",
        "title": "ООО «Алмаз»",
        "odata_name": "АЛМАЗ ООО",
        "org_guid": "fbca2146-6cfd-11e7-812d-001e67112509",
        "count_kpi_id": "YR-AL-Q",
        "sum_kpi_id": "YR-AL-S",
    },
    {
        "code": "mg",
        "title": "ООО «МГС»",
        "odata_name": "Метрогазсервис ООО",
        "org_guid": "fbca2145-6cfd-11e7-812d-001e67112509",
        "count_kpi_id": "YR-MG-Q",
        "sum_kpi_id": "YR-MG-S",
    },
    {
        "code": "am",
        "title": "ООО «Амурская легенда»",
        "odata_name": "Амурская легенда ТД ООО",
        "org_guid": "b3529fec-5813-11e8-8273-ac1f6b05524d",
        "count_kpi_id": "YR-AM-Q",
        "sum_kpi_id": "YR-AM-S",
    },
)

COUNT_KPI_IDS: frozenset[str] = frozenset(item["count_kpi_id"] for item in COMPANIES)
SUM_KPI_IDS: frozenset[str] = frozenset(item["sum_kpi_id"] for item in COMPANIES)
TILE_KPI_IDS: frozenset[str] = COUNT_KPI_IDS | SUM_KPI_IDS

_BY_KPI_ID: dict[str, tuple[dict, str]] = {}
for _company in COMPANIES:
    _BY_KPI_ID[_company["count_kpi_id"]] = (_company, "count")
    _BY_KPI_ID[_company["sum_kpi_id"]] = (_company, "sum")


def company_for_kpi(kpi_id: str) -> tuple[dict, str] | None:
    """Компания и метрика ('count' или 'sum') по коду плитки."""
    return _BY_KPI_ID.get(str(kpi_id or "").strip().upper())
