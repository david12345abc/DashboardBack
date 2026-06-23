"""
QD-M6 — предъявления продукции на входной контроль (все организации).

QD-M9 / QD-M10 — те же метрики с фильтром по организации (см. ``qd_m9``, ``qd_m10``).
"""

from __future__ import annotations

from qualdir.qd_otk_org_tile import OtkOrgTileConfig, build_tile_exports, legacy_month_cache_path

_CONFIG = OtkOrgTileConfig(
    kpi_id="QD-M6",
    org_slug="all",
    source_tag="qualdir_otk_predyavlenie_month_v5_all",
    cache_version=5,
    ytd_cache_prefix="qualdir_qd_m6_ytd",
    ytd_disk_tag="qualdir_qd_m6_ytd_payload_v5",
    ytd_disk_version=5,
    organization_key=None,
    organization_name=None,
    log_prefix="QD-M6",
)

_exports = build_tile_exports(_CONFIG)

get_qd_m6_ytd = _exports["get_ytd"]
otk_predyavlenie_month_cache_path = _exports["otk_predyavlenie_month_cache_path"]
legacy_otk_predyavlenie_month_cache_path = lambda year, month: legacy_month_cache_path(_CONFIG, year, month)
qd_m6_ytd_cache_path = _exports["ytd_cache_path"]
qd_m6_tile_cache_path = _exports["tile_cache_path"]
compute_qd_m6_month = _exports["compute_month"]
