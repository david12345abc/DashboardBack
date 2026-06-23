"""QD-M10 — предъявления на ОТК, организация АЛМАЗ ООО."""

from __future__ import annotations

from qualdir.otk_predyavlenie_data import ORG_ALMAZ_KEY, ORG_ALMAZ_NAME
from qualdir.qd_otk_org_tile import OtkOrgTileConfig, build_tile_exports

_CONFIG = OtkOrgTileConfig(
    kpi_id="QD-M10",
    org_slug="almaz",
    source_tag="qualdir_otk_predyavlenie_month_v5_almaz",
    cache_version=5,
    ytd_cache_prefix="qualdir_qd_m10_ytd",
    ytd_disk_tag="qualdir_qd_m10_ytd_payload_v5",
    ytd_disk_version=5,
    organization_key=ORG_ALMAZ_KEY,
    organization_name=ORG_ALMAZ_NAME,
    log_prefix="QD-M10",
)

_exports = build_tile_exports(_CONFIG)

get_qd_m10_ytd = _exports["get_ytd"]
otk_predyavlenie_almaz_month_cache_path = _exports["otk_predyavlenie_month_cache_path"]
qd_m10_ytd_cache_path = _exports["ytd_cache_path"]
qd_m10_tile_cache_path = _exports["tile_cache_path"]
compute_qd_m10_month = _exports["compute_month"]
