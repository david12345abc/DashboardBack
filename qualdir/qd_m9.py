"""QD-M9 — предъявления на ОТК, организация ТУРБУЛЕНТНОСТЬ-ДОН ООО НПО."""

from __future__ import annotations

from qualdir.otk_predyavlenie_data import ORG_NPO_KEY, ORG_NPO_NAME
from qualdir.qd_otk_org_tile import OtkOrgTileConfig, build_tile_exports

_CONFIG = OtkOrgTileConfig(
    kpi_id="QD-M9",
    org_slug="npo",
    source_tag="qualdir_otk_predyavlenie_month_v5_npo",
    cache_version=5,
    ytd_cache_prefix="qualdir_qd_m9_ytd",
    ytd_disk_tag="qualdir_qd_m9_ytd_payload_v5",
    ytd_disk_version=5,
    organization_key=ORG_NPO_KEY,
    organization_name=ORG_NPO_NAME,
    log_prefix="QD-M9",
)

_exports = build_tile_exports(_CONFIG)

get_qd_m9_ytd = _exports["get_ytd"]
otk_predyavlenie_npo_month_cache_path = _exports["otk_predyavlenie_month_cache_path"]
qd_m9_ytd_cache_path = _exports["ytd_cache_path"]
qd_m9_tile_cache_path = _exports["tile_cache_path"]
compute_qd_m9_month = _exports["compute_month"]
