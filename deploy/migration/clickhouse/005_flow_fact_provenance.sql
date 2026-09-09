-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Preserve the supplier enrichment before customer overrides. Existing
-- rows stay fact_schema=1 and must not be presented as supplier provenance.
-- New workers explicitly write fact_schema=2 and every supplier_* column.
ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS fact_schema UInt16 DEFAULT 1 AFTER classification_version;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_country FixedString(2) AFTER fact_schema;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_admin_code LowCardinality(String) AFTER supplier_remote_country;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_subdivision LowCardinality(String) AFTER supplier_remote_admin_code;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_city LowCardinality(String) AFTER supplier_remote_subdivision;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_geo_continent_id LowCardinality(String) AFTER supplier_remote_city;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_geo_region_id LowCardinality(String) AFTER supplier_remote_geo_continent_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_geo_country_id LowCardinality(String) AFTER supplier_remote_geo_region_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_geo_province_id LowCardinality(String) AFTER supplier_remote_geo_country_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_geo_city_id LowCardinality(String) AFTER supplier_remote_geo_province_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_isp_id UInt16 AFTER supplier_remote_geo_city_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_asn UInt32 AFTER supplier_remote_isp_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_remote_asn_source Enum8('unknown'=0,'exporter'=1,'flow-geo-v1'=2,'flow_geo_override'=3,'flow-geo-v2'=4) AFTER supplier_remote_asn;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_geo_version LowCardinality(String) AFTER supplier_remote_asn_source;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS supplier_category Enum8('unknown'=0,'on_net_local_city'=1,'on_net_cross_city'=2,'on_net_cross_province'=3,'off_net_in_province'=4,'off_net_cross_province'=5,'overseas'=6,'internal'=7,'transit'=8,'ambiguous'=9) AFTER supplier_geo_version;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS customer_geo_override_fields UInt8 AFTER supplier_category;
