-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Forward-only expansion for flow-geo-v2. Empty IDs represent a missing
-- hierarchy level and are materialized as _unassigned by rollup queries.
ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS remote_geo_continent_id LowCardinality(String) AFTER remote_city;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS remote_geo_region_id LowCardinality(String) AFTER remote_geo_continent_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS remote_geo_country_id LowCardinality(String) AFTER remote_geo_region_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS remote_geo_province_id LowCardinality(String) AFTER remote_geo_country_id;

ALTER TABLE watchdog_flow.flow_records
  ADD COLUMN IF NOT EXISTS remote_geo_city_id LowCardinality(String) AFTER remote_geo_province_id;

ALTER TABLE watchdog_flow.flow_records
  MODIFY COLUMN remote_asn_source Enum8(
    'unknown'=0,
    'exporter'=1,
    'flow-geo-v1'=2,
    'flow_geo_override'=3,
    'flow-geo-v2'=4);
