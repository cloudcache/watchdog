-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Query-time address classification (docs/flow-address-query-plan.md): the
-- source table for the IP_TRIE address dictionary. Instead of baking
-- classification into flow_records at ingest, the address library (the ready
-- geo hierarchy + admin geo-groups) is published here — one row per prefix per
-- version — and rollup/queries resolve an IP's classification with dictGet.
-- Older versions stay for as-of billing. The dictionary itself is created at
-- runtime because it needs injected ClickHouse credentials for its source, so
-- it is not part of this migration; only the versioned source table is.
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_address_dict_source (
  dict_version UInt64,
  prefix String,
  continent_id LowCardinality(String),
  region_id LowCardinality(String),
  country_id LowCardinality(String),
  province_id LowCardinality(String),
  city_id LowCardinality(String),
  isp_id UInt32,
  asn UInt32,
  group_ids Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (dict_version, prefix);
