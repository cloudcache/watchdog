-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- The 5-minute atomic tier splits two distinct responsibilities that must not
-- share one table (design 2026-09-23 §3.2, lifecycle review 2026-09-24 §4.6):
--   * flow_interface_traffic_5m  -- billing / interface evidence, built from raw
--   * flow_aggregate_5m          -- non-endpoint mid-range query layer, built from 1m
-- Both are generation-marked ReplacingMergeTree tiers, mirroring 1m/1h/1d, so the
-- existing marker, coverage-horizon, repair and conservation tooling applies
-- unchanged. Neither changes fast decode, Kafka identity, classification, or the
-- raw write protocol.

-- Billing / interface evidence layer. Shape mirrors snmp_interface_traffic_5m so
-- Flow / sFlow-counter / SNMP can be reconciled three ways on the same grid. It
-- carries provenance (snapshot / geo / classification versions, ingest-generation
-- span) and observed/known counts so a period's 95th/average/total is replayable
-- from this table alone. observed_records = interface+direction matched records
-- (identical across layers); known_records = layer-qualified estimated_valid
-- records (coverage = known/observed), matching internal/flowch/billing.go's
-- raw_observations / raw_known / *_known accounting. Layers: raw = all records;
-- supplier = fact_schema>=2; customer = disposition='count'.
-- Evidence layer: no unconditional ClickHouse TTL. Month-partitioned so the
-- existing archive-month deletion state machine (DropArchiveMonth + approval +
-- backup) reclaims it after billing/audit/hold clear (lifecycle review §11.2-A).
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_interface_traffic_5m (
  bucket_start              DateTime('UTC')                 CODEC(DoubleDelta, ZSTD(1)),
  row_kind                  Enum8('value' = 1, 'generation' = 2),
  device_id                 LowCardinality(String),
  exporter_id               LowCardinality(String),
  if_index                  UInt32                          CODEC(T64, ZSTD(1)),
  direction                 Enum8('na' = 0, 'in' = 1, 'out' = 2),
  layer                     Enum8('na' = 0, 'raw' = 1, 'supplier' = 2, 'customer' = 3),
  raw_bytes                 UInt64                          CODEC(T64, ZSTD(1)),
  raw_packets               UInt64                          CODEC(T64, ZSTD(1)),
  estimated_bytes           UInt64                          CODEC(T64, ZSTD(1)),
  estimated_packets         UInt64                          CODEC(T64, ZSTD(1)),
  observed_records          UInt64                          CODEC(T64, ZSTD(1)),
  known_records             UInt64                          CODEC(T64, ZSTD(1)),
  dimension_snapshot_ids    Array(LowCardinality(String)),
  geo_versions              Array(LowCardinality(String)),
  classification_versions   Array(UInt32),
  ingest_generation_min     UInt64                          CODEC(DoubleDelta, ZSTD(1)),
  ingest_generation_max     UInt64                          CODEC(DoubleDelta, ZSTD(1)),
  generation                UInt64                          CODEC(DoubleDelta, ZSTD(1)),
  generated_at              DateTime64(3, 'UTC')            CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, row_kind, device_id, exporter_id, if_index, direction, layer)
SETTINGS index_granularity = 8192;

-- Non-endpoint mid-range query layer. Same EAV structure as flow_aggregate_1h so
-- the query compiler and Layer-1 table composer reuse it verbatim; the rollup
-- writer excludes the high-cardinality src_ip / dst_ip / remote_port kinds (they
-- stay in raw and the 2-day 1m tier), which is where the capacity saving comes
-- from -- not from TTL alone. Query cache layer: it carries an online-adjustable
-- 90-day TTL with whole-part drops (Akvorado inverted-pyramid model, design §1,
-- lifecycle review §11.2-B), unlike the evidence tiers which stay on the state
-- machine.
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_aggregate_5m AS watchdog_flow.flow_aggregate_1h
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (
  bucket, dimension_kind, dimension_value,
  target_id, device_id, exporter_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version)
TTL bucket + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
