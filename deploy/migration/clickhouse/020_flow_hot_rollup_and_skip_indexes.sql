-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- flow_aggregate_1m is a recent-query cache, not lifecycle authority. Rebuild
-- it with day partitions so a two-day TTL can drop whole parts without row
-- mutations. The prior V2 table is retained for explicit rollback.
DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1m_hot_staging;

CREATE TABLE watchdog_flow.flow_aggregate_1m_hot_staging
AS watchdog_flow.flow_aggregate_1m
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (
  bucket, dimension_kind, dimension_value,
  target_id, device_id, exporter_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version)
TTL bucket + INTERVAL 2 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

INSERT INTO watchdog_flow.flow_aggregate_1m_hot_staging
SELECT * FROM watchdog_flow.flow_aggregate_1m FINAL;

RENAME TABLE
  watchdog_flow.flow_aggregate_1m TO watchdog_flow.flow_aggregate_1m_storage_v2_legacy,
  watchdog_flow.flow_aggregate_1m_hot_staging TO watchdog_flow.flow_aggregate_1m;

-- Long-range queries read one source bucket per UTC day. The table is derived
-- from reconciled latest-generation 1h rows and is dropped alongside the same
-- monthly 1h archive partition; it is not an independent retention authority.
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_aggregate_1d (
  bucket DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
  target_id LowCardinality(String),
  device_id LowCardinality(String),
  exporter_id LowCardinality(String),
  business_direction LowCardinality(String),
  category LowCardinality(String),
  business LowCardinality(String),
  dimension_kind LowCardinality(String),
  dimension_value String CODEC(ZSTD(1)),
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32 CODEC(DoubleDelta, ZSTD(1)),
  raw_bytes UInt64 CODEC(T64, ZSTD(1)),
  raw_packets UInt64 CODEC(T64, ZSTD(1)),
  estimated_bytes UInt64 CODEC(T64, ZSTD(1)),
  estimated_packets UInt64 CODEC(T64, ZSTD(1)),
  received_records UInt64 CODEC(T64, ZSTD(1)),
  unknown_sampling_records UInt64 CODEC(T64, ZSTD(1)),
  quality_records UInt64 CODEC(T64, ZSTD(1)),
  generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  generated_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket)
ORDER BY (
  bucket, dimension_kind, dimension_value,
  target_id, device_id, exporter_id, business_direction, category, business,
  dimension_snapshot_id, geo_version, classification_version)
SETTINGS index_granularity = 8192;

-- New raw parts receive indexes used by resource-scoped reports and endpoint
-- enrichment. Existing parts can be materialized later under an explicit I/O
-- budget; this migration intentionally avoids a 400M-row rewrite.
ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_device_set device_id TYPE set(1024) GRANULARITY 4;

ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_target_set target_id TYPE set(1024) GRANULARITY 4;

ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_exporter_set exporter_id TYPE set(1024) GRANULARITY 4;

ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_src_ip_bloom src_ip TYPE bloom_filter(0.001) GRANULARITY 4;

ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_dst_ip_bloom dst_ip TYPE bloom_filter(0.001) GRANULARITY 4;

ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_direction_set business_direction TYPE set(16) GRANULARITY 4;

ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_category_set category TYPE set(32) GRANULARITY 4;
