-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Reorder the aggregate sort keys so the forced dimension_kind filter prunes
-- (F7). Every read pins (bucket, dimension_kind) — see flowquery
-- query.go (dimension_kind = {dimension}) and the _generation lookup — but
-- dimension_kind sat at ORDER BY position 8, behind target/device/exporter/
-- business_direction/category/business, none of which a top-N query pins. The
-- primary index could prune only (bucket), then scanned every
-- dimension_kind. Moving dimension_kind + dimension_value directly after
-- (bucket) makes the equality filter a granule range, and puts the
-- grouped/ranked dimension_value next for scan locality.
--
-- MergeTree cannot ALTER ORDER BY, so each table is rebuilt: a reordered copy,
-- a full-history INSERT SELECT (aggregates are small and, beyond the 30-day raw
-- window, unrebuildable — so history must be copied, not re-rolled), an atomic
-- EXCHANGE (watchdog_flow is an Atomic database), then the old table dropped.
-- The ORDER BY column SET is unchanged, so the ReplacingMergeTree dedup identity
-- is identical — only the tuple order differs. Column definitions and codecs
-- mirror the post-008 live schema exactly; only ORDER BY changes.
--
-- Statements are resume-safe: the migrator resumes at the failed statement, and
-- a re-run of the INSERT only produces exact-duplicate rows that FINAL and the
-- next merge collapse (the temp table is never queried by the app).

DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1m_reordered;

CREATE TABLE watchdog_flow.flow_aggregate_1m_reordered (
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
TTL bucket + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

INSERT INTO watchdog_flow.flow_aggregate_1m_reordered SELECT * FROM watchdog_flow.flow_aggregate_1m;

EXCHANGE TABLES watchdog_flow.flow_aggregate_1m AND watchdog_flow.flow_aggregate_1m_reordered;

DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1m_reordered;

DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1h_reordered;

CREATE TABLE watchdog_flow.flow_aggregate_1h_reordered (
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
TTL bucket + INTERVAL 400 DAY
SETTINGS index_granularity = 8192;

INSERT INTO watchdog_flow.flow_aggregate_1h_reordered SELECT * FROM watchdog_flow.flow_aggregate_1h;

EXCHANGE TABLES watchdog_flow.flow_aggregate_1h AND watchdog_flow.flow_aggregate_1h_reordered;

DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1h_reordered;
