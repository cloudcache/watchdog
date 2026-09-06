-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Column compression codecs for the long-retention aggregate tables (F17
-- follow-up). flow_aggregate_1m keeps 180 days and flow_aggregate_1h keeps 400,
-- so codecs here have even longer-lived storage/scan-I/O benefit than on
-- flow_records. The LowCardinality dimension columns already dictionary-encode
-- well and are left as-is; this targets the timestamp, generation, counter and
-- high-cardinality dimension_value columns. MODIFY COLUMN ... CODEC is
-- metadata-only (new parts + merges).
ALTER TABLE watchdog_flow.flow_aggregate_1m
  MODIFY COLUMN bucket DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN dimension_value String CODEC(ZSTD(1)),
  MODIFY COLUMN classification_version UInt32 CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN raw_bytes UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN raw_packets UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN estimated_bytes UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN estimated_packets UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN received_records UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN unknown_sampling_records UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN quality_records UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN generated_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE watchdog_flow.flow_aggregate_1h
  MODIFY COLUMN bucket DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN dimension_value String CODEC(ZSTD(1)),
  MODIFY COLUMN classification_version UInt32 CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN raw_bytes UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN raw_packets UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN estimated_bytes UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN estimated_packets UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN received_records UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN unknown_sampling_records UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN quality_records UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN generated_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1));
