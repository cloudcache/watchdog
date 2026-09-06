-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Column compression codecs for the write-hot flow_records table. Every column
-- previously fell back to default LZ4; the timestamp, monotonic-offset, counter
-- and IPv6 columns compress substantially better with specialized codecs, which
-- cuts both on-disk size (30-day retention) and the scan I/O that the rollup and
-- ad-hoc detail reads pay. MODIFY COLUMN ... CODEC is metadata-only: it applies
-- to new parts and to existing parts as they merge, so it is fast and online.
ALTER TABLE watchdog_flow.flow_records
  MODIFY COLUMN event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN received_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN kafka_offset UInt64 CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN ingest_generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  MODIFY COLUMN raw_bytes UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN raw_packets UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN estimated_bytes UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN estimated_packets UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN sampling_rate UInt64 CODEC(T64, ZSTD(1)),
  MODIFY COLUMN src_ip IPv6 CODEC(ZSTD(1)),
  MODIFY COLUMN dst_ip IPv6 CODEC(ZSTD(1)),
  MODIFY COLUMN exporter_source_ip IPv6 CODEC(ZSTD(1)),
  MODIFY COLUMN agent_ip IPv6 CODEC(ZSTD(1));
