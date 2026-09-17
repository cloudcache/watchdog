-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- FLOW-06B keeps historical reinterpretations separate from immutable ingest
-- facts. The schema is cloned from the current Storage V2 fact contract so the
-- normal query compiler can read an activated generation without a runtime
-- join or dictGet. A generation is never selected until its completion marker
-- and MySQL activation row both exist.
CREATE TABLE IF NOT EXISTS watchdog_flow.flow_reclassified_records
ENGINE = ReplacingMergeTree(ingest_generation)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (
  reclassification_id, reclassification_generation,
  toStartOfHour(event_time), source_stream_id,
  kafka_partition, kafka_offset, record_index)
SETTINGS index_granularity = 8192
AS SELECT
  CAST('' AS String) AS reclassification_id,
  toUInt64(0) AS reclassification_generation,
  *
FROM watchdog_flow.flow_records
WHERE 0;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_reclassification_generations (
  reclassification_id String,
  generation UInt64,
  source_publication_id String,
  target_publication_id String,
  value_view Enum8('customer'=1,'supplier'=2),
  window_start DateTime64(3, 'UTC'),
  window_end DateTime64(3, 'UTC'),
  record_count UInt64,
  raw_bytes UInt64,
  raw_packets UInt64,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  estimated_valid_records UInt64,
  completed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (reclassification_id, generation)
SETTINGS index_granularity = 8192;
