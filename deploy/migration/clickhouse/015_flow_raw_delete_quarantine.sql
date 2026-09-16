-- L5B3b: exceptional late datagrams that touch a raw-delete tombstone are
-- stored outside event-time raw partitions. Kafka offsets are committed only
-- after this row and its ingest receipt are durable.

ALTER TABLE watchdog_flow.flow_ingest_receipts
  MODIFY COLUMN message_disposition Enum8(
    'persisted'=1,'template_missing'=2,'empty'=3,
    'decode_rejected'=4,'mapping_rejected'=5,'late_quarantined'=6);

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_quarantined_datagrams (
  quarantined_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  barrier_revision UInt64,
  barrier_deleted_through Date,
  matched_event_day Date,
  source_stream_id String,
  kafka_topic LowCardinality(String),
  kafka_partition UInt32,
  kafka_offset UInt64,
  collector_id LowCardinality(String),
  exporter_id LowCardinality(String),
  registry_version UInt64,
  received_at DateTime64(3, 'UTC'),
  flow_protocol UInt8,
  exporter_source_ip IPv6,
  observation_domain_id UInt64,
  sub_agent_id UInt32,
  datagram_sequence UInt32,
  agent_ip IPv6,
  exporter_epoch UInt64,
  decoded_record_count UInt64,
  raw_bytes UInt64,
  raw_packets UInt64,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  estimated_valid_records UInt64,
  min_event_time DateTime64(3, 'UTC'),
  max_event_time DateTime64(3, 'UTC'),
  raw_payload String CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree(barrier_revision)
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (source_stream_id, kafka_partition, kafka_offset)
SETTINGS index_granularity = 8192;
