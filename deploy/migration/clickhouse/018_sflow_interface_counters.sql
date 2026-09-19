-- Persist cumulative sFlow generic-interface counters as source truth. Rates,
-- resets and wrap handling are derived later; ingestion never rewrites them.

CREATE TABLE IF NOT EXISTS watchdog_flow.sflow_interface_counters (
  event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  received_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  source_stream_id LowCardinality(String),
  ingest_generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  kafka_topic LowCardinality(String),
  kafka_partition UInt32 CODEC(T64, ZSTD(1)),
  kafka_offset UInt64 CODEC(DoubleDelta, ZSTD(1)),
  sample_index UInt32 CODEC(T64, ZSTD(1)),
  record_index UInt32 CODEC(T64, ZSTD(1)),
  collector_id LowCardinality(String),
  exporter_id LowCardinality(String),
  target_id LowCardinality(String),
  device_id LowCardinality(String),
  registry_version UInt64 CODEC(DoubleDelta, ZSTD(1)),
  exporter_epoch UInt64 CODEC(DoubleDelta, ZSTD(1)),
  exporter_source_ip IPv6 CODEC(ZSTD(1)),
  sub_agent_id UInt32 CODEC(T64, ZSTD(1)),
  datagram_sequence UInt32 CODEC(Delta, ZSTD(1)),
  agent_ip IPv6 CODEC(ZSTD(1)),
  agent_ip_valid Bool CODEC(T64, ZSTD(1)),
  source_id_type UInt32 CODEC(T64, ZSTD(1)),
  source_id_value UInt32 CODEC(T64, ZSTD(1)),
  sample_sequence UInt32 CODEC(Delta, ZSTD(1)),
  if_index UInt32 CODEC(T64, ZSTD(1)),
  if_type UInt32 CODEC(T64, ZSTD(1)),
  if_speed UInt64 CODEC(T64, ZSTD(1)),
  if_direction UInt32 CODEC(T64, ZSTD(1)),
  if_status UInt32 CODEC(T64, ZSTD(1)),
  if_in_octets UInt64 CODEC(Delta, ZSTD(1)),
  if_in_ucast_pkts UInt32 CODEC(Delta, ZSTD(1)),
  if_in_multicast_pkts UInt32 CODEC(Delta, ZSTD(1)),
  if_in_broadcast_pkts UInt32 CODEC(Delta, ZSTD(1)),
  if_in_discards UInt32 CODEC(Delta, ZSTD(1)),
  if_in_errors UInt32 CODEC(Delta, ZSTD(1)),
  if_in_unknown_protos UInt32 CODEC(Delta, ZSTD(1)),
  if_out_octets UInt64 CODEC(Delta, ZSTD(1)),
  if_out_ucast_pkts UInt32 CODEC(Delta, ZSTD(1)),
  if_out_multicast_pkts UInt32 CODEC(Delta, ZSTD(1)),
  if_out_broadcast_pkts UInt32 CODEC(Delta, ZSTD(1)),
  if_out_discards UInt32 CODEC(Delta, ZSTD(1)),
  if_out_errors UInt32 CODEC(Delta, ZSTD(1)),
  if_promiscuous_mode UInt32 CODEC(T64, ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_generation)
PARTITION BY toYYYYMM(event_time)
ORDER BY (
  device_id, if_index, event_time,
  source_stream_id, kafka_partition, kafka_offset, sample_index, record_index
)
SETTINGS index_granularity = 8192;

ALTER TABLE watchdog_flow.flow_ingest_receipts
  ADD COLUMN IF NOT EXISTS counter_record_count UInt64 DEFAULT 0 CODEC(T64, ZSTD(1)) AFTER record_count;
