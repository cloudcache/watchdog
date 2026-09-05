CREATE DATABASE IF NOT EXISTS watchdog_flow;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_records (
  event_time DateTime64(3, 'UTC'),
  received_time DateTime64(3, 'UTC'),
  record_id FixedString(32),
  ingest_batch_id FixedString(32),
  ingest_generation UInt64,
  kafka_topic LowCardinality(String),
  kafka_partition UInt32,
  kafka_offset UInt64,
  record_index UInt32,
  tenant_id LowCardinality(String),
  collector_id LowCardinality(String),
  exporter_id LowCardinality(String),
  target_id LowCardinality(String),
  device_id LowCardinality(String),
  registry_version UInt64,
  exporter_epoch UInt64,
  exporter_source_ip IPv6,
  flow_protocol UInt8,
  observation_domain_id UInt64,
  sub_agent_id UInt32,
  datagram_sequence UInt32,
  agent_ip IPv6,
  agent_ip_valid Bool,
  observation_if_index UInt32,
  ingress_if_index UInt32,
  egress_if_index UInt32,
  observation_direction Enum8('unknown'=0,'ingress'=1,'egress'=2),
  src_ip IPv6,
  dst_ip IPv6,
  src_port UInt16,
  dst_port UInt16,
  ip_protocol UInt8,
  tcp_flags UInt8,
  source_asn UInt32,
  destination_asn UInt32,
  raw_bytes UInt64,
  raw_packets UInt64,
  sampling_mode Enum8('unknown'=0,'sampled'=1,'pre_scaled'=2),
  sampling_rate UInt64,
  sampling_source Enum8('unknown'=0,'protocol'=1,'plan_rule'=2,'exporter_default'=3,'counter_mode'=4),
  estimated_valid Bool,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  flow_duration_ms UInt64,
  quality_flags UInt64,
  source_id_type UInt32,
  source_id_value UInt32,
  sample_sequence UInt32,
  sample_pool UInt64,
  exporter_drops UInt64,
  sample_index UInt32,
  quality_epoch UInt64,
  dimension_snapshot_id LowCardinality(String),
  dimension_version UInt64,
  dimension_fingerprint UInt64,
  business_direction Enum8('ambiguous'=0,'in'=1,'out'=2,'internal'=3,'transit'=4),
  business LowCardinality(String),
  local_ip IPv6,
  local_ip_valid Bool,
  remote_ip IPv6,
  remote_ip_valid Bool,
  local_port UInt16,
  remote_port UInt16,
  local_prefix_id LowCardinality(String),
  remote_prefix_id LowCardinality(String),
  local_address_set_ids Array(String),
  remote_address_set_ids Array(String),
  remote_country FixedString(2),
  remote_admin_code LowCardinality(String),
  remote_subdivision LowCardinality(String),
  remote_city LowCardinality(String),
  remote_isp_id UInt16,
  remote_asn UInt32,
  remote_asn_source Enum8('unknown'=0,'exporter'=1,'flow_geo_v1'=2,'flow_geo_override'=3),
  geo_version LowCardinality(String),
  category Enum8(
    'unknown'=0,
    'on_net_local_city'=1,
    'on_net_cross_city'=2,
    'on_net_cross_province'=3,
    'off_net_in_province'=4,
    'off_net_cross_province'=5,
    'overseas'=6,
    'internal'=7,
    'transit'=8,
    'ambiguous'=9),
  disposition Enum8('drop'=0,'count'=1),
  classification_version UInt32
)
ENGINE = ReplacingMergeTree(ingest_generation)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (tenant_id, toStartOfHour(event_time), record_id)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_aggregate_1m (
  bucket DateTime('UTC'),
  tenant_id LowCardinality(String),
  target_id LowCardinality(String),
  device_id LowCardinality(String),
  exporter_id LowCardinality(String),
  business_direction LowCardinality(String),
  category LowCardinality(String),
  business LowCardinality(String),
  dimension_kind LowCardinality(String),
  dimension_value String,
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  raw_bytes UInt64,
  raw_packets UInt64,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  received_records UInt64,
  unknown_sampling_records UInt64,
  quality_records UInt64,
  generation UInt64,
  generated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket)
ORDER BY (
  tenant_id, bucket, target_id, device_id, exporter_id, business_direction,
  category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version)
TTL bucket + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_aggregate_1h (
  bucket DateTime('UTC'),
  tenant_id LowCardinality(String),
  target_id LowCardinality(String),
  device_id LowCardinality(String),
  exporter_id LowCardinality(String),
  business_direction LowCardinality(String),
  category LowCardinality(String),
  business LowCardinality(String),
  dimension_kind LowCardinality(String),
  dimension_value String,
  dimension_snapshot_id LowCardinality(String),
  geo_version LowCardinality(String),
  classification_version UInt32,
  raw_bytes UInt64,
  raw_packets UInt64,
  estimated_bytes UInt64,
  estimated_packets UInt64,
  received_records UInt64,
  unknown_sampling_records UInt64,
  quality_records UInt64,
  generation UInt64,
  generated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(bucket)
ORDER BY (
  tenant_id, bucket, target_id, device_id, exporter_id, business_direction,
  category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version)
TTL bucket + INTERVAL 400 DAY;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_ingest_batches (
  ingest_batch_id FixedString(32),
  worker_schema UInt32,
  kafka_topic LowCardinality(String),
  kafka_partition UInt32,
  first_offset UInt64,
  last_offset UInt64,
  source_batch_count UInt32,
  record_count UInt64,
  raw_bytes UInt64,
  estimated_bytes UInt64,
  checksum FixedString(32),
  generation UInt64,
  inserted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (kafka_topic, kafka_partition, first_offset, last_offset, ingest_batch_id)
TTL inserted_at + INTERVAL 45 DAY;

CREATE TABLE IF NOT EXISTS watchdog_flow.flow_vpn_candidates (
  window_start DateTime('UTC'),
  window_end DateTime('UTC'),
  tenant_id LowCardinality(String),
  conversation_key FixedString(32),
  local_ip IPv6,
  remote_ip IPv6,
  primary_protocol UInt8,
  primary_local_port UInt16,
  primary_remote_port UInt16,
  local_to_remote_bytes UInt64,
  remote_to_local_bytes UInt64,
  flow_record_count UInt64,
  active_bucket_count UInt32,
  max_duration_ms UInt64,
  remote_asn UInt32,
  remote_country FixedString(2),
  transport_hints Array(LowCardinality(String)),
  complete_ratio Float32,
  evidence_json String,
  rule_set_version LowCardinality(String),
  dimension_snapshot_id LowCardinality(String),
  generation UInt64,
  generated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (tenant_id, window_start, window_end, conversation_key, rule_set_version)
TTL window_end + INTERVAL 90 DAY;
