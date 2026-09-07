-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- BREAKING MAINTENANCE-WINDOW MIGRATION.
-- Stop every flow writer and legacy flow-rollup job before applying. The old
-- hash-keyed tables are retained under *_legacy_hash_v1 for explicit rollback;
-- this migration never drops them. See docs/flow-storage-v2-change-plan.md.

-- Storage V2 reserves the high 32 bits of aggregate generation for the MySQL
-- policy revision. Refuse an unsafe cut-over instead of allowing a legacy
-- generation to outrank every V2 rebuild.
INSERT INTO TABLE FUNCTION null('value UInt8')
SELECT throwIf(
  coalesce(max(generation), toUInt64(0)) >= 4294967296,
  'legacy flow_aggregate_1m generation overlaps the Storage V2 namespace')
FROM watchdog_flow.flow_aggregate_1m FINAL;

INSERT INTO TABLE FUNCTION null('value UInt8')
SELECT throwIf(
  coalesce(max(generation), toUInt64(0)) >= 4294967296,
  'legacy flow_aggregate_1h generation overlaps the Storage V2 namespace')
FROM watchdog_flow.flow_aggregate_1h FINAL;

DROP TABLE IF EXISTS watchdog_flow.flow_records_v2_staging;

CREATE TABLE watchdog_flow.flow_records_v2_staging (
  event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  received_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  source_stream_id LowCardinality(String),
  ingest_generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  kafka_topic LowCardinality(String),
  kafka_partition UInt32,
  kafka_offset UInt64 CODEC(DoubleDelta, ZSTD(1)),
  record_index UInt32,
  tenant_id LowCardinality(String),
  collector_id LowCardinality(String),
  exporter_id LowCardinality(String),
  target_id LowCardinality(String),
  device_id LowCardinality(String),
  registry_version UInt64,
  exporter_epoch UInt64,
  exporter_source_ip IPv6 CODEC(ZSTD(1)),
  flow_protocol UInt8,
  observation_domain_id UInt64,
  sub_agent_id UInt32,
  datagram_sequence UInt32,
  agent_ip IPv6 CODEC(ZSTD(1)),
  agent_ip_valid Bool,
  observation_if_index UInt32,
  ingress_if_index UInt32,
  egress_if_index UInt32,
  observation_direction Enum8('unknown'=0,'ingress'=1,'egress'=2),
  src_ip IPv6 CODEC(ZSTD(1)),
  dst_ip IPv6 CODEC(ZSTD(1)),
  src_port UInt16,
  dst_port UInt16,
  ip_protocol UInt8,
  tcp_flags UInt8,
  source_asn UInt32,
  destination_asn UInt32,
  raw_bytes UInt64 CODEC(T64, ZSTD(1)),
  raw_packets UInt64 CODEC(T64, ZSTD(1)),
  sampling_mode Enum8('unknown'=0,'sampled'=1,'pre_scaled'=2),
  sampling_rate UInt64 CODEC(T64, ZSTD(1)),
  sampling_source Enum8('unknown'=0,'protocol'=1,'plan_rule'=2,'exporter_default'=3,'counter_mode'=4),
  estimated_valid Bool,
  estimated_bytes UInt64 CODEC(T64, ZSTD(1)),
  estimated_packets UInt64 CODEC(T64, ZSTD(1)),
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
  remote_geo_continent_id LowCardinality(String),
  remote_geo_region_id LowCardinality(String),
  remote_geo_country_id LowCardinality(String),
  remote_geo_province_id LowCardinality(String),
  remote_geo_city_id LowCardinality(String),
  remote_isp_id UInt16,
  remote_asn UInt32,
  remote_asn_source Enum8('unknown'=0,'exporter'=1,'flow-geo-v1'=2,'flow_geo_override'=3,'flow-geo-v2'=4),
  geo_version LowCardinality(String),
  category Enum8(
    'unknown'=0,'on_net_local_city'=1,'on_net_cross_city'=2,
    'on_net_cross_province'=3,'off_net_in_province'=4,
    'off_net_cross_province'=5,'overseas'=6,'internal'=7,
    'transit'=8,'ambiguous'=9),
  disposition Enum8('drop'=0,'count'=1),
  classification_version UInt32,
  fact_schema UInt16 DEFAULT 3,
  supplier_remote_country FixedString(2),
  supplier_remote_admin_code LowCardinality(String),
  supplier_remote_subdivision LowCardinality(String),
  supplier_remote_city LowCardinality(String),
  supplier_remote_geo_continent_id LowCardinality(String),
  supplier_remote_geo_region_id LowCardinality(String),
  supplier_remote_geo_country_id LowCardinality(String),
  supplier_remote_geo_province_id LowCardinality(String),
  supplier_remote_geo_city_id LowCardinality(String),
  supplier_remote_isp_id UInt16,
  supplier_remote_asn UInt32,
  supplier_remote_asn_source Enum8('unknown'=0,'exporter'=1,'flow-geo-v1'=2,'flow_geo_override'=3,'flow-geo-v2'=4),
  supplier_geo_version LowCardinality(String),
  supplier_category Enum8(
    'unknown'=0,'on_net_local_city'=1,'on_net_cross_city'=2,
    'on_net_cross_province'=3,'off_net_in_province'=4,
    'off_net_cross_province'=5,'overseas'=6,'internal'=7,
    'transit'=8,'ambiguous'=9),
  customer_geo_override_fields UInt8,
  PROJECTION flow_ingest_audit_v2 (
    SELECT
      source_stream_id, kafka_topic, kafka_partition, kafka_offset,
      record_index, raw_bytes, raw_packets, estimated_valid,
      estimated_bytes, estimated_packets, ingest_generation
    ORDER BY (source_stream_id, kafka_partition, kafka_offset, record_index)
  )
)
ENGINE = ReplacingMergeTree(ingest_generation)
PARTITION BY (tenant_id, toYYYYMMDD(event_time))
ORDER BY (
  tenant_id, toStartOfHour(event_time), source_stream_id,
  kafka_partition, kafka_offset, record_index)
SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO watchdog_flow.flow_records_v2_staging
SELECT
  event_time, received_time, concat('legacy:', kafka_topic),
  ingest_generation, kafka_topic, kafka_partition, kafka_offset, record_index,
  tenant_id, collector_id, exporter_id, target_id, device_id,
  registry_version, exporter_epoch, exporter_source_ip, flow_protocol,
  observation_domain_id, sub_agent_id, datagram_sequence, agent_ip,
  agent_ip_valid, observation_if_index, ingress_if_index, egress_if_index,
  observation_direction, src_ip, dst_ip, src_port, dst_port, ip_protocol,
  tcp_flags, source_asn, destination_asn, raw_bytes, raw_packets,
  sampling_mode, sampling_rate, sampling_source, estimated_valid,
  estimated_bytes, estimated_packets, flow_duration_ms, quality_flags,
  source_id_type, source_id_value, sample_sequence, sample_pool,
  exporter_drops, sample_index, quality_epoch, dimension_snapshot_id,
  dimension_version, business_direction, business, local_ip, local_ip_valid,
  remote_ip, remote_ip_valid, local_port, remote_port, local_prefix_id,
  remote_prefix_id, local_address_set_ids, remote_address_set_ids,
  remote_country, remote_admin_code, remote_subdivision, remote_city,
  remote_geo_continent_id, remote_geo_region_id, remote_geo_country_id,
  remote_geo_province_id, remote_geo_city_id, remote_isp_id, remote_asn,
  remote_asn_source, geo_version, category, disposition,
  classification_version, toUInt16(3), supplier_remote_country,
  supplier_remote_admin_code, supplier_remote_subdivision,
  supplier_remote_city, supplier_remote_geo_continent_id,
  supplier_remote_geo_region_id, supplier_remote_geo_country_id,
  supplier_remote_geo_province_id, supplier_remote_geo_city_id,
  supplier_remote_isp_id, supplier_remote_asn, supplier_remote_asn_source,
  supplier_geo_version, supplier_category, customer_geo_override_fields
FROM watchdog_flow.flow_records FINAL;

DROP TABLE IF EXISTS watchdog_flow.flow_ingest_receipts_v2_staging;

CREATE TABLE watchdog_flow.flow_ingest_receipts_v2_staging (
  source_stream_id LowCardinality(String),
  worker_schema UInt32,
  receipt_schema UInt16 DEFAULT 4,
  message_disposition Enum8(
    'persisted'=1,'template_missing'=2,'empty'=3,
    'decode_rejected'=4,'mapping_rejected'=5),
  tenant_ids Array(String),
  kafka_topic LowCardinality(String),
  kafka_partition UInt32,
  kafka_offset UInt64 CODEC(DoubleDelta, ZSTD(1)),
  record_count UInt64 CODEC(T64, ZSTD(1)),
  raw_bytes UInt64 CODEC(T64, ZSTD(1)),
  raw_packets UInt64 CODEC(T64, ZSTD(1)),
  estimated_bytes UInt64 CODEC(T64, ZSTD(1)),
  estimated_packets UInt64 CODEC(T64, ZSTD(1)),
  estimated_valid_records UInt64 CODEC(T64, ZSTD(1)),
  min_event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  max_event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  generation UInt64 CODEC(DoubleDelta, ZSTD(1)),
  inserted_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(generation)
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (source_stream_id, kafka_partition, kafka_offset)
SETTINGS index_granularity = 8192;

INSERT INTO watchdog_flow.flow_ingest_receipts_v2_staging
SELECT
  concat('legacy:', kafka_topic), toUInt32(5), toUInt16(4), 'persisted',
  arraySort(groupUniqArray(tenant_id)), kafka_topic, kafka_partition,
  kafka_offset, count(), sum(raw_bytes), sum(raw_packets),
  sumIf(estimated_bytes, estimated_valid),
  sumIf(estimated_packets, estimated_valid), countIf(estimated_valid),
  min(event_time), max(event_time), max(ingest_generation), max(received_time)
FROM watchdog_flow.flow_records FINAL
GROUP BY kafka_topic, kafka_partition, kafka_offset;

-- One multi-table RENAME is atomic in the Atomic database engine and, unlike
-- EXCHANGE TABLES, an ambiguous acknowledgement cannot swap the tables back
-- when the migrator retries. A retry fails closed because the source names no
-- longer exist; the operator must inspect schemas before repairing the ledger.
RENAME TABLE
  watchdog_flow.flow_records TO watchdog_flow.flow_records_legacy_hash_v1,
  watchdog_flow.flow_records_v2_staging TO watchdog_flow.flow_records,
  watchdog_flow.flow_ingest_batches TO watchdog_flow.flow_ingest_batches_legacy_hash_v1,
  watchdog_flow.flow_ingest_receipts_v2_staging TO watchdog_flow.flow_ingest_receipts;

DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1m_v2_staging;

CREATE TABLE watchdog_flow.flow_aggregate_1m_v2_staging
AS watchdog_flow.flow_aggregate_1m
ENGINE = ReplacingMergeTree(generation)
PARTITION BY (tenant_id, toYYYYMM(bucket))
ORDER BY (
  tenant_id, bucket, dimension_kind, dimension_value,
  target_id, device_id, exporter_id, business_direction,
  category, business, dimension_snapshot_id, geo_version,
  classification_version)
SETTINGS index_granularity = 8192;

INSERT INTO watchdog_flow.flow_aggregate_1m_v2_staging
SELECT * FROM watchdog_flow.flow_aggregate_1m FINAL;

RENAME TABLE
  watchdog_flow.flow_aggregate_1m TO watchdog_flow.flow_aggregate_1m_legacy_ttl_v1,
  watchdog_flow.flow_aggregate_1m_v2_staging TO watchdog_flow.flow_aggregate_1m;

DROP TABLE IF EXISTS watchdog_flow.flow_aggregate_1h_v2_staging;

CREATE TABLE watchdog_flow.flow_aggregate_1h_v2_staging
AS watchdog_flow.flow_aggregate_1h
ENGINE = ReplacingMergeTree(generation)
PARTITION BY (tenant_id, toYYYYMM(bucket))
ORDER BY (
  tenant_id, bucket, dimension_kind, dimension_value,
  target_id, device_id, exporter_id, business_direction,
  category, business, dimension_snapshot_id, geo_version,
  classification_version)
SETTINGS index_granularity = 8192;

INSERT INTO watchdog_flow.flow_aggregate_1h_v2_staging
SELECT * FROM watchdog_flow.flow_aggregate_1h FINAL;

RENAME TABLE
  watchdog_flow.flow_aggregate_1h TO watchdog_flow.flow_aggregate_1h_legacy_ttl_v1,
  watchdog_flow.flow_aggregate_1h_v2_staging TO watchdog_flow.flow_aggregate_1h;
