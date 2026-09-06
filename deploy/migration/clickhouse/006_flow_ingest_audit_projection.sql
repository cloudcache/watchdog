-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- A narrow, offset-ordered copy for bounded receipt reconciliation. The base
-- table keeps its tenant/time order for user queries. ReplacingMergeTree must
-- rebuild this projection when it deduplicates parts; dropping it would make
-- audit cost silently regress to a base-table scan.
ALTER TABLE watchdog_flow.flow_records
  MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE watchdog_flow.flow_records
  ADD PROJECTION IF NOT EXISTS flow_ingest_audit_v1 (
    SELECT
      event_time,
      record_id,
      ingest_batch_id,
      ingest_generation,
      kafka_topic,
      kafka_partition,
      kafka_offset,
      record_index,
      raw_bytes,
      raw_packets,
      estimated_valid,
      estimated_bytes,
      estimated_packets,
      quality_flags,
      dimension_fingerprint,
      classification_version
    ORDER BY (kafka_topic, kafka_partition, kafka_offset, record_index, record_id)
  );

-- Existing parts must be queryable through the same path before migration is
-- reported complete. Operators must size the migration operation timeout for
-- their retained base volume.
ALTER TABLE watchdog_flow.flow_records
  MATERIALIZE PROJECTION flow_ingest_audit_v1
  SETTINGS mutations_sync = 2;
