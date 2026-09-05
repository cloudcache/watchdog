-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Forward-only receipt v2 audit metadata. Existing receipts remain schema 1
-- and must not be interpreted as complete v2 reconciliation evidence.
ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS receipt_schema UInt16 DEFAULT 1 AFTER worker_schema;

ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS tenant_ids Array(String) AFTER receipt_schema;

ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS raw_packets UInt64 AFTER raw_bytes;

ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS estimated_packets UInt64 AFTER estimated_bytes;

ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS estimated_valid_records UInt64 AFTER estimated_packets;

ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS min_event_time DateTime64(3, 'UTC') AFTER estimated_valid_records;

ALTER TABLE watchdog_flow.flow_ingest_batches
  ADD COLUMN IF NOT EXISTS max_event_time DateTime64(3, 'UTC') AFTER min_event_time;
