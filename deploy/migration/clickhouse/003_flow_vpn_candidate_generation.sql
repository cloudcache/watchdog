-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Forward-only candidate v1 completion. Existing rows are candidates; an
-- internal generation marker makes an empty repair authoritative without
-- deleting historical parts.
ALTER TABLE watchdog_flow.flow_vpn_candidates
  ADD COLUMN IF NOT EXISTS row_kind Enum8('candidate'=1,'_generation'=2) DEFAULT 'candidate' AFTER tenant_id;

ALTER TABLE watchdog_flow.flow_vpn_candidates
  ADD COLUMN IF NOT EXISTS remote_prefix_id LowCardinality(String) AFTER remote_country;

ALTER TABLE watchdog_flow.flow_vpn_candidates
  ADD COLUMN IF NOT EXISTS geo_version LowCardinality(String) AFTER dimension_snapshot_id;

ALTER TABLE watchdog_flow.flow_vpn_candidates
  ADD COLUMN IF NOT EXISTS classification_version UInt32 AFTER geo_version;

-- ClickHouse permits an ORDER BY extension only through columns added by the
-- same ALTER. Keep explicit, writer-populated identity columns so this
-- statement is replay-safe after an acknowledged or unacknowledged success.
-- Pre-v1 rows receive zero/empty identities and remain outside the marker-led
-- reader contract; v1 writers must always populate all four columns.
ALTER TABLE watchdog_flow.flow_vpn_candidates
  ADD COLUMN IF NOT EXISTS key_row_kind UInt8 AFTER classification_version,
  ADD COLUMN IF NOT EXISTS key_dimension_snapshot_id String AFTER key_row_kind,
  ADD COLUMN IF NOT EXISTS key_geo_version String AFTER key_dimension_snapshot_id,
  ADD COLUMN IF NOT EXISTS key_classification_version UInt32 AFTER key_geo_version,
  MODIFY ORDER BY (
    tenant_id, window_start, window_end, conversation_key, rule_set_version,
    key_row_kind, key_dimension_snapshot_id, key_geo_version, key_classification_version);
