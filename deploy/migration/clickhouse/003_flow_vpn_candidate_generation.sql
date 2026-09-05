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

-- The original key remains an exact prefix. Version identity is part of the
-- replacement key so publication changes cannot overwrite each other.
ALTER TABLE watchdog_flow.flow_vpn_candidates
  MODIFY ORDER BY (
    tenant_id, window_start, window_end, conversation_key, rule_set_version,
    row_kind, dimension_snapshot_id, geo_version, classification_version);
