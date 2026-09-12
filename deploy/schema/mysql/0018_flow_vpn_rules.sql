-- KISS-06 phase-1c: editable VPN scoring/suppression rules (management state).
-- De-tenanted port of the hub flow_vpn_rules table: no tenant_id column. The
-- hub's write-only behavior_json/intelligence_json/probe_policy_json placeholder
-- columns (never read back by the domain) are dropped as speculative; publication
-- freezes canonical rules from match_json/effect/weight/priority.
CREATE TABLE IF NOT EXISTS flow_vpn_rules (
  id                  CHAR(26)         NOT NULL,
  name                VARCHAR(190)     NOT NULL,
  kind                VARCHAR(16)      NOT NULL DEFAULT 'passive',
  rule_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  match_json          JSON             NOT NULL,
  effect              VARCHAR(16)      NOT NULL DEFAULT 'score',
  weight              SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  priority            SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  family_hint         VARCHAR(32)      NOT NULL DEFAULT '',  -- suspected proxy/VPN family tag (behavioral; '' = none)
  status              VARCHAR(16)      NOT NULL DEFAULT 'draft',
  row_version         BIGINT UNSIGNED  NOT NULL DEFAULT 1,
  created_by          CHAR(26)         NOT NULL,
  updated_by          CHAR(26)         NOT NULL,
  created_at          DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at          DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  deleted_at          DATETIME(3)      NULL DEFAULT NULL,
  PRIMARY KEY (id),
  -- Names are unique for the life of the install (a soft-deleted rule keeps its
  -- name reserved), matching the hub's UNIQUE(tenant_id, name), de-tenanted.
  UNIQUE KEY uq_flow_vpn_rules_name (name),
  KEY idx_flow_vpn_rules_status (status, deleted_at),
  KEY idx_flow_vpn_rules_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
