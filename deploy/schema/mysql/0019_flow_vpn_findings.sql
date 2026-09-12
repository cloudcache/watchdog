-- KISS-06 VPN Tier-1: materialized VPN/proxy findings. The scoring pipeline reads
-- CH flow_vpn_candidates + the published rule set (internal/flowvpn) and writes one
-- finding per (window, conversation, dimension/geo/classification version). De-tenanted
-- v2 of the hub findings domain: no tenant_id. finding_key is a deterministic
-- sha256 of the natural key so re-materialization UPSERTs in place, refreshing the
-- score/verdict/evidence while preserving the human disposition and probe workflow.
CREATE TABLE IF NOT EXISTS flow_vpn_findings (
  id                      CHAR(26)          NOT NULL,
  finding_key             CHAR(64)          NOT NULL,          -- sha256(window_start|window_end|conversation_key|dim|geo|class)

  window_start            DATETIME(3)       NOT NULL,
  window_end              DATETIME(3)       NOT NULL,
  conversation_key        CHAR(64)          NOT NULL,          -- hex sha256(local_ip, remote_ip)
  local_ip                VARCHAR(45)       NOT NULL,
  remote_ip               VARCHAR(45)       NOT NULL,
  primary_protocol        TINYINT UNSIGNED  NOT NULL DEFAULT 0,
  primary_local_port      SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  primary_remote_port     SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  local_to_remote_bytes   BIGINT UNSIGNED   NOT NULL DEFAULT 0,
  remote_to_local_bytes   BIGINT UNSIGNED   NOT NULL DEFAULT 0,
  flow_record_count       BIGINT UNSIGNED   NOT NULL DEFAULT 0,
  active_bucket_count     INT UNSIGNED      NOT NULL DEFAULT 0,
  max_duration_ms         BIGINT UNSIGNED   NOT NULL DEFAULT 0,
  packet_bytes_p50        BIGINT UNSIGNED   NOT NULL DEFAULT 0,
  remote_asn              INT UNSIGNED      NOT NULL DEFAULT 0,
  remote_country          VARCHAR(2)        NOT NULL DEFAULT '',
  remote_prefix_id        VARCHAR(128)      NOT NULL DEFAULT '',
  local_prefix_id         VARCHAR(128)      NOT NULL DEFAULT '',
  complete_ratio          DOUBLE            NOT NULL DEFAULT 0,

  score                   SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  risk_level              VARCHAR(16)       NOT NULL DEFAULT 'low',
  verdict                 VARCHAR(24)       NOT NULL DEFAULT 'observe',
  symmetry_ratio          DOUBLE            NOT NULL DEFAULT 0,
  dominance_ratio         DOUBLE            NOT NULL DEFAULT 0,
  probe_recommended       TINYINT(1)        NOT NULL DEFAULT 0,
  probe_block_reason      VARCHAR(64)       NOT NULL DEFAULT '',
  decision_rule_id        VARCHAR(128)      NOT NULL DEFAULT '',
  rule_set_version        VARCHAR(128)      NOT NULL DEFAULT '',
  family_hints            JSON              NULL,              -- suspected proxy/VPN families (behavioral)
  evidence_json           JSON              NOT NULL,          -- matched-rule evidence + materialization evidence

  source_generation       BIGINT UNSIGNED   NOT NULL DEFAULT 0,
  generated_at            DATETIME(3)       NOT NULL,

  disposition             VARCHAR(24)       NOT NULL DEFAULT 'unreviewed',
  disposition_note        VARCHAR(2000)     NOT NULL DEFAULT '',
  disposition_by          CHAR(26)          NULL DEFAULT NULL,
  disposition_at          DATETIME(3)       NULL DEFAULT NULL,
  probe_status            VARCHAR(24)       NOT NULL DEFAULT 'not_requested',
  probe_job_id            CHAR(26)          NULL DEFAULT NULL,
  probe_result_json       JSON              NULL DEFAULT NULL,

  row_version             BIGINT UNSIGNED   NOT NULL DEFAULT 1,
  created_at              DATETIME(3)       NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at              DATETIME(3)       NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_vpn_findings_key (finding_key),
  KEY idx_flow_vpn_findings_window (window_end),
  KEY idx_flow_vpn_findings_triage (disposition, verdict, score),
  KEY idx_flow_vpn_findings_probe (probe_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
