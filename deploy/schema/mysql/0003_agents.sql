-- Watchdog v2 agent registry. Agents are global to this installation.
-- A credential secret is returned/entered once and only its SHA-256 is stored.

CREATE TABLE IF NOT EXISTS agents (
  id                   CHAR(26)     NOT NULL,
  name                 VARCHAR(190) NOT NULL DEFAULT '',
  kind                 VARCHAR(32)  NOT NULL,
  status               VARCHAR(16)  NOT NULL DEFAULT 'pending',
  health               VARCHAR(16)  NOT NULL DEFAULT 'unknown',
  software_version     VARCHAR(64)  NOT NULL DEFAULT '',
  api_version          VARCHAR(32)  NOT NULL DEFAULT '',
  capabilities_json    JSON         NOT NULL,
  desired_plan_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  acked_plan_version   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_seen_at         DATETIME(3)  NULL,
  last_run_at          DATETIME(3)  NULL,
  last_success_at      DATETIME(3)  NULL,
  last_error           TEXT         NULL,
  run_count            BIGINT UNSIGNED NOT NULL DEFAULT 0,
  failure_count        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  row_version          BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by           CHAR(26)     NULL,
  updated_by           CHAR(26)     NULL,
  created_at           DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at           DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_agents_kind_status (kind, status),
  KEY idx_agents_last_seen (last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_credentials (
  id             CHAR(26)     NOT NULL,
  agent_id       CHAR(26)     NOT NULL,
  auth_type      VARCHAR(16)  NOT NULL DEFAULT 'token',
  token_sha256   CHAR(64)     NULL,
  mtls_fingerprint VARCHAR(128) NULL,
  status         VARCHAR(16)  NOT NULL DEFAULT 'active',
  issued_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at     DATETIME(3)  NULL,
  last_used_at   DATETIME(3)  NULL,
  revoked_at     DATETIME(3)  NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_agent_credentials_token (token_sha256),
  KEY idx_agent_credentials_agent (agent_id, status),
  CONSTRAINT fk_agent_credentials_agent FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_bindings (
  id          CHAR(26)     NOT NULL,
  agent_id    CHAR(26)     NOT NULL,
  device_id   CHAR(26)     NULL,
  role        VARCHAR(32)  NOT NULL DEFAULT '',
  mode        VARCHAR(16)  NOT NULL DEFAULT 'push',
  endpoint    VARCHAR(512) NOT NULL DEFAULT '',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_agent_bindings_agent (agent_id),
  KEY idx_agent_bindings_device (device_id),
  CONSTRAINT fk_agent_bindings_agent FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE,
  CONSTRAINT fk_agent_bindings_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_runs (
  id          CHAR(26)     NOT NULL,
  agent_id    CHAR(26)     NOT NULL,
  kind        VARCHAR(32)  NOT NULL DEFAULT 'collection',
  status      VARCHAR(16)  NOT NULL,
  error_text  TEXT         NULL,
  seen        TINYINT(1)   NOT NULL DEFAULT 0,
  started_at  DATETIME(3)  NOT NULL,
  ended_at    DATETIME(3)  NOT NULL,
  duration_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  summary_json JSON        NULL,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_agent_runs_agent_ended (agent_id, ended_at),
  KEY idx_agent_runs_status (agent_id, status, seen),
  CONSTRAINT fk_agent_runs_agent FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_enrollment_tokens (
  id             CHAR(26)    NOT NULL,
  token_sha256   CHAR(64)    NOT NULL,
  allowed_kind   VARCHAR(32) NOT NULL DEFAULT '',
  expires_at     DATETIME(3) NOT NULL,
  consumed_at    DATETIME(3) NULL,
  created_by     CHAR(26)    NULL,
  created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_agent_enrollment_token (token_sha256),
  KEY idx_agent_enrollment_expiry (expires_at, consumed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_plans (
  id             CHAR(26)     NOT NULL,
  agent_id       CHAR(26)     NOT NULL,
  plan_version   BIGINT UNSIGNED NOT NULL,
  schema_version INT UNSIGNED NOT NULL,
  payload_json   JSON         NOT NULL,
  signature      TEXT         NULL,
  created_by     CHAR(26)     NULL,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_agent_plan_version (agent_id, plan_version),
  CONSTRAINT fk_agent_plans_agent FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_plan_acks (
  id             CHAR(26)     NOT NULL,
  agent_id       CHAR(26)     NOT NULL,
  plan_version   BIGINT UNSIGNED NOT NULL,
  status         VARCHAR(16)  NOT NULL,
  error_text     TEXT         NULL,
  acknowledged_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_agent_plan_ack (agent_id, plan_version),
  CONSTRAINT fk_agent_plan_acks_agent FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
