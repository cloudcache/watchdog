-- L5B3b: monotonic raw-delete tombstones distributed to flow workers before
-- a ClickHouse event-day partition may be dropped.

CREATE TABLE IF NOT EXISTS flow_raw_delete_barriers (
  id                 CHAR(26)        NOT NULL,
  schema_version     SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  revision           BIGINT UNSIGNED NOT NULL,
  deleted_through    DATE            NOT NULL,
  exception_days_json JSON           NOT NULL,
  published_by       CHAR(26)        NULL,
  published_at       DATETIME(3)     NOT NULL,
  created_at         DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_raw_delete_barrier_revision (revision),
  CONSTRAINT fk_flow_raw_delete_barrier_user FOREIGN KEY (published_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT chk_flow_raw_delete_barrier_schema CHECK (schema_version = 1),
  CONSTRAINT chk_flow_raw_delete_barrier_revision CHECK (revision > 0),
  CONSTRAINT chk_flow_raw_delete_barrier_exceptions CHECK (JSON_TYPE(exception_days_json) = 'ARRAY')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_raw_delete_barrier_acks (
  barrier_id       CHAR(26)        NOT NULL,
  worker_id        CHAR(26)        NOT NULL,
  required         TINYINT(1)      NOT NULL DEFAULT 0,
  state            VARCHAR(16)     NOT NULL DEFAULT 'pending',
  boot_id          VARCHAR(128)    NOT NULL DEFAULT '',
  software_version VARCHAR(64)     NOT NULL DEFAULT '',
  attempted_at     DATETIME(3)     NULL,
  installed_at     DATETIME(3)     NULL,
  error_code       VARCHAR(64)     NULL,
  error_message    VARCHAR(512)    NULL,
  row_version      BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (barrier_id, worker_id),
  KEY idx_flow_raw_delete_barrier_ack_worker (worker_id, attempted_at),
  KEY idx_flow_raw_delete_barrier_ack_required (barrier_id, required, state),
  CONSTRAINT fk_flow_raw_delete_barrier_ack_barrier FOREIGN KEY (barrier_id) REFERENCES flow_raw_delete_barriers(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_raw_delete_barrier_ack_worker FOREIGN KEY (worker_id) REFERENCES agents(id) ON DELETE CASCADE,
  CONSTRAINT chk_flow_raw_delete_barrier_ack_required CHECK (required IN (0, 1)),
  CONSTRAINT chk_flow_raw_delete_barrier_ack_state CHECK (state IN ('pending','installed','failed')),
  CONSTRAINT chk_flow_raw_delete_barrier_ack_installed CHECK (state <> 'installed' OR installed_at IS NOT NULL),
  CONSTRAINT chk_flow_raw_delete_barrier_ack_failed CHECK ((state = 'failed') = (error_code IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
