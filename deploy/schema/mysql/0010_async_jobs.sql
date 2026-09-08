-- Watchdog v2 async job engine (KISS-05 support) — de-tenanted port of the SaaS
-- operation_jobs framework (legacy migrations 017 + 037), backing internal/opjob.
--
-- This is a dedicated table, NOT the simpler operation_jobs in 0001: that table is
-- co-owned/unused and lacks the lease-takeover + retry-backoff + attempt-budget
-- columns the ported worker relies on (the "exception handling" the address
-- import/publish chain depends on). Kept separate so the shared baseline table is
-- untouched; the two can be unified later.
--
-- De-tenaning vs the SaaS original: tenant_id, scope_type, schedule_id removed;
-- the idempotency unique drops its tenant column. Everything else — lease
-- (SKIP LOCKED takeover), next_attempt_at retry backoff, attempt_count budget,
-- heartbeat, versioned checkpoint_json — is preserved verbatim.

CREATE TABLE IF NOT EXISTS async_jobs (
  id                  CHAR(26)        NOT NULL,
  job_type            VARCHAR(64)     NOT NULL,
  status              VARCHAR(16)     NOT NULL DEFAULT 'queued', -- queued|running|cancel_requested|succeeded|failed|canceled
  idempotency_key     VARCHAR(190)    NOT NULL,
  request_hash        CHAR(64)        NOT NULL,
  progress_total      BIGINT UNSIGNED NOT NULL DEFAULT 0,
  progress_done       BIGINT UNSIGNED NOT NULL DEFAULT 0,
  checkpoint_json     JSON            NULL,
  result_ref          VARCHAR(255)    NULL,
  lease_owner         VARCHAR(128)    NULL,
  lease_token         VARCHAR(64)     NULL,   -- 32-hex worker lease token
  lease_expires_at    DATETIME(3)     NULL,
  next_attempt_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  attempt_count       INT UNSIGNED    NOT NULL DEFAULT 0,
  last_error_code     VARCHAR(64)     NULL,
  last_error_detail   TEXT            NULL,
  row_version         BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by          CHAR(26)        NULL,
  created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  started_at          DATETIME(3)     NULL,
  heartbeat_at        DATETIME(3)     NULL,
  cancel_requested_at DATETIME(3)     NULL,
  finished_at         DATETIME(3)     NULL,
  updated_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_async_jobs_idem (job_type, idempotency_key),
  KEY idx_async_jobs_due (status, job_type, next_attempt_at),
  KEY idx_async_jobs_created (created_at, id),
  CONSTRAINT fk_async_jobs_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
