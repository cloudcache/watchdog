-- PLAT-00A idempotent create/action support: a repeated request with the same
-- Idempotency-Key and request hash replays the stored response; the same key
-- with a different hash is a conflict. Records expire and are pruned lazily.
CREATE TABLE IF NOT EXISTS idempotency_records (
  tenant_id CHAR(26) NOT NULL,
  idempotency_key VARCHAR(128) NOT NULL,
  request_hash CHAR(64) NOT NULL,
  response_status SMALLINT UNSIGNED NOT NULL,
  response_body MEDIUMBLOB NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  PRIMARY KEY (tenant_id, idempotency_key),
  KEY idx_idempotency_expiry (expires_at),
  CONSTRAINT fk_idempotency_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
