CREATE TABLE IF NOT EXISTS collector_plan_signing_keys (
  key_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
  algorithm VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'ed25519',
  public_key VARBINARY(32) NOT NULL,
  public_key_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  activated_at DATETIME(3) NOT NULL,
  retiring_at DATETIME(3) NULL,
  trust_until DATETIME(3) NULL,
  revoked_at DATETIME(3) NULL,
  revocation_reason VARCHAR(512) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  active_identity TINYINT GENERATED ALWAYS AS
    (IF(status = 'active', 1, NULL)) STORED,
  UNIQUE KEY uq_collector_plan_signing_public_key (public_key_sha256),
  UNIQUE KEY uq_collector_plan_signing_active (active_identity),
  KEY idx_collector_plan_signing_lifecycle (status, trust_until, key_id),
  CHECK (algorithm = 'ed25519'),
  CHECK (status IN ('active','retiring','revoked')),
  CHECK (
    (status = 'active' AND retiring_at IS NULL AND trust_until IS NULL
      AND revoked_at IS NULL AND revocation_reason IS NULL)
    OR
    (status = 'retiring' AND retiring_at IS NOT NULL
      AND trust_until IS NOT NULL AND trust_until >= retiring_at
      AND revoked_at IS NULL AND revocation_reason IS NULL)
    OR
    (status = 'revoked' AND retiring_at IS NOT NULL
      AND trust_until IS NOT NULL AND revoked_at IS NOT NULL
      AND revocation_reason IS NOT NULL)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_plan_trust_state (
  singleton_id TINYINT UNSIGNED PRIMARY KEY,
  generation BIGINT UNSIGNED NOT NULL,
  bundle_json JSON NOT NULL,
  checksum_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  issued_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  CHECK (singleton_id = 1),
  CHECK (JSON_TYPE(bundle_json) = 'OBJECT'),
  CHECK ((generation = 0 AND issued_at IS NULL) OR (generation > 0 AND issued_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
