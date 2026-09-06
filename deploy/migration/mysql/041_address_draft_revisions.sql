-- PLAT-04C: immutable preview records for atomic address-library batch changes.
-- The API persists canonical operations and their preview before apply. Apply
-- compares base_digest again, commits the whole batch in one transaction and
-- records the resulting digest; a prepared revision is never rewritten into a
-- different request.
CREATE TABLE IF NOT EXISTS address_draft_revisions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  scope VARCHAR(16) NOT NULL,
  base_digest CHAR(71) NOT NULL,
  request_digest CHAR(71) NOT NULL,
  result_digest CHAR(71) NULL,
  operations_json JSON NOT NULL,
  preview_json JSON NOT NULL,
  operation_count INT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'prepared',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  applied_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  applied_at DATETIME(3) NULL,
  UNIQUE KEY uq_address_draft_revisions_tenant_id (tenant_id, id),
  UNIQUE KEY uq_address_draft_revisions_request (tenant_id, request_digest),
  KEY idx_address_draft_revisions_status (tenant_id, status, created_at, id),
  KEY idx_address_draft_revisions_expiry (status, expires_at),
  KEY fk_address_draft_revisions_created_by (created_by),
  KEY fk_address_draft_revisions_applied_by (applied_by),
  CONSTRAINT fk_address_draft_revisions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_address_draft_revisions_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_address_draft_revisions_applied_by FOREIGN KEY (applied_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (scope IN ('prefix','set','mixed')),
  CHECK (status IN ('prepared','applied','superseded','cancelled')),
  CHECK (operation_count > 0),
  CHECK (base_digest LIKE 'sha256:%' AND CHAR_LENGTH(base_digest) = 71),
  CHECK (request_digest LIKE 'sha256:%' AND CHAR_LENGTH(request_digest) = 71),
  CHECK (result_digest IS NULL OR (result_digest LIKE 'sha256:%' AND CHAR_LENGTH(result_digest) = 71)),
  CHECK (expires_at > created_at),
  CHECK ((status = 'applied' AND applied_at IS NOT NULL AND result_digest IS NOT NULL)
      OR (status <> 'applied' AND applied_at IS NULL AND result_digest IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
