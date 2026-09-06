-- PLAT-04H QueryGateway tenant policy. Provider endpoints and credentials stay
-- in deployment configuration; this table only stores tenant-scoped dataset
-- enablement and ceilings. A query is admitted only when both this policy and
-- the principal's value-layer action allow it.

CREATE TABLE IF NOT EXISTS query_dataset_policies (
  tenant_id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  dataset_key VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  allow_raw BOOLEAN NOT NULL DEFAULT FALSE,
  allow_supplier BOOLEAN NOT NULL DEFAULT FALSE,
  allow_customer BOOLEAN NOT NULL DEFAULT TRUE,
  max_range_seconds INT UNSIGNED NOT NULL DEFAULT 34560000,
  max_concurrent SMALLINT UNSIGNED NOT NULL DEFAULT 4,
  max_result_rows INT UNSIGNED NOT NULL DEFAULT 250000,
  query_timeout_ms INT UNSIGNED NOT NULL DEFAULT 90000,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_by CHAR(26) COLLATE utf8mb4_unicode_ci NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, dataset_key),
  KEY fk_query_dataset_policies_actor (updated_by),
  CONSTRAINT fk_query_dataset_policies_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_query_dataset_policies_actor FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT chk_query_dataset_policy_range CHECK (max_range_seconds BETWEEN 60 AND 315360000),
  CONSTRAINT chk_query_dataset_policy_concurrent CHECK (max_concurrent BETWEEN 1 AND 1024),
  CONSTRAINT chk_query_dataset_policy_rows CHECK (max_result_rows BETWEEN 1 AND 10000000),
  CONSTRAINT chk_query_dataset_policy_timeout CHECK (query_timeout_ms BETWEEN 100 AND 3600000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
