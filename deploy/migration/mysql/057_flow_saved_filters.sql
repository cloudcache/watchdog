-- Saved Flow filters are cold management objects. Query/export requests copy
-- the canonical AST by value and never retain this row ID, so soft deletion
-- cannot invalidate an already-issued query or immutable export snapshot.
CREATE TABLE IF NOT EXISTS flow_saved_filters (
  id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  owner_user_id CHAR(26) NULL,
  name VARCHAR(190) NOT NULL,
  description VARCHAR(1024) NOT NULL DEFAULT '',
  share_scope VARCHAR(16) NOT NULL DEFAULT 'private',
  filter_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  filter_json JSON NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  deleted_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_flow_saved_filters_visible (tenant_id, deleted_at, share_scope, updated_at, id),
  KEY idx_flow_saved_filters_owner (tenant_id, owner_user_id, deleted_at, updated_at, id),
  CONSTRAINT fk_flow_saved_filters_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_saved_filters_owner
    FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (share_scope IN ('private', 'tenant')),
  CHECK (filter_schema_version = 1),
  CHECK (JSON_TYPE(filter_json) = 'OBJECT')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
