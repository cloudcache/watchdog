-- PLAT-04C: tenant-scoped, typed address taxonomy. Display names are mutable;
-- durable relations use stable ids. (kind, code) remains the operator-facing
-- natural key because codes such as "AS" can exist in more than one kind.
CREATE TABLE IF NOT EXISTS geo_dict (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  code VARCHAR(64) NOT NULL,
  parent_id CHAR(26) NULL,
  name VARCHAR(190) NOT NULL,
  short_name VARCHAR(190) NULL,
  sort_order INT NOT NULL DEFAULT 0,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_geo_dict_tenant_id (tenant_id, id),
  UNIQUE KEY uq_geo_dict_kind_code (tenant_id, kind, code),
  KEY idx_geo_dict_parent (tenant_id, parent_id, sort_order, name),
  CONSTRAINT fk_geo_dict_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_geo_dict_parent FOREIGN KEY (tenant_id, parent_id) REFERENCES geo_dict(tenant_id, id) ON DELETE CASCADE,
  CHECK (kind IN ('continent','region','country','province','city'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS isp_operators (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  code VARCHAR(64) NOT NULL,
  name VARCHAR(190) NOT NULL,
  short_name VARCHAR(190) NULL,
  category VARCHAR(32) NOT NULL DEFAULT 'other',
  asns JSON NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_isp_operators_tenant_id (tenant_id, id),
  UNIQUE KEY uq_isp_operators_code (tenant_id, code),
  UNIQUE KEY uq_isp_operators_name (tenant_id, name),
  KEY idx_isp_operators_category (tenant_id, category, sort_order, name),
  CONSTRAINT fk_isp_operators_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE address_sets
  ADD UNIQUE KEY uq_address_sets_tenant_id (tenant_id, id),
  ADD COLUMN explicit_members JSON NULL AFTER selector,
  ADD COLUMN explicit_exclude_members JSON NULL AFTER explicit_members,
  ADD COLUMN include_set_ids JSON NULL AFTER explicit_exclude_members,
  ADD COLUMN exclude_set_ids JSON NULL AFTER include_set_ids,
  ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER enabled;

UPDATE address_sets
SET explicit_members = JSON_ARRAY(),
    explicit_exclude_members = JSON_ARRAY(),
    include_set_ids = JSON_ARRAY(),
    exclude_set_ids = JSON_ARRAY()
WHERE explicit_members IS NULL
   OR explicit_exclude_members IS NULL
   OR include_set_ids IS NULL
   OR exclude_set_ids IS NULL;

-- The legacy API admitted "source" even though the flow snapshot contract is
-- direction-normalized as in/out/both. "destination" never fitted the legacy
-- VARCHAR(8), but is included for databases created from hand-edited schemas.
UPDATE address_sets
SET match_direction = CASE match_direction
  WHEN 'source' THEN 'out'
  WHEN 'destination' THEN 'in'
  ELSE match_direction
END;

ALTER TABLE address_sets
  MODIFY explicit_members JSON NOT NULL,
  MODIFY explicit_exclude_members JSON NOT NULL,
  MODIFY include_set_ids JSON NOT NULL,
  MODIFY exclude_set_ids JSON NOT NULL,
  ADD CHECK (match_direction IN ('in','out','both'));

CREATE TABLE IF NOT EXISTS geo_lines (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  parent_id CHAR(26) NULL,
  code VARCHAR(64) NOT NULL,
  name VARCHAR(190) NOT NULL,
  description TEXT NULL,
  geo_selector JSON NOT NULL,
  operator_id CHAR(26) NULL,
  address_set_id CHAR(36) NULL,
  sort_order INT NOT NULL DEFAULT 0,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_geo_lines_tenant_id (tenant_id, id),
  UNIQUE KEY uq_geo_lines_code (tenant_id, code),
  KEY idx_geo_lines_parent (tenant_id, parent_id, sort_order, name),
  KEY idx_geo_lines_operator (tenant_id, operator_id),
  KEY idx_geo_lines_address_set (tenant_id, address_set_id),
  CONSTRAINT fk_geo_lines_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_geo_lines_parent FOREIGN KEY (tenant_id, parent_id) REFERENCES geo_lines(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_geo_lines_operator FOREIGN KEY (tenant_id, operator_id) REFERENCES isp_operators(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_geo_lines_address_set FOREIGN KEY (tenant_id, address_set_id) REFERENCES address_sets(tenant_id, id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE address_prefixes
  ADD COLUMN family TINYINT UNSIGNED NULL AFTER cidr,
  ADD COLUMN prefix_length TINYINT UNSIGNED NULL AFTER family,
  ADD COLUMN ip_start BINARY(16) NULL AFTER prefix_length,
  ADD COLUMN ip_end BINARY(16) NULL AFTER ip_start,
  ADD COLUMN geo_leaf_id CHAR(26) NULL AFTER labels,
  ADD COLUMN operator_id CHAR(26) NULL AFTER geo_leaf_id,
  ADD COLUMN asn BIGINT UNSIGNED NULL AFTER operator_id,
  ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER source,
  ADD KEY idx_address_prefix_lookup (tenant_id, family, ip_start),
  ADD KEY idx_address_prefix_geo (tenant_id, geo_leaf_id),
  ADD KEY idx_address_prefix_operator (tenant_id, operator_id),
  ADD CONSTRAINT fk_address_prefix_geo FOREIGN KEY (tenant_id, geo_leaf_id) REFERENCES geo_dict(tenant_id, id) ON DELETE CASCADE,
  ADD CONSTRAINT fk_address_prefix_operator FOREIGN KEY (tenant_id, operator_id) REFERENCES isp_operators(tenant_id, id) ON DELETE CASCADE,
  ADD CHECK (family IS NULL OR family IN (4,6)),
  ADD CHECK (family IS NULL OR (family = 4 AND prefix_length <= 32) OR (family = 6 AND prefix_length <= 128)),
  ADD CHECK (ip_start IS NULL OR ip_end IS NULL OR ip_start <= ip_end);
