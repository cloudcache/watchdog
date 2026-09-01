-- Address prefix table: CIDR → labels mapping
-- Labels is a JSON object like {"region":"杭州","type":"客户","provider":"电信","asn":"9898"}
CREATE TABLE IF NOT EXISTS address_prefixes (
  id CHAR(36) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  cidr VARCHAR(64) NOT NULL,
  labels JSON NOT NULL,
  source VARCHAR(32) NOT NULL DEFAULT 'manual',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_address_prefix (tenant_id, cidr),
  KEY idx_address_prefix_tenant (tenant_id),
  CONSTRAINT fk_address_prefix_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Address set: user-defined grouping based on label selectors
-- Selector is a JSON object like {"labels":{"region":["杭州","宁波"]}} or {"labels":{"asn":"9898"}}
CREATE TABLE IF NOT EXISTS address_sets (
  id CHAR(36) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  description TEXT,
  selector JSON NOT NULL,
  match_direction VARCHAR(8) NOT NULL DEFAULT 'both',
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_address_set (tenant_id, name),
  KEY idx_address_set_tenant (tenant_id),
  CONSTRAINT fk_address_set_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
