-- Tenant module enablement (PLAT-02). Absence of a row means the module's
-- DefaultEnabled applies; disabling stops entry points and workers but never
-- deletes module data.
CREATE TABLE IF NOT EXISTS tenant_modules (
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  updated_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, module_key),
  CONSTRAINT fk_tenant_modules_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
